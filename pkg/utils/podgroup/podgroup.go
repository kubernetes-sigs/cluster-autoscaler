/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package podgroup

import (
	"context"

	apiv1 "k8s.io/api/core/v1"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// Resolver resolves the PodGroups of pods. It caches successful lookups, so it
// is meant to live for a single autoscaler iteration: a node hosting many pods
// of the same group then costs one lookup, and answers stay consistent within
// the iteration.
type Resolver struct {
	// kubeClient resolves PodGroups, typically from the manager's cache.
	kubeClient client.Reader
	cache      map[string]*schedulingv1beta1.PodGroup
	// apiUnavailable is set once a lookup shows that the cluster does not
	// serve the PodGroup API, so that the iteration does not retry.
	// TODO: drop this once the PodGroup API is GA and served unconditionally.
	apiUnavailable bool
}

// NewResolver returns a Resolver backed by the given client. A nil client
// means PodGroups are unknown to this autoscaler: Get resolves nothing and no
// pod is a gang pod.
func NewResolver(kubeClient client.Reader) *Resolver {
	return &Resolver{
		kubeClient: kubeClient,
		cache:      make(map[string]*schedulingv1beta1.PodGroup),
	}
}

// Enabled reports whether the resolver can resolve PodGroups at all. When it
// cannot, no pod is a gang pod and callers may skip their work.
func (r *Resolver) Enabled() bool {
	return r != nil && r.kubeClient != nil && !r.apiUnavailable
}

// Get returns the PodGroup the pod belongs to, or nil if the pod does not
// reference one or PodGroups are unknown to this autoscaler. An error means
// the pod references a PodGroup that could not be resolved.
func (r *Resolver) Get(ctx context.Context, pod *apiv1.Pod) (*schedulingv1beta1.PodGroup, error) {
	if !r.Enabled() || pod == nil {
		return nil, nil
	}
	if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
		return nil, nil
	}
	key := client.ObjectKey{Namespace: pod.Namespace, Name: *pod.Spec.SchedulingGroup.PodGroupName}
	if podGroup, ok := r.cache[key.String()]; ok {
		return podGroup, nil
	}
	podGroup := &schedulingv1beta1.PodGroup{}
	err := r.kubeClient.Get(ctx, key, podGroup)
	// TODO: drop this once the PodGroup API is GA and served unconditionally.
	if meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
		klog.FromContext(ctx).Error(err, "PodGroup API is not available, pods referencing PodGroups will not be treated as gang pods")
		r.apiUnavailable = true
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.cache[key.String()] = podGroup
	return podGroup, nil
}

// IsGangPod reports whether the pod is a member of a PodGroup with the gang
// scheduling policy and minCount > 1. Terminal pods are never gang pods.
//
// A pod whose PodGroup cannot be resolved counts as a gang pod. The scheduler
// does not bind a pod before its PodGroup exists, so a missing group most
// likely means the PodGroup informer lags behind the pod informer.
func (r *Resolver) IsGangPod(ctx context.Context, pod *apiv1.Pod) bool {
	if pod == nil || pod.Status.Phase == apiv1.PodSucceeded || pod.Status.Phase == apiv1.PodFailed {
		return false
	}
	podGroup, err := r.Get(ctx, pod)
	if err != nil {
		klog.FromContext(ctx).V(2).Info("Treating pod as a gang pod, its PodGroup could not be resolved", "pod", klog.KObj(pod), "podGroup", klog.KRef(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName), "err", err)
		return true
	}
	return IsGang(podGroup)
}

// NodeHasGangPods reports whether any pod on the node is a gang pod.
func (r *Resolver) NodeHasGangPods(ctx context.Context, nodeInfo *framework.NodeInfo) bool {
	if !r.Enabled() || nodeInfo == nil {
		return false
	}
	for _, podInfo := range nodeInfo.Pods() {
		if r.IsGangPod(ctx, podInfo.Pod) {
			return true
		}
	}
	return false
}

// IsGang reports whether the PodGroup uses the gang scheduling policy with
// minCount > 1; a gang of one can be disrupted like an ordinary pod.
//
// TODO: honour PodGroup.spec.disruptionMode once its interaction with node
// removal is settled; it defaults to "single" today.
func IsGang(podGroup *schedulingv1beta1.PodGroup) bool {
	if podGroup == nil {
		return false
	}
	gang := podGroup.Spec.SchedulingPolicy.Gang
	return gang != nil && gang.MinCount > 1
}
