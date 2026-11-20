/*
Copyright 2017 The Kubernetes Authors.

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

package daemonset

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	v1helper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	daemonutil "k8s.io/kubernetes/pkg/controller/daemon/util"
	"k8s.io/kubernetes/pkg/features"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

const (
	// EnableDsEvictionKey is the name of annotation controlling whether a
	// certain DaemonSet pod should be evicted.
	EnableDsEvictionKey = "cluster-autoscaler.kubernetes.io/enable-ds-eviction"
)

// GetDaemonSetPodsForNode returns daemonset nodes for the given pod.
func GetDaemonSetPodsForNode(nodeInfo *framework.NodeInfo, daemonsets []*appsv1.DaemonSet) ([]*framework.PodInfo, error) {
	pods := DaemonSetPodsForNode(context.Background(), nodeInfo.Node(), daemonsets, nil)
	result := make([]*framework.PodInfo, 0, len(pods))
	for _, pod := range pods {
		result = append(result, framework.NewPodInfo(pod, nil))
	}
	return result, nil
}

// DaemonSetPodsForNode returns the pods the given DaemonSets would run on the given node.
//
// Building template NodeInfos calls this for every DaemonSet x node group template on every loop. Each
// DaemonSet's pod tolerations and parsed required node affinity only depend on its spec: if cache
// is not nil, they are computed once per DaemonSet generation and reused.
func DaemonSetPodsForNode(ctx context.Context, node *apiv1.Node, daemonsets []*appsv1.DaemonSet, cache *PodsForNodeCache) []*apiv1.Pod {
	logger := klog.FromContext(ctx)
	result := make([]*apiv1.Pod, 0)
	for _, ds := range daemonsets {
		prepared := cache.get(ds)
		if !nodeShouldRunDaemonPod(logger, node, ds, prepared) {
			continue
		}
		pod := prepared.newPod(ds, node.Name)
		pod.Name = fmt.Sprintf("%s-pod-%d", ds.Name, rand.Int63())
		ptrVal := true
		pod.ObjectMeta.OwnerReferences = []metav1.OwnerReference{
			{Kind: "DaemonSet", UID: ds.UID, Name: ds.Name, Controller: &ptrVal},
		}
		result = append(result, pod)
	}
	return result
}

// preparedDaemonSet is the part of the DaemonSet controller's NodeShouldRunDaemonPod check
// that only depends on the DaemonSet: the tolerations of its pods and its parsed required node
// affinity. Parsing the node affinity dominates the cost of the check.
type preparedDaemonSet struct {
	generation   int64
	tolerations  []apiv1.Toleration
	nodeAffinity nodeaffinity.RequiredNodeAffinity
}

// newPod is daemon.NewPod(ds, nodeName), reusing the prepared tolerations instead of
// re-adding the default DaemonSet tolerations to the template's on every call. Like
// daemon.NewPod, the pod shares the rest of its spec with the DaemonSet template.
func (p *preparedDaemonSet) newPod(ds *appsv1.DaemonSet, nodeName string) *apiv1.Pod {
	pod := &apiv1.Pod{Spec: ds.Spec.Template.Spec, ObjectMeta: ds.Spec.Template.ObjectMeta}
	pod.Namespace = ds.Namespace
	pod.Spec.NodeName = nodeName
	pod.Spec.Tolerations = append([]apiv1.Toleration(nil), p.tolerations...)
	return pod
}

func prepareDaemonSet(ds *appsv1.DaemonSet) *preparedDaemonSet {
	spec := ds.Spec.Template.Spec
	daemonutil.AddOrUpdateDaemonPodTolerations(&spec)
	return &preparedDaemonSet{
		generation:   ds.Generation,
		tolerations:  spec.Tolerations,
		nodeAffinity: nodeaffinity.NewRequiredNodeAffinity(ds.Spec.Template.Spec.NodeSelector, ds.Spec.Template.Spec.Affinity),
	}
}

// maxPreparedDaemonSets bounds a PodsForNodeCache: entries of deleted DaemonSets are only
// dropped when the cache is reset.
const maxPreparedDaemonSets = 10000

// PodsForNodeCache caches, by DaemonSet UID and for one generation (bumped on every spec change),
// the parts of DaemonSetPodsForNode that only depend on the DaemonSet. DaemonSets without UID or
// generation are prepared on every call. It is safe for concurrent use. A nil *PodsForNodeCache
// disables caching.
type PodsForNodeCache struct {
	mu      sync.Mutex
	entries map[types.UID]*preparedDaemonSet
}

// NewPodsForNodeCache returns an empty PodsForNodeCache.
func NewPodsForNodeCache() *PodsForNodeCache {
	return &PodsForNodeCache{entries: map[types.UID]*preparedDaemonSet{}}
}

func (c *PodsForNodeCache) get(ds *appsv1.DaemonSet) *preparedDaemonSet {
	if c == nil || ds.UID == "" || ds.Generation == 0 {
		return prepareDaemonSet(ds)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, found := c.entries[ds.UID]; found && p.generation == ds.Generation {
		return p
	}
	if len(c.entries) >= maxPreparedDaemonSets {
		c.entries = map[types.UID]*preparedDaemonSet{}
	}
	p := prepareDaemonSet(ds)
	c.entries[ds.UID] = p
	return p
}

// nodeShouldRunDaemonPod is the first result of the DaemonSet controller's NodeShouldRunDaemonPod
// (k8s.io/kubernetes/pkg/controller/daemon), using a preparedDaemonSet instead of re-deriving the
// tolerations and re-parsing the node affinity on every call.
func nodeShouldRunDaemonPod(logger klog.Logger, node *apiv1.Node, ds *appsv1.DaemonSet, p *preparedDaemonSet) bool {
	if ds.Spec.Template.Spec.NodeName != "" && ds.Spec.Template.Spec.NodeName != node.Name {
		return false
	}
	// Ignore parsing errors for backwards compatibility, like the DaemonSet controller.
	if fitsNodeAffinity, _ := p.nodeAffinity.Match(node); !fitsNodeAffinity {
		return false
	}
	_, hasUntoleratedTaint := v1helper.FindMatchingUntoleratedTaint(logger, node.Spec.Taints, p.tolerations, func(t *apiv1.Taint) bool {
		return t.Effect == apiv1.TaintEffectNoExecute || t.Effect == apiv1.TaintEffectNoSchedule
	}, utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators))
	return !hasUntoleratedTaint
}

// PodsToEvict returns a list of DaemonSet pods that should be evicted during scale down.
func PodsToEvict(pods []*apiv1.Pod, evictByDefault bool) (evictable []*apiv1.Pod) {
	for _, pod := range pods {
		if a, ok := pod.Annotations[EnableDsEvictionKey]; ok {
			if a == "true" {
				evictable = append(evictable, pod)
			}
		} else if evictByDefault {
			evictable = append(evictable, pod)
		}
	}
	return
}
