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
	"fmt"
	"math/rand"

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
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/labels"
)

const (
	// EnableDsEvictionKey is the name of annotation controlling whether a
	// certain DaemonSet pod should be evicted.
	EnableDsEvictionKey = "cluster-autoscaler.kubernetes.io/enable-ds-eviction"
)

// CompiledDaemonSet is a DaemonSet with its node affinity parsed and its pod built once, so it can
// be matched against many node groups cheaply. It must not be reused across autoscaling loops.
type CompiledDaemonSet struct {
	daemonSet            *appsv1.DaemonSet
	requiredNodeAffinity nodeaffinity.RequiredNodeAffinity
	templatePod          *apiv1.Pod
}

// CompileDaemonSets compiles daemonsets for matching against many node groups.
func CompileDaemonSets(daemonsets []*appsv1.DaemonSet) []*CompiledDaemonSet {
	compiled := make([]*CompiledDaemonSet, len(daemonsets))
	for i, ds := range daemonsets {
		compiled[i] = &CompiledDaemonSet{
			daemonSet:            ds,
			requiredNodeAffinity: nodeaffinity.NewRequiredNodeAffinity(ds.Spec.Template.Spec.NodeSelector, ds.Spec.Template.Spec.Affinity),
			templatePod:          newTemplatePod(ds),
		}
	}
	return compiled
}

// newTemplatePod builds the pod ds would create, with default tolerations added.
func newTemplatePod(ds *appsv1.DaemonSet) *apiv1.Pod {
	template := ds.Spec.Template.DeepCopy()
	pod := &apiv1.Pod{ObjectMeta: template.ObjectMeta, Spec: template.Spec}
	pod.Name = fmt.Sprintf("%s-pod-%d", ds.Name, rand.Int63())
	pod.Namespace = ds.Namespace
	pod.OwnerReferences = []metav1.OwnerReference{
		{Kind: "DaemonSet", UID: ds.UID, Name: ds.Name, Controller: ptr.To(true)},
	}
	daemonutil.AddOrUpdateDaemonPodTolerations(&pod.Spec)
	return pod
}

// TemplatePod returns the pod the DaemonSet would create. Callers must copy it before modifying it.
func (c *CompiledDaemonSet) TemplatePod() *apiv1.Pod {
	return c.templatePod
}

// UID returns the UID of the DaemonSet.
func (c *CompiledDaemonSet) UID() types.UID {
	return c.daemonSet.UID
}

// IsPreemptingSystemNodeCritical reports whether the DaemonSet pods have system-node-critical
// priority and preempt lower-priority pods.
func (c *CompiledDaemonSet) IsPreemptingSystemNodeCritical() bool {
	spec := &c.daemonSet.Spec.Template.Spec
	if spec.PriorityClassName != labels.SystemNodeCriticalLabel {
		return false
	}
	return spec.PreemptionPolicy == nil || *spec.PreemptionPolicy == apiv1.PreemptLowerPriority
}

// ShouldRunOnNode reports whether the DaemonSet would run a pod on the node.
//
// TODO: Call nodeShouldRunDaemonPod from k8s.io/kubernetes/pkg/controller/daemon instead, once it is
// exported. It takes the tolerations and parsed required node affinity that CompiledDaemonSet already
// holds. Until then this copies its shouldRun logic, and TestCompiledDaemonSetMatchesDaemonController
// checks that the copy matches NodeShouldRunDaemonPod.
func (c *CompiledDaemonSet) ShouldRunOnNode(node *apiv1.Node) bool {
	if nodeName := c.daemonSet.Spec.Template.Spec.NodeName; nodeName != "" && nodeName != node.Name {
		return false
	}
	if fits, _ := c.requiredNodeAffinity.Match(node); !fits {
		return false
	}
	_, untolerated := v1helper.FindMatchingUntoleratedTaint(klog.Background(), node.Spec.Taints, c.templatePod.Spec.Tolerations, func(t *apiv1.Taint) bool {
		return t.Effect == apiv1.TaintEffectNoExecute || t.Effect == apiv1.TaintEffectNoSchedule
	}, utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators))
	return !untolerated
}

// GetDaemonSetPodsForNode returns daemonset pods for the given node.
func GetDaemonSetPodsForNode(nodeInfo *framework.NodeInfo, daemonsets []*appsv1.DaemonSet) ([]*framework.PodInfo, error) {
	result := make([]*framework.PodInfo, 0)
	for _, ds := range CompileDaemonSets(daemonsets) {
		if ds.ShouldRunOnNode(nodeInfo.Node()) {
			// The compiled DaemonSets are local to this call, so their template pods can be modified in place.
			pod := ds.TemplatePod()
			pod.Spec.NodeName = nodeInfo.Node().Name
			result = append(result, framework.NewPodInfo(pod, nil))
		}
	}
	return result, nil
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
