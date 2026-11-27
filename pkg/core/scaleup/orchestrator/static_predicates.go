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

package orchestrator

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	v1helper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	nodeaffinityplugin "k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// untoleratedTaintReason matches the reason returned by the scheduler's TaintToleration Filter.
const untoleratedTaintReason = "node(s) had untolerated taint(s)"

// staticPredicatesError checks the scheduling constraints that only depend on the pod and the
// node: untolerated NoSchedule / NoExecute taints and required node affinity / nodeSelector.
// It returns an error only when the scheduler's TaintToleration or NodeAffinity Filter would
// also reject the pod, with the same predicate name and reasons, so it is safe to use as a
// fast negative pre-check when both plugins are enabled (see staticPredicatesUsable). Checks run
// in the framework's default Filter order, and cases where an earlier plugin would decide
// (unschedulable nodes, pods bound to a node, node name pins resolved in PreFilter) are left to
// the framework. If another PreFilter plugin fails for the pod (e.g. a missing
// PVC), or with a custom scheduler profile (e.g. NodeAffinity's addedAffinity, or other Filters
// ordered first), the framework may report a different plugin or reason: the pod is rejected
// either way.
//
// The full scheduler framework runs every PreFilter plugin before any Filter. InterPodAffinity's
// PreFilter scans all pods in the cluster, so a template node that a pod can never use (wrong
// instance type, node pool, arch...) costs O(cluster) per node group x pod group before being
// rejected by NodeAffinity. This pre-check rejects those in O(pod spec).
func staticPredicatesError(logger klog.Logger, pod *apiv1.Pod, node *apiv1.Node) clustersnapshot.SchedulingError {
	// NodeUnschedulable and NodeName run before TaintToleration, and node name pins (matchFields on
	// metadata.name) are resolved by NodeAffinity's PreFilter, before every Filter: these cases
	// report different errors, leave them to the framework.
	if node == nil || node.Spec.Unschedulable || pod.Spec.NodeName != "" || hasMatchFields(pod) {
		return nil
	}
	if _, untolerated := v1helper.FindMatchingUntoleratedTaint(logger, node.Spec.Taints, pod.Spec.Tolerations,
		helper.DoNotScheduleTaintsFilterFunc(),
		utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators)); untolerated {
		return clustersnapshot.NewFailingPredicateError(pod, names.TaintToleration, []string{untoleratedTaintReason}, "",
			fmt.Sprintf("nodeName: %q, nodeTaints: %#v", node.Name, node.Spec.Taints))
	}
	// Parsing errors are left to the framework, which reports them properly.
	if match, err := nodeaffinity.GetRequiredNodeAffinity(pod).Match(node); err == nil && !match {
		return clustersnapshot.NewFailingPredicateError(pod, names.NodeAffinity, []string{nodeaffinityplugin.ErrReasonPod}, "",
			fmt.Sprintf("nodeName: %q", node.Name))
	}
	return nil
}

func hasMatchFields(pod *apiv1.Pod) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil || pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		if len(term.MatchFields) > 0 {
			return true
		}
	}
	return false
}

// staticPredicatesUsable reports whether staticPredicatesError can be used with the given
// scheduler framework. Both TaintToleration and NodeAffinity must be enabled Filters, so that the
// pre-check only rejects what the framework would also reject, and in the default order, so that
// it reports the same predicate and reason as the framework.
func staticPredicatesUsable(handle *framework.Handle) bool {
	if handle == nil || handle.Framework == nil {
		return false
	}
	plugins := handle.Framework.ListPlugins()
	if plugins == nil {
		return false
	}
	taintToleration, nodeAffinity := -1, -1
	for i, plugin := range plugins.Filter.Enabled {
		switch plugin.Name {
		case names.TaintToleration:
			taintToleration = i
		case names.NodeAffinity:
			nodeAffinity = i
		}
	}
	return taintToleration >= 0 && nodeAffinity > taintToleration
}
