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

package skippednodegroups

import (
	"context"
	"maps"
	"reflect"
	"sort"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/equivalence"
	"sigs.k8s.io/cluster-autoscaler/pkg/metrics"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/drain"
)

// ScaleUpStatusProcessor rewrites NoScaleUpInfo.SkippedNodeGroups/RejectedNodeGroups of the pods
// that remain unschedulable after a scale-up attempt, so that every pod equivalence group only
// lists the skipped node groups it could actually schedule on; skipped node groups that fail the
// group's predicates are moved to RejectedNodeGroups together with the failing predicate.
//
// Verdicts come from the Evaluator's cache. Pairs without a verdict yet (first loop they are seen,
// or node groups without a template NodeInfo) are conservatively left in SkippedNodeGroups.
//
// It must run after processors that remove pods from the status (e.g. fake pod filters) and before
// processors that report it (events, visibility).
//
// Concurrency: Process is cheap (grouping, signatures and cache lookups) and everything it shares with
// the worker is guarded by the Evaluator's mutex, so it may be invoked from any goroutine (e.g. by
// asynchronous node group initializers). Predicate checks only ever run on the Evaluator's goroutine.
type ScaleUpStatusProcessor struct {
	evaluator *Evaluator
}

// NewScaleUpStatusProcessor creates a ScaleUpStatusProcessor backed by the given Evaluator.
func NewScaleUpStatusProcessor(evaluator *Evaluator) *ScaleUpStatusProcessor {
	return &ScaleUpStatusProcessor{evaluator: evaluator}
}

// skippedTemplate pairs a skipped node group id with its template NodeInfo.
type skippedTemplate struct {
	nodeGroup string
	nodeInfo  *framework.NodeInfo
}

// Process applies cached verdicts to the status and hands unknown pairs to the Evaluator.
func (p *ScaleUpStatusProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	if autoscalingCtx == nil || !autoscalingCtx.ScaleUpSimulationForSkippedNodeGroupsEnabled || len(scaleUpStatus.PodsRemainUnschedulable) == 0 {
		return
	}
	start := time.Now()
	defer metrics.UpdateDurationFromStart(ctx, metrics.SkipNodeGroupSimulation, start)
	logger := klog.FromContext(ctx)

	skippedIDs := skippedNodeGroupIDs(scaleUpStatus.PodsRemainUnschedulable)
	if len(skippedIDs) == 0 {
		return
	}
	templates := resolveSkippedTemplates(ctx, autoscalingCtx, scaleUpStatus.ConsideredNodeGroups, skippedIDs)
	if len(templates) == 0 {
		return
	}

	pods := make([]*apiv1.Pod, len(scaleUpStatus.PodsRemainUnschedulable))
	indicesByPod := make(map[*apiv1.Pod][]int, len(scaleUpStatus.PodsRemainUnschedulable))
	for idx, ns := range scaleUpStatus.PodsRemainUnschedulable {
		pods[idx] = ns.Pod
		indicesByPod[ns.Pod] = append(indicesByPod[ns.Pod], idx)
	}
	podGroups := equivalence.BuildPodGroups(pods)

	// Bound the work per loop: at most MaxPairsPerLoop (pod group, skipped node group) pairs. Whole pod
	// groups are kept or dropped (a half-attributed group would be confusing), biggest groups first in a
	// deterministic order so that the same groups are served loop after loop. Dropped groups keep the
	// plain skipped list, exactly like without this processor.
	remainingGroupBudget := len(podGroups)
	if budget := p.evaluator.opts.MaxPairsPerLoop; budget > 0 && budget/len(templates) < remainingGroupBudget {
		remainingGroupBudget = budget / len(templates)
		sort.SliceStable(podGroups, func(i, j int) bool {
			if len(podGroups[i].Pods) != len(podGroups[j].Pods) {
				return len(podGroups[i].Pods) > len(podGroups[j].Pods)
			}
			return podGroupSortKey(podGroups[i].Pods[0]) < podGroupSortKey(podGroups[j].Pods[0])
		})
		logger.V(2).Info("Skipped node groups: pair budget exhausted, attributing only the biggest pod groups", "podGroups", len(podGroups), "attributedPodGroups", remainingGroupBudget, "skippedNodeGroups", len(templates), "maxPairsPerLoop", budget)
	}

	samplePods := make([]*apiv1.Pod, len(podGroups))
	evaluable := make([]bool, len(podGroups))
	for gi, eg := range podGroups {
		samplePods[gi] = clearPodAffinityRequirements(eg.Pods[0])
		// The private snapshot has no ResourceClaims/DeviceClasses, so DRA pods would be rejected
		// everywhere for the wrong reason. Leave their skipped node groups as they are.
		if remainingGroupBudget > 0 && (!autoscalingCtx.DynamicResourceAllocationEnabled || len(eg.Pods[0].Spec.ResourceClaims) == 0) {
			evaluable[gi] = true
			remainingGroupBudget--
		}
	}

	results := p.evaluator.lookupAndSchedule(snapshotOptions{
		draEnabled:      autoscalingCtx.DynamicResourceAllocationEnabled,
		parallelism:     autoscalingCtx.PredicateParallelism,
		csiEnabled:      autoscalingCtx.CSINodeAwareSchedulingEnabled,
		verbosityOffset: autoscalingCtx.SchedulerVerbosityOffset,
	}, templates, samplePods, evaluable)

	templateIndex := make(map[string]int, len(templates))
	for ti, tmpl := range templates {
		templateIndex[tmpl.nodeGroup] = ti
	}

	// Pods of one equivalence group share the same input maps (the orchestrator hands the same
	// skipped/rejected maps to every pod of a group), so build the filtered maps once per distinct
	// pair of input maps and share them across the group's pods, exactly like the orchestrator does.
	type filteredMaps struct {
		skipped  map[string]status.Reasons
		rejected map[string]status.Reasons
	}
	for gi, eg := range podGroups {
		cache := make(map[[2]uintptr]filteredMaps, 1)
		for _, pod := range eg.Pods {
			for _, idx := range indicesByPod[pod] {
				ns := &scaleUpStatus.PodsRemainUnschedulable[idx]
				key := [2]uintptr{reflect.ValueOf(ns.SkippedNodeGroups).Pointer(), reflect.ValueOf(ns.RejectedNodeGroups).Pointer()}
				out, cached := cache[key]
				if !cached {
					out = filteredMaps{
						skipped:  make(map[string]status.Reasons, len(ns.SkippedNodeGroups)),
						rejected: maps.Clone(ns.RejectedNodeGroups),
					}
					if out.rejected == nil {
						out.rejected = make(map[string]status.Reasons)
					}
					for ng, reason := range ns.SkippedNodeGroups {
						ti, found := templateIndex[ng]
						if !found {
							out.skipped[ng] = reason
							continue
						}
						schedErr, known := results.get(gi, ti)
						if !known || schedErr == nil {
							out.skipped[ng] = reason
							continue
						}
						out.rejected[ng] = schedErr
					}
					cache[key] = out
				}
				ns.SkippedNodeGroups = out.skipped
				ns.RejectedNodeGroups = out.rejected
			}
		}
	}
}

// CleanUp cleans up the processor's internal structures.
func (p *ScaleUpStatusProcessor) CleanUp() {}

// skippedNodeGroupIDs returns the union of the skipped node groups of all remaining pods. The
// orchestrator assigns the same skipped map to every remaining pod, but other orchestrators or
// processors may filter entries, so take the union to be safe.
func skippedNodeGroupIDs(noScaleUpInfos []status.NoScaleUpInfo) map[string]struct{} {
	skippedIDs := make(map[string]struct{})
	for _, ns := range noScaleUpInfos {
		for id := range ns.SkippedNodeGroups {
			skippedIDs[id] = struct{}{}
		}
	}
	return skippedIDs
}

// resolveSkippedTemplates returns the template NodeInfos of the skipped node groups. Node groups
// without one are left out, so they stay untouched in SkippedNodeGroups.
func resolveSkippedTemplates(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, consideredNodeGroups []cloudprovider.NodeGroup, skippedIDs map[string]struct{}) []skippedTemplate {
	logger := klog.FromContext(ctx)
	nodeGroupsByID := make(map[string]cloudprovider.NodeGroup, len(consideredNodeGroups))
	for _, ng := range consideredNodeGroups {
		nodeGroupsByID[ng.Id()] = ng
	}
	templates := make([]skippedTemplate, 0, len(skippedIDs))
	for id := range skippedIDs {
		nodeInfo := templateNodeInfo(ctx, autoscalingCtx, nodeGroupsByID, id)
		if nodeInfo == nil || nodeInfo.Node() == nil {
			logger.V(4).Info("No template NodeInfo for skipped node group, leaving it as skipped", "nodeGroupId", id)
			continue
		}
		templates = append(templates, skippedTemplate{nodeGroup: id, nodeInfo: nodeInfo})
	}
	return templates
}

// templateNodeInfo returns the template NodeInfo of a node group: the registry's (processed by the
// TemplateNodeInfoProvider chain, like the one scale-up simulated with) if available, otherwise the
// cloud provider's.
func templateNodeInfo(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, consideredNodeGroups map[string]cloudprovider.NodeGroup, id string) *framework.NodeInfo {
	if autoscalingCtx.TemplateNodeInfoRegistry != nil {
		if nodeInfo, found := autoscalingCtx.TemplateNodeInfoRegistry.GetNodeInfo(id); found && nodeInfo != nil {
			return nodeInfo
		}
	}
	ng, found := consideredNodeGroups[id]
	if !found || ng == nil {
		return nil
	}
	nodeInfo, err := ng.TemplateNodeInfo(ctx)
	if err != nil {
		return nil
	}
	return nodeInfo
}

// clearPodAffinityRequirements returns the pod without inter-pod affinity/anti-affinity, copying it
// only if needed.
func clearPodAffinityRequirements(pod *apiv1.Pod) *apiv1.Pod {
	if pod == nil || pod.Spec.Affinity == nil || (pod.Spec.Affinity.PodAffinity == nil && pod.Spec.Affinity.PodAntiAffinity == nil) {
		return pod
	}
	podCopy := pod.DeepCopy()
	podCopy.Spec.Affinity = stripPodAffinity(podCopy.Spec.Affinity)
	return podCopy
}

// podGroupSortKey orders pod groups of equal size deterministically: by controller (pod groups never
// span controllers), then by the sample pod.
func podGroupSortKey(samplePod *apiv1.Pod) string {
	controller := ""
	if ref := drain.ControllerRef(samplePod); ref != nil {
		controller = string(ref.UID)
	}
	return controller + "/" + samplePod.Namespace + "/" + samplePod.Name
}
