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
	"context"
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/equivalence"
	"sigs.k8s.io/cluster-autoscaler/pkg/metrics"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// scaleUpReporting decides, for a single scale-up, which pods Cluster Autoscaler can't help.
//
// Schedulable pod groups are computed lazily during binpacking, so node groups after the point where
// the binpacking limiter stopped are never simulated. Before pod groups are reported or marked as
// unschedulable, evaluate simulates those node groups for the pod groups being reported, so that
// they are reported as if all node groups were evaluated during binpacking.
type scaleUpReporting struct {
	orchestrator *ScaleUpOrchestrator
	// cache holds schedulable pod groups computed during binpacking.
	cache *schedulablePodGroupsCache
	// covered holds pod groups which were already evaluated against the node groups not reached by
	// binpacking. Each pod group is evaluated at most once.
	covered map[*equivalence.PodGroup]bool
}

func (o *ScaleUpOrchestrator) newScaleUpReporting(cache *schedulablePodGroupsCache) *scaleUpReporting {
	return &scaleUpReporting{
		orchestrator: o,
		cache:        cache,
		covered:      make(map[*equivalence.PodGroup]bool),
	}
}

// evaluate evaluates eligible node groups which were not reached during binpacking, for pod groups
// which were not evaluated against them yet. It must be called after the last use of the
// schedulable pod groups cache by binpacking, and before pod groups are reported.
//
// includeSchedulable tells which pod groups need to be evaluated:
//   - true: all pod groups are about to be marked unschedulable (by markAllGroupsAsUnschedulable),
//     including the ones which are schedulable now, and reported with a reason for every node group
//     they fit. To report them as if all node groups were evaluated during binpacking, all of them
//     are evaluated first. This must happen before they are marked: afterwards, a pod group fitting
//     a node group not reached by binpacking would become schedulable again and wouldn't be
//     reported at all, although nothing was scaled up for it.
//   - false: pod groups which are schedulable now are not reported as unschedulable (they triggered
//     the scale-up or await evaluation), so evaluating them would only cost time. Only pod groups
//     which are unschedulable now are evaluated, including the ones marked unschedulable because
//     scaling up their node groups failed, and a pod group isn't evaluated anymore once it is
//     schedulable on one of the node groups.
//
// Always evaluating all pod groups would bring back the cost of computing schedulable pod groups
// for all node groups. Never evaluating schedulable ones would report pods with incomplete
// rejection reasons when all pod groups are marked unschedulable.
//
// Results are not stored in the cache: they cover only a subset of pod groups, so they must not be
// used for binpacking or for comparing similar node groups.
func (r *scaleUpReporting) evaluate(ctx context.Context, egs []*equivalence.PodGroup, includeSchedulable bool) {
	var candidates []*equivalence.PodGroup
	for _, eg := range egs {
		if !r.covered[eg] && (includeSchedulable || !eg.Schedulable) {
			r.covered[eg] = true
			candidates = append(candidates, eg)
		}
	}
	if len(candidates) == 0 {
		return
	}
	r.simulateUnevaluatedNodeGroups(ctx, candidates, includeSchedulable)
}

// simulateUnevaluatedNodeGroups simulates eligible node groups which were not reached during
// binpacking, for the candidate pod groups. Without includeSchedulable, candidates which became
// schedulable are not simulated anymore.
func (r *scaleUpReporting) simulateUnevaluatedNodeGroups(ctx context.Context, candidates []*equivalence.PodGroup, includeSchedulable bool) {
	unevaluated := r.cache.unevaluated()
	if len(unevaluated) == 0 {
		return
	}
	logger := klog.FromContext(ctx)
	start := time.Now()
	evaluations := 0
	for _, nodeGroup := range unevaluated {
		targets := candidates
		if !includeSchedulable {
			targets = unschedulablePodGroups(candidates)
		}
		if len(targets) == 0 {
			break
		}
		nodeInfo, found := r.cache.nodeInfos[nodeGroup.Id()]
		if !found {
			logger.Error(nil, "No node info for node group, skipping it in reporting simulation", "nodeGroupId", nodeGroup.Id())
			continue
		}
		evaluations++
		_ = r.orchestrator.SchedulablePodGroups(ctx, targets, nodeGroup, nodeInfo)
	}
	metrics.ObserveSchedulablePodGroupsEvaluations(metrics.SchedulablePodGroupsReporting, evaluations)
	metrics.UpdateDurationFromStart(ctx, metrics.UnevaluatedNodeGroupsSim, start)
}

// remainingPods returns information about pods which Cluster Autoscaler is unable to help at this
// moment.
func (r *scaleUpReporting) remainingPods(
	ctx context.Context,
	egs []*equivalence.PodGroup,
	nodeGroups []cloudprovider.NodeGroup,
	skipped map[string]status.Reasons,
	nodeInfos map[string]*framework.NodeInfo,
) []status.NoScaleUpInfo {
	// Pod groups marked as unschedulable after binpacking, e.g. because scaling up their node groups
	// failed, are evaluated against the node groups not reached by binpacking here.
	r.evaluate(ctx, egs, false)
	return r.orchestrator.GetRemainingPods(ctx, egs, nodeGroups, skipped, nodeInfos)
}

func unschedulablePodGroups(egs []*equivalence.PodGroup) []*equivalence.PodGroup {
	var result []*equivalence.PodGroup
	for _, eg := range egs {
		if !eg.Schedulable {
			result = append(result, eg)
		}
	}
	return result
}
