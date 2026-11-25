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
	"errors"
	"slices"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/equivalence"
	"sigs.k8s.io/cluster-autoscaler/pkg/metrics"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// errScaleUpReportingTimeout is the cause of the context of reporting simulations when the reporting
// simulation budget is exhausted.
var errScaleUpReportingTimeout = errors.New("scale-up reporting simulation budget exhausted")

// scaleUpReporting decides, for a single scale-up, which pods Cluster Autoscaler can't help and
// which pods still await evaluation.
//
// Schedulable pod groups are computed lazily during binpacking, so node groups after the point where
// the binpacking limiter stopped are never simulated. Before pod groups are reported or marked as
// unschedulable, evaluate simulates those node groups for the pod groups being reported. All
// simulations which only serve reporting (this one and the simulation for skipped node groups)
// share a single time budget (MaxScaleUpReportingSimulationDuration). Pod groups which could not be
// evaluated against all node groups not reached by binpacking within the budget are reported as
// awaiting evaluation instead of remaining unschedulable, so that no false "pod didn't trigger
// scale-up" is reported for them. Skipped node groups which were not simulated within the budget
// are reported as skipped, as if the simulation for skipped node groups was disabled.
type scaleUpReporting struct {
	orchestrator *ScaleUpOrchestrator
	// budget is the total time allowed for reporting simulations. Zero means no limit.
	budget time.Duration
	// spent is the time already spent on reporting simulations.
	spent time.Duration
	// cache holds schedulable pod groups computed during binpacking.
	cache *schedulablePodGroupsCache
	// covered holds pod groups which were already evaluated against the node groups not reached by
	// binpacking. Each pod group is evaluated at most once.
	covered map[*equivalence.PodGroup]bool
	// pending holds pod groups which Cluster Autoscaler couldn't fully evaluate, because they were not
	// evaluated against all node groups within the budget. They are reported as awaiting evaluation,
	// never as remaining unschedulable.
	pending  map[*equivalence.PodGroup]bool
	timedOut bool
}

func (o *ScaleUpOrchestrator) newScaleUpReporting(cache *schedulablePodGroupsCache) *scaleUpReporting {
	return &scaleUpReporting{
		orchestrator: o,
		budget:       o.autoscalingCtx.MaxScaleUpReportingSimulationDuration,
		cache:        cache,
		covered:      make(map[*equivalence.PodGroup]bool),
		pending:      make(map[*equivalence.PodGroup]bool),
	}
}

// simulationContext returns a context limited by the remaining reporting simulation budget, and a
// function which must be called when the simulation is done.
func (r *scaleUpReporting) simulationContext(ctx context.Context) (context.Context, func()) {
	if r.budget <= 0 {
		return ctx, func() {}
	}
	start := time.Now()
	ctx, cancel := context.WithTimeoutCause(ctx, r.budget-r.spent, errScaleUpReportingTimeout)
	return ctx, func() {
		// Only the budget running out is a timeout, not e.g. the parent context being canceled.
		if context.Cause(ctx) == errScaleUpReportingTimeout {
			r.registerTimeout()
		}
		r.spent += time.Since(start)
		cancel()
	}
}

// budgetExhausted returns whether the reporting simulation budget was used up.
func (r *scaleUpReporting) budgetExhausted() bool {
	return r.budget > 0 && r.spent >= r.budget
}

func (r *scaleUpReporting) registerTimeout() {
	if r.timedOut {
		return
	}
	r.timedOut = true
	metrics.RegisterTimeout(metrics.ScaleUpReportingSimulationTimeout)
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
	notEvaluated := r.simulateUnevaluatedNodeGroups(ctx, candidates, includeSchedulable)

	newPending := 0
	for _, eg := range candidates {
		// Pod groups which miss a result for any of the node groups which couldn't be evaluated were
		// not fully evaluated. Without includeSchedulable, pod groups which are schedulable don't
		// need results for all node groups.
		if !includeSchedulable && eg.Schedulable {
			continue
		}
		for _, nodeGroup := range notEvaluated {
			if !hasResult(eg, nodeGroup.Id()) {
				r.pending[eg] = true
				newPending++
				break
			}
		}
	}
	if newPending > 0 {
		klog.FromContext(ctx).V(1).Info("Couldn't fully evaluate some pods, they will be reported as awaiting evaluation",
			"budget", r.budget, "notEvaluatedNodeGroups", len(notEvaluated), "pendingPodGroups", newPending)
	}
}

// simulateUnevaluatedNodeGroups simulates eligible node groups which were not reached during
// binpacking, for the candidate pod groups, within the reporting budget. Without
// includeSchedulable, candidates which became schedulable are not simulated anymore. It returns the
// node groups which couldn't be evaluated because the reporting budget was exceeded.
func (r *scaleUpReporting) simulateUnevaluatedNodeGroups(ctx context.Context, candidates []*equivalence.PodGroup, includeSchedulable bool) []cloudprovider.NodeGroup {
	unevaluated := r.cache.unevaluated()
	if len(unevaluated) == 0 {
		return nil
	}
	logger := klog.FromContext(ctx)
	start := time.Now()
	ctx, done := r.simulationContext(ctx)
	defer done()

	var notEvaluated []cloudprovider.NodeGroup
	evaluations := 0
	for i, nodeGroup := range unevaluated {
		targets := candidates
		if !includeSchedulable {
			targets = unschedulablePodGroups(candidates)
		}
		if len(targets) == 0 {
			break
		}
		if ctx.Err() != nil {
			notEvaluated = append(notEvaluated, unevaluated[i:]...)
			break
		}
		nodeInfo, found := r.cache.nodeInfos[nodeGroup.Id()]
		if !found {
			logger.Error(nil, "No node info for node group, skipping it in reporting simulation", "nodeGroupId", nodeGroup.Id())
			continue
		}
		evaluations++
		if _, evaluated := r.orchestrator.schedulablePodGroups(ctx, targets, nodeGroup, nodeInfo, true); evaluated < len(targets) {
			notEvaluated = append(notEvaluated, nodeGroup)
		}
	}
	metrics.ObserveSchedulablePodGroupsEvaluations(metrics.SchedulablePodGroupsReporting, evaluations)
	metrics.UpdateDurationFromStart(ctx, metrics.UnevaluatedNodeGroupsSim, start)
	return notEvaluated
}

// hasResult returns whether the pod group was evaluated against the node group.
func hasResult(eg *equivalence.PodGroup, nodeGroupId string) bool {
	if _, found := eg.SchedulingErrors[nodeGroupId]; found {
		return true
	}
	return slices.Contains(eg.SchedulableGroups, nodeGroupId)
}

// remainingPods returns information about pods which Cluster Autoscaler is unable to help at this
// moment, and pods which Cluster Autoscaler couldn't fully evaluate, which await evaluation.
func (r *scaleUpReporting) remainingPods(
	ctx context.Context,
	egs []*equivalence.PodGroup,
	nodeGroups []cloudprovider.NodeGroup,
	skipped map[string]status.Reasons,
	nodeInfos map[string]*framework.NodeInfo,
) ([]status.NoScaleUpInfo, []*apiv1.Pod) {
	// Pod groups marked as unschedulable after binpacking, e.g. because scaling up their node groups
	// failed, are evaluated against the node groups not reached by binpacking here.
	r.evaluate(ctx, egs, false)

	evaluated := egs
	var notEvaluated []*apiv1.Pod
	if len(r.pending) > 0 {
		evaluated = make([]*equivalence.PodGroup, 0, len(egs))
		for _, eg := range egs {
			if r.pending[eg] {
				notEvaluated = append(notEvaluated, eg.Pods...)
				continue
			}
			evaluated = append(evaluated, eg)
		}
	}

	simulateSkipped := r.orchestrator.autoscalingCtx.ScaleUpSimulationForSkippedNodeGroupsEnabled &&
		len(skipped) > 0 && len(unschedulablePodGroups(evaluated)) > 0
	if simulateSkipped && r.budgetExhausted() {
		// Skipped node groups are reported without simulation, as if the simulation was disabled.
		klog.FromContext(ctx).V(2).Info("Reporting simulation budget exhausted, skipped node groups are reported without simulation", "budget", r.budget)
		simulateSkipped = false
	}
	simulationCtx, done := ctx, func() {}
	if simulateSkipped {
		simulationCtx, done = r.simulationContext(ctx)
	}
	defer done()
	return r.orchestrator.getRemainingPods(simulationCtx, evaluated, nodeGroups, skipped, nodeInfos, simulateSkipped), notEvaluated
}

// appendMissingPods appends pods which are not in dst yet.
func appendMissingPods(dst, pods []*apiv1.Pod) []*apiv1.Pod {
	present := make(map[*apiv1.Pod]bool, len(dst))
	for _, pod := range dst {
		present[pod] = true
	}
	for _, pod := range pods {
		if !present[pod] {
			dst = append(dst, pod)
		}
	}
	return dst
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
