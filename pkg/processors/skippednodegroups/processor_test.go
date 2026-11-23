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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	schedulerconfiglatest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	smallNodeGroup   = "ng-small"
	bigNodeGroup     = "ng-big"
	taintedNodeGroup = "ng-tainted"
	noTemplateGroup  = "ng-no-template"
)

// skipReason is a minimal status.Reasons implementation standing in for the orchestrator's skip reasons.
type skipReason string

func (r skipReason) Reasons() []string { return []string{string(r)} }

var (
	backoffReason  = skipReason("in backoff after failed scale-up")
	maxSizeReason  = skipReason("max node group size reached")
	allSkipReasons = map[string]status.Reasons{smallNodeGroup: backoffReason, bigNodeGroup: maxSizeReason}
)

// testFixture wires an Evaluator with a test framework handle, a test cloud provider with template
// NodeInfos and an AutoscalingContext with the feature enabled.
type testFixture struct {
	t         *testing.T
	clock     *clocktesting.FakePassiveClock
	evaluator *Evaluator
	processor *ScaleUpStatusProcessor
	provider  *testprovider.TestCloudProvider
	templates map[string]*framework.NodeInfo
	ctx       *ca_context.AutoscalingContext
}

func newTestFixture(t *testing.T, modify func(*EvaluatorOptions)) *testFixture {
	t.Helper()
	fakeClock := clocktesting.NewFakePassiveClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	opts := DefaultEvaluatorOptions()
	opts.Clock = fakeClock
	opts.PairsPerSecond = 0 // Unlimited: tests should not depend on the limiter.
	if modify != nil {
		modify(&opts)
	}
	evaluator := NewEvaluator(framework.NewTestFrameworkHandleOrDie(t), opts)

	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup(smallNodeGroup, 0, 10, 0)
	provider.AddNodeGroup(bigNodeGroup, 0, 10, 0)
	provider.AddNodeGroup(taintedNodeGroup, 0, 10, 0)
	provider.AddNodeGroup(noTemplateGroup, 0, 10, 0)
	f := &testFixture{
		t:         t,
		clock:     fakeClock,
		evaluator: evaluator,
		processor: NewScaleUpStatusProcessor(evaluator),
		provider:  provider,
		templates: map[string]*framework.NodeInfo{},
		ctx: &ca_context.AutoscalingContext{
			AutoscalingOptions: config.AutoscalingOptions{ScaleUpSimulationForSkippedNodeGroupsEnabled: true},
			CloudProvider:      provider,
		},
	}
	f.setTemplate(smallNodeGroup, templateNode("small-template", 1000, 1000, nil, nil))
	f.setTemplate(bigNodeGroup, templateNode("big-template", 8000, 8000, map[string]string{"tier": "big"}, nil))
	f.setTemplate(taintedNodeGroup, templateNode("tainted-template", 8000, 8000, nil, []apiv1.Taint{{Key: "dedicated", Value: "x", Effect: apiv1.TaintEffectNoSchedule}}))
	return f
}

func (f *testFixture) setTemplate(nodeGroup string, node *apiv1.Node) {
	f.templates[nodeGroup] = framework.NewTestNodeInfo(node)
	f.provider.SetMachineTemplates(f.templates)
}

func (f *testFixture) nodeGroups() []cloudprovider.NodeGroup {
	return f.provider.NodeGroups(context.Background())
}

// status builds a ScaleUpStatus the way the orchestrator does: every remaining pod references the same
// skipped map, and pods of one group (here: one controller) share one rejected map.
func (f *testFixture) status(skipped map[string]status.Reasons, pods ...*apiv1.Pod) *status.ScaleUpStatus {
	st := &status.ScaleUpStatus{Result: status.ScaleUpNoOptionsAvailable, ConsideredNodeGroups: f.nodeGroups()}
	rejectedByController := map[string]map[string]status.Reasons{}
	for _, pod := range pods {
		rejected := map[string]status.Reasons{}
		if len(pod.OwnerReferences) > 0 {
			owner := string(pod.OwnerReferences[0].UID)
			if shared, found := rejectedByController[owner]; found {
				rejected = shared
			} else {
				rejectedByController[owner] = rejected
			}
		}
		st.PodsRemainUnschedulable = append(st.PodsRemainUnschedulable, status.NoScaleUpInfo{
			Pod:                pod,
			RejectedNodeGroups: rejected,
			SkippedNodeGroups:  skipped,
		})
	}
	return st
}

// process runs the processor on a fresh status for the given pods and returns the status.
func (f *testFixture) process(pods ...*apiv1.Pod) *status.ScaleUpStatus {
	st := f.status(allSkipReasons, pods...)
	f.processor.Process(context.Background(), f.ctx, st)
	return st
}

func templateNode(name string, millicpu, mem int64, labels map[string]string, taints []apiv1.Taint) *apiv1.Node {
	node := BuildTestNode(name, millicpu, mem*1024*1024)
	SetNodeReadyState(node, true, time.Time{})
	for k, v := range labels {
		node.Labels[k] = v
	}
	node.Spec.Taints = taints
	return node
}

func smallPod(name string) *apiv1.Pod {
	return BuildTestPod(name, 500, 100*1024*1024, MarkUnschedulable(), WithControllerOwnerRef("small-rs", "ReplicaSet", "small-rs-uid"))
}

func bigPod(name string) *apiv1.Pod {
	return BuildTestPod(name, 4000, 100*1024*1024, MarkUnschedulable(), WithControllerOwnerRef("big-rs", "ReplicaSet", "big-rs-uid"))
}

func selectorPod(name string) *apiv1.Pod {
	pod := BuildTestPod(name, 500, 100*1024*1024, MarkUnschedulable(), WithControllerOwnerRef("selector-rs", "ReplicaSet", "selector-rs-uid"))
	pod.Spec.NodeSelector = map[string]string{"tier": "big"}
	return pod
}

func keys(m map[string]status.Reasons) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func failingPredicate(t *testing.T, reasons status.Reasons) string {
	t.Helper()
	schedErr, ok := reasons.(clustersnapshot.SchedulingError)
	if !assert.True(t, ok, "expected a SchedulingError, got %T", reasons) {
		return ""
	}
	return schedErr.FailingPredicateName()
}

func TestProcessIsNoOpWhenDisabled(t *testing.T) {
	f := newTestFixture(t, nil)
	f.ctx.ScaleUpSimulationForSkippedNodeGroupsEnabled = false
	st := f.status(allSkipReasons, bigPod("big-1"))
	f.processor.Process(context.Background(), f.ctx, st)
	assert.Equal(t, allSkipReasons, st.PodsRemainUnschedulable[0].SkippedNodeGroups)
	assert.Empty(t, st.PodsRemainUnschedulable[0].RejectedNodeGroups)
	assert.False(t, f.evaluator.HasPendingJob())
}

func TestProcessColdThenWarm(t *testing.T) {
	f := newTestFixture(t, nil)
	pods := []*apiv1.Pod{smallPod("small-1"), smallPod("small-2"), bigPod("big-1"), selectorPod("selector-1")}

	// First loop: nothing is known yet, so the status is left as it was and one job is queued.
	st := f.process(pods...)
	for i, ns := range st.PodsRemainUnschedulable {
		assert.ElementsMatch(t, []string{smallNodeGroup, bigNodeGroup}, keys(ns.SkippedNodeGroups), "pod %d", i)
		assert.Empty(t, ns.RejectedNodeGroups, "pod %d", i)
	}
	assert.True(t, f.evaluator.HasPendingJob())
	evaluatedPairs, completedJobs, cacheSize := f.evaluator.Stats()
	assert.Equal(t, 0, evaluatedPairs)
	assert.Equal(t, 0, completedJobs)
	assert.Equal(t, 0, cacheSize)

	// The worker evaluates 3 pod groups x 2 skipped node groups.
	assert.Equal(t, 6, f.evaluator.ProcessPendingNow(context.Background()))
	assert.False(t, f.evaluator.HasPendingJob())
	evaluatedPairs, completedJobs, cacheSize = f.evaluator.Stats()
	assert.Equal(t, 6, evaluatedPairs)
	assert.Equal(t, 1, completedJobs)
	assert.Equal(t, 6, cacheSize)

	// Second loop: verdicts are applied from the cache, nothing new is queued.
	st = f.process(pods...)
	assert.False(t, f.evaluator.HasPendingJob())
	small1, small2, big, selector := st.PodsRemainUnschedulable[0], st.PodsRemainUnschedulable[1], st.PodsRemainUnschedulable[2], st.PodsRemainUnschedulable[3]

	// Small pods fit both templates: both node groups stay skipped, with the original reasons.
	assert.Equal(t, allSkipReasons, small1.SkippedNodeGroups)
	assert.Empty(t, small1.RejectedNodeGroups)
	// Pods of one equivalence group share the rewritten maps, like they shared the input maps.
	assert.Equal(t, fmt.Sprintf("%p", small1.SkippedNodeGroups), fmt.Sprintf("%p", small2.SkippedNodeGroups), "pods of one group should share the skipped map")
	assert.Equal(t, fmt.Sprintf("%p", small1.RejectedNodeGroups), fmt.Sprintf("%p", small2.RejectedNodeGroups), "pods of one group should share the rejected map")

	// The big pod does not fit the small template: it is rejected there and stays skipped on the big one.
	assert.Equal(t, map[string]status.Reasons{bigNodeGroup: maxSizeReason}, big.SkippedNodeGroups)
	assert.ElementsMatch(t, []string{smallNodeGroup}, keys(big.RejectedNodeGroups))
	assert.Equal(t, "NodeResourcesFit", failingPredicate(t, big.RejectedNodeGroups[smallNodeGroup]))

	// The selector pod only matches the big template's labels.
	assert.Equal(t, map[string]status.Reasons{bigNodeGroup: maxSizeReason}, selector.SkippedNodeGroups)
	assert.ElementsMatch(t, []string{smallNodeGroup}, keys(selector.RejectedNodeGroups))
	assert.Equal(t, "NodeAffinity", failingPredicate(t, selector.RejectedNodeGroups[smallNodeGroup]))

	// The input maps were not mutated.
	assert.Len(t, allSkipReasons, 2)
}

func TestProcessKeepsExistingRejectionsAndUnknownNodeGroups(t *testing.T) {
	f := newTestFixture(t, nil)
	skipped := map[string]status.Reasons{smallNodeGroup: backoffReason, noTemplateGroup: backoffReason, "ng-not-considered": backoffReason}
	pod := bigPod("big-1")
	rejected := map[string]status.Reasons{"ng-other": skipReason("predicate failed in scale-up")}

	for loop := 0; loop < 2; loop++ {
		st := f.status(skipped, pod)
		st.PodsRemainUnschedulable[0].RejectedNodeGroups = rejected
		f.processor.Process(context.Background(), f.ctx, st)
		f.evaluator.ProcessPendingNow(context.Background())
		ns := st.PodsRemainUnschedulable[0]
		if loop == 0 {
			assert.Equal(t, skipped, ns.SkippedNodeGroups)
			assert.Equal(t, rejected, ns.RejectedNodeGroups)
			continue
		}
		// Node groups without a template are left skipped; the pre-existing rejection is preserved.
		assert.ElementsMatch(t, []string{noTemplateGroup, "ng-not-considered"}, keys(ns.SkippedNodeGroups))
		assert.ElementsMatch(t, []string{"ng-other", smallNodeGroup}, keys(ns.RejectedNodeGroups))
		assert.Equal(t, rejected["ng-other"], ns.RejectedNodeGroups["ng-other"])
	}
	// The caller's maps were not mutated.
	assert.Len(t, rejected, 1)
	assert.Len(t, skipped, 3)
}

func TestVerdictSurvivesTemplateRegeneration(t *testing.T) {
	f := newTestFixture(t, nil)
	pod := bigPod("big-1")
	f.process(pod)
	f.evaluator.ProcessPendingNow(context.Background())

	// Template NodeInfos are rebuilt every loop under a new random name that is also written into the
	// hostname label; that must not invalidate the verdicts.
	renamed := templateNode("small-template-other-name", 1000, 1000, nil, nil)
	renamed.Labels[apiv1.LabelHostname] = renamed.Name
	f.setTemplate(smallNodeGroup, renamed)

	st := f.process(pod)
	assert.False(t, f.evaluator.HasPendingJob(), "renamed template must hit the cache")
	assert.ElementsMatch(t, []string{smallNodeGroup}, keys(st.PodsRemainUnschedulable[0].RejectedNodeGroups))
}

func TestTemplateChangeInvalidatesVerdict(t *testing.T) {
	f := newTestFixture(t, nil)
	pod := bigPod("big-1")
	f.process(pod)
	f.evaluator.ProcessPendingNow(context.Background())
	st := f.process(pod)
	assert.ElementsMatch(t, []string{smallNodeGroup}, keys(st.PodsRemainUnschedulable[0].RejectedNodeGroups))

	// The small node group gets a bigger machine type: the pair is unknown again (left skipped) until the
	// worker has looked at the new template, after which the pod fits and the node group stays skipped.
	f.setTemplate(smallNodeGroup, templateNode("small-template", 8000, 8000, nil, nil))
	st = f.process(pod)
	assert.Equal(t, allSkipReasons, st.PodsRemainUnschedulable[0].SkippedNodeGroups)
	assert.True(t, f.evaluator.HasPendingJob())
	assert.Equal(t, 1, f.evaluator.ProcessPendingNow(context.Background()), "only the changed template should be re-evaluated")

	st = f.process(pod)
	assert.Equal(t, allSkipReasons, st.PodsRemainUnschedulable[0].SkippedNodeGroups)
	assert.Empty(t, st.PodsRemainUnschedulable[0].RejectedNodeGroups)
}

func TestStaleVerdictsAreAppliedAndRefreshed(t *testing.T) {
	f := newTestFixture(t, nil)
	pod := bigPod("big-1")
	f.process(pod)
	f.evaluator.ProcessPendingNow(context.Background())

	f.clock.SetTime(f.clock.Now().Add(DefaultRefreshAfter + time.Second))
	st := f.process(pod)
	// The old verdict is still applied (never regress to unknown)...
	assert.ElementsMatch(t, []string{smallNodeGroup}, keys(st.PodsRemainUnschedulable[0].RejectedNodeGroups))
	// ...and a refresh is queued for both pairs.
	assert.True(t, f.evaluator.HasPendingJob())
	assert.Equal(t, 2, f.evaluator.ProcessPendingNow(context.Background()))

	// Fresh again: nothing queued.
	f.process(pod)
	assert.False(t, f.evaluator.HasPendingJob())
}

func TestIdleVerdictsAreEvicted(t *testing.T) {
	// No background refresh here, so that a pending job unambiguously means "unknown pair".
	f := newTestFixture(t, func(o *EvaluatorOptions) { o.RefreshAfter = 10 * DefaultEvictAfter })
	first := bigPod("big-1")
	f.process(first)
	f.evaluator.ProcessPendingNow(context.Background())
	_, _, cacheSize := f.evaluator.Stats()
	assert.Equal(t, 2, cacheSize)

	// Still used within EvictAfter: the sweep after the next job keeps the verdicts.
	f.clock.SetTime(f.clock.Now().Add(DefaultEvictAfter / 2))
	f.process(first)
	f.clock.SetTime(f.clock.Now().Add(DefaultEvictAfter/2 + time.Second))
	others := []*apiv1.Pod{smallPod("small-1"), selectorPod("selector-1")}
	f.process(others...)
	f.evaluator.ProcessPendingNow(context.Background())
	_, _, cacheSize = f.evaluator.Stats()
	assert.Equal(t, 6, cacheSize, "verdicts looked up within EvictAfter must survive the sweep")

	// Nobody asks for the first pod for EvictAfter: the sweep after the next job drops its verdicts
	// and keeps the ones that were looked up.
	f.clock.SetTime(f.clock.Now().Add(DefaultEvictAfter + time.Second))
	f.process(others...)
	otherController := BuildTestPod("other-1", 4000, 100*1024*1024, MarkUnschedulable(), WithControllerOwnerRef("other-rs", "ReplicaSet", "other-rs-uid"))
	f.process(otherController)
	f.evaluator.ProcessPendingNow(context.Background())
	_, _, cacheSize = f.evaluator.Stats()
	assert.Equal(t, 6, cacheSize, "idle verdicts must be gone (2), used (4) and new (2) ones must stay")

	f.process(others...)
	assert.False(t, f.evaluator.HasPendingJob(), "fresh verdicts must still be cached")
	f.process(first)
	assert.True(t, f.evaluator.HasPendingJob(), "idle verdicts must have been evicted")
}

func TestPairBudgetServesBiggestPodGroupsDeterministically(t *testing.T) {
	// Two skipped node groups and a budget of 4 pairs: only the two biggest pod groups are attributed.
	f := newTestFixture(t, func(o *EvaluatorOptions) { o.MaxPairsPerLoop = 4 })
	pods := []*apiv1.Pod{selectorPod("selector-1"), bigPod("big-1"), bigPod("big-2"), bigPod("big-3"), smallPod("small-1"), smallPod("small-2")}

	f.process(pods...)
	assert.Equal(t, 4, f.evaluator.ProcessPendingNow(context.Background()), "only 2 groups x 2 templates should be evaluated")
	for loop := 0; loop < 3; loop++ {
		st := f.process(pods...)
		assert.False(t, f.evaluator.HasPendingJob(), "the same groups must be served every loop")
		selector, big, small := st.PodsRemainUnschedulable[0], st.PodsRemainUnschedulable[1], st.PodsRemainUnschedulable[4]
		assert.Equal(t, allSkipReasons, selector.SkippedNodeGroups, "the smallest group is beyond the budget and stays untouched")
		assert.Empty(t, selector.RejectedNodeGroups)
		assert.ElementsMatch(t, []string{smallNodeGroup}, keys(big.RejectedNodeGroups))
		assert.Equal(t, allSkipReasons, small.SkippedNodeGroups)
	}

	// Without a budget the selector group is attributed too.
	f.evaluator.opts.MaxPairsPerLoop = 0
	f.process(pods...)
	assert.Equal(t, 2, f.evaluator.ProcessPendingNow(context.Background()))
	st := f.process(pods...)
	assert.ElementsMatch(t, []string{smallNodeGroup}, keys(st.PodsRemainUnschedulable[0].RejectedNodeGroups))
}

func TestPendingJobsAreMerged(t *testing.T) {
	f := newTestFixture(t, nil)
	older, newer := bigPod("big-1"), selectorPod("selector-1")
	// Two statuses in one loop with different pods (e.g. two scale-up salvo iterations): the second
	// one is merged into the pending job instead of replacing it.
	f.process(older)
	f.process(newer)
	assert.Equal(t, 4, f.evaluator.ProcessPendingNow(context.Background()), "the pairs of both statuses should be evaluated")
	f.process(newer)
	assert.False(t, f.evaluator.HasPendingJob())
	f.process(older)
	assert.False(t, f.evaluator.HasPendingJob(), "the pairs of the older status must not have been dropped")
}

func TestOverlappingStatusesEvaluateEachPairOnce(t *testing.T) {
	f := newTestFixture(t, nil)
	// Salvo iterations see shrinking, overlapping sets of pods.
	f.process(bigPod("big-1"), selectorPod("selector-1"))
	f.process(selectorPod("selector-1"))
	assert.Equal(t, 4, f.evaluator.ProcessPendingNow(context.Background()), "shared pairs must be evaluated once")
}

func TestPendingJobsRespectPairBudget(t *testing.T) {
	f := newTestFixture(t, func(o *EvaluatorOptions) { o.MaxPairsPerLoop = 2 })
	f.process(bigPod("big-1"))
	f.process(selectorPod("selector-1"))
	assert.Equal(t, 2, f.evaluator.ProcessPendingNow(context.Background()), "the merged job is bounded by MaxPairsPerLoop")
	f.process(selectorPod("selector-1"))
	assert.False(t, f.evaluator.HasPendingJob(), "the newest pairs go first")
	f.process(bigPod("big-1"))
	assert.True(t, f.evaluator.HasPendingJob(), "pairs beyond the budget are requested again")
}

func TestRunningJobYieldsAndRequeuesItsRest(t *testing.T) {
	f := newTestFixture(t, nil)
	// The worker picks up a job for two templates...
	f.process(bigPod("big-1"))
	running := f.evaluator.takePending()
	// ...and a newer status arrives before it reaches the second template.
	f.process(selectorPod("selector-1"))
	assert.Equal(t, 1, f.evaluator.processJob(context.Background(), running), "the running job should yield after its first template")
	assert.Equal(t, 3, f.evaluator.ProcessPendingNow(context.Background()), "the newer pairs and the rest of the yielded job should be evaluated")
	f.process(bigPod("big-1"))
	assert.False(t, f.evaluator.HasPendingJob(), "the yielded job's rest must not have been dropped")
	f.process(selectorPod("selector-1"))
	assert.False(t, f.evaluator.HasPendingJob())
}

func TestPodsWithResourceClaimsAreNotEvaluated(t *testing.T) {
	f := newTestFixture(t, nil)
	f.ctx.DynamicResourceAllocationEnabled = true
	draPod := bigPod("dra-1")
	draPod.Spec.ResourceClaims = []apiv1.PodResourceClaim{{Name: "gpu", ResourceClaimTemplateName: ptr("gpu-template")}}
	plain := bigPod("big-1")

	st := f.process(draPod, plain)
	assert.Equal(t, 2, f.evaluator.ProcessPendingNow(context.Background()), "only the plain pod's group should be evaluated")
	st = f.process(draPod, plain)
	assert.Equal(t, allSkipReasons, st.PodsRemainUnschedulable[0].SkippedNodeGroups, "DRA pod must be left untouched")
	assert.Empty(t, st.PodsRemainUnschedulable[0].RejectedNodeGroups)
	assert.ElementsMatch(t, []string{smallNodeGroup}, keys(st.PodsRemainUnschedulable[1].RejectedNodeGroups))
}

func TestInterPodAffinityIsIgnored(t *testing.T) {
	f := newTestFixture(t, nil)
	// Required pod affinity can never be satisfied on a lone, empty template node; it must not turn a
	// fitting node group into a rejected one.
	pod := BuildTestPod("affinity-1", 500, 100*1024*1024, MarkUnschedulable(), WithPodAffinity(map[string]string{"app": "db"}, apiv1.LabelHostname))
	f.process(pod)
	f.evaluator.ProcessPendingNow(context.Background())
	st := f.process(pod)
	assert.Equal(t, allSkipReasons, st.PodsRemainUnschedulable[0].SkippedNodeGroups)
	assert.Empty(t, st.PodsRemainUnschedulable[0].RejectedNodeGroups)
	assert.NotNil(t, pod.Spec.Affinity.PodAffinity, "the caller's pod must not be modified")
}

func TestTaintedTemplateRejectsIntolerantPod(t *testing.T) {
	f := newTestFixture(t, nil)
	skipped := map[string]status.Reasons{taintedNodeGroup: backoffReason}
	pod := smallPod("small-1")
	st := f.status(skipped, pod)
	f.processor.Process(context.Background(), f.ctx, st)
	f.evaluator.ProcessPendingNow(context.Background())
	st = f.status(skipped, pod)
	f.processor.Process(context.Background(), f.ctx, st)
	assert.Empty(t, st.PodsRemainUnschedulable[0].SkippedNodeGroups)
	assert.Equal(t, "TaintToleration", failingPredicate(t, st.PodsRemainUnschedulable[0].RejectedNodeGroups[taintedNodeGroup]))
}

func TestStatusWithoutRemainingPodsIsIgnored(t *testing.T) {
	f := newTestFixture(t, nil)
	st := &status.ScaleUpStatus{Result: status.ScaleUpSuccessful, ConsideredNodeGroups: f.nodeGroups()}
	f.processor.Process(context.Background(), f.ctx, st)
	assert.False(t, f.evaluator.HasPendingJob())
}

// TestWorkerRunsConcurrentlyWithMainLoopPredicateChecks exercises the real deployment shape under the race
// detector: the evaluator goroutine runs predicate checks on its own handle while the "main loop" keeps
// running predicate checks on the main handle and feeding statuses to the processor.
func TestWorkerRunsConcurrentlyWithMainLoopPredicateChecks(t *testing.T) {
	f := newTestFixture(t, func(o *EvaluatorOptions) { o.Clock = clock.RealClock{} })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.evaluator.Run(ctx)

	mainHandle := framework.NewTestFrameworkHandleOrDie(t)
	mainSnapshot := predicate.NewPredicateSnapshot(store.NewBasicSnapshotStore(), mainHandle, false, 1, false, 0)
	mainNode := templateNode("main-node", 8000, 8000, map[string]string{"tier": "big"}, nil)
	assert.NoError(t, mainSnapshot.AddNodeInfo(framework.NewTestNodeInfo(mainNode)))

	pods := []*apiv1.Pod{smallPod("small-1"), bigPod("big-1"), selectorPod("selector-1")}
	for i := 0; i < 200; i++ {
		// Main loop work on the main handle...
		assert.Nil(t, mainSnapshot.CheckPredicates(pods[i%len(pods)], mainNode.Name))
		// ...interleaved with statuses handed to the processor (and so to the worker).
		pod := BuildTestPod(fmt.Sprintf("pod-%d", i), int64(100+i*50), 100*1024*1024, MarkUnschedulable())
		f.process(append([]*apiv1.Pod{pod}, pods...)...)
	}

	// Eventually every verdict for the stable pods is known and consistent.
	assert.Eventually(t, func() bool {
		st := f.process(pods...)
		if f.evaluator.HasPendingJob() {
			return false
		}
		small, big, selector := st.PodsRemainUnschedulable[0], st.PodsRemainUnschedulable[1], st.PodsRemainUnschedulable[2]
		return len(small.RejectedNodeGroups) == 0 && len(small.SkippedNodeGroups) == 2 &&
			len(big.RejectedNodeGroups) == 1 && len(big.SkippedNodeGroups) == 1 &&
			len(selector.RejectedNodeGroups) == 1 && len(selector.SkippedNodeGroups) == 1
	}, 10*time.Second, 10*time.Millisecond)
}

func TestNewEvaluatorFrameworkHandleCoexistsWithMainHandle(t *testing.T) {
	// Both handles share one informer factory, like in the real builder; plugins register indexers keyed
	// by profile name, so the evaluator's handle must use a different one.
	informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewSimpleClientset(), 0)
	cfg, err := schedulerconfiglatest.Default()
	assert.NoError(t, err)
	_, err = framework.NewHandle(context.Background(), informerFactory, cfg, true, true)
	assert.NoError(t, err)
	_, err = NewEvaluatorFrameworkHandle(context.Background(), informerFactory, cfg, true, true)
	assert.NoError(t, err)
	assert.Equal(t, "default-scheduler", cfg.Profiles[0].SchedulerName, "the caller's config must not be modified")

	// nil config falls back to the defaults.
	_, err = NewEvaluatorFrameworkHandle(context.Background(), informers.NewSharedInformerFactory(clientsetfake.NewSimpleClientset(), 0), nil, false, false)
	assert.NoError(t, err)
}

func TestPodGroupSignature(t *testing.T) {
	base := smallPod("small-1")
	sameGroup := smallPod("small-2")
	sameGroup.Spec.Containers[0].Env = []apiv1.EnvVar{{Name: "POD_NAME", Value: "small-2"}}
	sameGroup.Spec.Hostname = "small-2"
	sameGroup.Spec.NodeName = "some-node"
	sameGroup.Spec.Volumes = append(sameGroup.Spec.Volumes, apiv1.Volume{Name: "token", VolumeSource: apiv1.VolumeSource{Projected: &apiv1.ProjectedVolumeSource{}}})
	sameGroup.Spec.Containers[0].VolumeMounts = append(sameGroup.Spec.Containers[0].VolumeMounts, apiv1.VolumeMount{Name: "token", MountPath: "/var/run/secrets"})
	assert.Equal(t, podGroupSignature(base), podGroupSignature(sameGroup), "per-replica differences must not change the signature")

	otherController := smallPod("small-3")
	otherController.OwnerReferences[0].UID = "other-rs-uid"
	assert.NotEqual(t, podGroupSignature(base), podGroupSignature(otherController))

	otherRequests := smallPod("small-4")
	otherRequests.Spec.Containers[0].Resources.Requests[apiv1.ResourceCPU] = *resource.NewMilliQuantity(600, resource.DecimalSI)
	assert.NotEqual(t, podGroupSignature(base), podGroupSignature(otherRequests))

	otherLabels := smallPod("small-5")
	otherLabels.Labels = map[string]string{"app": "other"}
	assert.NotEqual(t, podGroupSignature(base), podGroupSignature(otherLabels))

	withAffinity := smallPod("small-6")
	WithPodAffinity(map[string]string{"app": "db"}, apiv1.LabelHostname)(withAffinity)
	assert.Equal(t, podGroupSignature(base), podGroupSignature(withAffinity), "inter-pod affinity is ignored by the evaluator and so by the signature")
	assert.NotNil(t, withAffinity.Spec.Affinity.PodAffinity, "the pod must not be modified")
}

func TestTemplateSignature(t *testing.T) {
	base := templateNode("template-a", 1000, 1000, map[string]string{"tier": "small"}, nil)
	renamed := templateNode("template-b", 1000, 1000, map[string]string{"tier": "small"}, nil)
	renamed.Labels[apiv1.LabelHostname] = renamed.Name
	assert.Equal(t, templateSignature(framework.NewTestNodeInfo(base)), templateSignature(framework.NewTestNodeInfo(renamed)))

	otherLabel := templateNode("template-a", 1000, 1000, map[string]string{"tier": "big"}, nil)
	assert.NotEqual(t, templateSignature(framework.NewTestNodeInfo(base)), templateSignature(framework.NewTestNodeInfo(otherLabel)))

	tainted := templateNode("template-a", 1000, 1000, map[string]string{"tier": "small"}, []apiv1.Taint{{Key: "k", Effect: apiv1.TaintEffectNoSchedule}})
	assert.NotEqual(t, templateSignature(framework.NewTestNodeInfo(base)), templateSignature(framework.NewTestNodeInfo(tainted)))

	bigger := templateNode("template-a", 2000, 1000, map[string]string{"tier": "small"}, nil)
	assert.NotEqual(t, templateSignature(framework.NewTestNodeInfo(base)), templateSignature(framework.NewTestNodeInfo(bigger)))

	// DaemonSet pods on the template consume resources and are part of the signature.
	withDaemonSet := framework.NewTestNodeInfo(base, BuildTestPod("ds-pod", 100, 100*1024*1024, WithDSController()))
	assert.NotEqual(t, templateSignature(framework.NewTestNodeInfo(base)), templateSignature(withDaemonSet))
}

func ptr[T any](v T) *T { return &v }
