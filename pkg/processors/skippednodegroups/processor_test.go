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
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	schedulerconfiglatest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
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
	evaluator *Evaluator
	processor *ScaleUpStatusProcessor
	provider  *testprovider.TestCloudProvider
	templates map[string]*framework.NodeInfo
	ctx       *ca_context.AutoscalingContext
}

func newTestFixture(t *testing.T, modify func(*EvaluatorOptions)) *testFixture {
	t.Helper()
	opts := DefaultEvaluatorOptions()
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

func TestProcessFirstLoopThenNextLoop(t *testing.T) {
	f := newTestFixture(t, nil)
	pods := []*apiv1.Pod{smallPod("small-1"), smallPod("small-2"), bigPod("big-1"), selectorPod("selector-1")}

	// First loop: nothing is known yet, so the status is left as it was and one job is queued.
	st := f.process(pods...)
	for i, ns := range st.PodsRemainUnschedulable {
		assert.ElementsMatch(t, []string{smallNodeGroup, bigNodeGroup}, keys(ns.SkippedNodeGroups), "pod %d", i)
		assert.Empty(t, ns.RejectedNodeGroups, "pod %d", i)
	}
	assert.True(t, f.evaluator.HasPendingJob())
	evaluatedPairs, completedJobs, published := f.evaluator.Stats()
	assert.Equal(t, 0, evaluatedPairs)
	assert.Equal(t, 0, completedJobs)
	assert.Equal(t, 0, published)

	// The worker evaluates 3 pod groups x 2 skipped node groups and publishes one verdict per pod and node group.
	assert.Equal(t, 6, f.evaluator.ProcessPendingNow(context.Background()))
	assert.False(t, f.evaluator.HasPendingJob())
	evaluatedPairs, completedJobs, published = f.evaluator.Stats()
	assert.Equal(t, 6, evaluatedPairs)
	assert.Equal(t, 1, completedJobs)
	assert.Equal(t, 8, published)

	// Second loop: the published verdicts are applied, and the current pairs are handed over again.
	st = f.process(pods...)
	assert.True(t, f.evaluator.HasPendingJob(), "every loop hands its pairs to the worker")
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

func TestNewReplicaOfKnownGroupIsServedImmediately(t *testing.T) {
	f := newTestFixture(t, nil)
	f.process(bigPod("big-1"))
	f.evaluator.ProcessPendingNow(context.Background())

	// big-2 has never been evaluated, but it is in the same equivalence group as big-1.
	st := f.process(bigPod("big-1"), bigPod("big-2"))
	for i, ns := range st.PodsRemainUnschedulable {
		assert.ElementsMatch(t, []string{smallNodeGroup}, keys(ns.RejectedNodeGroups), "pod %d", i)
	}

	// A group none of whose pods has been evaluated stays unknown.
	st = f.process(bigPod("big-3"))
	assert.Equal(t, allSkipReasons, st.PodsRemainUnschedulable[0].SkippedNodeGroups)
	assert.Empty(t, st.PodsRemainUnschedulable[0].RejectedNodeGroups)
}

func TestTemplateChangeIsPickedUpByNextEvaluation(t *testing.T) {
	f := newTestFixture(t, nil)
	pod := bigPod("big-1")
	f.process(pod)
	f.evaluator.ProcessPendingNow(context.Background())
	st := f.process(pod)
	assert.ElementsMatch(t, []string{smallNodeGroup}, keys(st.PodsRemainUnschedulable[0].RejectedNodeGroups))

	// The small node group gets a bigger machine type. Verdicts are keyed by node group id, so the
	// previous verdict is applied until the next evaluation, which sees the new template.
	f.setTemplate(smallNodeGroup, templateNode("small-template", 8000, 8000, nil, nil))
	f.evaluator.ProcessPendingNow(context.Background()) // Drop the job queued with the old template.
	st = f.process(pod)
	assert.ElementsMatch(t, []string{smallNodeGroup}, keys(st.PodsRemainUnschedulable[0].RejectedNodeGroups))
	f.evaluator.ProcessPendingNow(context.Background())

	st = f.process(pod)
	assert.Equal(t, allSkipReasons, st.PodsRemainUnschedulable[0].SkippedNodeGroups)
	assert.Empty(t, st.PodsRemainUnschedulable[0].RejectedNodeGroups)
}

func TestVerdictsOfVanishedPodsAreDropped(t *testing.T) {
	f := newTestFixture(t, nil)
	f.process(bigPod("big-1"))
	f.evaluator.ProcessPendingNow(context.Background())
	_, _, published := f.evaluator.Stats()
	assert.Equal(t, 2, published)

	// The next evaluation only covers the selector pod: its verdicts replace the previous ones.
	f.process(selectorPod("selector-1"))
	f.evaluator.ProcessPendingNow(context.Background())
	_, _, published = f.evaluator.Stats()
	assert.Equal(t, 2, published)

	st := f.process(bigPod("big-1"))
	assert.Empty(t, st.PodsRemainUnschedulable[0].RejectedNodeGroups, "verdicts of the vanished pod must be gone")
}

func TestPairBudgetServesBiggestPodGroupsDeterministically(t *testing.T) {
	// Two skipped node groups and a budget of 4 pairs: only the two biggest pod groups are attributed.
	f := newTestFixture(t, func(o *EvaluatorOptions) { o.MaxPairsPerLoop = 4 })
	pods := []*apiv1.Pod{selectorPod("selector-1"), bigPod("big-1"), bigPod("big-2"), bigPod("big-3"), smallPod("small-1"), smallPod("small-2")}

	f.process(pods...)
	assert.Equal(t, 4, f.evaluator.ProcessPendingNow(context.Background()), "only 2 groups x 2 templates should be evaluated")
	for loop := 0; loop < 3; loop++ {
		st := f.process(pods...)
		assert.Equal(t, 4, f.evaluator.ProcessPendingNow(context.Background()), "the same groups must be served every loop")
		selector, big, small := st.PodsRemainUnschedulable[0], st.PodsRemainUnschedulable[1], st.PodsRemainUnschedulable[4]
		assert.Equal(t, allSkipReasons, selector.SkippedNodeGroups, "the smallest group is beyond the budget and stays untouched")
		assert.Empty(t, selector.RejectedNodeGroups)
		assert.ElementsMatch(t, []string{smallNodeGroup}, keys(big.RejectedNodeGroups))
		assert.Equal(t, allSkipReasons, small.SkippedNodeGroups)
	}

	// Without a budget the selector group is attributed too.
	f.evaluator.opts.MaxPairsPerLoop = 0
	f.process(pods...)
	assert.Equal(t, 6, f.evaluator.ProcessPendingNow(context.Background()))
	st := f.process(pods...)
	assert.ElementsMatch(t, []string{smallNodeGroup}, keys(st.PodsRemainUnschedulable[0].RejectedNodeGroups))
}

func TestNewestJobWins(t *testing.T) {
	f := newTestFixture(t, nil)
	older, newer := bigPod("big-1"), selectorPod("selector-1")
	f.process(older)
	f.process(newer) // Replaces the pending job before the worker picked it up.
	assert.Equal(t, 2, f.evaluator.ProcessPendingNow(context.Background()), "only the newest job should be evaluated")

	st := f.process(older)
	assert.Empty(t, st.PodsRemainUnschedulable[0].RejectedNodeGroups, "the dropped job's pairs are unknown")
	st = f.process(newer)
	assert.ElementsMatch(t, []string{smallNodeGroup}, keys(st.PodsRemainUnschedulable[0].RejectedNodeGroups))
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
	f := newTestFixture(t, nil)
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

func ptr[T any](v T) *T { return &v }
