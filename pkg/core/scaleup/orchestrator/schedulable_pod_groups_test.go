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
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	cacontext "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/equivalence"
	coretest "sigs.k8s.io/cluster-autoscaler/pkg/core/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupconfig"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	processorstest "sigs.k8s.io/cluster-autoscaler/pkg/processors/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	kubeutil "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	utilstest "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

// recordingSnapshot records schedulable pod groups simulations, i.e. predicate checks and node
// additions done against node group templates.
type recordingSnapshot struct {
	clustersnapshot.ClusterSnapshot
	// templates maps template node names to node group ids.
	templates map[string]string
	// checks counts predicate checks per node group.
	checks map[string]int
	// forks counts template nodes added to the snapshot (one per snapshot fork) per node group.
	forks map[string]int
	// onCheck, if set, is called after each predicate check against a node group template.
	onCheck func(nodeGroupId string, checks int)
}

func newRecordingSnapshot(snapshot clustersnapshot.ClusterSnapshot) *recordingSnapshot {
	return &recordingSnapshot{
		ClusterSnapshot: snapshot,
		templates:       map[string]string{},
		checks:          map[string]int{},
		forks:           map[string]int{},
	}
}

func (s *recordingSnapshot) AddNodeInfo(nodeInfo *framework.NodeInfo) error {
	if nodeGroupId, found := s.templates[nodeInfo.Node().Name]; found {
		s.forks[nodeGroupId]++
	}
	return s.ClusterSnapshot.AddNodeInfo(nodeInfo)
}

func (s *recordingSnapshot) CheckPredicates(pod *apiv1.Pod, nodeName string) clustersnapshot.SchedulingError {
	err := s.ClusterSnapshot.CheckPredicates(pod, nodeName)
	if nodeGroupId, found := s.templates[nodeName]; found {
		s.checks[nodeGroupId]++
		if s.onCheck != nil {
			s.onCheck(nodeGroupId, s.checks[nodeGroupId])
		}
	}
	return err
}

func (s *recordingSnapshot) evaluatedNodeGroups() []string {
	var result []string
	for nodeGroupId := range s.forks {
		result = append(result, nodeGroupId)
	}
	slices.Sort(result)
	return result
}

// sortingNodeGroupListProcessor makes the binpacking order deterministic.
type sortingNodeGroupListProcessor struct{}

func (sortingNodeGroupListProcessor) Process(_ context.Context, _ *cacontext.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup,
	nodeInfos map[string]*framework.NodeInfo, _ []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	slices.SortFunc(nodeGroups, func(a, b cloudprovider.NodeGroup) int { return strings.Compare(a.Id(), b.Id()) })
	return nodeGroups, nodeInfos, nil
}

func (sortingNodeGroupListProcessor) CleanUp() {}

// firstOptionStrategy picks the first expansion option and records the options it was given. If
// rejectAll is set, it filters out all options.
type firstOptionStrategy struct {
	options   []expander.Option
	rejectAll bool
}

func (s *firstOptionStrategy) BestOption(_ context.Context, options []expander.Option, _ map[string]*framework.NodeInfo) *expander.Option {
	s.options = options
	if len(options) == 0 || s.rejectAll {
		return nil
	}
	return &options[0]
}

// stopAfterOptionsLimiter stops binpacking once the given number of expansion options was found.
type stopAfterOptionsLimiter struct {
	options int
}

func (l *stopAfterOptionsLimiter) InitBinpacking(*cacontext.AutoscalingContext, []cloudprovider.NodeGroup) {
}
func (l *stopAfterOptionsLimiter) MarkProcessed(*cacontext.AutoscalingContext, string) {}
func (l *stopAfterOptionsLimiter) StopBinpacking(_ context.Context, _ *cacontext.AutoscalingContext, options []expander.Option) bool {
	return len(options) >= l.options
}
func (l *stopAfterOptionsLimiter) FinalizeBinpacking(*cacontext.AutoscalingContext, []expander.Option) {
}

type lazyTestNodeGroup struct {
	id        string
	cpu, mem  int64
	size, max int
}

type lazyTestEnv struct {
	provider     *testprovider.TestCloudProvider
	orchestrator *ScaleUpOrchestrator
	snapshot     *recordingSnapshot
	nodes        []*apiv1.Node
	nodeInfos    map[string]*framework.NodeInfo
	expander     *firstOptionStrategy
	// failScaleUp holds node groups whose scale-ups fail.
	failScaleUp map[string]bool
}

func newLazyTestEnv(t testing.TB, nodeGroups []lazyTestNodeGroup, modifyOptions func(*config.AutoscalingOptions), stopAfterOptions int) *lazyTestEnv {
	t.Helper()
	now := time.Now()
	failScaleUp := map[string]bool{}
	provider := testprovider.NewTestCloudProviderBuilder().WithOnScaleUp(func(id string, _ int) error {
		if failScaleUp[id] {
			return fmt.Errorf("scale-up of %s failed", id)
		}
		return nil
	}).Build()

	var nodes []*apiv1.Node
	for _, ng := range nodeGroups {
		provider.AddNodeGroup(ng.id, 0, ng.max, ng.size)
		for i := 0; i < ng.size; i++ {
			node := utilstest.BuildTestNode(fmt.Sprintf("%s-node-%d", ng.id, i), ng.cpu, ng.mem)
			utilstest.SetNodeReadyState(node, true, now.Add(-2*time.Minute))
			provider.AddNode(ng.id, node)
			nodes = append(nodes, node)
		}
	}

	options := config.AutoscalingOptions{
		EstimatorName:                  estimator.BinpackingEstimatorName,
		MaxCoresTotal:                  config.DefaultMaxClusterCores,
		MaxMemoryTotal:                 config.DefaultMaxClusterMemory * units.GiB,
		MaxNodeGroupBinpackingDuration: 1 * time.Second,
		// Tests of the eager computation disable it explicitly.
		LazySchedulablePodGroupsEnabled: true,
	}
	if modifyOptions != nil {
		modifyOptions(&options)
	}

	listers := kubeutil.NewListerRegistry(nil, nil, kubeutil.NewTestPodLister(nil), nil, nil, nil, nil, nil, nil)
	processors, templateNodeInfoRegistry := processorstest.NewTestProcessors(options)
	processors.NodeGroupListProcessor = sortingNodeGroupListProcessor{}
	if stopAfterOptions > 0 {
		processors.BinpackingLimiter = &stopAfterOptionsLimiter{options: stopAfterOptions}
	}
	autoscalingCtx, err := coretest.NewScaleTestAutoscalingContext(options, &fake.Clientset{}, listers, provider, nil, nil, templateNodeInfoRegistry)
	assert.NoError(t, err)
	assert.NoError(t, autoscalingCtx.ClusterSnapshot.SetClusterState(context.Background(), nodes, nil, nil, nil))
	assert.NoError(t, autoscalingCtx.TemplateNodeInfoRegistry.Recompute(context.Background(), &autoscalingCtx, nodes, []*appsv1.DaemonSet{}, taints.TaintConfig{}, now))
	nodeInfos := autoscalingCtx.TemplateNodeInfoRegistry.GetNodeInfos()

	snapshot := newRecordingSnapshot(autoscalingCtx.ClusterSnapshot)
	for nodeGroupId, nodeInfo := range nodeInfos {
		snapshot.templates[nodeInfo.Node().Name] = nodeGroupId
	}
	autoscalingCtx.ClusterSnapshot = snapshot

	clusterState := clusterstate.NewClusterStateRegistry(provider, autoscalingCtx.LogRecorder, coretest.NewBackoff(),
		nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 15 * time.Minute}),
		autoscalingCtx.TemplateNodeInfoRegistry, clusterstate.WithScaleStateNotifier(processors.ScaleStateNotifier))
	assert.NoError(t, clusterState.UpdateNodes(context.Background(), nodes, now))

	strategy := &firstOptionStrategy{}
	autoscalingCtx.ExpanderStrategy = strategy

	estimatorBuilder, err := estimator.NewEstimatorBuilder(estimator.BinpackingEstimatorName, estimator.NewThresholdBasedEstimationLimiter(nil), estimator.NewDecreasingPodOrderer(), nil, false)
	assert.NoError(t, err)
	trackerFactory := resourcequotas.NewTrackerFactory(resourcequotas.TrackerOptions{
		QuotaProvider:            resourcequotas.NewCloudQuotasProvider(provider),
		CustomResourcesProcessor: processors.CustomResourcesProcessor,
	})
	o := New()
	o.Initialize(&autoscalingCtx, processors, clusterState, estimatorBuilder, taints.TaintConfig{}, trackerFactory)

	return &lazyTestEnv{provider: provider, orchestrator: o, snapshot: snapshot, nodes: nodes, nodeInfos: nodeInfos, expander: strategy, failScaleUp: failScaleUp}
}

func (e *lazyTestEnv) scaleUp(t testing.TB, pods ...*apiv1.Pod) *status.ScaleUpStatus {
	t.Helper()
	scaleUpStatus, err := e.tryScaleUp(false, pods...)
	assert.NoError(t, err)
	return scaleUpStatus
}

func (e *lazyTestEnv) tryScaleUp(allOrNothing bool, pods ...*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
	return e.orchestrator.ScaleUp(context.Background(), pods, e.nodes, []*appsv1.DaemonSet{}, e.nodeInfos, allOrNothing)
}

func podNames(pods []*apiv1.Pod) []string {
	var names []string
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	slices.Sort(names)
	return names
}

func remainingPodNames(infos []status.NoScaleUpInfo) []string {
	var names []string
	for _, info := range infos {
		names = append(names, info.Pod.Name)
	}
	slices.Sort(names)
	return names
}

func TestLazySchedulablePodGroupsOnlyEvaluatesNodeGroupsReachedByBinpacking(t *testing.T) {
	env := newLazyTestEnv(t, []lazyTestNodeGroup{
		{id: "ng1", cpu: 1000, mem: 1000, size: 1, max: 10},
		{id: "ng2", cpu: 1000, mem: 1000, size: 1, max: 10},
		{id: "ng3", cpu: 1000, mem: 1000, size: 1, max: 10},
	}, nil, 1)

	// Three distinct pod equivalence groups, all fitting every node group.
	scaleUpStatus := env.scaleUp(t,
		utilstest.BuildTestPod("p1", 500, 0),
		utilstest.BuildTestPod("p2", 400, 0),
		utilstest.BuildTestPod("p3", 300, 0),
	)

	assert.True(t, scaleUpStatus.WasSuccessful())
	assert.Equal(t, "ng1", scaleUpStatus.ScaleUpInfos[0].Group.Id())
	// Binpacking stopped after ng1, and all pods are schedulable, so ng2 and ng3 are never simulated.
	assert.Equal(t, []string{"ng1"}, env.snapshot.evaluatedNodeGroups())
	// All pod equivalence groups are evaluated within a single snapshot fork.
	assert.Equal(t, 1, env.snapshot.forks["ng1"])
	assert.Equal(t, 3, env.snapshot.checks["ng1"])
	assert.Empty(t, scaleUpStatus.PodsRemainUnschedulable)
	assert.Empty(t, scaleUpStatus.PodsAwaitEvaluation)
}

func TestLazySchedulablePodGroupsWithoutBinpackingLimitEvaluatesEachNodeGroupOnce(t *testing.T) {
	env := newLazyTestEnv(t, []lazyTestNodeGroup{
		{id: "ng1", cpu: 1000, mem: 1000, size: 1, max: 10},
		{id: "ng2", cpu: 2000, mem: 1000, size: 1, max: 10},
		{id: "ng3", cpu: 3000, mem: 1000, size: 1, max: 10},
	}, func(o *config.AutoscalingOptions) { o.BalanceSimilarNodeGroups = true }, 0)

	scaleUpStatus := env.scaleUp(t,
		utilstest.BuildTestPod("p1", 500, 0),
		utilstest.BuildTestPod("p2", 1500, 0),
		utilstest.BuildTestPod("p3", 2500, 0),
		utilstest.BuildTestPod("p4", 5000, 0),
	)

	assert.True(t, scaleUpStatus.WasSuccessful())
	assert.Len(t, env.expander.options, 3)
	// Without a binpacking limit every node group is evaluated, each exactly once (no extra forks
	// for similarity checks or reporting).
	assert.Equal(t, []string{"ng1", "ng2", "ng3"}, env.snapshot.evaluatedNodeGroups())
	for _, ng := range []string{"ng1", "ng2", "ng3"} {
		assert.Equal(t, 1, env.snapshot.forks[ng], ng)
	}
	assert.Equal(t, []string{"p4"}, remainingPodNames(scaleUpStatus.PodsRemainUnschedulable))
	assert.Len(t, scaleUpStatus.PodsRemainUnschedulable[0].RejectedNodeGroups, 3)
}

func TestLazySchedulablePodGroupsReportingCompletesUnevaluatedNodeGroups(t *testing.T) {
	env := newLazyTestEnv(t, []lazyTestNodeGroup{
		{id: "ng1", cpu: 1000, mem: 1000, size: 1, max: 10},
		{id: "ng2", cpu: 4000, mem: 1000, size: 1, max: 10},
		{id: "ng3", cpu: 1000, mem: 1000, size: 1, max: 10},
	}, nil, 1)

	scaleUpStatus := env.scaleUp(t,
		utilstest.BuildTestPod("small", 500, 0),
		utilstest.BuildTestPod("big", 3000, 0),
		utilstest.BuildTestPod("huge", 10000, 0),
	)

	assert.True(t, scaleUpStatus.WasSuccessful())
	assert.Equal(t, "ng1", scaleUpStatus.ScaleUpInfos[0].Group.Id())
	// Binpacking stopped after ng1. ng2 and ng3 were simulated only for reporting, and only for the
	// pod groups not schedulable on ng1.
	assert.Equal(t, []string{"ng1", "ng2", "ng3"}, env.snapshot.evaluatedNodeGroups())
	assert.Equal(t, 3, env.snapshot.checks["ng1"])
	assert.Equal(t, 2, env.snapshot.checks["ng2"])
	// "big" became schedulable on ng2, so only "huge" is checked against ng3.
	assert.Equal(t, 1, env.snapshot.checks["ng3"])

	// Same reporting as with eager evaluation: "big" fits ng2, so it awaits evaluation, while "huge"
	// fits nowhere and is rejected by all node groups.
	assert.Equal(t, []string{"big"}, podNames(scaleUpStatus.PodsAwaitEvaluation))
	assert.Equal(t, []string{"huge"}, remainingPodNames(scaleUpStatus.PodsRemainUnschedulable))
	rejected := scaleUpStatus.PodsRemainUnschedulable[0].RejectedNodeGroups
	assert.Contains(t, rejected, "ng1")
	assert.Contains(t, rejected, "ng2")
	assert.Contains(t, rejected, "ng3")
}

func TestLazySchedulablePodGroupsNoOptionsReportsAllNodeGroups(t *testing.T) {
	env := newLazyTestEnv(t, []lazyTestNodeGroup{
		{id: "ng1", cpu: 1000, mem: 1000, size: 1, max: 10},
		{id: "ng2", cpu: 1000, mem: 1000, size: 1, max: 10},
	}, nil, 1)

	scaleUpStatus := env.scaleUp(t, utilstest.BuildTestPod("huge", 10000, 0))

	assert.Equal(t, status.ScaleUpNoOptionsAvailable, scaleUpStatus.Result)
	assert.Equal(t, []string{"huge"}, remainingPodNames(scaleUpStatus.PodsRemainUnschedulable))
	assert.Len(t, scaleUpStatus.PodsRemainUnschedulable[0].RejectedNodeGroups, 2)
	assert.Empty(t, scaleUpStatus.PodsAwaitEvaluation)
}

func TestLazySchedulablePodGroupsSimilarNodeGroups(t *testing.T) {
	env := newLazyTestEnv(t, []lazyTestNodeGroup{
		{id: "ng1", cpu: 1000, mem: 1000, size: 1, max: 10},
		{id: "ng2", cpu: 1000, mem: 1000, size: 1, max: 10},
		// Similar to ng1 and ng2, but skipped because it reached its max size.
		{id: "ng3", cpu: 1000, mem: 1000, size: 1, max: 1},
		// Not similar.
		{id: "ng4", cpu: 2000, mem: 1000, size: 1, max: 10},
	}, func(o *config.AutoscalingOptions) { o.BalanceSimilarNodeGroups = true }, 1)

	pods := make([]*apiv1.Pod, 0, 4)
	for i := 0; i < 4; i++ {
		pods = append(pods, utilstest.BuildTestPod(fmt.Sprintf("p%d", i), 800, 0))
	}
	scaleUpStatus := env.scaleUp(t, pods...)

	assert.True(t, scaleUpStatus.WasSuccessful())
	// ng2 is simulated as a similar node group of ng1, once. The skipped ng3 is never simulated,
	// and neither is ng4, which binpacking didn't reach.
	assert.Equal(t, []string{"ng1", "ng2"}, env.snapshot.evaluatedNodeGroups())
	assert.Equal(t, 1, env.snapshot.forks["ng2"])
	var scaledUp []string
	for _, info := range scaleUpStatus.ScaleUpInfos {
		scaledUp = append(scaledUp, info.Group.Id())
	}
	slices.Sort(scaledUp)
	assert.Equal(t, []string{"ng1", "ng2"}, scaledUp)
}

func TestLazySchedulablePodGroupsDisabledEvaluatesAllNodeGroups(t *testing.T) {
	env := newLazyTestEnv(t, []lazyTestNodeGroup{
		{id: "ng1", cpu: 1000, mem: 1000, size: 1, max: 10},
		{id: "ng2", cpu: 1000, mem: 1000, size: 1, max: 10},
		{id: "ng3", cpu: 1000, mem: 1000, size: 1, max: 10},
	}, func(o *config.AutoscalingOptions) {
		o.LazySchedulablePodGroupsEnabled = false
	}, 1)

	scaleUpStatus := env.scaleUp(t, utilstest.BuildTestPod("p1", 500, 0), utilstest.BuildTestPod("huge", 10000, 0))

	assert.True(t, scaleUpStatus.WasSuccessful())
	assert.Equal(t, "ng1", scaleUpStatus.ScaleUpInfos[0].Group.Id())
	// All node groups are evaluated for all pods before binpacking, each exactly once, even though
	// binpacking stopped after ng1. Nothing is left for reporting (which would evaluate only huge).
	assert.Equal(t, []string{"ng1", "ng2", "ng3"}, env.snapshot.evaluatedNodeGroups())
	for _, ng := range []string{"ng1", "ng2", "ng3"} {
		assert.Equal(t, 1, env.snapshot.forks[ng], ng)
		assert.Equal(t, 2, env.snapshot.checks[ng], ng)
	}
	assert.Equal(t, []string{"huge"}, remainingPodNames(scaleUpStatus.PodsRemainUnschedulable))
	assert.Len(t, scaleUpStatus.PodsRemainUnschedulable[0].RejectedNodeGroups, 3)
	assert.Empty(t, scaleUpStatus.PodsAwaitEvaluation)
}

func TestLazySchedulablePodGroupsFailedScaleUpEvaluatesUnevaluatedNodeGroups(t *testing.T) {
	for _, tc := range []struct {
		name                string
		wantRemaining       []string
		wantAwaitEvaluation []string
		wantSimulatedNg2    bool
	}{
		{
			name:             "no deadline",
			wantSimulatedNg2: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newLazyTestEnv(t, []lazyTestNodeGroup{
				{id: "ng1", cpu: 1000, mem: 1000, size: 1, max: 10},
				{id: "ng2", cpu: 1000, mem: 1000, size: 1, max: 10},
			}, nil, 1)
			env.failScaleUp["ng1"] = true

			scaleUpStatus, err := env.tryScaleUp(false, utilstest.BuildTestPod("p1", 500, 0))

			assert.Error(t, err)
			assert.Equal(t, status.ScaleUpError, scaleUpStatus.Result)
			// p1 also fits ng2, which binpacking didn't reach. As with eager evaluation, it is not
			// reported as unschedulable because scaling up ng1 failed.
			assert.Equal(t, tc.wantSimulatedNg2, env.snapshot.forks["ng2"] > 0)
			assert.Equal(t, tc.wantRemaining, remainingPodNames(scaleUpStatus.PodsRemainUnschedulable))
			assert.Equal(t, tc.wantAwaitEvaluation, podNames(scaleUpStatus.PodsAwaitEvaluation))
		})
	}
}

func TestLazySchedulablePodGroupsAllOptionsFilteredOutReportsAllNodeGroups(t *testing.T) {
	for _, tc := range []struct {
		name                string
		wantRemaining       []string
		wantAwaitEvaluation []string
	}{
		{
			name:          "no deadline",
			wantRemaining: []string{"p1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newLazyTestEnv(t, []lazyTestNodeGroup{
				{id: "ng1", cpu: 1000, mem: 1000, size: 1, max: 10},
				{id: "ng2", cpu: 1000, mem: 1000, size: 1, max: 10},
			}, nil, 1)
			env.expander.rejectAll = true

			scaleUpStatus := env.scaleUp(t, utilstest.BuildTestPod("p1", 500, 0))

			assert.Equal(t, status.ScaleUpNoOptionsAvailable, scaleUpStatus.Result)
			assert.Equal(t, tc.wantRemaining, remainingPodNames(scaleUpStatus.PodsRemainUnschedulable))
			assert.Equal(t, tc.wantAwaitEvaluation, podNames(scaleUpStatus.PodsAwaitEvaluation))
			if len(tc.wantRemaining) > 0 {
				// p1 was schedulable on ng1 before all options were filtered out. As with eager
				// evaluation, it is rejected by all node groups, including ng2 which binpacking
				// didn't reach.
				rejected := scaleUpStatus.PodsRemainUnschedulable[0].RejectedNodeGroups
				assert.Equal(t, ExpansionOptionsFilteredOutReason, rejected["ng1"])
				assert.Equal(t, ExpansionOptionsFilteredOutReason, rejected["ng2"])
			}
		})
	}
}

func TestLazySchedulablePodGroupsErrorAfterBestOptionReportsAllNodeGroups(t *testing.T) {
	for _, tc := range []struct {
		name                string
		wantRemaining       []string
		wantAwaitEvaluation []string
	}{
		{
			name:          "no deadline",
			wantRemaining: []string{"p1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newLazyTestEnv(t, []lazyTestNodeGroup{
				{id: "ng1", cpu: 1000, mem: 1000, size: 1, max: 10},
				{id: "ng2", cpu: 1000, mem: 1000, size: 1, max: 10},
			}, func(o *config.AutoscalingOptions) {
				// The cluster is full: capping the best option's node count fails.
				o.MaxNodesTotal = 2
			}, 1)

			scaleUpStatus, err := env.tryScaleUp(false, utilstest.BuildTestPod("p1", 500, 0))

			assert.Error(t, err)
			assert.Equal(t, status.ScaleUpError, scaleUpStatus.Result)
			assert.Equal(t, tc.wantRemaining, remainingPodNames(scaleUpStatus.PodsRemainUnschedulable))
			assert.Equal(t, tc.wantAwaitEvaluation, podNames(scaleUpStatus.PodsAwaitEvaluation))
			if len(tc.wantRemaining) > 0 {
				// As with eager evaluation, p1 is rejected by all node groups, including ng2 which
				// binpacking didn't reach.
				rejected := scaleUpStatus.PodsRemainUnschedulable[0].RejectedNodeGroups
				assert.Equal(t, ScaleUpExecutionErrorReason, rejected["ng1"])
				assert.Equal(t, ScaleUpExecutionErrorReason, rejected["ng2"])
			}
		})
	}
}

func TestSchedulablePodGroupsCache(t *testing.T) {
	env := newLazyTestEnv(t, []lazyTestNodeGroup{
		{id: "ng1", cpu: 1000, mem: 1000, size: 1, max: 10},
		{id: "ng2", cpu: 1000, mem: 1000, size: 1, max: 10},
		{id: "ng3", cpu: 1000, mem: 1000, size: 1, max: 10},
	}, nil, 0)
	ng1, ng2, ng3 := env.provider.GetNodeGroup("ng1"), env.provider.GetNodeGroup("ng2"), env.provider.GetNodeGroup("ng3")
	egs := equivalence.BuildPodGroups([]*apiv1.Pod{
		utilstest.BuildTestPod("fits", 500, 0),
		utilstest.BuildTestPod("too-big", 5000, 0),
	})
	ctx := context.Background()

	cache := newSchedulablePodGroupsCache(env.orchestrator, egs, env.nodeInfos, []cloudprovider.NodeGroup{ng1, ng2})
	assert.Equal(t, []cloudprovider.NodeGroup{ng1, ng2}, cache.unevaluated())

	// Computed on first request, served from the cache afterwards.
	podGroups, found := cache.Get(ctx, "ng1")
	assert.True(t, found)
	assert.Len(t, podGroups, 1)
	_, _ = cache.Get(ctx, "ng1")
	assert.Equal(t, 1, env.snapshot.forks["ng1"])
	assert.Equal(t, 2, env.snapshot.checks["ng1"])
	assert.Equal(t, 1, cache.evaluations)
	assert.Equal(t, []cloudprovider.NodeGroup{ng2}, cache.unevaluated())

	// Node groups which aren't eligible are unknown and never simulated.
	_, found = cache.Get(ctx, "ng3")
	assert.False(t, found)
	assert.Zero(t, env.snapshot.forks["ng3"])

	// Set makes a node group eligible without simulating it.
	cache.Set(ng3, podGroups)
	podGroups, found = cache.Get(ctx, "ng3")
	assert.True(t, found)
	assert.Len(t, podGroups, 1)
	assert.Zero(t, env.snapshot.forks["ng3"])

	cache.Delete("ng2")
	_, found = cache.Get(ctx, "ng2")
	assert.False(t, found)
	assert.Empty(t, cache.unevaluated())
	assert.Zero(t, env.snapshot.forks["ng2"])
}

// BenchmarkLazySchedulablePodGroups measures scale-ups with many node groups and pod equivalence
// groups, counting node group simulations (snapshot forks for schedulable pod groups):
//   - binpacking stopping after the first option, when all pods fit it (best case for lazy evaluation),
//   - binpacking evaluating all node groups (no limiter stop, the cost of eager evaluation),
//   - binpacking stopping after the first option, when almost all pods fit no node group (worst
//     case for lazy evaluation: reporting has to simulate the remaining node groups for the
//     unschedulable pods),
//   - binpacking stopping after the first option, when all node groups are similar to it (worst
//     case for balancing: similar node groups are simulated in the first binpacking iteration).
func BenchmarkLazySchedulablePodGroups(b *testing.B) {
	const (
		nodeGroupCount = 50
		podGroupCount  = 100
	)
	for _, bc := range []struct {
		name             string
		stopAfterOptions int
		unschedulable    bool
		balanceSimilar   bool
		lazyDisabled     bool
	}{
		{name: "lazy computation disabled", stopAfterOptions: 1, lazyDisabled: true},
		{name: "stop after first option, all pods fit", stopAfterOptions: 1},
		{name: "evaluate all node groups", stopAfterOptions: nodeGroupCount + 1},
		{name: "stop after first option, 99% pods fit nowhere", stopAfterOptions: 1, unschedulable: true},
		// All node groups are identical, so they are all similar to the first one and simulated
		// in the first binpacking iteration.
		{name: "stop after first option, all node groups similar", stopAfterOptions: 1, balanceSimilar: true},
	} {
		b.Run(bc.name, func(b *testing.B) {
			var nodeGroups []lazyTestNodeGroup
			for i := 0; i < nodeGroupCount; i++ {
				nodeGroups = append(nodeGroups, lazyTestNodeGroup{id: fmt.Sprintf("ng%03d", i), cpu: 4000, mem: 16000, size: 1, max: 1000})
			}
			var forks int
			for b.Loop() {
				b.StopTimer()
				env := newLazyTestEnv(b, nodeGroups, func(o *config.AutoscalingOptions) {
					o.BalanceSimilarNodeGroups = bc.balanceSimilar
					o.LazySchedulablePodGroupsEnabled = !bc.lazyDisabled
				}, bc.stopAfterOptions)
				pods := make([]*apiv1.Pod, 0, podGroupCount)
				for i := 0; i < podGroupCount; i++ {
					cpu := int64(100 + i)
					if bc.unschedulable && i > 0 {
						cpu = int64(100000 + i)
					}
					pods = append(pods, utilstest.BuildTestPod(fmt.Sprintf("p%d", i), cpu, 0))
				}
				b.StartTimer()
				env.scaleUp(b, pods...)
				forks = 0
				for _, count := range env.snapshot.forks {
					forks += count
				}
			}
			b.ReportMetric(float64(forks), "node-group-simulations/op")
		})
	}
}
