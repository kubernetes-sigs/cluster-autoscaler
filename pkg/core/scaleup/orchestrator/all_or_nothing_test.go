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
	goerrors "errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	. "sigs.k8s.io/cluster-autoscaler/pkg/core/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupconfig"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroups"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupset"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	processorstest "sigs.k8s.io/cluster-autoscaler/pkg/processors/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestAllOrNothingAbortStatus checks how an all-or-nothing scale-up that can't accommodate every
// pod is reported: callers such as best-effort-atomic ProvisioningRequests use the rejection
// reason to retry fewer pods, and must know about any node group created along the way.
func TestAllOrNothingAbortStatus(t *testing.T) {
	t.Run("estimate capped by node group capacity", func(t *testing.T) {
		node := BuildTestNode("ng-node", 1000, 1000)
		SetNodeReadyState(node, true, time.Now().Add(-time.Minute))
		var resizes []int
		provider := testprovider.NewTestCloudProviderBuilder().WithOnScaleUp(func(_ string, delta int) error {
			resizes = append(resizes, delta)
			return nil
		}).Build()
		provider.AddNodeGroup("ng", 0, 3, 1)
		provider.AddNode("ng", node)
		// Production caps every estimate at the node group's remaining capacity, so the expansion
		// option only covers some of the pods.
		limiter := estimator.NewThresholdBasedEstimationLimiter([]estimator.Threshold{estimator.NewSngCapacityThreshold()})

		st, err := allOrNothingScaleUp(t, provider, []*apiv1.Node{node}, testPods(5, 1000), limiter, nil)
		require.NoError(t, err)
		assert.Equal(t, status.ScaleUpNoOptionsAvailable, st.Result)
		assert.Empty(t, resizes)
		require.Len(t, st.PodsRemainUnschedulable, 5)
		for _, info := range st.PodsRemainUnschedulable {
			assert.Equal(t, AllOrNothingReason, info.RejectedNodeGroups["ng"], "pod %s", info.Pod.Name)
		}
	})

	t.Run("node group created before the abort", func(t *testing.T) {
		template := BuildTestNode("t1", 4000, 1000000)
		SetNodeReadyState(template, true, time.Time{})
		var resizes []int
		provider := testprovider.NewTestCloudProviderBuilder().WithOnScaleUp(func(_ string, delta int) error {
			resizes = append(resizes, delta)
			return nil
		}).WithOnNodeGroupCreate(func(string) error {
			return nil
		}).WithMachineTypes([]string{"T1"}).WithMachineTemplates(map[string]*framework.NodeInfo{"T1": framework.NewTestNodeInfo(template)}).Build()

		st, err := allOrNothingScaleUp(t, provider, nil, testPods(3, 3000), estimator.NewThresholdBasedEstimationLimiter(nil), func(p *processors.AutoscalingProcessors) {
			p.NodeGroupListProcessor = &MockAutoprovisioningNodeGroupListProcessor{T: t}
			p.NodeGroupManager = &cappedNodeGroupManager{MockAutoprovisioningNodeGroupManager: &MockAutoprovisioningNodeGroupManager{T: t}, provider: provider, maxSize: 2}
		})
		require.NoError(t, err)
		assert.Equal(t, status.ScaleUpNoOptionsAvailable, st.Result)
		assert.Empty(t, resizes)
		require.Len(t, st.CreateNodeGroupResults, 1, "the created node group must be reported")
		assert.Equal(t, "autoprovisioned-T1", st.CreateNodeGroupResults[0].MainCreatedNodeGroup.Id())
		for _, info := range st.PodsRemainUnschedulable {
			assert.Equal(t, AllOrNothingReason, info.RejectedNodeGroups["autoprovisioned-T1"], "pod %s", info.Pod.Name)
		}
	})
}

func allOrNothingScaleUp(t *testing.T, provider *testprovider.TestCloudProvider, nodes []*apiv1.Node, pods []*apiv1.Pod, limiter estimator.EstimationLimiter, configure func(*processors.AutoscalingProcessors)) (*status.ScaleUpStatus, errors.AutoscalerError) {
	ctx := context.Background()
	options := config.AutoscalingOptions{
		EstimatorName:                  estimator.BinpackingEstimatorName,
		MaxCoresTotal:                  5000 * 64,
		MaxMemoryTotal:                 5000 * 64 * 20,
		MaxNodeGroupBinpackingDuration: time.Second,
	}
	listers := kube_util.NewListerRegistry(nil, nil, kube_util.NewTestPodLister(nil), nil, nil, nil, nil, nil, nil)
	scaleUpProcessors, templateNodeInfoRegistry := processorstest.NewTestProcessors(options)
	if configure != nil {
		configure(scaleUpProcessors)
	}
	autoscalingCtx, err := NewScaleTestAutoscalingContext(options, &fake.Clientset{}, listers, provider, nil, nil, templateNodeInfoRegistry)
	require.NoError(t, err)
	require.NoError(t, autoscalingCtx.ClusterSnapshot.SetClusterState(ctx, nodes, nil, nil, nil))
	require.NoError(t, autoscalingCtx.TemplateNodeInfoRegistry.Recompute(ctx, &autoscalingCtx, nodes, []*appsv1.DaemonSet{}, taints.TaintConfig{}, time.Now()))
	nodeInfos := autoscalingCtx.TemplateNodeInfoRegistry.GetNodeInfos()
	clusterState := clusterstate.NewClusterStateRegistry(provider, autoscalingCtx.LogRecorder, NewBackoff(), nodegroupconfig.NewDefaultNodeGroupConfigProcessor(options.NodeGroupDefaults), autoscalingCtx.TemplateNodeInfoRegistry)
	clusterState.UpdateNodes(ctx, nodes, time.Now())
	estimatorBuilder, err := estimator.NewEstimatorBuilder(estimator.BinpackingEstimatorName, limiter, estimator.NewDecreasingPodOrderer(), nil, false)
	require.NoError(t, err)
	trackerFactory := resourcequotas.NewTrackerFactory(resourcequotas.TrackerOptions{
		QuotaProvider:            resourcequotas.NewCloudQuotasProvider(provider),
		CustomResourcesProcessor: scaleUpProcessors.CustomResourcesProcessor,
	})
	orchestrator := New()
	orchestrator.Initialize(&autoscalingCtx, scaleUpProcessors, clusterState, estimatorBuilder, taints.TaintConfig{}, trackerFactory)
	return orchestrator.ScaleUp(ctx, pods, nodes, []*appsv1.DaemonSet{}, nodeInfos, true)
}

func testPods(count int, milliCPU int64) []*apiv1.Pod {
	var pods []*apiv1.Pod
	for i := 0; i < count; i++ {
		pods = append(pods, BuildTestPod(fmt.Sprintf("p%d", i), milliCPU, 100))
	}
	return pods
}

// cappedNodeGroupManager creates node groups with a smaller maximum size than the candidates
// they're created from, so that an all-or-nothing scale-up is aborted after the creation.
type cappedNodeGroupManager struct {
	*MockAutoprovisioningNodeGroupManager
	provider *testprovider.TestCloudProvider
	maxSize  int
}

func (m *cappedNodeGroupManager) CreateNodeGroup(_ *ca_context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup) (nodegroups.CreateNodeGroupResult, errors.AutoscalerError) {
	created := m.provider.BuildNodeGroup(nodeGroup.Id(), 0, m.maxSize, 0, true, true, "T1", nil)
	m.provider.InsertNodeGroup(created)
	return nodegroups.CreateNodeGroupResult{MainCreatedNodeGroup: created}, nil
}

// TestAtomicScaleUpRejectedByIncreaseSize checks that when a cloud provider doesn't implement
// AtomicIncreaseSize, a rejection reported by IncreaseSize, which is called instead, still shows
// that the node group rejected the increase without changing.
func TestAtomicScaleUpRejectedByIncreaseSize(t *testing.T) {
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup("ng", 0, 10, 1)
	group := &nonAtomicNodeGroup{NodeGroup: provider.GetNodeGroup("ng")}
	options := config.AutoscalingOptions{}
	scaleUpProcessors, templateNodeInfoRegistry := processorstest.NewTestProcessors(options)
	listers := kube_util.NewListerRegistry(nil, nil, kube_util.NewTestPodLister(nil), nil, nil, nil, nil, nil, nil)
	autoscalingCtx, err := NewScaleTestAutoscalingContext(options, &fake.Clientset{}, listers, provider, nil, nil, templateNodeInfoRegistry)
	require.NoError(t, err)
	executor := newScaleUpExecutor(&autoscalingCtx, scaleUpProcessors.ScaleStateNotifier, scaleUpProcessors.AsyncNodeGroupStateChecker)

	aErr, failedGroups, failedErrors := executor.ExecuteScaleUps(context.Background(),
		[]nodegroupset.ScaleUpInfo{{Group: group, CurrentSize: 1, NewSize: 4, MaxSize: 10}}, time.Now(), true)
	require.Error(t, aErr)
	assert.Equal(t, []int{3}, group.increases)
	require.Len(t, failedGroups, 1)
	assert.True(t, goerrors.Is(failedErrors[group.Id()], cloudprovider.ErrAtomicIncreaseRejected))
}

// nonAtomicNodeGroup is a node group that doesn't implement AtomicIncreaseSize, and whose
// IncreaseSize rejects every increase without changing the node group.
type nonAtomicNodeGroup struct {
	cloudprovider.NodeGroup
	increases []int
}

func (g *nonAtomicNodeGroup) AtomicIncreaseSize(context.Context, int) error {
	return cloudprovider.ErrNotImplemented
}

func (g *nonAtomicNodeGroup) IncreaseSize(_ context.Context, delta int) error {
	g.increases = append(g.increases, delta)
	return fmt.Errorf("%w: no capacity for %d nodes", cloudprovider.ErrAtomicIncreaseRejected, delta)
}
