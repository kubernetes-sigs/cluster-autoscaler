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
	goerrors "errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestBatchRetryBudgetPreservesProgress checks that later iterations continue an exhausted search
// where it stopped, instead of restarting at the configured maximum, and that the remembered batch
// size follows the capacity that becomes available. A batch that needed a scale-up only lets the
// remembered size grow once its nodes have arrived. The test brings them up after every iteration,
// and batches are settled before the next one is picked, so the growth shows in the next batch.
func TestBatchRetryBudgetPreservesProgress(t *testing.T) {
	for _, tc := range []struct {
		name             string
		capacityRecovers bool
		wantBatches      []int
		wantResizes      []int
	}{
		{
			// A request whose node arrives lets the remembered size grow to two; two being rejected
			// before one succeeds brings it back to the size that worked.
			name:        "capacity stays at one node per resize",
			wantBatches: []int{1, 2, 1, 2, 1, 2, 1, 2, 1, 1},
			wantResizes: []int{1, 2, 1, 1, 2, 1, 1, 2, 1, 1, 2, 1, 1, 1},
		},
		{
			name:             "capacity recovers",
			capacityRecovers: true,
			wantBatches:      []int{1, 2, 4, 3},
			wantResizes:      []int{1, 2, 4, 3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			now := time.Now()
			clock := clocktesting.NewFakePassiveClock(now)
			node := BuildTestNode("pool-node", 100, 10)
			SetNodeReadyState(node, true, now.Add(-time.Minute))
			occupied := BuildTestPod("occupied", 100, 1)
			occupied.Spec.NodeName = node.Name
			requests, requestPods := adaptiveRequests(t, []int32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
			client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, requests...)
			var environment *batchTestEnvironment
			var resizes []int
			scarce := true
			environment = setupTestEnvironment(t, client, []*apiv1.Node{node}, func(_ string, delta int) error {
				resizes = append(resizes, delta)
				if scarce && delta > 1 {
					ng := environment.provider.GetNodeGroup("pool").(*testprovider.TestNodeGroup)
					size, _ := ng.TargetSize(ctx)
					ng.SetTargetSize(size - delta)
					return fmt.Errorf("%w: only one node available", cloudprovider.ErrAtomicIncreaseRejected)
				}
				return nil
			}, false, batchTestOptions{
				bestEffortAtomic: true, maxBatchSize: 10, scheduledPods: []*apiv1.Pod{occupied},
				backoff:    &clockedBackoff{state: backoff.NewIdBasedExponentialBackoff(5*time.Minute, 30*time.Minute, 3*time.Hour), clock: clock},
				nodeGroups: []batchTestNodeGroup{{name: "pool", maxSize: 100, nodes: []*apiv1.Node{node}}},
			})
			cycles := &capacityCycles{t: t, environment: environment, client: client, requests: requests, requestPods: requestPods,
				clock: clock, nodes: []*apiv1.Node{node}, scheduled: []*apiv1.Pod{occupied}, admitted: map[string]bool{}}

			st, err := cycles.run()
			require.Error(t, err)
			assert.Equal(t, 10, cycles.lastBatch)
			assert.Equal(t, []int{10, 5, 3, 2}, resizes)
			assert.Len(t, st.PodsAwaitEvaluation, 8)
			assertRequestStates(t, environment.podsInjector, client, requests, nil, []int{0, 1})

			st, err = cycles.run()
			require.NoError(t, err)
			assert.Equal(t, 1, cycles.lastBatch)
			assert.Equal(t, status.ScaleUpNoOptionsAvailable, st.Result)
			assert.Equal(t, []int{10, 5, 3, 2}, resizes, "a continuation must respect group backoff")
			assertRequestStates(t, environment.podsInjector, client, requests, nil, []int{0, 1, 2})

			// Simulate the next eligible loop after the group and request backoffs expire.
			updated, err := client.ProvisioningRequestsNoCache()
			require.NoError(t, err)
			for _, request := range updated {
				if condition := apimeta.FindStatusCondition(request.Status.Conditions, v1.Provisioned); condition != nil {
					condition.LastTransitionTime = metav1.NewTime(now.Add(-time.Hour))
					_, err := client.UpdateProvisioningRequest(ctx, request.ProvisioningRequest)
					require.NoError(t, err)
				}
			}
			clock.SetTime(now.Add(6 * time.Minute))
			require.NoError(t, environment.clusterState.UpdateNodes(ctx, cycles.nodes, clock.Now()))
			scarce = !tc.capacityRecovers

			resizes = nil
			var batches []int
			for len(cycles.admitted) < len(requests) && len(batches) < 2*len(requests) {
				st, err := cycles.run()
				require.NoError(t, err)
				assert.Equal(t, status.ScaleUpSuccessful, st.Result)
				batches = append(batches, cycles.lastBatch)
			}
			assert.Len(t, cycles.admitted, len(requests))
			assert.Equal(t, tc.wantBatches, batches)
			assert.Equal(t, tc.wantResizes, resizes)
		})
	}
}

func TestBatchLimitWithExistingCapacity(t *testing.T) {
	for _, tc := range []struct {
		name            string
		freeNodes       int
		maxNodes        int
		initialLimit    int
		wantProvisioned []int
		wantNextBatch   int
	}{
		{name: "grow after immediate admission", freeNodes: 10, maxNodes: 20, initialLimit: 1, wantProvisioned: []int{0}, wantNextBatch: 2},
		{name: "remember a shrunken batch", freeNodes: 4, maxNodes: 5, wantProvisioned: []int{0, 1, 2, 3}, wantNextBatch: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			var nodes []*apiv1.Node
			for i := 0; i < tc.freeNodes; i++ {
				node := BuildTestNode(fmt.Sprintf("pool-node-%d", i), 100, 10)
				SetNodeReadyState(node, true, time.Now().Add(-time.Minute))
				nodes = append(nodes, node)
			}
			requests, _ := adaptiveRequests(t, []int32{1, 1, 1, 1, 1, 1, 1, 1})
			client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, requests...)
			cloudCalls := 0
			environment := setupTestEnvironment(t, client, nodes, func(_ string, _ int) error {
				cloudCalls++
				return nil
			}, false, batchTestOptions{
				bestEffortAtomic: true, maxBatchSize: 8,
				nodeGroups: []batchTestNodeGroup{{name: "pool", maxSize: tc.maxNodes, nodes: nodes}},
			})
			if tc.initialLimit > 0 {
				environment.podsInjector.RememberBestEffortAtomicBatchLimit(ctx, requests[0], tc.initialLimit)
			}
			injected, err := environment.podsInjector.Process(ctx, nil, nil)
			require.NoError(t, err)
			if tc.initialLimit > 0 {
				require.Len(t, injected, tc.initialLimit)
			} else {
				require.Len(t, injected, len(requests))
			}
			st, scaleErr := environment.orchestrator.ScaleUp(ctx, injected, nodes, nil, environment.nodeInfos, false)
			require.NoError(t, scaleErr)
			require.Equal(t, status.ScaleUpNotNeeded, st.Result)
			require.Zero(t, cloudCalls, "admit using existing capacity without resizing")
			assertRequestStates(t, environment.podsInjector, client, requests, tc.wantProvisioned, nil)
			assert.Len(t, st.PodsAwaitEvaluation, len(injected)-len(tc.wantProvisioned))

			// Keep enough compatible requests queued to expose a missing remembered limit.
			for i := 0; i < 8; i++ {
				request := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
					Name: fmt.Sprintf("later-%d", i), CPU: "100m", Memory: "1", PodCount: 1,
					Class: v1.ProvisioningClassBestEffortAtomicScaleUp,
				})
				request.Spec.PodSets[0].PodTemplateRef = requests[0].Spec.PodSets[0].PodTemplateRef
				request.PodTemplates = requests[0].PodTemplates
				require.NoError(t, client.CreateProvisioningRequestForTesting(ctx, request))
			}
			waitForCache(t, client)
			batch, err := environment.podsInjector.GetBestEffortAtomicBatch(ctx, 8)
			require.NoError(t, err)
			assert.Equal(t, tc.wantNextBatch, len(batch), "existing-capacity admission must update the next batch size")
		})
	}
}

// capacityCycles runs autoscaler iterations. After each one it brings up the capacity requested for
// newly admitted single-pod requests: a ready node per request, with the request's pod on it.
type capacityCycles struct {
	t           *testing.T
	environment *batchTestEnvironment
	client      *provreqclient.ProvisioningRequestClient
	requests    []*provreqwrapper.ProvisioningRequest
	requestPods []*apiv1.Pod
	clock       *clocktesting.FakePassiveClock
	nodes       []*apiv1.Node
	scheduled   []*apiv1.Pod
	admitted    map[string]bool
	lastBatch   int
}

func (c *capacityCycles) run() (*status.ScaleUpStatus, error) {
	ctx := c.t.Context()
	waitForCache(c.t, c.client)
	injected, err := c.environment.podsInjector.Process(ctx, nil, nil)
	require.NoError(c.t, err)
	c.lastBatch = len(injected)
	st, scaleErr := c.environment.orchestrator.ScaleUp(ctx, injected, c.nodes, nil, c.environment.nodeInfos, false)
	for i, request := range c.requests {
		updated, err := c.client.ProvisioningRequestNoCache(request.Namespace, request.Name)
		require.NoError(c.t, err)
		if c.admitted[request.Name] || !apimeta.IsStatusConditionTrue(updated.Status.Conditions, v1.Provisioned) {
			continue
		}
		c.admitted[request.Name] = true
		node := BuildTestNode(fmt.Sprintf("pool-new-%d", i), 100, 10)
		SetNodeReadyState(node, true, c.clock.Now())
		c.environment.provider.AddNode("pool", node)
		c.nodes = append(c.nodes, node)
		pod := c.requestPods[i].DeepCopy()
		pod.Spec.NodeName = node.Name
		c.scheduled = append(c.scheduled, pod)
	}
	clustersnapshot.InitializeClusterSnapshotOrDie(c.t, c.environment.clusterSnapshot, c.nodes, c.scheduled)
	require.NoError(c.t, c.environment.clusterState.UpdateNodes(ctx, c.nodes, c.clock.Now()))
	return st, scaleErr
}

func TestParallelBatchResizeRejections(t *testing.T) {
	for _, tc := range []struct {
		name            string
		unknownGroup    string
		successfulGroup string
		wantCalls       map[string][]int
		wantProvisioned int
	}{
		{name: "all clean rejections", wantCalls: map[string][]int{"pool-a": {4, 2, 1}, "pool-b": {4, 2, 1}}, wantProvisioned: 2},
		{name: "clean then unknown", unknownGroup: "pool-b", wantCalls: map[string][]int{"pool-a": {4}, "pool-b": {4}}},
		{name: "unknown then clean", unknownGroup: "pool-a", wantCalls: map[string][]int{"pool-a": {4}, "pool-b": {4}}},
		{name: "success and clean rejection", successfulGroup: "pool-a", wantCalls: map[string][]int{"pool-a": {4}, "pool-b": {4}}, wantProvisioned: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			nodes := []*apiv1.Node{BuildTestNode("pool-a-node", 100, 10), BuildTestNode("pool-b-node", 100, 10)}
			var occupied []*apiv1.Pod
			for i, node := range nodes {
				SetNodeReadyState(node, true, time.Now().Add(-time.Minute))
				pod := BuildTestPod(fmt.Sprintf("occupied-%d", i), 100, 1)
				pod.Spec.NodeName = node.Name
				occupied = append(occupied, pod)
			}
			requests, injected := adaptiveRequests(t, []int32{1, 1, 1, 1, 1, 1, 1, 1})
			client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, requests...)
			var environment *batchTestEnvironment
			var mu sync.Mutex
			calls := map[string][]int{}
			environment = setupTestEnvironment(t, client, nodes, func(group string, delta int) error {
				mu.Lock()
				defer mu.Unlock()
				calls[group] = append(calls[group], delta)
				if group == tc.unknownGroup {
					// Leave the fake target changed: the caller cannot assume rejection.
					return fmt.Errorf("group %s resize timed out", group)
				}
				if delta > 1 && group != tc.successfulGroup {
					ng := environment.provider.GetNodeGroup(group).(*testprovider.TestNodeGroup)
					size, _ := ng.TargetSize(ctx)
					ng.SetTargetSize(size - delta)
					return fmt.Errorf("%w: group %s rejected %d nodes", cloudprovider.ErrAtomicIncreaseRejected, group, delta)
				}
				return nil
			}, false, batchTestOptions{
				bestEffortAtomic: true, maxBatchSize: 8, scheduledPods: occupied,
				balanceNodeGroups: true, parallelScaleUp: true,
				backoff: backoff.NewIdBasedExponentialBackoff(time.Minute, 10*time.Minute, time.Hour),
				nodeGroups: []batchTestNodeGroup{
					{name: "pool-a", maxSize: 20, nodes: nodes[:1]},
					{name: "pool-b", maxSize: 20, nodes: nodes[1:]},
				},
			})
			st, scaleErr := environment.orchestrator.ScaleUp(ctx, injected, nodes, nil, environment.nodeInfos, false)
			assert.Equal(t, tc.wantCalls, calls)
			if tc.unknownGroup == "" && tc.successfulGroup == "" {
				require.NoError(t, scaleErr)
				assert.Equal(t, status.ScaleUpSuccessful, st.Result)
				for _, group := range environment.provider.NodeGroups(ctx) {
					assert.False(t, environment.clusterState.BackoffStatusForNodeGroup(ctx, group, time.Now()).IsBackedOff)
				}
			} else {
				require.Error(t, scaleErr)
				assert.Equal(t, status.ScaleUpError, st.Result)
				assert.Len(t, st.FailedResizeErrors, len(st.FailedResizeNodeGroups))
				for _, group := range st.FailedResizeNodeGroups {
					err := st.FailedResizeErrors[group.Id()]
					require.NotNil(t, err)
					assert.Equal(t, group.Id() != tc.unknownGroup, goerrors.Is(err, cloudprovider.ErrAtomicIncreaseRejected))
					assert.True(t, environment.clusterState.BackoffStatusForNodeGroup(ctx, group, time.Now()).IsBackedOff)
				}
			}
			updated, err := client.ProvisioningRequestsNoCache()
			require.NoError(t, err)
			assert.Equal(t, tc.wantProvisioned, NumProvisioningRequestsWithCondition(updated, v1.Provisioned, metav1.ConditionTrue))
		})
	}
}
