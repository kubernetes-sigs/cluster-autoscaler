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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/provreq"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/conditions"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestAdaptiveBatchPlanning(t *testing.T) {
	for _, tc := range []struct {
		name                string
		counts              []int32
		freeNode            bool
		productionEstimator bool
		wantProvisioned     []int
		wantNotFound        []int
		wantResize          []int
		wantDeferredPods    int
	}{
		{name: "halve until a subset fits", counts: []int32{1, 1, 1, 1, 1, 1, 1, 1}, wantProvisioned: []int{0, 1}, wantResize: []int{2}, wantDeferredPods: 6},
		{name: "halve until a subset fits the capped estimate", counts: []int32{1, 1, 1, 1, 1, 1, 1, 1}, productionEstimator: true, wantProvisioned: []int{0, 1}, wantResize: []int{2}, wantDeferredPods: 6},
		{name: "odd batch of indivisible workloads", counts: []int32{2, 2, 2, 2, 2}, wantProvisioned: []int{0}, wantResize: []int{2}, wantDeferredPods: 8},
		{name: "never split a single workload", counts: []int32{3}, wantNotFound: []int{0}},
		{name: "release tentative existing-node placements", counts: []int32{1, 1, 1, 1, 1, 1, 1, 1}, freeNode: true, wantProvisioned: []int{0, 1}, wantResize: []int{1}, wantDeferredPods: 6},
		{name: "skip an oldest request that can't fit alone", counts: []int32{5, 1, 1, 1, 1, 1, 1, 1}, wantProvisioned: []int{1, 2}, wantNotFound: []int{0}, wantResize: []int{2}, wantDeferredPods: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			node := BuildTestNode("pool-node", 100, 10)
			SetNodeReadyState(node, true, time.Now().Add(-time.Minute))
			var occupied []*apiv1.Pod
			if !tc.freeNode {
				pod := BuildTestPod("occupied", 100, 1)
				pod.Spec.NodeName = node.Name
				occupied = append(occupied, pod)
			}
			requests, injected := adaptiveRequests(t, tc.counts)
			client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, requests...)
			var thresholds []estimator.Threshold
			if tc.productionEstimator {
				// Like production, cap estimates at the node group's remaining capacity.
				thresholds = []estimator.Threshold{estimator.NewSngCapacityThreshold(), estimator.NewClusterCapacityThreshold()}
			}
			var resizes []int
			environment := setupTestEnvironment(t, client, []*apiv1.Node{node}, func(_ string, delta int) error {
				resizes = append(resizes, delta)
				return nil
			}, false, batchTestOptions{
				bestEffortAtomic: true, maxBatchSize: max(2, len(requests)), scheduledPods: occupied, estimatorThresholds: thresholds,
				nodeGroups: []batchTestNodeGroup{{name: "pool", maxSize: 3, nodes: []*apiv1.Node{node}}},
			})
			st, scaleErr := environment.orchestrator.ScaleUp(ctx, injected, []*apiv1.Node{node}, nil, environment.nodeInfos, false)
			require.NoError(t, scaleErr)
			assert.Equal(t, tc.wantResize, resizes)
			assert.Len(t, st.PodsAwaitEvaluation, tc.wantDeferredPods)
			if len(tc.wantProvisioned) > 0 {
				assert.Equal(t, status.ScaleUpSuccessful, st.Result)
			} else {
				assert.Equal(t, status.ScaleUpNoOptionsAvailable, st.Result)
			}
			assertRequestStates(t, environment.podsInjector, client, requests, tc.wantProvisioned, tc.wantNotFound)
			nodeInfos, err := environment.clusterSnapshot.ListNodeInfos()
			require.NoError(t, err)
			require.Len(t, nodeInfos, 1)
			assert.Len(t, nodeInfos[0].Pods(), len(occupied), "attempts must not leak snapshot bookings")
		})
	}
}

// TestAdaptiveBatchResizeRejection covers a single node group whose cloud provider rejects a
// resize for the full batch, but would accept a smaller one.
func TestAdaptiveBatchResizeRejection(t *testing.T) {
	ctx := t.Context()
	environment, client, requests, resizes, available := setupResizeTest(t, true, false)
	group := environment.provider.GetNodeGroup("pool")
	run := func() (*status.ScaleUpStatus, errors.AutoscalerError) {
		waitForCache(t, client)
		injected, err := environment.podsInjector.Process(ctx, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, injected)
		return environment.orchestrator.ScaleUp(ctx, injected, environment.nodes, nil, environment.nodeInfos, false)
	}

	*available = 2
	st, err := run()
	require.NoError(t, err)
	assert.Equal(t, status.ScaleUpSuccessful, st.Result)
	assert.Equal(t, []int{8, 4, 2}, *resizes, "rejected resizes are retried at half size in the same iteration")
	assertRequestStates(t, environment.podsInjector, client, requests, []int{0, 1}, nil)
	assert.False(t, environment.clusterState.BackoffStatusForNodeGroup(ctx, group, environment.clock.Now()).IsBackedOff,
		"a node group that accepted the smaller resize must not be backed off")

	*resizes = nil
	st, err = run()
	require.Error(t, err)
	assert.Equal(t, status.ScaleUpError, st.Result)
	// The next search starts at the size that worked, and stops once the oldest request is rejected
	// on its own: the requests behind it need as many nodes, so they're left for later iterations.
	assert.Equal(t, []int{2, 1}, *resizes, "deferred requests are retried straight away from the size that worked")
	assertRequestStates(t, environment.podsInjector, client, requests, []int{0, 1}, []int{2})
	assert.True(t, environment.clusterState.BackoffStatusForNodeGroup(ctx, group, environment.clock.Now()).IsBackedOff,
		"the node group is backed off once no smaller resize succeeds")

	*resizes = nil
	st, err = run()
	require.NoError(t, err)
	assert.Equal(t, status.ScaleUpNoOptionsAvailable, st.Result)
	assert.Empty(t, *resizes, "backoff blocks further cloud calls")
	assertRequestStates(t, environment.podsInjector, client, requests, []int{0, 1}, []int{2, 3})
	assert.Len(t, st.PodsRemainUnschedulable, 1, "the remembered singleton limit also applies while the group is backed off")
}

// TestAdaptiveBatchResizeWithUnknownOutcome checks that a resize failure the cloud provider doesn't
// report as a clean rejection is never followed by a smaller resize in the same iteration, which
// could request capacity twice.
func TestAdaptiveBatchResizeWithUnknownOutcome(t *testing.T) {
	ctx := t.Context()
	environment, client, requests, resizes, available := setupResizeTest(t, false, false)
	*available = 2
	injected, err := environment.podsInjector.Process(ctx, nil, nil)
	require.NoError(t, err)
	st, scaleErr := environment.orchestrator.ScaleUp(ctx, injected, environment.nodes, nil, environment.nodeInfos, false)
	require.Error(t, scaleErr)
	assert.Equal(t, status.ScaleUpError, st.Result)
	assert.Equal(t, []int{8}, *resizes)
	assertRequestStates(t, environment.podsInjector, client, requests, nil, []int{0, 1, 2, 3, 4, 5, 6, 7})
	assert.True(t, environment.clusterState.BackoffStatusForNodeGroup(ctx, environment.provider.GetNodeGroup("pool"), environment.clock.Now()).IsBackedOff)
}

// TestAdaptiveBatchUnknownOutcomeShrinksLaterIterations covers a cloud provider that can't report
// clean rejections: the resize for the whole batch fails although the node group is left unchanged
// and a smaller resize would succeed. The next eligible iteration requests half of the batch.
func TestAdaptiveBatchUnknownOutcomeShrinksLaterIterations(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	clock := clocktesting.NewFakePassiveClock(now)
	node := BuildTestNode("pool-node", 100, 10)
	SetNodeReadyState(node, true, now.Add(-time.Minute))
	occupied := BuildTestPod("occupied", 100, 1)
	occupied.Spec.NodeName = node.Name
	nodes := []*apiv1.Node{node}
	requests, _ := adaptiveRequests(t, []int32{1, 1, 1, 1, 1, 1, 1, 1})
	client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, requests...)
	var environment *batchTestEnvironment
	var resizes []int
	environment = setupTestEnvironment(t, client, nodes, func(_ string, delta int) error {
		resizes = append(resizes, delta)
		if delta > 4 {
			// The fake provider raises its target before calling back. The failed resize leaves the
			// node group unchanged, but the provider can't tell, so the error doesn't say so.
			group := environment.provider.GetNodeGroup("pool").(*testprovider.TestNodeGroup)
			size, _ := group.TargetSize(ctx)
			group.SetTargetSize(size - delta)
			return fmt.Errorf("allocation failed for %d nodes", delta)
		}
		return nil
	}, false, batchTestOptions{
		bestEffortAtomic: true, maxBatchSize: 8, scheduledPods: []*apiv1.Pod{occupied},
		backoff:    &clockedBackoff{state: backoff.NewIdBasedExponentialBackoff(5*time.Minute, 30*time.Minute, 3*time.Hour), clock: clock},
		nodeGroups: []batchTestNodeGroup{{name: "pool", maxSize: 20, nodes: nodes}},
	})
	group := environment.provider.GetNodeGroup("pool")
	run := func() (*status.ScaleUpStatus, errors.AutoscalerError) {
		waitForCache(t, client)
		injected, err := environment.podsInjector.Process(ctx, nil, nil)
		require.NoError(t, err)
		return environment.orchestrator.ScaleUp(ctx, injected, nodes, nil, environment.nodeInfos, false)
	}

	st, err := run()
	require.Error(t, err)
	assert.Equal(t, status.ScaleUpError, st.Result)
	assert.Equal(t, []int{8}, resizes, "a resize with an unknown outcome is never retried in the same iteration")
	assertRequestStates(t, environment.podsInjector, client, requests, nil, []int{0, 1, 2, 3, 4, 5, 6, 7})
	assert.True(t, environment.clusterState.BackoffStatusForNodeGroup(ctx, group, clock.Now()).IsBackedOff)

	// Let the requests' retry delays and the node group's backoff expire.
	stored, listErr := client.ProvisioningRequestsNoCache()
	require.NoError(t, listErr)
	for _, request := range stored {
		apimeta.FindStatusCondition(request.Status.Conditions, v1.Provisioned).LastTransitionTime = metav1.NewTime(now.Add(-time.Hour))
		_, updateErr := client.UpdateProvisioningRequest(ctx, request.ProvisioningRequest)
		require.NoError(t, updateErr)
	}
	clock.SetTime(now.Add(6 * time.Minute))
	require.NoError(t, environment.clusterState.UpdateNodes(ctx, nodes, clock.Now()))

	resizes = nil
	st, err = run()
	require.NoError(t, err)
	assert.Equal(t, status.ScaleUpSuccessful, st.Result)
	assert.Equal(t, []int{4}, resizes, "the next eligible iteration requests half of the batch")
	assertRequestStates(t, environment.podsInjector, client, requests, []int{0, 1, 2, 3}, []int{4, 5, 6, 7})
	assert.False(t, environment.clusterState.BackoffStatusForNodeGroup(ctx, group, clock.Now()).IsBackedOff)
}

// TestAdaptiveBatchDelayedFailureShrinksLaterBatches covers a cloud provider without atomic
// resizes: it accepts the resize for the whole batch, so every request is admitted, but the nodes
// never arrive. Once cluster state reports the timed-out scale-up, later batches are half as large.
func TestAdaptiveBatchDelayedFailureShrinksLaterBatches(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	clock := clocktesting.NewFakePassiveClock(now)
	node := BuildTestNode("pool-node", 100, 10)
	SetNodeReadyState(node, true, now.Add(-time.Minute))
	occupied := BuildTestPod("occupied", 100, 1)
	occupied.Spec.NodeName = node.Name
	nodes := []*apiv1.Node{node}
	requests, _ := adaptiveRequests(t, []int32{1, 1, 1, 1, 1, 1, 1, 1})
	client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, requests...)
	var resizes []int
	environment := setupTestEnvironment(t, client, nodes, func(_ string, delta int) error {
		resizes = append(resizes, delta)
		return nil
	}, false, batchTestOptions{
		bestEffortAtomic: true, maxBatchSize: 8, scheduledPods: []*apiv1.Pod{occupied},
		backoff:    &clockedBackoff{state: backoff.NewIdBasedExponentialBackoff(5*time.Minute, 30*time.Minute, 3*time.Hour), clock: clock},
		nodeGroups: []batchTestNodeGroup{{name: "pool", maxSize: 20, nodes: nodes}},
	})
	group := environment.provider.GetNodeGroup("pool")
	run := func() int {
		waitForCache(t, client)
		injected, err := environment.podsInjector.Process(ctx, nil, nil)
		require.NoError(t, err)
		st, scaleErr := environment.orchestrator.ScaleUp(ctx, injected, nodes, nil, environment.nodeInfos, false)
		require.NoError(t, scaleErr)
		assert.Equal(t, status.ScaleUpSuccessful, st.Result)
		return len(injected)
	}

	assert.Equal(t, 8, run())
	assertRequestStates(t, environment.podsInjector, client, requests, []int{0, 1, 2, 3, 4, 5, 6, 7}, nil)

	// The test environment's maximum node provision time is zero, so the next update times out the
	// scale-up, which also backs the node group off.
	clock.SetTime(now.Add(time.Minute))
	require.NoError(t, environment.clusterState.UpdateNodes(ctx, nodes, clock.Now()))
	assert.True(t, environment.clusterState.BackoffStatusForNodeGroup(ctx, group, clock.Now()).IsBackedOff)

	for i := 0; i < 8; i++ {
		request := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
			Name: fmt.Sprintf("later-%d", i), CPU: "100m", Memory: "1", PodCount: 1,
			Class: v1.ProvisioningClassBestEffortAtomicScaleUp,
		})
		request.Spec.PodSets[0].PodTemplateRef = requests[0].Spec.PodSets[0].PodTemplateRef
		request.PodTemplates = requests[0].PodTemplates
		require.NoError(t, client.CreateProvisioningRequestForTesting(ctx, request))
	}
	clock.SetTime(now.Add(10 * time.Minute))
	require.NoError(t, environment.clusterState.UpdateNodes(ctx, nodes, clock.Now()))

	assert.Equal(t, 4, run(), "the delayed failure halves the next batch")
	assert.Equal(t, []int{8, -8, 4}, resizes, "cluster state reverts the increase that timed out")
}

// TestAdaptiveBatchGrowsOnceNodesArrive checks that a remembered batch size grows as soon as the
// nodes of a batch that needed a scale-up arrive, so that the batch selected in the same iteration
// is already larger.
func TestAdaptiveBatchGrowsOnceNodesArrive(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	clock := clocktesting.NewFakePassiveClock(now)
	node := BuildTestNode("pool-node", 100, 10)
	SetNodeReadyState(node, true, now.Add(-time.Minute))
	occupied := BuildTestPod("occupied", 100, 1)
	occupied.Spec.NodeName = node.Name
	nodes := []*apiv1.Node{node}
	requests, _ := adaptiveRequests(t, []int32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
	client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, requests...)
	var resizes []int
	environment := setupTestEnvironment(t, client, nodes, func(_ string, delta int) error {
		resizes = append(resizes, delta)
		return nil
	}, false, batchTestOptions{
		bestEffortAtomic: true, maxBatchSize: 8, scheduledPods: []*apiv1.Pod{occupied},
		backoff:    &clockedBackoff{state: backoff.NewIdBasedExponentialBackoff(5*time.Minute, 30*time.Minute, 3*time.Hour), clock: clock},
		nodeGroups: []batchTestNodeGroup{{name: "pool", maxSize: 20, nodes: nodes}},
	})
	environment.podsInjector.RememberBestEffortAtomicBatchLimit(ctx, requests[0], 4)
	run := func() int {
		waitForCache(t, client)
		injected, err := environment.podsInjector.Process(ctx, nil, nil)
		require.NoError(t, err)
		st, scaleErr := environment.orchestrator.ScaleUp(ctx, injected, nodes, nil, environment.nodeInfos, false)
		require.NoError(t, scaleErr)
		assert.Equal(t, status.ScaleUpSuccessful, st.Result)
		return len(injected)
	}

	assert.Equal(t, 4, run(), "the remembered size limits the batch")
	for i := 0; i < 4; i++ {
		arrived := BuildTestNode(fmt.Sprintf("pool-node-%d", i), 100, 10)
		SetNodeReadyState(arrived, true, now)
		environment.provider.AddNode("pool", arrived)
		nodes = append(nodes, arrived)
	}
	clock.SetTime(now.Add(time.Minute))
	require.NoError(t, environment.clusterState.UpdateNodes(ctx, nodes, clock.Now()))

	assert.Equal(t, 8, run(), "the batch selected once the nodes arrived is already larger")
	assert.Equal(t, []int{4, 8}, resizes)
}

// TestFailedBatchAdmitsRequestsThatFitExistingNodes checks that whole requests that fit existing
// nodes are admitted, oldest first, even when no capacity can be requested for the rest of the batch.
func TestFailedBatchAdmitsRequestsThatFitExistingNodes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		backedOff   bool
		wantResizes []int
	}{
		{name: "the resize for the rest of the batch fails", wantResizes: []int{2}},
		{name: "the node group is backed off", backedOff: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			var nodes []*apiv1.Node
			for i := 0; i < 3; i++ {
				node := BuildTestNode(fmt.Sprintf("pool-node-%d", i), 100, 10)
				SetNodeReadyState(node, true, time.Now().Add(-time.Minute))
				nodes = append(nodes, node)
			}
			// One node is occupied, so two of the four single-pod requests fit existing nodes.
			occupied := BuildTestPod("occupied", 100, 1)
			occupied.Spec.NodeName = nodes[0].Name
			requests, injected := adaptiveRequests(t, []int32{1, 1, 1, 1})
			client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, requests...)
			var resizes []int
			environment := setupTestEnvironment(t, client, nodes, func(_ string, delta int) error {
				resizes = append(resizes, delta)
				return fmt.Errorf("allocation failed for %d nodes", delta)
			}, false, batchTestOptions{
				bestEffortAtomic: true, maxBatchSize: 4, scheduledPods: []*apiv1.Pod{occupied},
				nodeGroups: []batchTestNodeGroup{{name: "pool", maxSize: 20, nodes: nodes}},
			})
			if tc.backedOff {
				environment.clusterState.RegisterFailedScaleUp(ctx, environment.provider.GetNodeGroup("pool"), 1,
					cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OutOfResourcesErrorClass}, time.Now())
			}

			_, _ = environment.orchestrator.ScaleUp(ctx, injected, nodes, nil, environment.nodeInfos, false)
			assert.Equal(t, tc.wantResizes, resizes)
			assertRequestStates(t, environment.podsInjector, client, requests, []int{0, 1}, []int{2, 3})
			for _, request := range requests[:2] {
				updated, err := client.ProvisioningRequestNoCache(request.Namespace, request.Name)
				require.NoError(t, err)
				assert.Equal(t, conditions.CapacityIsFoundReason, apimeta.FindStatusCondition(updated.Status.Conditions, v1.Provisioned).Reason)
			}
		})
	}
}

// TestAdaptiveBatchDeferredRetriesStayEligible checks that requests which already failed once,
// and are left out of the attempt that is finally made, can be retried by the next iteration.
func TestAdaptiveBatchDeferredRetriesStayEligible(t *testing.T) {
	ctx := t.Context()
	environment, client, requests, resizes, available := setupResizeTest(t, true, true)
	*available = 2
	injected, err := environment.podsInjector.Process(ctx, nil, nil)
	require.NoError(t, err)
	require.Len(t, injected, len(requests), "every request is due for a retry")
	st, scaleErr := environment.orchestrator.ScaleUp(ctx, injected, environment.nodes, nil, environment.nodeInfos, false)
	require.NoError(t, scaleErr)
	assert.Equal(t, status.ScaleUpSuccessful, st.Result)
	assert.Equal(t, []int{8, 4, 2}, *resizes)
	assertRequestStates(t, environment.podsInjector, client, requests, []int{0, 1}, nil)
}

type resizeTestEnvironment struct {
	*batchTestEnvironment
	nodes []*apiv1.Node
	clock *clocktesting.FakePassiveClock
}

// setupResizeTest builds a pool whose provider accepts resizes up to the available capacity. A
// clean rejection leaves the node group unchanged and says so by wrapping
// cloudprovider.ErrAtomicIncreaseRejected; otherwise the outcome is unknown. With failedBefore,
// every request already failed once and is due for a retry.
func setupResizeTest(t *testing.T, cleanRejection, failedBefore bool) (*resizeTestEnvironment, *provreqclient.ProvisioningRequestClient, []*provreqwrapper.ProvisioningRequest, *[]int, *int) {
	now := time.Now()
	clock := clocktesting.NewFakePassiveClock(now)
	groupBackoff := &clockedBackoff{
		state: backoff.NewIdBasedExponentialBackoff(5*time.Minute, 30*time.Minute, 3*time.Hour),
		clock: clock,
	}
	node := BuildTestNode("pool-node", 100, 10)
	SetNodeReadyState(node, true, now.Add(-time.Minute))
	occupied := BuildTestPod("occupied", 100, 1)
	occupied.Spec.NodeName = node.Name
	nodes := []*apiv1.Node{node}
	requests, _ := adaptiveRequests(t, []int32{1, 1, 1, 1, 1, 1, 1, 1})
	if failedBefore {
		for _, request := range requests {
			request.Status.Conditions = []metav1.Condition{{
				Type: v1.Provisioned, Status: metav1.ConditionFalse, Reason: "CapacityIsNotFound",
				LastTransitionTime: metav1.NewTime(now.Add(-90 * time.Second).Truncate(time.Second)),
			}}
		}
	}
	client := provreqclient.NewFakeProvisioningRequestClient(t.Context(), t, requests...)
	var environment *batchTestEnvironment
	resizes := &[]int{}
	available := new(int)
	environment = setupTestEnvironment(t, client, nodes, func(_ string, delta int) error {
		*resizes = append(*resizes, delta)
		if delta > *available {
			if !cleanRejection {
				return fmt.Errorf("timed out waiting for %d nodes", delta)
			}
			// The fake provider raises its target before calling back.
			group := environment.provider.GetNodeGroup("pool").(*testprovider.TestNodeGroup)
			size, _ := group.TargetSize(t.Context())
			group.SetTargetSize(size - delta)
			return fmt.Errorf("%w: insufficient capacity for %d nodes", cloudprovider.ErrAtomicIncreaseRejected, delta)
		}
		*available -= delta
		return nil
	}, false, batchTestOptions{
		bestEffortAtomic: true, maxBatchSize: 8, scheduledPods: []*apiv1.Pod{occupied}, backoff: groupBackoff,
		nodeGroups: []batchTestNodeGroup{{name: "pool", maxSize: 20, nodes: nodes}},
	})
	return &resizeTestEnvironment{batchTestEnvironment: environment, nodes: nodes, clock: clock}, client, requests, resizes, available
}

// assertRequestStates checks the Provisioned condition of every request. Requests that are
// neither provisioned nor not found weren't evaluated: their Provisioned condition must be
// untouched, and they must be eligible for the next iteration straight away.
func assertRequestStates(t *testing.T, injector *provreq.ProvisioningRequestPodsInjector, client *provreqclient.ProvisioningRequestClient, requests []*provreqwrapper.ProvisioningRequest, provisioned, notFound []int) {
	t.Helper()
	for i, request := range requests {
		updated, err := client.ProvisioningRequestNoCache(request.Namespace, request.Name)
		require.NoError(t, err)
		condition := apimeta.FindStatusCondition(updated.Status.Conditions, v1.Provisioned)
		switch {
		case slices.Contains(provisioned, i):
			assert.True(t, condition != nil && condition.Status == metav1.ConditionTrue, "%s should be provisioned", request.Name)
		case slices.Contains(notFound, i):
			assert.True(t, condition != nil && condition.Status == metav1.ConditionFalse, "%s should be retried later", request.Name)
		default:
			original := apimeta.FindStatusCondition(request.Status.Conditions, v1.Provisioned)
			if original == nil {
				assert.Nil(t, condition, "%s wasn't evaluated, so it must have no Provisioned condition", request.Name)
			} else if assert.NotNil(t, condition, "%s lost its Provisioned condition", request.Name) {
				assert.Equal(t, original.Status, condition.Status, "%s wasn't evaluated, so its Provisioned condition must be untouched", request.Name)
				assert.True(t, original.LastTransitionTime.Equal(&condition.LastTransitionTime), "%s wasn't evaluated, so its Provisioned condition must be untouched", request.Name)
			}
			assert.True(t, injector.IsAvailableForProvisioning(updated), "%s must be eligible for the next iteration", request.Name)
		}
		assert.False(t, apimeta.IsStatusConditionTrue(updated.Status.Conditions, v1.Failed), "%s must stay retryable", request.Name)
	}
}

// waitForCache waits until the informer cache reflects every ProvisioningRequest status update.
func waitForCache(t *testing.T, client *provreqclient.ProvisioningRequestClient) {
	t.Helper()
	stored, err := client.ProvisioningRequestsNoCache()
	require.NoError(t, err)
	want := map[string]v1.ProvisioningRequestStatus{}
	for _, request := range stored {
		want[request.Name] = request.Status
	}
	require.Eventually(t, func() bool {
		cached, err := client.ProvisioningRequests(t.Context())
		if err != nil || len(cached) != len(want) {
			return false
		}
		for _, request := range cached {
			if !apiequality.Semantic.DeepEqual(want[request.Name], request.Status) {
				return false
			}
		}
		return true
	}, 5*time.Second, 5*time.Millisecond)
}

func adaptiveRequests(t *testing.T, counts []int32) ([]*provreqwrapper.ProvisioningRequest, []*apiv1.Pod) {
	t.Helper()
	var requests []*provreqwrapper.ProvisioningRequest
	var injected []*apiv1.Pod
	for i, count := range counts {
		request := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
			Name: fmt.Sprintf("adaptive-%03d", i), CPU: "100m", Memory: "1", PodCount: count,
			CreationTimestamp: time.Now().Add(time.Duration(i) * time.Second), Class: v1.ProvisioningClassBestEffortAtomicScaleUp,
		})
		request.UID = types.UID(request.Name)
		requestPods, err := pods.PodsForProvisioningRequest(request)
		require.NoError(t, err)
		requests = append(requests, request)
		injected = append(injected, requestPods...)
	}
	return requests, injected
}

type clockedBackoff struct {
	state backoff.Backoff
	clock *clocktesting.FakePassiveClock
}

func (b *clockedBackoff) Backoff(ng cloudprovider.NodeGroup, ni *framework.NodeInfo, info cloudprovider.InstanceErrorInfo, _ time.Time) time.Time {
	return b.state.Backoff(ng, ni, info, b.clock.Now())
}

func (b *clockedBackoff) BackoffStatus(ng cloudprovider.NodeGroup, ni *framework.NodeInfo, _ time.Time) backoff.Status {
	return b.state.BackoffStatus(ng, ni, b.clock.Now())
}

func (b *clockedBackoff) RemoveBackoff(ng cloudprovider.NodeGroup, ni *framework.NodeInfo) {
	b.state.RemoveBackoff(ng, ni)
}

func (b *clockedBackoff) RemoveStaleBackoffData(_ time.Time) {
	b.state.RemoveStaleBackoffData(b.clock.Now())
}
