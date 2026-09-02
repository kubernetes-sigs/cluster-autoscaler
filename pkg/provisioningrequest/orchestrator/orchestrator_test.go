/*
Copyright 2024 The Kubernetes Authors.

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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/client-go/kubernetes/fake"
	clocktesting "k8s.io/utils/clock/testing"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	. "sigs.k8s.io/cluster-autoscaler/pkg/core/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupconfig"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/provreq"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	processorstest "sigs.k8s.io/cluster-autoscaler/pkg/processors/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/besteffortatomic"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/checkcapacity"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestScaleUp(t *testing.T) {
	// Set up a cluster with 200 nodes:
	// - 100 nodes with high cpu, low memory in autoscaled group with max 150
	// - 100 nodes with high memory, low cpu not in autoscaled group
	now := time.Now()
	allNodes := []*apiv1.Node{}
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("test-cpu-node-%d", i)
		node := BuildTestNode(name, 100, 10)
		SetNodeReadyState(node, true, now.Add(-2*time.Minute))
		allNodes = append(allNodes, node)
	}
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("test-mem-node-%d", i)
		node := BuildTestNode(name, 1, 1000)
		SetNodeReadyState(node, true, now.Add(-2*time.Minute))
		allNodes = append(allNodes, node)
	}

	// Active check capacity requests.
	newCheckCapacityCpuProvReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "newCheckCapacityCpuProvReq",
			CPU:      "5m",
			Memory:   "5",
			PodCount: int32(100),
			Class:    v1.ProvisioningClassCheckCapacity,
		})

	anotherCheckCapacityCpuProvReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "anotherCheckCapacityCpuProvReq",
			CPU:      "5m",
			Memory:   "5",
			PodCount: int32(100),
			Class:    v1.ProvisioningClassCheckCapacity,
		})

	newCheckCapacityMemProvReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "newCheckCapacityMemProvReq",
			CPU:      "1m",
			Memory:   "100",
			PodCount: int32(100),
			Class:    v1.ProvisioningClassCheckCapacity,
		})
	impossibleCheckCapacityReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "impossibleCheckCapacityRequest",
			CPU:      "1m",
			Memory:   "1",
			PodCount: int32(5001),
			Class:    v1.ProvisioningClassCheckCapacity,
		})

	anotherImpossibleCheckCapacityReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "anotherImpossibleCheckCapacityRequest",
			CPU:      "1m",
			Memory:   "1",
			PodCount: int32(5001),
			Class:    v1.ProvisioningClassCheckCapacity,
		})

	// Active atomic scale up requests.
	atomicScaleUpProvReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "atomicScaleUpProvReq",
			CPU:      "5m",
			Memory:   "5",
			PodCount: int32(5),
			Class:    v1.ProvisioningClassBestEffortAtomicScaleUp,
		})
	largeAtomicScaleUpProvReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "largeAtomicScaleUpProvReq",
			CPU:      "1m",
			Memory:   "100",
			PodCount: int32(100),
			Class:    v1.ProvisioningClassBestEffortAtomicScaleUp,
		})
	impossibleAtomicScaleUpReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "impossibleAtomicScaleUpRequest",
			CPU:      "1m",
			Memory:   "1",
			PodCount: int32(5001),
			Class:    v1.ProvisioningClassBestEffortAtomicScaleUp,
		})
	possibleAtomicScaleUpReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "possibleAtomicScaleUpReq",
			CPU:      "100m",
			Memory:   "1",
			PodCount: int32(120),
			Class:    v1.ProvisioningClassBestEffortAtomicScaleUp,
		})
	autoprovisioningAtomicScaleUpReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "autoprovisioningAtomicScaleUpReq",
			CPU:      "100m",
			Memory:   "100",
			PodCount: int32(5),
			Class:    v1.ProvisioningClassBestEffortAtomicScaleUp,
		})

	// Already provisioned provisioning request - capacity should be booked before processing a new request.
	// Books 20 out of 100 high-memory nodes.
	bookedCapacityProvReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "bookedCapacityProvReq",
			CPU:      "1m",
			Memory:   "200",
			PodCount: int32(100),
			Class:    v1.ProvisioningClassCheckCapacity,
		})
	bookedCapacityProvReq.SetConditions([]metav1.Condition{{Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()}})

	// Expired provisioning request - should be ignored.
	expiredProvReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "expiredProvReq",
			CPU:      "1m",
			Memory:   "200",
			PodCount: int32(100),
			Class:    v1.ProvisioningClassCheckCapacity,
		})
	expiredProvReq.SetConditions([]metav1.Condition{{Type: v1.BookingExpired, Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()}})

	// Unsupported provisioning request - should be ignored.
	unsupportedProvReq := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
		provreqwrapper.TestProvReqOptions{
			Name:     "unsupportedProvReq",
			CPU:      "1",
			Memory:   "1",
			PodCount: int32(5),
			Class:    "very much unsupported",
		})

	testCases := []struct {
		name                string
		provReqs            []*provreqwrapper.ProvisioningRequest
		provReqToScaleUp    *provreqwrapper.ProvisioningRequest
		scaleUpResult       status.ScaleUpResult
		autoprovisioning    bool
		err                 bool
		batchProcessing     bool
		maxBatchSize        int
		batchTimebox        time.Duration
		numProvisionedTrue  int
		numProvisionedFalse int
		numFailedTrue       int
	}{
		{
			name:          "no ProvisioningRequests",
			provReqs:      []*provreqwrapper.ProvisioningRequest{},
			scaleUpResult: status.ScaleUpNotTried,
		},
		{
			name:             "one ProvisioningRequest of check capacity class",
			provReqs:         []*provreqwrapper.ProvisioningRequest{newCheckCapacityCpuProvReq},
			provReqToScaleUp: newCheckCapacityCpuProvReq,
			scaleUpResult:    status.ScaleUpSuccessful,
		},
		{
			name:             "one ProvisioningRequest of atomic scale up class",
			provReqs:         []*provreqwrapper.ProvisioningRequest{atomicScaleUpProvReq},
			provReqToScaleUp: atomicScaleUpProvReq,
			scaleUpResult:    status.ScaleUpNotNeeded,
		},
		{
			name:             "capacity is there, check-capacity class",
			provReqs:         []*provreqwrapper.ProvisioningRequest{newCheckCapacityMemProvReq},
			provReqToScaleUp: newCheckCapacityMemProvReq,
			scaleUpResult:    status.ScaleUpSuccessful,
		},
		{
			name:             "unsupported ProvisioningRequest is ignored",
			provReqs:         []*provreqwrapper.ProvisioningRequest{newCheckCapacityCpuProvReq, bookedCapacityProvReq, atomicScaleUpProvReq, unsupportedProvReq},
			provReqToScaleUp: unsupportedProvReq,
			scaleUpResult:    status.ScaleUpNotTried,
		},
		{
			name:             "some capacity is pre-booked, successful capacity check",
			provReqs:         []*provreqwrapper.ProvisioningRequest{newCheckCapacityCpuProvReq, bookedCapacityProvReq, atomicScaleUpProvReq},
			provReqToScaleUp: newCheckCapacityCpuProvReq,
			scaleUpResult:    status.ScaleUpSuccessful,
		},
		{
			name: "impossible check-capacity, with noRetry parameter",
			provReqs: []*provreqwrapper.ProvisioningRequest{
				impossibleCheckCapacityReq.CopyWithParameters(map[string]v1.Parameter{"noRetry": "true"}),
			},
			provReqToScaleUp: impossibleCheckCapacityReq,
			scaleUpResult:    status.ScaleUpNoOptionsAvailable,
			numFailedTrue:    1,
		},
		{
			name:             "some capacity is pre-booked, atomic scale-up not needed",
			provReqs:         []*provreqwrapper.ProvisioningRequest{bookedCapacityProvReq, atomicScaleUpProvReq},
			provReqToScaleUp: atomicScaleUpProvReq,
			scaleUpResult:    status.ScaleUpNotNeeded,
		},
		{
			name:             "capacity is there, large atomic scale-up request doesn't require scale-up",
			provReqs:         []*provreqwrapper.ProvisioningRequest{largeAtomicScaleUpProvReq},
			provReqToScaleUp: largeAtomicScaleUpProvReq,
			scaleUpResult:    status.ScaleUpNotNeeded,
		},
		{
			name:             "impossible atomic scale-up request doesn't trigger scale-up",
			provReqs:         []*provreqwrapper.ProvisioningRequest{impossibleAtomicScaleUpReq},
			provReqToScaleUp: impossibleAtomicScaleUpReq,
			scaleUpResult:    status.ScaleUpNoOptionsAvailable,
		},
		{
			name:             "possible atomic scale-up request triggers scale-up",
			provReqs:         []*provreqwrapper.ProvisioningRequest{possibleAtomicScaleUpReq},
			provReqToScaleUp: possibleAtomicScaleUpReq,
			scaleUpResult:    status.ScaleUpSuccessful,
		},
		{
			name:             "autoprovisioning atomic scale-up request triggers scale-up",
			provReqs:         []*provreqwrapper.ProvisioningRequest{autoprovisioningAtomicScaleUpReq},
			provReqToScaleUp: autoprovisioningAtomicScaleUpReq,
			autoprovisioning: true,
			scaleUpResult:    status.ScaleUpSuccessful,
		},
		// Batch processing tests
		{
			name:               "batch processing of check capacity requests with one request",
			provReqs:           []*provreqwrapper.ProvisioningRequest{newCheckCapacityCpuProvReq},
			provReqToScaleUp:   newCheckCapacityCpuProvReq,
			scaleUpResult:      status.ScaleUpSuccessful,
			batchProcessing:    true,
			maxBatchSize:       3,
			batchTimebox:       5 * time.Minute,
			numProvisionedTrue: 1,
		},
		{
			name:               "batch processing of check capacity requests with less requests than max batch size",
			provReqs:           []*provreqwrapper.ProvisioningRequest{newCheckCapacityCpuProvReq, newCheckCapacityMemProvReq},
			provReqToScaleUp:   newCheckCapacityCpuProvReq,
			scaleUpResult:      status.ScaleUpSuccessful,
			batchProcessing:    true,
			maxBatchSize:       3,
			batchTimebox:       5 * time.Minute,
			numProvisionedTrue: 2,
		},
		{
			name:               "batch processing of check capacity requests with requests equal to max batch size",
			provReqs:           []*provreqwrapper.ProvisioningRequest{newCheckCapacityCpuProvReq, newCheckCapacityMemProvReq},
			provReqToScaleUp:   newCheckCapacityCpuProvReq,
			scaleUpResult:      status.ScaleUpSuccessful,
			batchProcessing:    true,
			maxBatchSize:       2,
			batchTimebox:       5 * time.Minute,
			numProvisionedTrue: 2,
		},
		{
			name:               "batch processing of check capacity requests with more requests than max batch size",
			provReqs:           []*provreqwrapper.ProvisioningRequest{newCheckCapacityCpuProvReq, newCheckCapacityMemProvReq, anotherCheckCapacityCpuProvReq},
			provReqToScaleUp:   newCheckCapacityCpuProvReq,
			scaleUpResult:      status.ScaleUpSuccessful,
			batchProcessing:    true,
			maxBatchSize:       2,
			batchTimebox:       5 * time.Minute,
			numProvisionedTrue: 2,
		},
		{
			name:               "batch processing of check capacity requests where cluster contains already provisioned requests",
			provReqs:           []*provreqwrapper.ProvisioningRequest{newCheckCapacityCpuProvReq, bookedCapacityProvReq, anotherCheckCapacityCpuProvReq},
			provReqToScaleUp:   newCheckCapacityCpuProvReq,
			scaleUpResult:      status.ScaleUpSuccessful,
			batchProcessing:    true,
			maxBatchSize:       2,
			batchTimebox:       5 * time.Minute,
			numProvisionedTrue: 3,
		},
		{
			name:               "batch processing of check capacity requests where timebox is exceeded",
			provReqs:           []*provreqwrapper.ProvisioningRequest{newCheckCapacityCpuProvReq, newCheckCapacityMemProvReq},
			provReqToScaleUp:   newCheckCapacityCpuProvReq,
			scaleUpResult:      status.ScaleUpSuccessful,
			batchProcessing:    true,
			maxBatchSize:       5,
			batchTimebox:       0 * time.Nanosecond,
			numProvisionedTrue: 1,
		},
		{
			name:               "batch processing of check capacity requests where max batch size is invalid",
			provReqs:           []*provreqwrapper.ProvisioningRequest{newCheckCapacityCpuProvReq, newCheckCapacityMemProvReq},
			provReqToScaleUp:   newCheckCapacityCpuProvReq,
			scaleUpResult:      status.ScaleUpSuccessful,
			batchProcessing:    true,
			maxBatchSize:       0,
			batchTimebox:       5 * time.Minute,
			numProvisionedTrue: 1,
		},
		{
			name:               "batch processing of check capacity requests where best effort atomic scale-up request is also present in cluster",
			provReqs:           []*provreqwrapper.ProvisioningRequest{newCheckCapacityMemProvReq, newCheckCapacityCpuProvReq, atomicScaleUpProvReq},
			provReqToScaleUp:   newCheckCapacityMemProvReq,
			scaleUpResult:      status.ScaleUpSuccessful,
			batchProcessing:    true,
			maxBatchSize:       2,
			batchTimebox:       5 * time.Minute,
			numProvisionedTrue: 2,
		},
		{
			name:               "process atomic scale-up requests where batch processing of check capacity requests is enabled",
			provReqs:           []*provreqwrapper.ProvisioningRequest{possibleAtomicScaleUpReq},
			provReqToScaleUp:   possibleAtomicScaleUpReq,
			scaleUpResult:      status.ScaleUpSuccessful,
			batchProcessing:    true,
			maxBatchSize:       3,
			batchTimebox:       5 * time.Minute,
			numProvisionedTrue: 1,
		},
		{
			name:               "process atomic scale-up requests where batch processing of check capacity requests is enabled and check capacity requests are present in cluster",
			provReqs:           []*provreqwrapper.ProvisioningRequest{newCheckCapacityMemProvReq, newCheckCapacityCpuProvReq, atomicScaleUpProvReq},
			provReqToScaleUp:   atomicScaleUpProvReq,
			scaleUpResult:      status.ScaleUpSuccessful,
			batchProcessing:    true,
			maxBatchSize:       3,
			batchTimebox:       5 * time.Minute,
			numProvisionedTrue: 2,
		},
		{
			name:                "batch processing of check capacity requests where some requests' capacity is not available",
			provReqs:            []*provreqwrapper.ProvisioningRequest{newCheckCapacityMemProvReq, impossibleCheckCapacityReq, newCheckCapacityCpuProvReq},
			provReqToScaleUp:    newCheckCapacityMemProvReq,
			scaleUpResult:       status.ScaleUpSuccessful,
			batchProcessing:     true,
			maxBatchSize:        3,
			batchTimebox:        5 * time.Minute,
			numProvisionedTrue:  2,
			numProvisionedFalse: 1,
		},
		{
			name:                "batch processing of check capacity requests where all requests' capacity is not available",
			provReqs:            []*provreqwrapper.ProvisioningRequest{impossibleCheckCapacityReq, anotherImpossibleCheckCapacityReq},
			provReqToScaleUp:    impossibleCheckCapacityReq,
			scaleUpResult:       status.ScaleUpNoOptionsAvailable,
			batchProcessing:     true,
			maxBatchSize:        3,
			batchTimebox:        5 * time.Minute,
			numProvisionedFalse: 2,
		},
	}
	for _, tc := range testCases {
		tc := tc

		nodes := []*apiv1.Node{}
		for _, n := range allNodes {
			nodes = append(nodes, n.DeepCopy())
		}

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			prPods, err := pods.PodsForProvisioningRequest(tc.provReqToScaleUp)
			assert.NoError(t, err)

			onScaleUpFunc := func(name string, n int) error {
				if tc.scaleUpResult == status.ScaleUpSuccessful {
					return nil
				}
				return fmt.Errorf("unexpected scale-up of %s by %d", name, n)
			}

			testProvReqs := []*provreqwrapper.ProvisioningRequest{}
			for _, pr := range tc.provReqs {
				testProvReqs = append(testProvReqs, &provreqwrapper.ProvisioningRequest{ProvisioningRequest: pr.DeepCopy(), PodTemplates: pr.PodTemplates})
			}

			client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, testProvReqs...)
			orchestrator, nodeInfos := setupTest(t, client, nodes, onScaleUpFunc, tc.autoprovisioning, batchTestOptions{checkCapacity: tc.batchProcessing, maxBatchSize: tc.maxBatchSize, timebox: tc.batchTimebox})

			st, err := orchestrator.ScaleUp(context.Background(), prPods, []*apiv1.Node{}, []*appsv1.DaemonSet{}, nodeInfos, false)
			if !tc.err {
				assert.NoError(t, err)
				if tc.scaleUpResult != st.Result && len(st.PodsRemainUnschedulable) > 0 {
					// We expected all pods to be scheduled, but some remain unschedulable.
					// Let's add the reason groups were rejected to errors. This is useful for debugging.
					t.Errorf("noScaleUpInfo: %#v", st.PodsRemainUnschedulable[0].RejectedNodeGroups)
				}
				assert.Equal(t, tc.scaleUpResult, st.Result)

				provReqsAfterScaleUp, err := client.ProvisioningRequestsNoCache()
				assert.NoError(t, err)
				assert.Equal(t, len(tc.provReqs), len(provReqsAfterScaleUp))
				assert.Equal(t, tc.numFailedTrue, NumProvisioningRequestsWithCondition(provReqsAfterScaleUp, v1.Failed, metav1.ConditionTrue))

				if tc.batchProcessing {
					// Since batch processing returns aggregated result, we need to check the number of provisioned requests which have the provisioned condition.
					assert.Equal(t, tc.numProvisionedTrue, NumProvisioningRequestsWithCondition(provReqsAfterScaleUp, v1.Provisioned, metav1.ConditionTrue))
					assert.Equal(t, tc.numProvisionedFalse, NumProvisioningRequestsWithCondition(provReqsAfterScaleUp, v1.Provisioned, metav1.ConditionFalse))
				}
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// batchTestOptions configures batch processing for a test cluster.
type batchTestOptions struct {
	checkCapacity     bool
	bestEffortAtomic  bool
	maxBatchSize      int
	timebox           time.Duration
	scheduledPods     []*apiv1.Pod
	nodeGroupMaxSize  int
	nodeGroups        []batchTestNodeGroup
	balanceNodeGroups bool
}

type batchTestNodeGroup struct {
	name    string
	minSize int
	maxSize int
	nodes   []*apiv1.Node
}

type batchTestEnvironment struct {
	orchestrator    *provReqOrchestrator
	nodeInfos       map[string]*framework.NodeInfo
	provider        *testprovider.TestCloudProvider
	clusterSnapshot clustersnapshot.ClusterSnapshot
	clusterState    *clusterstate.ClusterStateRegistry
}

// TestBestEffortAtomicBatchScaleUp verifies that several best-effort-atomic ProvisioningRequests
// are flattened into one all-or-nothing capacity calculation.
//
// The test cluster has 100 schedulable nodes with 100 millicpu each, in a node group that can grow
// to 150. Each request asks for 40 pods of 100 millicpu, so exactly one pod fits per node:
// the combined 120 pods consume the 100 existing nodes and trigger one 20-node scale-up.
func TestBestEffortAtomicBatchScaleUp(t *testing.T) {
	now := time.Now()
	allNodes := []*apiv1.Node{}
	for i := 0; i < 100; i++ {
		node := BuildTestNode(fmt.Sprintf("test-cpu-node-%d", i), 100, 10)
		SetNodeReadyState(node, true, now.Add(-2*time.Minute))
		allNodes = append(allNodes, node)
	}
	for i := 0; i < 100; i++ {
		node := BuildTestNode(fmt.Sprintf("test-mem-node-%d", i), 1, 1000)
		SetNodeReadyState(node, true, now.Add(-2*time.Minute))
		allNodes = append(allNodes, node)
	}

	newBatchProvReq := func(name string) *provreqwrapper.ProvisioningRequest {
		return provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
			provreqwrapper.TestProvReqOptions{
				Name:     name,
				CPU:      "100m",
				Memory:   "1",
				PodCount: int32(40),
				Class:    v1.ProvisioningClassBestEffortAtomicScaleUp,
			})
	}

	testCases := []struct {
		name                string
		batchProcessing     bool
		maxBatchSize        int
		wantResult          status.ScaleUpResult
		wantProvisionedTrue int
		wantScaleUpCalls    int
		wantNodesAdded      int
	}{
		{
			name:            "batching disabled, only the first request is processed",
			batchProcessing: false,
			// Only the first request fits without a scale-up, and the other two are left for
			// the following iterations - this is the one-request-per-loop behaviour.
			wantResult:          status.ScaleUpNotNeeded,
			wantProvisionedTrue: 1,
			wantScaleUpCalls:    0,
			wantNodesAdded:      0,
		},
		{
			name:            "batching enabled, all requests are flattened into one scale-up calculation",
			batchProcessing: true,
			maxBatchSize:    10,
			// The third request needs 20 more nodes, which makes the aggregated result successful.
			wantResult:          status.ScaleUpSuccessful,
			wantProvisionedTrue: 3,
			wantScaleUpCalls:    1,
			wantNodesAdded:      20,
		},
		{
			name:            "batch size caps the number of requests processed",
			batchProcessing: true,
			maxBatchSize:    2,
			// Only the first two requests are injected, and both fit without a scale-up.
			wantResult:          status.ScaleUpNotNeeded,
			wantProvisionedTrue: 2,
			wantScaleUpCalls:    0,
			wantNodesAdded:      0,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nodes := []*apiv1.Node{}
			for _, n := range allNodes {
				nodes = append(nodes, n.DeepCopy())
			}

			provReqs := []*provreqwrapper.ProvisioningRequest{
				newBatchProvReq("batchProvReqA"),
				newBatchProvReq("batchProvReqB"),
				newBatchProvReq("batchProvReqC"),
			}

			// The injector only injects pods for as many requests as the batch allows, and a
			// single request when batching is disabled.
			injectedProvReqs := provReqs
			if !tc.batchProcessing {
				injectedProvReqs = provReqs[:1]
			} else if tc.maxBatchSize < len(provReqs) {
				injectedProvReqs = provReqs[:tc.maxBatchSize]
			}
			var prPods []*apiv1.Pod
			for _, pr := range injectedProvReqs {
				podsForPr, err := pods.PodsForProvisioningRequest(pr)
				assert.NoError(t, err)
				prPods = append(prPods, podsForPr...)
			}

			var mu sync.Mutex
			scaleUpCalls := 0
			nodesAdded := 0
			onScaleUpFunc := func(_ string, n int) error {
				mu.Lock()
				defer mu.Unlock()
				scaleUpCalls++
				nodesAdded += n
				return nil
			}

			testProvReqs := []*provreqwrapper.ProvisioningRequest{}
			for _, pr := range provReqs {
				testProvReqs = append(testProvReqs, &provreqwrapper.ProvisioningRequest{ProvisioningRequest: pr.DeepCopy(), PodTemplates: pr.PodTemplates})
			}
			client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, testProvReqs...)

			orchestrator, nodeInfos := setupTest(t, client, nodes, onScaleUpFunc, false, batchTestOptions{
				bestEffortAtomic: tc.batchProcessing,
				maxBatchSize:     tc.maxBatchSize,
			})

			st, aErr := orchestrator.ScaleUp(context.Background(), prPods, []*apiv1.Node{}, []*appsv1.DaemonSet{}, nodeInfos, false)
			assert.NoError(t, aErr)
			assert.Equal(t, tc.wantResult, st.Result)

			provReqsAfterScaleUp, err := client.ProvisioningRequestsNoCache()
			assert.NoError(t, err)
			assert.Equal(t, tc.wantProvisionedTrue, NumProvisioningRequestsWithCondition(provReqsAfterScaleUp, v1.Provisioned, metav1.ConditionTrue))
			assert.Equal(t, 0, NumProvisioningRequestsWithCondition(provReqsAfterScaleUp, v1.Failed, metav1.ConditionTrue))

			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, tc.wantScaleUpCalls, scaleUpCalls, "unexpected number of scale-up requests")
			assert.Equal(t, tc.wantNodesAdded, nodesAdded, "unexpected number of nodes requested")
		})
	}
}

// TestBestEffortAtomicBatchCoalescesInfrastructureScaleUp covers the customer scenario where
// many one-pod ProvisioningRequests each need one new node. Flattening must turn the requests into
// one estimator input and one infrastructure resize, rather than issuing one resize per request.
func TestBestEffortAtomicBatchCoalescesInfrastructureScaleUp(t *testing.T) {
	const requestCount = 100
	now := time.Now()
	allNodes := make([]*apiv1.Node, 0, 200)
	occupiedPods := make([]*apiv1.Pod, 0, requestCount)
	for i := 0; i < requestCount; i++ {
		node := BuildTestNode(fmt.Sprintf("test-cpu-node-%d", i), 100, 10)
		SetNodeReadyState(node, true, now.Add(-2*time.Minute))
		allNodes = append(allNodes, node)

		pod := BuildTestPod(fmt.Sprintf("occupied-pod-%d", i), 100, 1)
		pod.Spec.NodeName = node.Name
		occupiedPods = append(occupiedPods, pod)
	}
	for i := 0; i < 100; i++ {
		node := BuildTestNode(fmt.Sprintf("test-mem-node-%d", i), 1, 1000)
		SetNodeReadyState(node, true, now.Add(-2*time.Minute))
		allNodes = append(allNodes, node)
	}

	provReqs := make([]*provreqwrapper.ProvisioningRequest, 0, requestCount)
	var injectedPods []*apiv1.Pod
	for i := 0; i < requestCount; i++ {
		pr := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(
			provreqwrapper.TestProvReqOptions{
				Name:              fmt.Sprintf("batch-provreq-%03d", i),
				CPU:               "100m",
				Memory:            "1",
				PodCount:          1,
				CreationTimestamp: now.Add(time.Duration(i) * time.Nanosecond),
				Class:             v1.ProvisioningClassBestEffortAtomicScaleUp,
			})
		provReqs = append(provReqs, pr)
		podsForPr, err := pods.PodsForProvisioningRequest(pr)
		assert.NoError(t, err)
		injectedPods = append(injectedPods, podsForPr...)
	}

	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, provReqs...)
	var mu sync.Mutex
	var scaleUpDeltas []int
	onScaleUpFunc := func(_ string, delta int) error {
		mu.Lock()
		defer mu.Unlock()
		scaleUpDeltas = append(scaleUpDeltas, delta)
		return nil
	}
	orchestrator, nodeInfos := setupTest(t, client, allNodes, onScaleUpFunc, false, batchTestOptions{
		bestEffortAtomic: true,
		maxBatchSize:     requestCount,
		scheduledPods:    occupiedPods,
		nodeGroupMaxSize: 250,
	})

	st, aErr := orchestrator.ScaleUp(context.Background(), injectedPods, allNodes, []*appsv1.DaemonSet{}, nodeInfos, false)
	assert.NoError(t, aErr)
	assert.Equal(t, status.ScaleUpSuccessful, st.Result)

	updatedProvReqs, err := client.ProvisioningRequestsNoCache()
	assert.NoError(t, err)
	assert.Equal(t, requestCount, NumProvisioningRequestsWithCondition(updatedProvReqs, v1.Provisioned, metav1.ConditionTrue))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []int{requestCount}, scaleUpDeltas, "the flattened batch should produce one combined infrastructure resize")
}

func TestBestEffortAtomicFlattenedBatchFailsTogether(t *testing.T) {
	now := time.Now()
	allNodes := make([]*apiv1.Node, 0, 200)
	for i := 0; i < 100; i++ {
		node := BuildTestNode(fmt.Sprintf("test-cpu-node-%d", i), 100, 10)
		SetNodeReadyState(node, true, now.Add(-2*time.Minute))
		allNodes = append(allNodes, node)
	}
	for i := 0; i < 100; i++ {
		node := BuildTestNode(fmt.Sprintf("test-mem-node-%d", i), 1, 1000)
		SetNodeReadyState(node, true, now.Add(-2*time.Minute))
		allNodes = append(allNodes, node)
	}

	possible := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
		Name: "possible-batch-request", CPU: "100m", Memory: "1", PodCount: 1, Class: v1.ProvisioningClassBestEffortAtomicScaleUp,
	})
	impossible := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
		Name: "impossible-batch-request", CPU: "101m", Memory: "1", PodCount: 1, Class: v1.ProvisioningClassBestEffortAtomicScaleUp,
	})
	provReqs := []*provreqwrapper.ProvisioningRequest{possible, impossible}
	var injectedPods []*apiv1.Pod
	for _, pr := range provReqs {
		podsForPr, err := pods.PodsForProvisioningRequest(pr)
		assert.NoError(t, err)
		injectedPods = append(injectedPods, podsForPr...)
	}

	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, provReqs...)
	scaleUpCalls := 0
	orchestrator, nodeInfos := setupTest(t, client, allNodes, func(_ string, _ int) error {
		scaleUpCalls++
		return nil
	}, false, batchTestOptions{bestEffortAtomic: true, maxBatchSize: 2})

	st, aErr := orchestrator.ScaleUp(context.Background(), injectedPods, allNodes, []*appsv1.DaemonSet{}, nodeInfos, false)
	assert.NoError(t, aErr)
	assert.Equal(t, status.ScaleUpNoOptionsAvailable, st.Result)
	assert.Equal(t, 0, scaleUpCalls)

	updatedProvReqs, err := client.ProvisioningRequestsNoCache()
	assert.NoError(t, err)
	assert.Equal(t, len(provReqs), NumProvisioningRequestsWithCondition(updatedProvReqs, v1.Provisioned, metav1.ConditionFalse))
	assert.Equal(t, 0, NumProvisioningRequestsWithCondition(updatedProvReqs, v1.Provisioned, metav1.ConditionTrue))
}

// TestBestEffortAtomicFlattenedBatchScalesMultipleNodeGroups verifies that the generic scale-up
// recipe can distribute one flattened ProvisioningRequest batch across similar node groups. The
// infrastructure operation remains atomic per node group, so this produces one atomic resize for
// each participating group rather than one provider call spanning both groups.
func TestBestEffortAtomicFlattenedBatchScalesMultipleNodeGroups(t *testing.T) {
	now := time.Now()
	nodeA := BuildTestNode("test-cpu-a-node", 100, 10)
	nodeB := BuildTestNode("test-cpu-b-node", 100, 10)
	SetNodeReadyState(nodeA, true, now.Add(-2*time.Minute))
	SetNodeReadyState(nodeB, true, now.Add(-2*time.Minute))
	allNodes := []*apiv1.Node{nodeA, nodeB}

	occupiedPodA := BuildTestPod("occupied-pod-a", 100, 1)
	occupiedPodA.Spec.NodeName = nodeA.Name
	occupiedPodB := BuildTestPod("occupied-pod-b", 100, 1)
	occupiedPodB.Spec.NodeName = nodeB.Name

	provReqs := []*provreqwrapper.ProvisioningRequest{
		provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
			Name: "batch-request-a", CPU: "100m", Memory: "1", PodCount: 1, Class: v1.ProvisioningClassBestEffortAtomicScaleUp,
		}),
		provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
			Name: "batch-request-b", CPU: "100m", Memory: "1", PodCount: 1, Class: v1.ProvisioningClassBestEffortAtomicScaleUp,
		}),
	}
	var injectedPods []*apiv1.Pod
	for _, pr := range provReqs {
		podsForPr, err := pods.PodsForProvisioningRequest(pr)
		assert.NoError(t, err)
		injectedPods = append(injectedPods, podsForPr...)
	}

	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, provReqs...)
	var mu sync.Mutex
	scaleUpDeltas := make(map[string]int)
	onScaleUpFunc := func(group string, delta int) error {
		mu.Lock()
		defer mu.Unlock()
		scaleUpDeltas[group] += delta
		return nil
	}
	orchestrator, nodeInfos := setupTest(t, client, allNodes, onScaleUpFunc, false, batchTestOptions{
		bestEffortAtomic:  true,
		maxBatchSize:      len(provReqs),
		scheduledPods:     []*apiv1.Pod{occupiedPodA, occupiedPodB},
		balanceNodeGroups: true,
		nodeGroups: []batchTestNodeGroup{
			{name: "test-cpu-a", minSize: 0, maxSize: 2, nodes: []*apiv1.Node{nodeA}},
			{name: "test-cpu-b", minSize: 0, maxSize: 2, nodes: []*apiv1.Node{nodeB}},
		},
	})

	st, aErr := orchestrator.ScaleUp(context.Background(), injectedPods, allNodes, []*appsv1.DaemonSet{}, nodeInfos, false)
	assert.NoError(t, aErr)
	assert.Equal(t, status.ScaleUpSuccessful, st.Result)
	assert.Len(t, st.ScaleUpInfos, 2)

	updatedProvReqs, err := client.ProvisioningRequestsNoCache()
	assert.NoError(t, err)
	assert.Equal(t, len(provReqs), NumProvisioningRequestsWithCondition(updatedProvReqs, v1.Provisioned, metav1.ConditionTrue))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, map[string]int{"test-cpu-a": 1, "test-cpu-b": 1}, scaleUpDeltas)
}

// TestBestEffortAtomicFlattenedBatchConvergesAfterPartialScaleUpFailure verifies that
// successful capacity from a partially failed multi-node-group scale-up is reused by the next
// planning pass. Pool A starts 20 nodes smaller than pool B, so balancing 100 one-node requests
// produces A +60 and B +40. After B rejects its first atomic resize, all ProvisioningRequests
// remain unfulfilled. Once A's 60 nodes are visible, the retry requests only B's missing 40 nodes
// and fulfills the whole batch.
func TestBestEffortAtomicFlattenedBatchConvergesAfterPartialScaleUpFailure(t *testing.T) {
	testBestEffortAtomicFlattenedBatchConvergesAfterPartialScaleUpFailure(t, 0, 100, map[string]int{"test-cpu-b": 40})
}

// TestBestEffortAtomicFlattenedBatchConvergesWithNewRequests verifies that requests arriving
// after a partial failure join the next flattened planning pass. Pool B is capped after its
// original 40-node allocation, so the three new requests add three nodes to Pool A while Pool B's
// rejected 40-node allocation is retried.
func TestBestEffortAtomicFlattenedBatchConvergesWithNewRequests(t *testing.T) {
	testBestEffortAtomicFlattenedBatchConvergesAfterPartialScaleUpFailure(t, 3, 61, map[string]int{"test-cpu-a": 3, "test-cpu-b": 40})
}

func testBestEffortAtomicFlattenedBatchConvergesAfterPartialScaleUpFailure(t *testing.T, newRequestCount, poolBMaxSize int, expectedRetryDeltas map[string]int) {
	const (
		initialRequestCount = 100
		poolAName           = "test-cpu-a"
		poolBName           = "test-cpu-b"
		poolAStart          = 1
		poolBStart          = 21
	)
	totalRequestCount := initialRequestCount + newRequestCount
	now := time.Now()
	poolANodes := make([]*apiv1.Node, 0, poolAStart)
	poolBNodes := make([]*apiv1.Node, 0, poolBStart)
	occupiedPods := make([]*apiv1.Pod, 0, poolAStart+poolBStart)
	for i := 0; i < poolAStart; i++ {
		node := BuildTestNode(fmt.Sprintf("%s-node-%d", poolAName, i), 100, 10)
		SetNodeReadyState(node, true, now.Add(-2*time.Minute))
		poolANodes = append(poolANodes, node)
		pod := BuildTestPod(fmt.Sprintf("%s-occupied-%d", poolAName, i), 100, 1)
		pod.Spec.NodeName = node.Name
		occupiedPods = append(occupiedPods, pod)
	}
	for i := 0; i < poolBStart; i++ {
		node := BuildTestNode(fmt.Sprintf("%s-node-%d", poolBName, i), 100, 10)
		SetNodeReadyState(node, true, now.Add(-2*time.Minute))
		poolBNodes = append(poolBNodes, node)
		pod := BuildTestPod(fmt.Sprintf("%s-occupied-%d", poolBName, i), 100, 1)
		pod.Spec.NodeName = node.Name
		occupiedPods = append(occupiedPods, pod)
	}
	initialNodes := append(append([]*apiv1.Node{}, poolANodes...), poolBNodes...)

	provReqs := make([]*provreqwrapper.ProvisioningRequest, 0, totalRequestCount)
	var injectedPods []*apiv1.Pod
	for i := 0; i < initialRequestCount; i++ {
		pr := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
			Name:              fmt.Sprintf("partial-failure-request-%03d", i),
			CPU:               "100m",
			Memory:            "1",
			PodCount:          1,
			CreationTimestamp: now.Add(time.Duration(i) * time.Nanosecond),
			Class:             v1.ProvisioningClassBestEffortAtomicScaleUp,
		})
		provReqs = append(provReqs, pr)
		podsForPr, err := pods.PodsForProvisioningRequest(pr)
		assert.NoError(t, err)
		injectedPods = append(injectedPods, podsForPr...)
	}

	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, provReqs...)
	var environment *batchTestEnvironment
	var mu sync.Mutex
	attempt := 1
	scaleUpDeltas := map[int]map[string]int{1: {}, 2: {}}
	onScaleUpFunc := func(group string, delta int) error {
		mu.Lock()
		defer mu.Unlock()
		scaleUpDeltas[attempt][group] += delta
		if attempt == 1 && group == poolBName {
			// TestNodeGroup advances target size before invoking this callback. Restore the
			// target to model the AtomicIncreaseSize contract: an error changes no capacity.
			environment.provider.GetNodeGroup(poolBName).(*testprovider.TestNodeGroup).SetTargetSize(poolBStart)
			return fmt.Errorf("simulated atomic resize rejection for %s", poolBName)
		}
		return nil
	}
	environment = setupTestEnvironment(t, client, initialNodes, onScaleUpFunc, false, batchTestOptions{
		bestEffortAtomic:  true,
		maxBatchSize:      totalRequestCount,
		scheduledPods:     occupiedPods,
		balanceNodeGroups: true,
		nodeGroups: []batchTestNodeGroup{
			{name: poolAName, minSize: 0, maxSize: 100, nodes: poolANodes},
			{name: poolBName, minSize: 0, maxSize: poolBMaxSize, nodes: poolBNodes},
		},
	})

	firstStatus, firstErr := environment.orchestrator.ScaleUp(context.Background(), injectedPods, initialNodes, []*appsv1.DaemonSet{}, environment.nodeInfos, false)
	assert.Error(t, firstErr)
	assert.Equal(t, status.ScaleUpError, firstStatus.Result)
	assert.Equal(t, map[string]int{poolAName: 60, poolBName: 40}, scaleUpDeltas[1])

	updatedProvReqs, err := client.ProvisioningRequestsNoCache()
	assert.NoError(t, err)
	assert.Equal(t, initialRequestCount, NumProvisioningRequestsWithCondition(updatedProvReqs, v1.Provisioned, metav1.ConditionFalse))
	assert.Equal(t, 0, NumProvisioningRequestsWithCondition(updatedProvReqs, v1.Provisioned, metav1.ConditionTrue))

	poolATarget, err := environment.provider.GetNodeGroup(poolAName).TargetSize(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, poolAStart+60, poolATarget)
	poolBTarget, err := environment.provider.GetNodeGroup(poolBName).TargetSize(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, poolBStart, poolBTarget)

	// Add requests that arrive after the partial scale-up result but before the next planning
	// pass. They reuse an existing PodTemplate so the fake client only needs to create the new
	// ProvisioningRequest resources and wait for its informer to observe them.
	for i := 0; i < newRequestCount; i++ {
		pr := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
			Name:              fmt.Sprintf("new-request-%03d", i),
			CPU:               "100m",
			Memory:            "1",
			PodCount:          1,
			CreationTimestamp: now.Add(time.Minute + time.Duration(i)*time.Nanosecond),
			Class:             v1.ProvisioningClassBestEffortAtomicScaleUp,
		})
		pr.Spec.PodSets[0].PodTemplateRef = provReqs[0].Spec.PodSets[0].PodTemplateRef
		pr.PodTemplates = provReqs[0].PodTemplates
		assert.NoError(t, client.CreateProvisioningRequestForTesting(context.Background(), pr))
		provReqs = append(provReqs, pr)
		podsForPr, err := pods.PodsForProvisioningRequest(pr)
		assert.NoError(t, err)
		injectedPods = append(injectedPods, podsForPr...)
	}
	updatedProvReqs, err = client.ProvisioningRequestsNoCache()
	assert.NoError(t, err)
	assert.Len(t, updatedProvReqs, totalRequestCount)
	assert.Equal(t, initialRequestCount, NumProvisioningRequestsWithCondition(updatedProvReqs, v1.Provisioned, metav1.ConditionFalse))
	assert.Equal(t, 0, NumProvisioningRequestsWithCondition(updatedProvReqs, v1.Provisioned, metav1.ConditionTrue))

	// Model the next CA loop after Pool A's successful nodes have registered. The failed
	// ProvisioningRequests and any newly arrived requests are evaluated as one batch against this
	// refreshed cluster state.
	retryNodes := append([]*apiv1.Node{}, initialNodes...)
	for i := 0; i < 60; i++ {
		node := BuildTestNode(fmt.Sprintf("%s-new-node-%d", poolAName, i), 100, 10)
		SetNodeReadyState(node, true, now)
		environment.provider.AddNode(poolAName, node)
		retryNodes = append(retryNodes, node)
	}
	clustersnapshot.InitializeClusterSnapshotOrDie(t, environment.clusterSnapshot, retryNodes, occupiedPods)
	assert.NoError(t, environment.clusterState.UpdateNodes(context.Background(), retryNodes, now.Add(time.Minute)))

	mu.Lock()
	attempt = 2
	mu.Unlock()
	secondStatus, secondErr := environment.orchestrator.ScaleUp(context.Background(), injectedPods, retryNodes, []*appsv1.DaemonSet{}, environment.nodeInfos, false)
	assert.NoError(t, secondErr)
	assert.Equal(t, status.ScaleUpSuccessful, secondStatus.Result)
	assert.Equal(t, expectedRetryDeltas, scaleUpDeltas[2])

	updatedProvReqs, err = client.ProvisioningRequestsNoCache()
	assert.NoError(t, err)
	assert.Equal(t, totalRequestCount, NumProvisioningRequestsWithCondition(updatedProvReqs, v1.Provisioned, metav1.ConditionTrue))
	assert.Equal(t, 0, NumProvisioningRequestsWithCondition(updatedProvReqs, v1.Provisioned, metav1.ConditionFalse))
}

func setupTest(t *testing.T, client *provreqclient.ProvisioningRequestClient, nodes []*apiv1.Node, onScaleUpFunc func(string, int) error, autoprovisioning bool, batch batchTestOptions) (*provReqOrchestrator, map[string]*framework.NodeInfo) {
	environment := setupTestEnvironment(t, client, nodes, onScaleUpFunc, autoprovisioning, batch)
	return environment.orchestrator, environment.nodeInfos
}

func setupTestEnvironment(t *testing.T, client *provreqclient.ProvisioningRequestClient, nodes []*apiv1.Node, onScaleUpFunc func(string, int) error, autoprovisioning bool, batch batchTestOptions) *batchTestEnvironment {
	provider := testprovider.NewTestCloudProviderBuilder().WithOnScaleUp(onScaleUpFunc).Build()
	clock := clocktesting.NewFakePassiveClock(time.Now())
	now := clock.Now()
	if autoprovisioning {
		machineTypes := []string{"large-machine"}
		template := BuildTestNode("large-node-template", 100, 100)
		SetNodeReadyState(template, true, now)
		nodeInfoTemplate := framework.NewTestNodeInfo(template)
		machineTemplates := map[string]*framework.NodeInfo{
			"large-machine": nodeInfoTemplate,
		}
		onNodeGroupCreateFunc := func(name string) error { return nil }
		provider = testprovider.NewTestCloudProviderBuilder().WithOnScaleUp(onScaleUpFunc).WithOnNodeGroupCreate(onNodeGroupCreateFunc).WithMachineTypes(machineTypes).WithMachineTemplates(machineTemplates).Build()
	}

	if len(batch.nodeGroups) > 0 {
		for _, group := range batch.nodeGroups {
			provider.AddNodeGroup(group.name, group.minSize, group.maxSize, len(group.nodes))
			for _, node := range group.nodes {
				provider.AddNode(group.name, node)
			}
		}
	} else {
		nodeGroupMaxSize := batch.nodeGroupMaxSize
		if nodeGroupMaxSize == 0 {
			nodeGroupMaxSize = 150
		}
		provider.AddNodeGroup("test-cpu", 50, nodeGroupMaxSize, 100)
		for _, n := range nodes[:100] {
			provider.AddNode("test-cpu", n)
		}
	}

	podLister := kube_util.NewTestPodLister(nil)
	listers := kube_util.NewListerRegistry(nil, nil, podLister, nil, nil, nil, nil, nil, nil)

	options := config.AutoscalingOptions{
		MaxNodeGroupBinpackingDuration: 1 * time.Second,
		BalanceSimilarNodeGroups:       batch.balanceNodeGroups,
	}
	if batch.checkCapacity {
		options.CheckCapacityBatchProcessing = true
		options.CheckCapacityProvisioningRequestMaxBatchSize = batch.maxBatchSize
		options.CheckCapacityProvisioningRequestBatchTimebox = batch.timebox
	}
	if batch.bestEffortAtomic {
		options.BestEffortAtomicBatchProcessing = true
		options.BestEffortAtomicProvisioningRequestMaxBatchSize = batch.maxBatchSize
	}

	processors, templateNodeInfoRegistry := processorstest.NewTestProcessors(options)
	autoscalingCtx, err := NewScaleTestAutoscalingContext(options, &fake.Clientset{}, listers, provider, nil, nil, templateNodeInfoRegistry)
	assert.NoError(t, err)

	clustersnapshot.InitializeClusterSnapshotOrDie(t, autoscalingCtx.ClusterSnapshot, nodes, batch.scheduledPods)
	if autoprovisioning {
		processors.NodeGroupListProcessor = &MockAutoprovisioningNodeGroupListProcessor{T: t}
		processors.NodeGroupManager = &MockAutoprovisioningNodeGroupManager{T: t, ExtraGroups: 2}
	}
	err = autoscalingCtx.TemplateNodeInfoRegistry.Recompute(context.Background(), &autoscalingCtx, nodes, []*appsv1.DaemonSet{}, taints.TaintConfig{}, now)
	assert.NoError(t, err)
	nodeInfos := autoscalingCtx.TemplateNodeInfoRegistry.GetNodeInfos()

	estimatorBuilder, _ := estimator.NewEstimatorBuilder(
		estimator.BinpackingEstimatorName,
		estimator.NewThresholdBasedEstimationLimiter(nil),
		estimator.NewDecreasingPodOrderer(),
		nil,
		false,
	)

	clusterState := clusterstate.NewClusterStateRegistry(provider, autoscalingCtx.LogRecorder, NewBackoff(), nodegroupconfig.NewDefaultNodeGroupConfigProcessor(autoscalingCtx.NodeGroupDefaults), templateNodeInfoRegistry)
	clusterState.UpdateNodes(context.Background(), nodes, now)

	var injector *provreq.ProvisioningRequestPodsInjector
	if batch.checkCapacity {
		injector = provreq.NewFakePodsInjector(client, clocktesting.NewFakePassiveClock(now))
	}

	quotasTrackerFactory := resourcequotas.NewTrackerFactory(resourcequotas.TrackerOptions{
		QuotaProvider:            resourcequotas.NewFakeProvider(nil),
		CustomResourcesProcessor: processors.CustomResourcesProcessor,
	})
	orchestrator := &provReqOrchestrator{
		client:              client,
		provisioningClasses: []ProvisioningClass{checkcapacity.New(client, injector), besteffortatomic.New(client)},
	}
	orchestrator.Initialize(&autoscalingCtx, processors, clusterState, estimatorBuilder, taints.TaintConfig{}, quotasTrackerFactory)
	return &batchTestEnvironment{
		orchestrator:    orchestrator,
		nodeInfos:       nodeInfos,
		provider:        provider,
		clusterSnapshot: autoscalingCtx.ClusterSnapshot,
		clusterState:    clusterState,
	}
}

func NumProvisioningRequestsWithCondition(prList []*provreqwrapper.ProvisioningRequest, conditionType string, conditionStatus metav1.ConditionStatus) int {
	count := 0

	for _, pr := range prList {
		for _, c := range pr.Status.Conditions {
			if c.Type == conditionType && c.Status == conditionStatus {
				count++
				break
			}
		}
	}

	return count
}
