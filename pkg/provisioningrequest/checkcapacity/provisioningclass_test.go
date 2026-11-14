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

package checkcapacity

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/provreq"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/conditions"
	provreqpods "sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	csisnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/csi/snapshot"
	drasnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/snapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestCombinedStatusSet(t *testing.T) {
	// TestCombinedStatusSet tests the CombinedStatusSet function.
	testCases := []struct {
		name          string
		statuses      []*status.ScaleUpStatus
		exportedResut status.ScaleUpResult
		exportedError errors.AutoscalerError
		returnedError errors.AutoscalerError
	}{
		{
			name:          "empty",
			statuses:      []*status.ScaleUpStatus{},
			exportedResut: status.ScaleUpNotTried,
		},
		{
			name:          "all successful",
			statuses:      generateStatuses(2, status.ScaleUpSuccessful),
			exportedResut: status.ScaleUpSuccessful,
		},
		{
			name:          "all errors",
			statuses:      generateStatuses(2, status.ScaleUpError),
			exportedResut: status.ScaleUpError,
			exportedError: errors.NewAutoscalerError(errors.InternalError, "error 0 ...and other concurrent errors: [\"error 1\"]"),
			returnedError: errors.NewAutoscalerError(errors.InternalError, "error 0 ...and other concurrent errors: [\"error 1\"]"),
		},
		{
			name:          "all no options available",
			statuses:      generateStatuses(2, status.ScaleUpNoOptionsAvailable),
			exportedResut: status.ScaleUpNoOptionsAvailable,
		},
		{
			name:          "error and successful",
			statuses:      append(generateStatuses(1, status.ScaleUpError), generateStatuses(1, status.ScaleUpSuccessful)...),
			exportedResut: status.ScaleUpSuccessful,
			exportedError: errors.NewAutoscalerError(errors.InternalError, "error 0"),
		},
		{
			name:          "error and no options available",
			statuses:      append(generateStatuses(1, status.ScaleUpError), generateStatuses(1, status.ScaleUpNoOptionsAvailable)...),
			exportedResut: status.ScaleUpError,
			exportedError: errors.NewAutoscalerError(errors.InternalError, "error 0"),
			returnedError: errors.NewAutoscalerError(errors.InternalError, "error 0"),
		},
		{
			name:          "successful and no options available",
			statuses:      append(generateStatuses(1, status.ScaleUpSuccessful), generateStatuses(1, status.ScaleUpNoOptionsAvailable)...),
			exportedResut: status.ScaleUpSuccessful,
		},
		{
			name:          "error, successful and no options available",
			statuses:      append(generateStatuses(1, status.ScaleUpNoOptionsAvailable), append(generateStatuses(1, status.ScaleUpError), generateStatuses(1, status.ScaleUpSuccessful)...)...),
			exportedResut: status.ScaleUpSuccessful,
			exportedError: errors.NewAutoscalerError(errors.InternalError, "error 0"),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			combinedStatus := NewCombinedStatusSet()

			for _, s := range tc.statuses {
				combinedStatus.Add(s)
			}

			export, retErr := combinedStatus.Export()

			assert.Equal(t, export.Result, tc.exportedResut)

			if tc.exportedError == nil {
				assert.Nil(t, export.ScaleUpError)
			} else {
				assert.Equal(t, tc.exportedError.Error(), (*export.ScaleUpError).Error())
			}

			if tc.returnedError == nil {
				assert.Nil(t, retErr)
			} else {
				assert.Equal(t, tc.returnedError.Error(), retErr.Error())
			}
		})
	}
}

func generateStatuses(n int, result status.ScaleUpResult) []*status.ScaleUpStatus {
	// generateStatuses generates n statuses with the given result.
	statuses := make([]*status.ScaleUpStatus, n)
	for i := 0; i < n; i++ {
		var scaleUpErr *errors.AutoscalerError

		if result == status.ScaleUpError {
			newErr := errors.NewAutoscalerError(errors.InternalError, fmt.Sprintf("error %d", i))
			scaleUpErr = &newErr
		}

		statuses[i] = &status.ScaleUpStatus{Result: result, ScaleUpError: scaleUpErr}
	}
	return statuses
}

func TestGetProvisioningRequestsAndPodsNonBatchIgnoresUnrelatedRCTPod(t *testing.T) {
	const (
		namespace    = "test-ns"
		templateName = "gpu-template"
	)
	pr := provreqclient.ProvisioningRequestWrapperForTesting(namespace, "test-pr")
	pr.Spec.ProvisioningClassName = provreqv1.ProvisioningClassCheckCapacity
	pr.PodTemplates[0].Template.Spec.ResourceClaims = []corev1.PodResourceClaim{
		{Name: "gpu", ResourceClaimTemplateName: ptr.To(templateName)},
	}
	virtualPods, err := provreqpods.PodsForProvisioningRequest(pr)
	require.NoError(t, err)

	realClaimName := "real-pod-gpu-controller-suffix"
	realPod := testutils.BuildTestPod("real-pod", 100, 100, func(pod *corev1.Pod) {
		pod.Namespace = namespace
	})
	realPod.Spec.ResourceClaims = []corev1.PodResourceClaim{
		{Name: "gpu", ResourceClaimTemplateName: ptr.To(templateName)},
	}
	realPod.Status.ResourceClaimStatuses = []corev1.PodResourceClaimStatus{
		{Name: "gpu", ResourceClaimName: ptr.To(realClaimName)},
	}
	realPodBefore := realPod.DeepCopy()

	template := &resourcev1.ResourceClaimTemplate{ObjectMeta: metav1.ObjectMeta{Name: templateName, Namespace: namespace}}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, indexer.Add(template))
	checkCapacityClass := &checkCapacityProvClass{
		autoscalingCtx: &ca_context.AutoscalingContext{},
		client:         provreqclient.NewFakeProvisioningRequestClient(t.Context(), t, pr),
		simulationWorkloadBuilder: provreqpods.NewSimulationWorkloadBuilder(
			resourcelisters.NewResourceClaimTemplateLister(indexer),
		),
	}

	requests, err := checkCapacityClass.getProvisioningRequestsAndPods(t.Context(), append(virtualPods, realPod))
	require.NoError(t, err)
	require.Len(t, requests, 1)
	assert.Equal(t, pr.Name, requests[0].PrWrapper.Name)
	require.NotNil(t, requests[0].Workload)
	require.Len(t, requests[0].Workload.Pods, len(virtualPods))
	require.Len(t, requests[0].Workload.Claims, len(virtualPods))
	for _, pod := range requests[0].Workload.Pods {
		assert.Equal(t, pr.Name, pod.Annotations[provreqv1.ProvisioningRequestPodAnnotationKey])
		assert.NotEqual(t, realPod.Name, pod.Name)
	}
	assert.Equal(t, realPodBefore, realPod, "the unrelated real Pod was mutated")
}

func TestGetProvisioningRequestsAndPodsNonBatchMarksMaterializationErrorsFailed(t *testing.T) {
	pr := provreqclient.ProvisioningRequestWrapperForTesting("test-ns", "test-pr")
	pr.Spec.ProvisioningClassName = provreqv1.ProvisioningClassCheckCapacity
	pr.PodTemplates[0].Template.Spec.ResourceClaims = []corev1.PodResourceClaim{
		{Name: "gpu", ResourceClaimTemplateName: ptr.To("missing-template")},
	}
	virtualPods, err := provreqpods.PodsForProvisioningRequest(pr)
	require.NoError(t, err)
	client := provreqclient.NewFakeProvisioningRequestClient(t.Context(), t, pr)
	builder := provreqpods.NewSimulationWorkloadBuilder(resourcelisters.NewResourceClaimTemplateLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})))
	checkCapacityClass := New(client, nil, builder)
	checkCapacityClass.autoscalingCtx = &ca_context.AutoscalingContext{}

	requests, err := checkCapacityClass.getProvisioningRequestsAndPods(t.Context(), virtualPods)
	require.NoError(t, err)
	assert.Empty(t, requests, "a request with an invalid simulation workload must be skipped")

	updated, err := client.ProvisioningRequestNoCache(pr.Namespace, pr.Name)
	require.NoError(t, err)
	failed := apimeta.FindStatusCondition(updated.Status.Conditions, provreqv1.Failed)
	require.NotNil(t, failed)
	assert.Equal(t, metav1.ConditionTrue, failed.Status)
	assert.Equal(t, conditions.FailedToCreatePodsReason, failed.Reason)
	assert.Contains(t, failed.Message, "ResourceClaimTemplate test-ns/missing-template")
}

func TestCheckCapacityBatchTreatsSchedulingErrorAsInternalError(t *testing.T) {
	baseSnapshot := testsnapshot.NewTestSnapshotOrDie(t)
	require.NoError(t, baseSnapshot.SetClusterState(t.Context(), nil, nil, nil, nil))
	snapshot := &schedulingInternalErrorSnapshot{ClusterSnapshot: baseSnapshot}
	checkCapacityClass := newCheckCapacityTestClass(snapshot)
	pr := provreqclient.ProvisioningRequestWrapperForTesting("test-ns", "test-pr")
	pr.Spec.Parameters = map[string]provreqv1.Parameter{NoRetryParameterKey: "true"}
	combinedStatus := NewCombinedStatusSet()

	updates := checkCapacityClass.checkCapacityBatch(
		t.Context(),
		[]provreq.ProvisioningRequestWithPods{
			{
				PrWrapper: pr,
				Workload: &provreqpods.SimulationWorkload{
					Pods: []*corev1.Pod{testutils.BuildTestPod("virtual-pod", 100, 100)},
				},
			},
		},
		&combinedStatus,
		time.Now(),
	)

	assert.Empty(t, updates, "transient internal errors must not trigger a ProvisioningRequest update")
	assertNoCapacityMissCondition(t, pr)
	assertInternalScaleUpError(t, &combinedStatus, "scheduling failed")
}

func TestCheckCapacityBatchTreatsCanceledSimulationAsInternalError(t *testing.T) {
	baseSnapshot := testsnapshot.NewTestSnapshotOrDie(t)
	node := testutils.BuildTestNode("node", 10000, 10000, testutils.IsReady(true))
	require.NoError(t, baseSnapshot.SetClusterState(t.Context(), []*corev1.Node{node}, nil, nil, nil))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	snapshot := &cancelAfterFirstScheduleSnapshot{ClusterSnapshot: baseSnapshot, cancel: cancel}
	checkCapacityClass := newCheckCapacityTestClass(snapshot)
	pr := provreqclient.ProvisioningRequestWrapperForTesting("test-ns", "test-pr")
	pr.Spec.Parameters = map[string]provreqv1.Parameter{NoRetryParameterKey: "true"}
	combinedStatus := NewCombinedStatusSet()
	claim := &resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: "simulated-claim", Namespace: "test-ns"}}

	updates := checkCapacityClass.checkCapacityBatch(
		ctx,
		[]provreq.ProvisioningRequestWithPods{
			{
				PrWrapper: pr,
				Workload: &provreqpods.SimulationWorkload{
					Pods: []*corev1.Pod{
						testutils.BuildTestPod("virtual-pod-1", 100, 100),
						testutils.BuildTestPod("virtual-pod-2", 100, 100),
					},
					Claims: []*resourcev1.ResourceClaim{claim},
				},
			},
		},
		&combinedStatus,
		time.Now(),
	)

	assert.Empty(t, updates, "a canceled simulation must not trigger a ProvisioningRequest update")
	assertNoCapacityMissCondition(t, pr)
	assertInternalScaleUpError(t, &combinedStatus, context.Canceled.Error())
	assert.Equal(t, 1, snapshot.scheduled, "the test must cancel after a partial simulation")
	nodeInfos, err := snapshot.ListNodeInfos()
	require.NoError(t, err)
	require.Len(t, nodeInfos, 1)
	assert.Empty(t, nodeInfos[0].Pods(), "a canceled simulation must revert partially scheduled Pods")
	require.NoError(t, snapshot.DraSnapshot().AddClaims([]*resourcev1.ResourceClaim{claim.DeepCopy()}), "a canceled simulation must revert materialized claims")
}

func TestCheckCapacityBatchTreatsAddClaimsErrorAsInternalError(t *testing.T) {
	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	require.NoError(t, snapshot.SetClusterState(t.Context(), nil, nil, nil, nil))
	claim := &resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: "simulated-claim", Namespace: "test-ns"}}
	require.NoError(t, snapshot.DraSnapshot().AddClaims([]*resourcev1.ResourceClaim{claim}))
	checkCapacityClass := newCheckCapacityTestClass(snapshot)
	pr := provreqclient.ProvisioningRequestWrapperForTesting("test-ns", "test-pr")
	pr.Spec.Parameters = map[string]provreqv1.Parameter{NoRetryParameterKey: "true"}
	combinedStatus := NewCombinedStatusSet()

	updates := checkCapacityClass.checkCapacityBatch(
		t.Context(),
		[]provreq.ProvisioningRequestWithPods{
			{
				PrWrapper: pr,
				Workload: &provreqpods.SimulationWorkload{
					Pods:   []*corev1.Pod{testutils.BuildTestPod("virtual-pod", 100, 100)},
					Claims: []*resourcev1.ResourceClaim{claim.DeepCopy()},
				},
			},
		},
		&combinedStatus,
		time.Now(),
	)

	require.Equal(t, []*provreqwrapper.ProvisioningRequest{pr}, updates)
	failed := apimeta.FindStatusCondition(pr.Status.Conditions, provreqv1.Failed)
	require.NotNil(t, failed)
	assert.Equal(t, conditions.FailedToCreatePodsReason, failed.Reason)
	assertNoCapacityMissCondition(t, pr)
	assertInternalScaleUpError(t, &combinedStatus, "already tracked")
}

func TestCheckCapacityAddsMaterializedClaimsBeforeScheduling(t *testing.T) {
	baseSnapshot := testsnapshot.NewTestSnapshotOrDie(t)
	require.NoError(t, baseSnapshot.SetClusterState(t.Context(), nil, nil, nil, nil))
	snapshot := &claimObservingSnapshot{ClusterSnapshot: baseSnapshot}
	checkCapacityClass := newCheckCapacityTestClass(snapshot)
	pr := provreqclient.ProvisioningRequestWrapperForTesting("test-ns", "test-pr")
	combinedStatus := NewCombinedStatusSet()
	claim := &resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: "materialized-claim", Namespace: "test-ns"}}
	pod := testutils.BuildTestPod("virtual-pod", 100, 100, func(pod *corev1.Pod) {
		pod.Namespace = "test-ns"
	}, testutils.WithResourceClaim("device", claim.Name, "device-template"))

	updates := checkCapacityClass.checkCapacityBatch(
		t.Context(),
		[]provreq.ProvisioningRequestWithPods{
			{
				PrWrapper: pr,
				Workload: &provreqpods.SimulationWorkload{
					Pods:   []*corev1.Pod{pod},
					Claims: []*resourcev1.ResourceClaim{claim},
				},
			},
		},
		&combinedStatus,
		time.Now(),
	)

	require.Equal(t, []*provreqwrapper.ProvisioningRequest{pr}, updates)
	assert.True(t, snapshot.observedClaim, "scheduler did not observe the materialized ResourceClaim")
	provisioned := apimeta.FindStatusCondition(pr.Status.Conditions, provreqv1.Provisioned)
	require.NotNil(t, provisioned)
	assert.Equal(t, metav1.ConditionTrue, provisioned.Status)
	assert.Equal(t, conditions.CapacityIsFoundReason, provisioned.Reason)
	exported, err := combinedStatus.Export()
	require.NoError(t, err)
	assert.Equal(t, status.ScaleUpSuccessful, exported.Result)
	assert.ErrorContains(t, snapshot.DraSnapshot().AddClaims([]*resourcev1.ResourceClaim{claim.DeepCopy()}), "already tracked", "successful simulation did not commit the claim")
}

func TestMaterializedWorkloadSchedulesInDraSnapshot(t *testing.T) {
	claimTemplate := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-template", Namespace: "default"},
		Spec: resourcev1.ResourceClaimTemplateSpec{
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{
						{
							Name: "gpu",
							Exactly: &resourcev1.ExactDeviceRequest{
								DeviceClassName: "gpu.example.com",
								AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
								Count:           1,
							},
						},
					},
				},
			},
		},
	}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, indexer.Add(claimTemplate))
	builder := provreqpods.NewSimulationWorkloadBuilder(resourcelisters.NewResourceClaimTemplateLister(indexer))
	pr := provreqclient.ProvisioningRequestWrapperForTesting("default", "test-pr")
	pr.PodTemplates[0].Template.Spec.ResourceClaims = []corev1.PodResourceClaim{
		{Name: "gpu", ResourceClaimTemplateName: ptr.To(claimTemplate.Name)},
	}

	workload, err := builder.ForProvisioningRequest(pr)
	require.NoError(t, err)
	require.Len(t, workload.Claims, 1)

	node := testutils.BuildTestNode("node", 1000, 1000)
	resourceSlice := &resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "node-gpus"},
		Spec: resourcev1.ResourceSliceSpec{
			NodeName: ptr.To(node.Name),
			Driver:   "gpu.example.com",
			Pool: resourcev1.ResourcePool{
				Name:               "node-gpus",
				ResourceSliceCount: 1,
			},
			Devices: []resourcev1.Device{{Name: "gpu-0"}},
		},
	}
	draState := drasnapshot.NewSnapshot(
		nil,
		map[string][]*resourcev1.ResourceSlice{node.Name: {resourceSlice}},
		nil,
		map[string]*resourcev1.DeviceClass{
			"gpu.example.com": {ObjectMeta: metav1.ObjectMeta{Name: "gpu.example.com"}},
		},
	)
	clusterSnapshot := testsnapshot.NewTestSnapshotOrDie(t)
	require.NoError(t, clusterSnapshot.SetClusterState(t.Context(), []*corev1.Node{node}, nil, draState, csisnapshot.NewEmptySnapshot()))

	clusterSnapshot.Fork()
	require.NoError(t, clusterSnapshot.DraSnapshot().AddClaims(workload.Claims))
	require.NoError(t, clusterSnapshot.SchedulePod(workload.Pods[0], node.Name))
	require.NoError(t, clusterSnapshot.Commit())

	claims, err := clusterSnapshot.DraSnapshot().ResourceClaims().List()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	assert.NotNil(t, claims[0].Status.Allocation)
	assert.NotEmpty(t, claims[0].Status.ReservedFor)
}

func TestCheckCapacityPreservesNoCapacityConditions(t *testing.T) {
	tests := []struct {
		name       string
		parameters map[string]provreqv1.Parameter
		wantType   string
		wantStatus metav1.ConditionStatus
	}{
		{
			name:       "retry later",
			wantType:   provreqv1.Provisioned,
			wantStatus: metav1.ConditionFalse,
		},
		{
			name:       "do not retry",
			parameters: map[string]provreqv1.Parameter{NoRetryParameterKey: "true"},
			wantType:   provreqv1.Failed,
			wantStatus: metav1.ConditionTrue,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			require.NoError(t, snapshot.SetClusterState(t.Context(), nil, nil, nil, nil))
			checkCapacityClass := newCheckCapacityTestClass(snapshot)
			pr := provreqclient.ProvisioningRequestWrapperForTesting("test-ns", "test-pr")
			pr.Spec.Parameters = test.parameters
			combinedStatus := NewCombinedStatusSet()

			updates := checkCapacityClass.checkCapacityBatch(
				t.Context(),
				[]provreq.ProvisioningRequestWithPods{
					{
						PrWrapper: pr,
						Workload: &provreqpods.SimulationWorkload{
							Pods: []*corev1.Pod{testutils.BuildTestPod("virtual-pod", 100, 100)},
						},
					},
				},
				&combinedStatus,
				time.Now(),
			)

			require.Equal(t, []*provreqwrapper.ProvisioningRequest{pr}, updates)
			capacityMiss := apimeta.FindStatusCondition(pr.Status.Conditions, test.wantType)
			require.NotNil(t, capacityMiss)
			assert.Equal(t, test.wantStatus, capacityMiss.Status)
			assert.Equal(t, conditions.CapacityIsNotFoundReason, capacityMiss.Reason)
			exported, err := combinedStatus.Export()
			require.NoError(t, err)
			assert.Equal(t, status.ScaleUpNoOptionsAvailable, exported.Result)
			assert.Nil(t, exported.ScaleUpError)
		})
	}
}

func newCheckCapacityTestClass(snapshot clustersnapshot.ClusterSnapshot) *checkCapacityProvClass {
	autoscalingCtx := &ca_context.AutoscalingContext{ClusterSnapshot: snapshot}
	return &checkCapacityProvClass{
		autoscalingCtx:      autoscalingCtx,
		schedulingSimulator: scheduling.NewHintingSimulator(),
	}
}

func assertNoCapacityMissCondition(t *testing.T, pr *provreqwrapper.ProvisioningRequest) {
	t.Helper()
	for _, condition := range pr.Status.Conditions {
		assert.NotEqual(t, conditions.CapacityIsNotFoundReason, condition.Reason)
	}
}

func assertInternalScaleUpError(t *testing.T, combinedStatus *combinedStatusSet, message string) {
	t.Helper()
	exported, err := combinedStatus.Export()
	require.ErrorContains(t, err, message)
	assert.Equal(t, status.ScaleUpError, exported.Result)
	require.NotNil(t, exported.ScaleUpError)
	assert.Contains(t, (*exported.ScaleUpError).Error(), message)
}

type schedulingInternalErrorSnapshot struct {
	clustersnapshot.ClusterSnapshot
}

func (s *schedulingInternalErrorSnapshot) SchedulePodOnAnyNodeMatching(pod *corev1.Pod, _ clustersnapshot.SchedulingOptions) (string, clustersnapshot.SchedulingError) {
	return "", clustersnapshot.NewSchedulingInternalError(pod, "scheduling failed")
}

type cancelAfterFirstScheduleSnapshot struct {
	clustersnapshot.ClusterSnapshot
	cancel    context.CancelFunc
	scheduled int
}

func (s *cancelAfterFirstScheduleSnapshot) SchedulePodOnAnyNodeMatching(pod *corev1.Pod, opts clustersnapshot.SchedulingOptions) (string, clustersnapshot.SchedulingError) {
	nodeName, err := s.ClusterSnapshot.SchedulePodOnAnyNodeMatching(pod, opts)
	if err == nil && nodeName != "" {
		s.scheduled++
		if s.scheduled == 1 {
			s.cancel()
		}
	}
	return nodeName, err
}

type claimObservingSnapshot struct {
	clustersnapshot.ClusterSnapshot
	observedClaim bool
}

func (s *claimObservingSnapshot) SchedulePodOnAnyNodeMatching(pod *corev1.Pod, _ clustersnapshot.SchedulingOptions) (string, clustersnapshot.SchedulingError) {
	claims, err := s.DraSnapshot().PodClaims(pod)
	if err != nil {
		return "", clustersnapshot.NewSchedulingInternalError(pod, fmt.Sprintf("materialized claim not available: %v", err))
	}
	if len(claims) != 1 {
		return "", clustersnapshot.NewSchedulingInternalError(pod, fmt.Sprintf("got %d claims, want 1", len(claims)))
	}
	s.observedClaim = true
	return "test-node", nil
}
