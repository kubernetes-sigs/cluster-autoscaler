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

package provreq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	coretest "sigs.k8s.io/cluster-autoscaler/pkg/core/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/conditions"
	provreqpods "sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const bookingProcessorInstance = "test-instance"

func TestBookCapacityPreservesPriorityAcrossProvisioningRequests(t *testing.T) {
	lowPriorityPr := ordinaryBookingProvisioningRequest("low-priority")
	setBookingPodPriorityAndCPU(lowPriorityPr, 10)
	highPriorityPr := priorityBookingProvisioningRequest("high-priority", 100)
	highPriorityPr.Spec.Parameters = nil
	highPriorityPr.PodTemplates[0].Template.Spec.ResourceClaims = []corev1.PodResourceClaim{
		{Name: "gpu", ResourceClaimTemplateName: ptr.To("gpu-template")},
	}
	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, lowPriorityPr, highPriorityPr)
	autoscalingCtx, err := coretest.NewScaleTestAutoscalingContext(config.AutoscalingOptions{}, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	node := testutils.BuildTestNode("node", 1000, 10000)
	require.NoError(t, autoscalingCtx.ClusterSnapshot.SetClusterState(t.Context(), []*corev1.Node{node}, nil, nil, nil))
	simulator := scheduling.NewHintingSimulator()
	injector := &bookingInjector{schedule: func(snapshot clustersnapshot.ClusterSnapshot, pods []*corev1.Pod) (scheduling.Result, error) {
		return simulator.TrySchedulePods(t.Context(), snapshot, pods, false, clustersnapshot.SchedulingOptions{})
	}}
	processor := &provReqProcessor{
		now:                       time.Now,
		maxUpdated:                20,
		client:                    client,
		injector:                  injector,
		simulationWorkloadBuilder: provreqpods.NewSimulationWorkloadBuilder(bookingTemplateLister(t, bookingTemplate("gpu-template"))),
	}

	require.NoError(t, processor.bookProvisioningRequests(t.Context(), &autoscalingCtx, []*provreqwrapper.ProvisioningRequest{lowPriorityPr, highPriorityPr}))
	assert.Equal(t, 1, injector.calls, "all booking Pods must share one priority-sorted scheduling call")
	nodeInfos, err := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	require.NoError(t, err)
	require.Len(t, nodeInfos, 1)
	require.Len(t, nodeInfos[0].Pods(), 1)
	scheduledPod := nodeInfos[0].Pods()[0].Pod
	assert.Equal(t, highPriorityPr.Name, scheduledPod.Annotations[v1.ProvisioningRequestPodAnnotationKey])
	claims := snapshotClaims(t, &autoscalingCtx)
	require.Len(t, claims, 1)
	require.Len(t, claims[0].OwnerReferences, 1)
	assert.Equal(t, scheduledPod.UID, claims[0].OwnerReferences[0].UID)
}

func TestBookCapacityAddsMaterializedClaimsToSnapshot(t *testing.T) {
	pr := bookingProvisioningRequest("gpu-template", 2)
	injector := &bookingInjector{}
	processor, client, autoscalingCtx := newBookingProcessor(t, pr, bookingTemplateLister(t, bookingTemplate("gpu-template")), injector)
	injector.schedule = func(snapshot clustersnapshot.ClusterSnapshot, pods []*corev1.Pod) (scheduling.Result, error) {
		claims, err := snapshot.DraSnapshot().ResourceClaims().List()
		require.NoError(t, err)
		assert.Len(t, claims, len(pods), "claims must be in the same snapshot transaction before scheduling")
		return allPodsScheduled(pods), nil
	}

	require.NoError(t, processor.bookCapacity(t.Context(), autoscalingCtx))
	assert.Len(t, snapshotClaims(t, autoscalingCtx), 2)
	assert.Equal(t, 1, injector.calls)
	assertProvisioningRequestNotFailed(t, client, pr)
}

func TestBookCapacityKeepsOnlyClaimsForScheduledPodUIDs(t *testing.T) {
	pr := bookingProvisioningRequest("gpu-template", 3)
	injector := &bookingInjector{}
	processor, client, autoscalingCtx := newBookingProcessor(t, pr, bookingTemplateLister(t, bookingTemplate("gpu-template")), injector)
	var scheduledUIDs []string
	injector.schedule = func(_ clustersnapshot.ClusterSnapshot, pods []*corev1.Pod) (scheduling.Result, error) {
		require.Len(t, pods, 3)
		// Return deep copies in a different order than the input. Booking must be
		// matched by immutable Pod UID, not by pointer or slice position.
		scheduledUIDs = []string{string(pods[2].UID), string(pods[0].UID)}
		return scheduling.Result{Statuses: []scheduling.Status{
			{Pod: pods[2].DeepCopy(), NodeName: "node"},
			{Pod: pods[0].DeepCopy(), NodeName: "node"},
		}}, nil
	}

	require.NoError(t, processor.bookCapacity(t.Context(), autoscalingCtx))
	claims := snapshotClaims(t, autoscalingCtx)
	require.Len(t, claims, 2)
	ownerUIDs := make([]string, 0, len(claims))
	for _, claim := range claims {
		require.Len(t, claim.OwnerReferences, 1)
		ownerUIDs = append(ownerUIDs, string(claim.OwnerReferences[0].UID))
	}
	assert.ElementsMatch(t, scheduledUIDs, ownerUIDs)
	assertProvisioningRequestNotFailed(t, client, pr)
}

func TestBookCapacityZeroFitRemovesAllMaterializedClaims(t *testing.T) {
	pr := bookingProvisioningRequest("gpu-template", 2)
	injector := &bookingInjector{schedule: func(_ clustersnapshot.ClusterSnapshot, _ []*corev1.Pod) (scheduling.Result, error) {
		return scheduling.Result{}, nil
	}}
	processor, client, autoscalingCtx := newBookingProcessor(t, pr, bookingTemplateLister(t, bookingTemplate("gpu-template")), injector)

	require.NoError(t, processor.bookCapacity(t.Context(), autoscalingCtx))
	assert.Empty(t, snapshotClaims(t, autoscalingCtx))
	assertProvisioningRequestNotFailed(t, client, pr)
}

func TestBookCapacitySchedulingErrorRevertsMaterializedClaims(t *testing.T) {
	pr := bookingProvisioningRequest("gpu-template", 2)
	injector := &bookingInjector{schedule: func(_ clustersnapshot.ClusterSnapshot, pods []*corev1.Pod) (scheduling.Result, error) {
		return allPodsScheduled(pods[:1]), errors.New("scheduling failed")
	}}
	processor, client, autoscalingCtx := newBookingProcessor(t, pr, bookingTemplateLister(t, bookingTemplate("gpu-template")), injector)

	err := processor.bookCapacity(t.Context(), autoscalingCtx)
	require.ErrorContains(t, err, "scheduling failed")
	assert.Empty(t, snapshotClaims(t, autoscalingCtx))
	assertProvisioningRequestNotFailed(t, client, pr)
}

func TestBookCapacityCancellationRevertsPartialBookingWithClaims(t *testing.T) {
	pr := bookingProvisioningRequest("gpu-template", 2)
	injector := &bookingInjector{}
	processor, client, autoscalingCtx := newBookingProcessor(t, pr, bookingTemplateLister(t, bookingTemplate("gpu-template")), injector)
	node := testutils.BuildTestNode("node", 10000, 10000)
	require.NoError(t, autoscalingCtx.ClusterSnapshot.SetClusterState(t.Context(), []*corev1.Node{node}, nil, nil, nil))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	injector.schedule = func(snapshot clustersnapshot.ClusterSnapshot, pods []*corev1.Pod) (scheduling.Result, error) {
		require.Len(t, pods, 2)
		require.NoError(t, snapshot.SchedulePod(pods[0], node.Name))
		cancel()
		result := allPodsScheduled(pods[:1])
		result.UnprocessedPods = pods[1:]
		return result, nil
	}

	err := processor.bookCapacity(ctx, autoscalingCtx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, snapshotClaims(t, autoscalingCtx), "a canceled booking must revert materialized claims")
	nodeInfos, listErr := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	require.NoError(t, listErr)
	require.Len(t, nodeInfos, 1)
	assert.Empty(t, nodeInfos[0].Pods(), "a canceled booking must revert partially scheduled Pods")
	assertProvisioningRequestNotFailed(t, client, pr)
}

func TestBookCapacitySchedulingErrorRevertsAggregateBooking(t *testing.T) {
	ordinaryPr := ordinaryBookingProvisioningRequest("ordinary-pr")
	checkCapacityPr := bookingProvisioningRequest("gpu-template", 1)
	checkCapacityPr.Spec.Parameters = nil
	injector := &bookingInjector{}
	processor, _, autoscalingCtx := newBookingProcessorForRequests(t, []*provreqwrapper.ProvisioningRequest{ordinaryPr, checkCapacityPr}, bookingTemplateLister(t, bookingTemplate("gpu-template")), injector)
	processor.checkCapacityProcessorInstance = ""
	node := testutils.BuildTestNode("node", 10000, 10000)
	require.NoError(t, autoscalingCtx.ClusterSnapshot.SetClusterState(t.Context(), []*corev1.Node{node}, nil, nil, nil))

	injector.schedule = func(snapshot clustersnapshot.ClusterSnapshot, pods []*corev1.Pod) (scheduling.Result, error) {
		require.Len(t, pods, 2)
		assert.Equal(t, ordinaryPr.Name, pods[0].Annotations[v1.ProvisioningRequestPodAnnotationKey])
		assert.Equal(t, checkCapacityPr.Name, pods[1].Annotations[v1.ProvisioningRequestPodAnnotationKey])
		claims, err := snapshot.DraSnapshot().ResourceClaims().List()
		require.NoError(t, err)
		require.Len(t, claims, 1)
		require.NoError(t, snapshot.SchedulePod(pods[0], node.Name))
		return allPodsScheduled(pods[:1]), errors.New("aggregate booking failed")
	}

	err := processor.bookProvisioningRequests(t.Context(), autoscalingCtx, []*provreqwrapper.ProvisioningRequest{ordinaryPr, checkCapacityPr})
	require.ErrorContains(t, err, "aggregate booking failed")
	nodeInfos, err := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	require.NoError(t, err)
	require.Len(t, nodeInfos, 1)
	assert.Empty(t, nodeInfos[0].Pods(), "failed aggregate booking must revert partially scheduled Pods")
	assert.Empty(t, snapshotClaims(t, autoscalingCtx), "failed aggregate booking must revert materialized claims")
	assert.Equal(t, 1, injector.calls)
}

func TestBookCapacityCancellationRevertsPartialOrdinaryBooking(t *testing.T) {
	pr := ordinaryBookingProvisioningRequest("ordinary-pr")
	pr.Spec.PodSets[0].Count = 2
	injector := &bookingInjector{}
	processor, _, autoscalingCtx := newBookingProcessor(t, pr, bookingTemplateLister(t), injector)
	processor.checkCapacityProcessorInstance = ""
	node := testutils.BuildTestNode("node", 10000, 10000)
	require.NoError(t, autoscalingCtx.ClusterSnapshot.SetClusterState(t.Context(), []*corev1.Node{node}, nil, nil, nil))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	injector.schedule = func(snapshot clustersnapshot.ClusterSnapshot, pods []*corev1.Pod) (scheduling.Result, error) {
		require.NoError(t, snapshot.SchedulePod(pods[0], node.Name))
		cancel()
		result := allPodsScheduled(pods[:1])
		result.UnprocessedPods = pods[1:]
		return result, nil
	}

	err := processor.bookCapacity(ctx, autoscalingCtx)
	require.ErrorIs(t, err, context.Canceled)
	nodeInfos, listErr := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	require.NoError(t, listErr)
	require.Len(t, nodeInfos, 1)
	assert.Empty(t, nodeInfos[0].Pods(), "a canceled booking must revert partially scheduled Pods")
}

func TestBookCapacityCommitsSuccessfulOrdinaryScheduling(t *testing.T) {
	pr := ordinaryBookingProvisioningRequest("ordinary-pr")
	injector := &bookingInjector{}
	processor, _, autoscalingCtx := newBookingProcessor(t, pr, bookingTemplateLister(t), injector)
	processor.checkCapacityProcessorInstance = ""
	node := testutils.BuildTestNode("node", 10000, 10000)
	require.NoError(t, autoscalingCtx.ClusterSnapshot.SetClusterState(t.Context(), []*corev1.Node{node}, nil, nil, nil))
	injector.schedule = func(snapshot clustersnapshot.ClusterSnapshot, pods []*corev1.Pod) (scheduling.Result, error) {
		require.NoError(t, snapshot.SchedulePod(pods[0], node.Name))
		return allPodsScheduled(pods), nil
	}

	require.NoError(t, processor.bookCapacity(t.Context(), autoscalingCtx))
	nodeInfos, err := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	require.NoError(t, err)
	require.Len(t, nodeInfos, 1)
	require.Len(t, nodeInfos[0].Pods(), 1)
	assert.Equal(t, pr.Name, nodeInfos[0].Pods()[0].Pod.Annotations[v1.ProvisioningRequestPodAnnotationKey])
}

func TestBookCapacityClaimCollisionPreservesSnapshotAndMarksProvisioningRequestFailed(t *testing.T) {
	pr := bookingProvisioningRequest("gpu-template", 1)
	injector := &bookingInjector{}
	processor, client, autoscalingCtx := newBookingProcessor(t, pr, bookingTemplateLister(t, bookingTemplate("gpu-template")), injector)
	workload, err := processor.simulationWorkloadBuilder.ForProvisioningRequest(pr)
	require.NoError(t, err)
	require.Len(t, workload.Claims, 1)
	existingClaim := workload.Claims[0].DeepCopy()
	require.NoError(t, autoscalingCtx.ClusterSnapshot.DraSnapshot().AddClaims([]*resourcev1.ResourceClaim{existingClaim}))

	require.NoError(t, processor.bookCapacity(t.Context(), autoscalingCtx))
	require.Equal(t, []*resourcev1.ResourceClaim{existingClaim}, snapshotClaims(t, autoscalingCtx))
	assert.Zero(t, injector.calls, "claim collision must fail before scheduling")
	failed := apimeta.FindStatusCondition(getProvisioningRequest(t, client, pr).Status.Conditions, v1.Failed)
	require.NotNil(t, failed)
	assert.Equal(t, conditions.FailedToBookCapacityReason, failed.Reason)
	assert.Contains(t, failed.Message, existingClaim.Name)
}

func TestBookCapacityClaimCollisionSkipsOnlyAffectedProvisioningRequest(t *testing.T) {
	collidingPr := bookingProvisioningRequest("gpu-template", 1)
	validPr := bookingProvisioningRequest("gpu-template", 1)
	validPr.Name = "valid-pr"
	validPr.Spec.PodSets[0].PodTemplateRef.Name = "valid-pr-pod-template"
	validPr.PodTemplates[0].Name = "valid-pr-pod-template"
	injector := &bookingInjector{}
	processor, client, autoscalingCtx := newBookingProcessorForRequests(
		t,
		[]*provreqwrapper.ProvisioningRequest{collidingPr, validPr},
		bookingTemplateLister(t, bookingTemplate("gpu-template")),
		injector,
	)
	collidingWorkload, err := processor.simulationWorkloadBuilder.ForProvisioningRequest(collidingPr)
	require.NoError(t, err)
	require.Len(t, collidingWorkload.Claims, 1)
	existingClaim := collidingWorkload.Claims[0].DeepCopy()
	require.NoError(t, autoscalingCtx.ClusterSnapshot.DraSnapshot().AddClaims([]*resourcev1.ResourceClaim{existingClaim}))

	require.NoError(t, processor.bookProvisioningRequests(t.Context(), autoscalingCtx, []*provreqwrapper.ProvisioningRequest{collidingPr, validPr}))
	require.Equal(t, 1, injector.calls)
	require.Len(t, injector.lastPods, 1)
	assert.Equal(t, validPr.Name, injector.lastPods[0].Annotations[v1.ProvisioningRequestPodAnnotationKey])
	assert.Len(t, snapshotClaims(t, autoscalingCtx), 2, "the existing claim and the valid request's claim must remain")
	failed := apimeta.FindStatusCondition(getProvisioningRequest(t, client, collidingPr).Status.Conditions, v1.Failed)
	require.NotNil(t, failed)
	assert.Equal(t, conditions.FailedToBookCapacityReason, failed.Reason)
	assertProvisioningRequestNotFailed(t, client, validPr)
}

func TestBookCapacityMissingTemplateMarksProvisioningRequestFailed(t *testing.T) {
	pr := bookingProvisioningRequest("missing-template", 2)
	injector := &bookingInjector{}
	processor, client, autoscalingCtx := newBookingProcessor(t, pr, bookingTemplateLister(t), injector)

	require.NoError(t, processor.bookCapacity(t.Context(), autoscalingCtx))
	assert.Empty(t, snapshotClaims(t, autoscalingCtx))
	assert.Zero(t, injector.calls)
	updated := getProvisioningRequest(t, client, pr)
	failed := apimeta.FindStatusCondition(updated.Status.Conditions, v1.Failed)
	require.NotNil(t, failed)
	assert.Equal(t, conditions.FailedToBookCapacityReason, failed.Reason)
	assert.Contains(t, failed.Message, "missing-template")
}

func TestBookCapacityConsumedRequestSkipsMissingTemplateMaterialization(t *testing.T) {
	pr := bookingProvisioningRequest("missing-template", 2)
	injector := &bookingInjector{}
	processor, client, autoscalingCtx := newBookingProcessor(t, pr, bookingTemplateLister(t), injector)
	node := testutils.BuildTestNode("node", 10000, 10000)
	scheduledPods := make([]*corev1.Pod, 0, pr.PodCount())
	for i := 0; i < pr.PodCount(); i++ {
		pod := testutils.BuildTestPod(fmt.Sprintf("consumer-%d", i), 100, 100, func(pod *corev1.Pod) {
			pod.Namespace = pr.Namespace
			pod.Spec.NodeName = node.Name
		})
		pod.Annotations[v1.ProvisioningRequestPodAnnotationKey] = pr.Name
		scheduledPods = append(scheduledPods, pod)
	}
	require.NoError(t, autoscalingCtx.ClusterSnapshot.SetClusterState(t.Context(), []*corev1.Node{node}, scheduledPods, nil, nil))

	require.NoError(t, processor.bookCapacity(t.Context(), autoscalingCtx))
	assert.Empty(t, snapshotClaims(t, autoscalingCtx))
	assert.Zero(t, injector.calls, "a consumed booking must be detected before materialization")
	updated := getProvisioningRequest(t, client, pr)
	expired := apimeta.FindStatusCondition(updated.Status.Conditions, v1.BookingExpired)
	require.NotNil(t, expired)
	assert.Equal(t, metav1.ConditionTrue, expired.Status)
	assert.Equal(t, conditions.CapacityBookingConsumedReason, expired.Reason)
	assert.Nil(t, apimeta.FindStatusCondition(updated.Status.Conditions, v1.Failed))
}

type bookingInjector struct {
	calls    int
	lastPods []*corev1.Pod
	schedule func(clustersnapshot.ClusterSnapshot, []*corev1.Pod) (scheduling.Result, error)
}

func (i *bookingInjector) TrySchedulePods(_ context.Context, snapshot clustersnapshot.ClusterSnapshot, pods []*corev1.Pod, _ bool, _ clustersnapshot.SchedulingOptions) (scheduling.Result, error) {
	i.calls++
	i.lastPods = pods
	if i.schedule != nil {
		return i.schedule(snapshot, pods)
	}
	return allPodsScheduled(pods), nil
}

func allPodsScheduled(pods []*corev1.Pod) scheduling.Result {
	statuses := make([]scheduling.Status, 0, len(pods))
	for _, pod := range pods {
		statuses = append(statuses, scheduling.Status{Pod: pod, NodeName: "node"})
	}
	return scheduling.Result{Statuses: statuses}
}

func bookingProvisioningRequest(templateName string, count int32) *provreqwrapper.ProvisioningRequest {
	pr := provreqclient.ProvisioningRequestWrapperForTesting("ns", "test-pr")
	pr.Spec.ProvisioningClassName = v1.ProvisioningClassCheckCapacity
	pr.Spec.Parameters = map[string]v1.Parameter{
		provisioningrequest.CheckCapacityProcessorInstanceKey: bookingProcessorInstance,
	}
	pr.Spec.PodSets[0].Count = count
	pr.PodTemplates[0].Template.Spec.ResourceClaims = []corev1.PodResourceClaim{
		{Name: "gpu", ResourceClaimTemplateName: ptr.To(templateName)},
	}
	conditions.AddOrUpdateCondition(context.Background(), pr, v1.Provisioned, metav1.ConditionTrue, conditions.CapacityIsFoundReason, conditions.CapacityIsFoundMsg, metav1.Now())
	return pr
}

func ordinaryBookingProvisioningRequest(name string) *provreqwrapper.ProvisioningRequest {
	pr := provreqclient.ProvisioningRequestWrapperForTesting("ns", name)
	pr.Spec.ProvisioningClassName = v1.ProvisioningClassBestEffortAtomicScaleUp
	conditions.AddOrUpdateCondition(context.Background(), pr, v1.Provisioned, metav1.ConditionTrue, conditions.CapacityIsFoundReason, conditions.CapacityIsFoundMsg, metav1.Now())
	return pr
}

func priorityBookingProvisioningRequest(name string, priority int32) *provreqwrapper.ProvisioningRequest {
	pr := provreqclient.ProvisioningRequestWrapperForTesting("ns", name)
	pr.Spec.ProvisioningClassName = v1.ProvisioningClassCheckCapacity
	pr.Spec.Parameters = map[string]v1.Parameter{
		provisioningrequest.CheckCapacityProcessorInstanceKey: bookingProcessorInstance,
	}
	setBookingPodPriorityAndCPU(pr, priority)
	conditions.AddOrUpdateCondition(context.Background(), pr, v1.Provisioned, metav1.ConditionTrue, conditions.CapacityIsFoundReason, conditions.CapacityIsFoundMsg, metav1.Now())
	return pr
}

func setBookingPodPriorityAndCPU(pr *provreqwrapper.ProvisioningRequest, priority int32) {
	pr.PodTemplates[0].Template.Spec.Priority = ptr.To(priority)
	pr.PodTemplates[0].Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("1"),
	}
}

func bookingTemplate(name string) *resourcev1.ResourceClaimTemplate {
	return &resourcev1.ResourceClaimTemplate{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}}
}

func bookingTemplateLister(t *testing.T, templates ...*resourcev1.ResourceClaimTemplate) resourcelisters.ResourceClaimTemplateLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, template := range templates {
		require.NoError(t, indexer.Add(template))
	}
	return resourcelisters.NewResourceClaimTemplateLister(indexer)
}

func newBookingProcessor(t *testing.T, pr *provreqwrapper.ProvisioningRequest, templateLister resourcelisters.ResourceClaimTemplateLister, injector *bookingInjector) (*provReqProcessor, *provreqclient.ProvisioningRequestClient, *ca_context.AutoscalingContext) {
	t.Helper()
	return newBookingProcessorForRequests(t, []*provreqwrapper.ProvisioningRequest{pr}, templateLister, injector)
}

func newBookingProcessorForRequests(t *testing.T, prs []*provreqwrapper.ProvisioningRequest, templateLister resourcelisters.ResourceClaimTemplateLister, injector *bookingInjector) (*provReqProcessor, *provreqclient.ProvisioningRequestClient, *ca_context.AutoscalingContext) {
	t.Helper()
	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, prs...)
	autoscalingCtx, err := coretest.NewScaleTestAutoscalingContext(config.AutoscalingOptions{}, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	return &provReqProcessor{
		now:                            time.Now,
		maxUpdated:                     20,
		client:                         client,
		injector:                       injector,
		checkCapacityProcessorInstance: bookingProcessorInstance,
		simulationWorkloadBuilder:      provreqpods.NewSimulationWorkloadBuilder(templateLister),
	}, client, &autoscalingCtx
}

func snapshotClaims(t *testing.T, autoscalingCtx *ca_context.AutoscalingContext) []*resourcev1.ResourceClaim {
	t.Helper()
	claims, err := autoscalingCtx.ClusterSnapshot.DraSnapshot().ResourceClaims().List()
	require.NoError(t, err)
	return claims
}

func getProvisioningRequest(t *testing.T, client *provreqclient.ProvisioningRequestClient, pr *provreqwrapper.ProvisioningRequest) *provreqwrapper.ProvisioningRequest {
	t.Helper()
	updated, err := client.ProvisioningRequestNoCache(pr.Namespace, pr.Name)
	require.NoError(t, err)
	return updated
}

func assertProvisioningRequestNotFailed(t *testing.T, client *provreqclient.ProvisioningRequestClient, pr *provreqwrapper.ProvisioningRequest) {
	t.Helper()
	updated := getProvisioningRequest(t, client, pr)
	assert.Nil(t, apimeta.FindStatusCondition(updated.Status.Conditions, v1.Failed))
	assert.True(t, apimeta.IsStatusConditionTrue(updated.Status.Conditions, v1.Provisioned))
}
