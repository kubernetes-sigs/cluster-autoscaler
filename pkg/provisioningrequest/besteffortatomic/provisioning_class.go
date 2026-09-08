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

package besteffortatomic

import (
	"context"
	"fmt"
	"strings"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"

	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	v1ac "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/applyconfiguration/autoscaling.x-k8s.io/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"

	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/orchestrator"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupset"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/conditions"
	provreqpods "sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"

	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
)

// fieldManager is the field manager used when server-side applying ProvisioningRequest conditions.
const fieldManager = "cluster-autoscaler"

// Best effort atomic provisionig class requests scale-up only if it's possible
// to atomically request enough resources for all pods specified in a
// ProvisioningRequest. It's "best effort" as it admits workload immediately
// after successful request, without waiting to verify that resources started.
type bestEffortAtomicProvClass struct {
	autoscalingCtx       *ca_context.AutoscalingContext
	client               *provreqclient.ProvisioningRequestClient
	injector             *scheduling.HintingSimulator
	scaleUpOrchestrator  scaleup.Orchestrator
	batchProcessing      bool
	maxConcurrentUpdates int
}

// New creates best effort atomic provisioning class supporting create capacity scale-up mode.
func New(
	client *provreqclient.ProvisioningRequestClient,
) *bestEffortAtomicProvClass {
	return &bestEffortAtomicProvClass{client: client, scaleUpOrchestrator: orchestrator.New()}
}

func (o *bestEffortAtomicProvClass) Initialize(
	autoscalingCtx *ca_context.AutoscalingContext,
	processors *ca_processors.AutoscalingProcessors,
	clusterStateRegistry *clusterstate.ClusterStateRegistry,
	estimatorBuilder estimator.EstimatorBuilder,
	taintConfig taints.TaintConfig,
	injector *scheduling.HintingSimulator,
	quotasTrackerFactory *resourcequotas.TrackerFactory,
) {
	o.autoscalingCtx = autoscalingCtx
	o.injector = injector
	o.batchProcessing = autoscalingCtx.BestEffortAtomicBatchProcessing && autoscalingCtx.BestEffortAtomicProvisioningRequestMaxBatchSize > 1
	o.maxConcurrentUpdates = max(1, autoscalingCtx.KubeClientOpts.KubeClientBurst)
	o.scaleUpOrchestrator.Initialize(autoscalingCtx, processors, clusterStateRegistry, estimatorBuilder, taintConfig, quotasTrackerFactory)
}

// Provision returns success if there is, or has just been requested, sufficient capacity in the cluster for pods from ProvisioningRequest.
//
// When batch processing is disabled, exactly one ProvisioningRequest is handled per iteration.
// When it is enabled, all ProvisioningRequests whose pods the injector added to
// unschedulablePods are flattened into one all-or-nothing scale-up calculation. This allows
// compatible requests to be coalesced into a single infrastructure resize. If only some
// resizes succeed, whole requests are admitted oldest-first against the successful capacity.
func (o *bestEffortAtomicProvClass) Provision(
	ctx context.Context,
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	if len(unschedulablePods) == 0 {
		return &status.ScaleUpStatus{Result: status.ScaleUpNotTried}, nil
	}
	prs := provreqclient.ProvisioningRequestsForPods(ctx, o.client, unschedulablePods)
	prs = provreqclient.FilterOutProvisioningClass(ctx, prs, v1.ProvisioningClassBestEffortAtomicScaleUp, "")
	if len(prs) == 0 {
		return &status.ScaleUpStatus{Result: status.ScaleUpNotTried}, nil
	}
	// ProvisioningRequestsForPods returns requests in map iteration order. Sort them so that a
	// batch is processed oldest-first and the outcome of an iteration is reproducible.
	provreqwrapper.SortProvisioningRequests(prs)

	o.autoscalingCtx.ClusterSnapshot.Fork()
	defer o.autoscalingCtx.ClusterSnapshot.Revert()

	if !o.batchProcessing {
		// Pick 1 ProvisioningRequest.
		return o.provisionRequests(ctx, prs[:1], unschedulablePods, nodes, daemonSets, nodeInfos)
	}

	return o.provisionBatch(ctx, prs, unschedulablePods, nodes, daemonSets, nodeInfos)
}

// provisionBatch flattens all pods from the selected ProvisioningRequests into one atomic
// capacity plan, preserving per-request admission when execution only partially succeeds.
func (o *bestEffortAtomicProvClass) provisionBatch(
	ctx context.Context,
	prs []*provreqwrapper.ProvisioningRequest,
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	logger := klog.FromContext(ctx)
	logger.Info("Processing best-effort-atomic provisioning requests as one flattened batch", "batchSize", len(prs), "podsCount", len(unschedulablePods))

	return o.provisionRequests(ctx, prs, unschedulablePods, nodes, daemonSets, nodeInfos)
}

// provisionRequests runs one all-or-nothing scale-up for all supplied ProvisioningRequests and
// admits requests supported by the resulting capacity.
func (o *bestEffortAtomicProvClass) provisionRequests(
	ctx context.Context,
	prs []*provreqwrapper.ProvisioningRequest,
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {

	// For provisioning requests, unschedulablePods are actually all injected pods. Some may even be schedulable!
	snapshot := o.autoscalingCtx.ClusterSnapshot
	snapshot.Fork()
	actuallyUnschedulablePods, err := o.filterOutSchedulable(ctx, unschedulablePods)
	if err != nil {
		snapshot.Revert()
		_ = o.updateConditions(ctx, prs, v1.Provisioned, metav1.ConditionFalse, conditions.FailedToCheckCapacityReason, conditions.FailedToCheckCapacityMsg)
		st, aErr := status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "error during ScaleUp: %s", err.Error()))
		return st, aErr
	}

	if len(actuallyUnschedulablePods) == 0 {
		snapshot.Revert()
		// Nothing to do here - everything fits without scale-up.
		if updateErr := o.updateConditions(ctx, prs, v1.Provisioned, metav1.ConditionTrue, conditions.CapacityIsFoundReason, conditions.CapacityIsFoundMsg); updateErr != nil {
			st, aErr := status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "capacity available, but failed to admit ProvisioningRequest batch: %s", updateErr.Error()))
			return st, aErr
		}
		return &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}, nil
	}

	st, scaleUpErr := o.scaleUpOrchestrator.ScaleUp(ctx, actuallyUnschedulablePods, nodes, daemonSets, nodeInfos, true)
	snapshot.Revert()
	if scaleUpErr == nil && st.Result == status.ScaleUpSuccessful {
		// The capacity has already been requested from the cloud provider, so every request in
		// this all-or-nothing batch is admitted together.
		if updateErr := o.updateConditions(ctx, prs, v1.Provisioned, metav1.ConditionTrue, conditions.CapacityIsProvisionedReason, conditions.CapacityIsProvisionedMsg); updateErr != nil {
			return st, errors.NewAutoscalerErrorf(errors.InternalError, "scale up requested, but failed to admit ProvisioningRequest batch: %s", updateErr.Error())
		}
		return st, nil
	}

	if o.batchProcessing && st != nil && len(st.ScaleUpInfos) > 0 {
		if admissionErr := o.admitPartialBatch(ctx, prs, st.ScaleUpInfos, nodeInfos); admissionErr != nil {
			return st, errors.NewAutoscalerErrorf(errors.InternalError, "partial scale up, but failed to admit ProvisioningRequests: %v; scale-up error: %v", admissionErr, scaleUpErr)
		}
		return st, scaleUpErr
	}

	// No requested capacity was confirmed. Give every ProvisioningRequest a retryable outcome.
	_ = o.updateConditions(ctx, prs, v1.Provisioned, metav1.ConditionFalse, conditions.CapacityIsNotFoundReason, "Capacity is not found for the ProvisioningRequest batch, CA will try to find it later.")
	if scaleUpErr != nil {
		errStatus, aErr := status.UpdateScaleUpError(st, errors.NewAutoscalerErrorf(errors.InternalError, "error during ScaleUp: %s", scaleUpErr.Error()))
		return errStatus, aErr
	}
	return st, nil
}

func (o *bestEffortAtomicProvClass) admitPartialBatch(
	ctx context.Context,
	prs []*provreqwrapper.ProvisioningRequest,
	successfulScaleUps []nodegroupset.ScaleUpInfo,
	nodeInfos map[string]*framework.NodeInfo,
) error {
	snapshot := o.autoscalingCtx.ClusterSnapshot
	snapshot.Fork()
	defer snapshot.Revert()

	for groupIndex, scaleUpInfo := range successfulScaleUps {
		template, found := nodeInfos[scaleUpInfo.Group.Id()]
		if !found {
			return fmt.Errorf("no node template for successful scale-up of %s", scaleUpInfo.Group.Id())
		}
		for nodeIndex := 0; nodeIndex < scaleUpInfo.NewSize-scaleUpInfo.CurrentSize; nodeIndex++ {
			nodeInfo, err := simulator.SanitizedNodeInfo(ctx, template, fmt.Sprintf("provreq-%d-%d", groupIndex, nodeIndex))
			if err != nil {
				return err
			}
			if err := snapshot.AddNodeInfo(nodeInfo); err != nil {
				return err
			}
		}
	}

	var admitted, unfulfilled []*provreqwrapper.ProvisioningRequest
	var failures []string
	for _, request := range prs {
		requestPods, err := provreqpods.PodsForProvisioningRequest(request)
		if err != nil {
			unfulfilled = append(unfulfilled, request)
			failures = append(failures, err.Error())
			continue
		}
		snapshot.Fork()
		result, err := o.injector.TrySchedulePods(ctx, snapshot, requestPods, true, clustersnapshot.SchedulingOptions{})
		if err == nil && len(result.Statuses) == len(requestPods) {
			if err = snapshot.Commit(); err == nil {
				admitted = append(admitted, request)
				continue
			}
		}
		snapshot.Revert()
		unfulfilled = append(unfulfilled, request)
		if err != nil {
			failures = append(failures, err.Error())
		}
	}
	if err := o.updateConditions(ctx, admitted, v1.Provisioned, metav1.ConditionTrue, conditions.CapacityIsProvisionedReason, conditions.CapacityIsProvisionedMsg); err != nil {
		failures = append(failures, err.Error())
	}
	if err := o.updateConditions(ctx, unfulfilled, v1.Provisioned, metav1.ConditionFalse, conditions.CapacityIsNotFoundReason, "Capacity is not found for the ProvisioningRequest, CA will try to find it later."); err != nil {
		failures = append(failures, err.Error())
	}
	if len(failures) > 0 {
		return fmt.Errorf("failed to admit partial ProvisioningRequest batch: %s", strings.Join(failures, "; "))
	}
	return nil
}

func (o *bestEffortAtomicProvClass) updateCondition(
	ctx context.Context,
	pr *provreqwrapper.ProvisioningRequest,
	conditionType string,
	conditionStatus metav1.ConditionStatus,
	reason string,
	message string,
) error {
	logger := klog.FromContext(ctx)
	prAC := v1ac.ProvisioningRequest(pr.Name, pr.Namespace)
	condition := metav1ac.Condition().
		WithType(conditionType).
		WithStatus(conditionStatus).
		WithReason(reason).
		WithMessage(message).
		WithLastTransitionTime(metav1.Now())
	prAC.WithStatus(v1ac.ProvisioningRequestStatus().WithConditions(condition))
	if _, err := o.client.ApplyProvisioningRequest(prAC, fieldManager); err != nil {
		logger.Error(err, "failed to add condition to ProvReq", "provReq", klog.KObj(pr), "conditionType", conditionType, "conditionStatus", conditionStatus)
		return err
	}
	return nil
}

// updateConditions applies the same condition to every ProvisioningRequest in a flattened batch.
// API calls are concurrent but bounded so large batches don't exhaust client-side rate limits.
func (o *bestEffortAtomicProvClass) updateConditions(
	ctx context.Context,
	prs []*provreqwrapper.ProvisioningRequest,
	conditionType string,
	conditionStatus metav1.ConditionStatus,
	reason string,
	message string,
) error {
	if len(prs) == 0 {
		return nil
	}

	updateErrors := make([]error, len(prs))
	semaphore := make(chan struct{}, min(o.maxConcurrentUpdates, len(prs)))
	var wg sync.WaitGroup
	for i, pr := range prs {
		wg.Add(1)
		go func(index int, request *provreqwrapper.ProvisioningRequest) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			updateErrors[index] = o.updateCondition(ctx, request, conditionType, conditionStatus, reason, message)
		}(i, pr)
	}
	wg.Wait()

	var failures []string
	for i, updateErr := range updateErrors {
		if updateErr != nil {
			failures = append(failures, fmt.Sprintf("%s/%s: %v", prs[i].Namespace, prs[i].Name, updateErr))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("failed to update %s condition for %d of %d ProvisioningRequests: %s", conditionType, len(failures), len(prs), strings.Join(failures, "; "))
	}
	return nil
}

func (o *bestEffortAtomicProvClass) filterOutSchedulable(ctx context.Context, pods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	schedulingResult, err := o.injector.TrySchedulePods(ctx, o.autoscalingCtx.ClusterSnapshot, pods, false, clustersnapshot.SchedulingOptions{})
	if err != nil {
		return nil, err
	}

	scheduledPods := make(map[types.UID]bool)
	for _, status := range schedulingResult.Statuses {
		scheduledPods[status.Pod.UID] = true
	}

	var unschedulablePods []*apiv1.Pod
	for _, pod := range pods {
		if !scheduledPods[pod.UID] {
			unschedulablePods = append(unschedulablePods, pod)
		}
	}
	return unschedulablePods, nil

}
