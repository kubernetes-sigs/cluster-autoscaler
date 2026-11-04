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

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"

	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	v1ac "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/applyconfiguration/autoscaling.x-k8s.io/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"

	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/orchestrator"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/conditions"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"

	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
)

const (
	// WaitForNodesReadyParameterKey is a key for ProvReq's Parameters that describes
	// if Provisioned=True should only be set once the requested capacity is on ready nodes.
	// Supported values are "true" and "false" - by default Provisioned=True is set as soon
	// as the scale-up request succeeds.
	WaitForNodesReadyParameterKey = "waitForNodesReady"
)

// Best effort atomic provisionig class requests scale-up only if it's possible
// to atomically request enough resources for all pods specified in a
// ProvisioningRequest. It's "best effort" as it admits workload immediately
// after successful request, without waiting to verify that resources started,
// unless the ProvisioningRequest sets WaitForNodesReadyParameterKey.
type bestEffortAtomicProvClass struct {
	autoscalingCtx      *ca_context.AutoscalingContext
	client              *provreqclient.ProvisioningRequestClient
	injector            *scheduling.HintingSimulator
	scaleUpOrchestrator scaleup.Orchestrator
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
	o.scaleUpOrchestrator.Initialize(autoscalingCtx, processors, clusterStateRegistry, estimatorBuilder, taintConfig, quotasTrackerFactory)
}

// Provision returns success if there is, or has just been requested, sufficient capacity in the cluster for pods from ProvisioningRequest.
func (o *bestEffortAtomicProvClass) Provision(
	ctx context.Context,
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	logger := klog.FromContext(ctx)
	if len(unschedulablePods) == 0 {
		return &status.ScaleUpStatus{Result: status.ScaleUpNotTried}, nil
	}
	prs := provreqclient.ProvisioningRequestsForPods(ctx, o.client, unschedulablePods)
	prs = provreqclient.FilterOutProvisioningClass(ctx, prs, v1.ProvisioningClassBestEffortAtomicScaleUp, "")
	if len(prs) == 0 {
		return &status.ScaleUpStatus{Result: status.ScaleUpNotTried}, nil
	}
	// Pick 1 ProvisioningRequest.
	pr := prs[0]
	waitForNodesReady := shouldWaitForNodesReady(ctx, pr)

	o.autoscalingCtx.ClusterSnapshot.Fork()
	defer o.autoscalingCtx.ClusterSnapshot.Revert()

	if waitForNodesReady {
		fits, err := o.fitsOnStartedNodes(ctx, unschedulablePods)
		if err != nil {
			o.applyProvisionedCondition(ctx, pr, metav1.ConditionFalse, conditions.FailedToCheckCapacityReason, conditions.FailedToCheckCapacityMsg)
			return status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "error during ScaleUp: %s", err.Error()))
		}
		if fits {
			reason, message := conditions.CapacityIsFoundReason, conditions.CapacityIsFoundMsg
			if isWaitingForNodes(pr) {
				reason, message = conditions.CapacityIsProvisionedReason, conditions.CapacityIsProvisionedMsg
			}
			if updateErr := o.applyProvisionedCondition(ctx, pr, metav1.ConditionTrue, reason, message); updateErr != nil {
				return status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "capacity available, but failed to admit workload: %s", updateErr.Error()))
			}
			return &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}, nil
		}
	}

	// For provisioning requests, unschedulablePods are actually all injected pods. Some may even be schedulable!
	actuallyUnschedulablePods, err := o.filterOutSchedulable(unschedulablePods)
	if err != nil {
		prAC := v1ac.ProvisioningRequest(pr.Name, pr.Namespace)
		condition := metav1ac.Condition().
			WithType(v1.Provisioned).
			WithStatus(metav1.ConditionFalse).
			WithReason(conditions.FailedToCheckCapacityReason).
			WithMessage(conditions.FailedToCheckCapacityMsg).
			WithLastTransitionTime(metav1.Now())
		prAC.WithStatus(v1ac.ProvisioningRequestStatus().WithConditions(condition))
		if _, updateErr := o.client.ApplyProvisioningRequest(prAC, "cluster-autoscaler"); updateErr != nil {
			logger.Error(updateErr, "failed to add Provisioned=false condition to ProvReq", "provReq", klog.KObj(pr))
		}
		return status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "error during ScaleUp: %s", err.Error()))
	}

	if len(actuallyUnschedulablePods) == 0 && waitForNodesReady {
		// Everything fits, but only on upcoming nodes - wait for them to become ready.
		o.applyProvisionedCondition(ctx, pr, metav1.ConditionFalse, conditions.WaitingForNodesReason, conditions.WaitingForNodesMsg)
		return &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}, nil
	}

	if len(actuallyUnschedulablePods) == 0 {
		// Nothing to do here - everything fits without scale-up.
		prAC := v1ac.ProvisioningRequest(pr.Name, pr.Namespace)
		condition := metav1ac.Condition().
			WithType(v1.Provisioned).
			WithStatus(metav1.ConditionTrue).
			WithReason(conditions.CapacityIsFoundReason).
			WithMessage(conditions.CapacityIsFoundMsg).
			WithLastTransitionTime(metav1.Now())
		prAC.WithStatus(v1ac.ProvisioningRequestStatus().WithConditions(condition))
		if _, updateErr := o.client.ApplyProvisioningRequest(prAC, "cluster-autoscaler"); updateErr != nil {
			logger.Error(updateErr, "failed to add Provisioned=true condition to ProvReq", "provReq", klog.KObj(pr))
			return status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "capacity available, but failed to admit workload: %s", updateErr.Error()))
		}
		return &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}, nil
	}

	if waitForNodesReady && isWaitingForNodes(pr) {
		// The upcoming nodes are gone, e.g. the scale-up timed out and was reverted. Back off before retrying.
		o.applyProvisionedCondition(ctx, pr, metav1.ConditionFalse, conditions.CapacityIsNotFoundReason, conditions.NodesNotReadyMsg)
		return &status.ScaleUpStatus{Result: status.ScaleUpNotTried}, nil
	}

	st, err := o.scaleUpOrchestrator.ScaleUp(ctx, actuallyUnschedulablePods, nodes, daemonSets, nodeInfos, true)
	if err == nil && st.Result == status.ScaleUpSuccessful && waitForNodesReady {
		if updateErr := o.applyProvisionedCondition(ctx, pr, metav1.ConditionFalse, conditions.WaitingForNodesReason, conditions.WaitingForNodesMsg); updateErr != nil {
			return st, errors.NewAutoscalerErrorf(errors.InternalError, "scale up requested, but failed to update ProvisioningRequest: %s", updateErr.Error())
		}
		return st, nil
	}
	if err == nil && st.Result == status.ScaleUpSuccessful {
		// Happy path - all is well.
		prAC := v1ac.ProvisioningRequest(pr.Name, pr.Namespace)
		condition := metav1ac.Condition().
			WithType(v1.Provisioned).
			WithStatus(metav1.ConditionTrue).
			WithReason(conditions.CapacityIsProvisionedReason).
			WithMessage(conditions.CapacityIsProvisionedMsg).
			WithLastTransitionTime(metav1.Now())
		prAC.WithStatus(v1ac.ProvisioningRequestStatus().WithConditions(condition))
		if _, updateErr := o.client.ApplyProvisioningRequest(prAC, "cluster-autoscaler"); updateErr != nil {
			logger.Error(updateErr, "failed to add Provisioned=true condition to ProvReq", "provReq", klog.KObj(pr))
			return st, errors.NewAutoscalerErrorf(errors.InternalError, "scale up requested, but failed to admit workload: %s", updateErr.Error())
		}
		return st, nil
	}

	// We are not happy with the results.
	prAC := v1ac.ProvisioningRequest(pr.Name, pr.Namespace)
	condition := metav1ac.Condition().
		WithType(v1.Provisioned).
		WithStatus(metav1.ConditionFalse).
		WithReason(conditions.CapacityIsNotFoundReason).
		WithMessage("Capacity is not found, CA will try to find it later.").
		WithLastTransitionTime(metav1.Now())
	prAC.WithStatus(v1ac.ProvisioningRequestStatus().WithConditions(condition))
	if _, updateErr := o.client.ApplyProvisioningRequest(prAC, "cluster-autoscaler"); updateErr != nil {
		logger.Error(updateErr, "failed to add Provisioned=false condition to ProvReq", "provReq", klog.KObj(pr))
	}
	if err != nil {
		return status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "error during ScaleUp: %s", err.Error()))
	}
	return st, nil
}

func (o *bestEffortAtomicProvClass) filterOutSchedulable(pods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	schedulingResult, err := o.injector.TrySchedulePods(context.Background(), o.autoscalingCtx.ClusterSnapshot, pods, false, clustersnapshot.SchedulingOptions{})
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

// fitsOnStartedNodes checks if pods fit in the cluster without the upcoming nodes, which stand in
// for nodes that haven't registered or become ready yet.
func (o *bestEffortAtomicProvClass) fitsOnStartedNodes(ctx context.Context, pods []*apiv1.Pod) (bool, error) {
	snapshot := o.autoscalingCtx.ClusterSnapshot
	snapshot.Fork()
	defer snapshot.Revert()

	nodeInfos, err := snapshot.ListNodeInfos()
	if err != nil {
		return false, err
	}
	var upcomingNodes []string
	for _, nodeInfo := range nodeInfos {
		if _, found := nodeInfo.Node().Annotations[annotations.NodeUpcomingAnnotation]; found {
			upcomingNodes = append(upcomingNodes, nodeInfo.Node().Name)
		}
	}
	for _, name := range upcomingNodes {
		if err := snapshot.RemoveNodeInfo(ctx, name); err != nil {
			return false, err
		}
	}
	unschedulablePods, err := o.filterOutSchedulable(pods)
	if err != nil {
		return false, err
	}
	return len(unschedulablePods) == 0, nil
}

func (o *bestEffortAtomicProvClass) applyProvisionedCondition(ctx context.Context, pr *provreqwrapper.ProvisioningRequest, conditionStatus metav1.ConditionStatus, reason, message string) error {
	prAC := v1ac.ProvisioningRequest(pr.Name, pr.Namespace)
	condition := metav1ac.Condition().
		WithType(v1.Provisioned).
		WithStatus(conditionStatus).
		WithReason(reason).
		WithMessage(message).
		WithLastTransitionTime(metav1.Now())
	prAC.WithStatus(v1ac.ProvisioningRequestStatus().WithConditions(condition))
	if _, err := o.client.ApplyProvisioningRequest(prAC, "cluster-autoscaler"); err != nil {
		klog.FromContext(ctx).Error(err, "failed to update Provisioned condition of ProvReq", "provReq", klog.KObj(pr), "status", conditionStatus, "reason", reason)
		return err
	}
	return nil
}

func shouldWaitForNodesReady(ctx context.Context, pr *provreqwrapper.ProvisioningRequest) bool {
	value, found := pr.Spec.Parameters[WaitForNodesReadyParameterKey]
	if !found {
		return false
	}
	if value != "true" && value != "false" {
		klog.FromContext(ctx).Error(nil, "Ignoring Parameter with invalid value in ProvisioningRequest. Supported values are: \"true\", \"false\"", "parameter", WaitForNodesReadyParameterKey, "value", value, "provReq", klog.KObj(pr))
	}
	return value == "true"
}

func isWaitingForNodes(pr *provreqwrapper.ProvisioningRequest) bool {
	provisioned := apimeta.FindStatusCondition(pr.Status.Conditions, v1.Provisioned)
	return provisioned != nil && provisioned.Status == metav1.ConditionFalse && provisioned.Reason == conditions.WaitingForNodesReason
}
