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
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/provreq"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/combinedstatus"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/conditions"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"

	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
)

const (
	// NoRetryParameterKey is a a key for ProvReq's Parameters that describes
	// if ProvisioningRequest should be retried in case CA cannot provision it.
	// Supported values are "true" and "false" - by default ProvisioningRequests are always retried.
	// Currently supported only for checkcapacity class.
	NoRetryParameterKey = "noRetry"
)

type checkCapacityProvClass struct {
	autoscalingCtx                               *ca_context.AutoscalingContext
	client                                       *provreqclient.ProvisioningRequestClient
	schedulingSimulator                          *scheduling.HintingSimulator
	checkCapacityProvisioningRequestMaxBatchSize int
	checkCapacityProvisioningRequestBatchTimebox time.Duration
	provreqInjector                              *provreq.ProvisioningRequestPodsInjector
}

// New create check-capacity scale-up mode.
func New(
	client *provreqclient.ProvisioningRequestClient,
	provreqInjector *provreq.ProvisioningRequestPodsInjector,
) *checkCapacityProvClass {
	return &checkCapacityProvClass{client: client, provreqInjector: provreqInjector}
}

func (o *checkCapacityProvClass) Initialize(
	autoscalingCtx *ca_context.AutoscalingContext,
	processors *ca_processors.AutoscalingProcessors,
	clusterStateRegistry *clusterstate.ClusterStateRegistry,
	estimatorBuilder estimator.EstimatorBuilder,
	taintConfig taints.TaintConfig,
	schedulingSimulator *scheduling.HintingSimulator,
	quotasTrackerFactory *resourcequotas.TrackerFactory,
) {
	o.autoscalingCtx = autoscalingCtx
	o.schedulingSimulator = schedulingSimulator
	if autoscalingCtx.CheckCapacityBatchProcessing {
		o.checkCapacityProvisioningRequestBatchTimebox = autoscalingCtx.CheckCapacityProvisioningRequestBatchTimebox
		o.checkCapacityProvisioningRequestMaxBatchSize = autoscalingCtx.CheckCapacityProvisioningRequestMaxBatchSize
	} else {
		o.checkCapacityProvisioningRequestMaxBatchSize = 1
	}
}

// Provision return if there is capacity in the cluster for pods from ProvisioningRequest.
func (o *checkCapacityProvClass) Provision(
	ctx context.Context,
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	combinedStatus := combinedstatus.New()
	startTime := time.Now()

	o.autoscalingCtx.ClusterSnapshot.Fork()
	defer o.autoscalingCtx.ClusterSnapshot.Revert()

	// Gather ProvisioningRequests.
	prs, err := o.getProvisioningRequestsAndPods(ctx, unschedulablePods)
	if err != nil {
		return status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "Error fetching provisioning requests and associated pods: %s", err.Error()))
	} else if len(prs) == 0 {
		return &status.ScaleUpStatus{Result: status.ScaleUpNotTried}, nil
	}

	if o.provreqInjector != nil {
		// for more frequent iterations.
		// See https://github.com/kubernetes/autoscaler/pull/7271
		o.provreqInjector.UpdateLastProcessTime()
	}

	// Add accepted condition to ProvisioningRequests.
	for _, pr := range prs {
		conditions.AddOrUpdateCondition(ctx, pr.PrWrapper, v1.Accepted, metav1.ConditionTrue, conditions.AcceptedReason, conditions.AcceptedMsg, metav1.Now())
	}

	// Check Capacity. Add Provisioned or Failed conditions.
	processedPrs := o.checkCapacityBatch(ctx, prs, &combinedStatus, startTime)

	// Use client to update ProvisioningRequests conditions.
	updateRequests(ctx, o.client, processedPrs, &combinedStatus)

	return combinedStatus.Export()
}

func (o *checkCapacityProvClass) getProvisioningRequestsAndPods(ctx context.Context, unschedulablePods []*apiv1.Pod) ([]provreq.ProvisioningRequestWithPods, error) {
	logger := klog.FromContext(ctx)
	if !o.isBatchEnabled() {
		logger.Info("Processing single provisioning request (non-batch)")
		prs := provreqclient.ProvisioningRequestsForPods(ctx, o.client, unschedulablePods)
		prs = provreqclient.FilterOutProvisioningClass(ctx, prs, v1.ProvisioningClassCheckCapacity, o.autoscalingCtx.CheckCapacityProcessorInstance)
		if len(prs) == 0 {
			return nil, nil
		}
		return []provreq.ProvisioningRequestWithPods{{PrWrapper: prs[0], Pods: unschedulablePods}}, nil
	}

	batch, err := o.provreqInjector.GetCheckCapacityBatch(ctx, o.checkCapacityProvisioningRequestMaxBatchSize)
	if err != nil {
		return nil, err
	}
	logger.Info("Processing provisioning requests as batch", "batchSize", len(batch))
	return batch, nil
}

func (o *checkCapacityProvClass) isBatchEnabled() bool {
	return o.provreqInjector != nil && o.checkCapacityProvisioningRequestMaxBatchSize > 1
}

func (o *checkCapacityProvClass) checkCapacityBatch(ctx context.Context, reqs []provreq.ProvisioningRequestWithPods, combinedStatus *combinedstatus.Set, startTime time.Time) []*provreqwrapper.ProvisioningRequest {
	logger := klog.FromContext(ctx)
	updates := make([]*provreqwrapper.ProvisioningRequest, 0, len(reqs))
	for _, req := range reqs {
		if err := o.checkCapacity(ctx, req.Pods, req.PrWrapper, combinedStatus); err != nil {
			logger.Error(err, "Error checking capacity")
			continue
		}

		updates = append(updates, req.PrWrapper)

		// timebox checkCapacity when batch processing.
		if o.isBatchEnabled() && time.Since(startTime) > o.checkCapacityProvisioningRequestBatchTimebox {
			logger.Info("Batch timebox exceeded, logging number of processed check capacity provisioning requests in this iteration", "provReqCount", len(updates))
			break
		}
	}
	return updates
}

// checkCapacity checks if there is capacity, updates combinedStatus and Conditions. If capacity is found, it commits to the clusterSnapshot.
func (o *checkCapacityProvClass) checkCapacity(ctx context.Context, unschedulablePods []*apiv1.Pod, provReq *provreqwrapper.ProvisioningRequest, combinedStatus *combinedstatus.Set) error {
	logger := klog.FromContext(ctx)
	o.autoscalingCtx.ClusterSnapshot.Fork()

	// Case 1: Capacity fits.
	schedulingResult, err := o.schedulingSimulator.TrySchedulePods(context.Background(), o.autoscalingCtx.ClusterSnapshot, unschedulablePods, true, clustersnapshot.SchedulingOptions{})
	if err == nil && len(schedulingResult.Statuses) == len(unschedulablePods) {
		commitError := o.autoscalingCtx.ClusterSnapshot.Commit()
		if commitError != nil {
			o.autoscalingCtx.ClusterSnapshot.Revert()
			return commitError
		}
		combinedStatus.Add(&status.ScaleUpStatus{Result: status.ScaleUpSuccessful})
		conditions.AddOrUpdateCondition(ctx, provReq, v1.Provisioned, metav1.ConditionTrue, conditions.CapacityIsFoundReason, conditions.CapacityIsFoundMsg, metav1.Now())
		return nil
	}
	// Case 2: Capacity doesn't fit.
	o.autoscalingCtx.ClusterSnapshot.Revert()
	combinedStatus.Add(&status.ScaleUpStatus{Result: status.ScaleUpNoOptionsAvailable})
	if noRetry, ok := provReq.Spec.Parameters[NoRetryParameterKey]; ok && noRetry == "true" {
		// Failed=true condition triggers retry in Kueue. Otherwise ProvisioningRequest with Provisioned=Failed
		// condition block capacity in Kueue even if it's in the middle of backoff waiting time.
		conditions.AddOrUpdateCondition(ctx, provReq, v1.Failed, metav1.ConditionTrue, conditions.CapacityIsNotFoundReason, "CA could not find requested capacity", metav1.Now())
	} else {
		if noRetry, ok := provReq.Spec.Parameters[NoRetryParameterKey]; ok && noRetry != "false" {
			logger.Error(nil, "Ignoring Parameter with invalid value in ProvisioningRequest. Supported values are: \"true\", \"false\"", "parameter", NoRetryParameterKey, "value", noRetry, "provReq", klog.KObj(provReq))
		}
		conditions.AddOrUpdateCondition(ctx, provReq, v1.Provisioned, metav1.ConditionFalse, conditions.CapacityIsNotFoundReason, "Capacity is not found, CA will try to find it later.", metav1.Now())
	}
	return err
}

// updateRequests calls the client to update ProvisioningRequests, in parallel.
func updateRequests(ctx context.Context, client *provreqclient.ProvisioningRequestClient, prWrappers []*provreqwrapper.ProvisioningRequest, combinedStatus *combinedstatus.Set) {
	wg := sync.WaitGroup{}
	wg.Add(len(prWrappers))
	lock := sync.Mutex{}
	for _, wrapper := range prWrappers {
		go func() {
			provReq := wrapper.ProvisioningRequest
			_, updErr := client.UpdateProvisioningRequest(ctx, provReq)
			if updErr != nil {
				err := fmt.Errorf("failed to update ProvReq %s/%s, err: %v", provReq.Namespace, provReq.Name, updErr)
				scaleUpStatus, _ := status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "error during ScaleUp: %s", err.Error()))
				lock.Lock()
				combinedStatus.Add(scaleUpStatus)
				lock.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
