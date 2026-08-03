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

package provreq

import (
	"context"
	"errors"
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/klog/v2"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/conditions"
	provreq_pods "sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/klogx"
)

const (
	defaultReservationTime    = 10 * time.Minute
	defaultExpirationTime     = 7 * 24 * time.Hour // 7 days
	defaultTerminalProvReqTTL = 7 * 24 * time.Hour // 7 days
	// defaultMaxUpdated is a limit for ProvisioningRequest to update conditions in one ClusterAutoscaler loop.
	defaultMaxUpdated = 20
)

type injector interface {
	TrySchedulePods(ctx context.Context, clusterSnapshot clustersnapshot.ClusterSnapshot, pods []*apiv1.Pod, breakOnFailure bool, opts clustersnapshot.SchedulingOptions) (scheduling.Result, error)
}

type provReqProcessor struct {
	now                            func() time.Time
	maxUpdated                     int
	client                         *provreqclient.ProvisioningRequestClient
	injector                       injector
	checkCapacityProcessorInstance string
	simulationWorkloadBuilder      *provreq_pods.SimulationWorkloadBuilder
}

// NewProvReqProcessor return ProvisioningRequestProcessor.
func NewProvReqProcessor(client *provreqclient.ProvisioningRequestClient, checkCapacityProcessorInstance string, simulationWorkloadBuilder *provreq_pods.SimulationWorkloadBuilder) *provReqProcessor {
	return &provReqProcessor{now: time.Now, maxUpdated: defaultMaxUpdated, client: client, injector: scheduling.NewHintingSimulator(), checkCapacityProcessorInstance: checkCapacityProcessorInstance, simulationWorkloadBuilder: simulationWorkloadBuilder}
}

// Refresh implements loop.Observer interface and will be run at the start
// of every iteration of the main loop. It tries to fetch current
// ProvisioningRequests and processes up to p.maxUpdated of them.
func (p *provReqProcessor) Refresh() {
	provReqs, err := p.client.ProvisioningRequests()
	if err != nil {
		klog.Errorf("Failed to get ProvisioningRequests list, err: %v", err)
		return
	}
	p.refresh(provReqs)
}

// refresh iterates over ProvisioningRequests and apply:
// -BookingExpired condition for Provisioned ProvisioningRequest if capacity reservation time is expired.
// -Failed condition for ProvisioningRequest that were not provisioned during defaultExpirationTime.
// TODO(yaroslava): fetch reservation and expiration time from ProvisioningRequest
func (p *provReqProcessor) refresh(provReqs []*provreqwrapper.ProvisioningRequest) {
	expiredProvReq := []*provreqwrapper.ProvisioningRequest{}
	failedProvReq := []*provreqwrapper.ProvisioningRequest{}
	for _, provReq := range provReqs {
		if len(expiredProvReq) >= p.maxUpdated {
			break
		}
		if !provisioningrequest.SupportedProvisioningClass(provReq.ProvisioningRequest, p.checkCapacityProcessorInstance) {
			continue
		}
		conditions := provReq.Status.Conditions
		if apimeta.IsStatusConditionTrue(conditions, v1.BookingExpired) || apimeta.IsStatusConditionTrue(conditions, v1.Failed) {
			continue
		}
		provisioned := apimeta.FindStatusCondition(conditions, v1.Provisioned)
		if provisioned != nil && provisioned.Status == metav1.ConditionTrue {
			if provisioned.LastTransitionTime.Add(defaultReservationTime).Before(p.now()) {
				expiredProvReq = append(expiredProvReq, provReq)
			}
		} else if len(failedProvReq) < p.maxUpdated-len(expiredProvReq) {
			created := provReq.CreationTimestamp
			if created.Add(defaultExpirationTime).Before(p.now()) {
				failedProvReq = append(failedProvReq, provReq)
			}
		}
	}
	for _, provReq := range expiredProvReq {
		conditions.AddOrUpdateCondition(provReq, v1.BookingExpired, metav1.ConditionTrue, conditions.CapacityReservationTimeExpiredReason, conditions.CapacityReservationTimeExpiredMsg, metav1.NewTime(p.now()))
		_, updErr := p.client.UpdateProvisioningRequest(provReq.ProvisioningRequest)
		if updErr != nil {
			klog.Errorf("failed to add BookingExpired condition to ProvReq %s/%s, err: %v", provReq.Namespace, provReq.Name, updErr)
			continue
		}
	}
	for _, provReq := range failedProvReq {
		conditions.AddOrUpdateCondition(provReq, v1.Failed, metav1.ConditionTrue, conditions.ExpiredReason, conditions.ExpiredMsg, metav1.NewTime(p.now()))
		_, updErr := p.client.UpdateProvisioningRequest(provReq.ProvisioningRequest)
		if updErr != nil {
			klog.Errorf("failed to add Failed condition to ProvReq %s/%s, err: %v", provReq.Namespace, provReq.Name, updErr)
			continue
		}
	}
	p.DeleteOldProvReqs(provReqs)
}

// CleanUp cleans up internal state
func (p *provReqProcessor) CleanUp() {}

// Process implements PodListProcessor.Process() and inject fake pods to the cluster snapshoot for Provisioned ProvReqs in order to
// reserve capacity from ScaleDown.
func (p *provReqProcessor) Process(autoscalingCtx *ca_context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	err := p.bookCapacity(autoscalingCtx)
	if err != nil {
		klog.Warningf("Failed to book capacity for ProvisioningRequests: %s", err)
	}
	return unschedulablePods, nil
}

// bookCapacity schedule fake pods for ProvisioningRequest that should have reserved capacity
// in the cluster.
func (p *provReqProcessor) bookCapacity(autoscalingCtx *ca_context.AutoscalingContext) error {
	provReqs, err := p.client.ProvisioningRequests()
	if err != nil {
		return fmt.Errorf("couldn't fetch ProvisioningRequests in the cluster: %v", err)
	}
	nodeInfos, err := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return fmt.Errorf("couldn't list nodes: %v", err)
	}
	// Count pods already scheduled per ProvisioningRequest to exclude consumed PRs from booking.
	scheduledPods := map[string]int{}
	for _, nodeInfo := range nodeInfos {
		for _, podInfo := range nodeInfo.Pods() {
			if name, ok := provisioningRequestName(podInfo.Pod); ok {
				scheduledPods[podInfo.Pod.Namespace+"/"+name]++
			}
		}
	}
	podsToCreate := []*apiv1.Pod{}
	var bookingErrors []error
	flushPods := func() {
		if len(podsToCreate) == 0 {
			return
		}
		if err := p.bookPods(autoscalingCtx, podsToCreate); err != nil {
			bookingErrors = append(bookingErrors, err)
		}
		podsToCreate = nil
	}
	for _, provReq := range provReqs {
		if !conditions.ShouldCapacityBeBooked(provReq, p.checkCapacityProcessorInstance) {
			continue
		}
		// All pods already scheduled
		if scheduledPods[provReq.Namespace+"/"+provReq.Name] >= provReq.PodCount() {
			conditions.AddOrUpdateCondition(provReq, v1.BookingExpired, metav1.ConditionTrue, conditions.CapacityBookingConsumedReason, conditions.CapacityBookingConsumedMsg, metav1.NewTime(p.now()))
			if _, err := p.client.UpdateProvisioningRequest(provReq.ProvisioningRequest); err != nil {
				klog.Errorf("failed to add BookingExpired condition to ProvReq %s/%s, err: %v", provReq.Namespace, provReq.Name, err)
			}
			continue
		}
		if provisioningrequest.SupportedCheckCapacityClass(provReq.ProvisioningRequest, p.checkCapacityProcessorInstance) {
			// Preserve the lister order across provisioning classes while still
			// keeping each check-capacity request in its own DRA transaction.
			flushPods()
			if err := p.bookCheckCapacityProvisioningRequest(autoscalingCtx, provReq); err != nil {
				bookingErrors = append(bookingErrors, err)
			}
			continue
		}
		pods, err := provreq_pods.PodsForProvisioningRequest(provReq)
		if err != nil {
			// ClusterAutoscaler was able to create pods before, so we shouldn't have error here.
			// If there is an error, mark PR as invalid, because we won't be able to book capacity
			// for it anyway.
			p.markFailedToBookCapacity(provReq, fmt.Sprintf("Couldn't create pods, err: %v", err))
			continue
		}
		podsToCreate = append(podsToCreate, pods...)
	}
	flushPods()
	return errors.Join(bookingErrors...)
}

func (p *provReqProcessor) bookPods(autoscalingCtx *ca_context.AutoscalingContext, pods []*apiv1.Pod) error {
	autoscalingCtx.ClusterSnapshot.Fork()
	if _, err := p.injector.TrySchedulePods(context.Background(), autoscalingCtx.ClusterSnapshot, pods, false, clustersnapshot.SchedulingOptions{}); err != nil {
		autoscalingCtx.ClusterSnapshot.Revert()
		return err
	}
	if err := autoscalingCtx.ClusterSnapshot.Commit(); err != nil {
		autoscalingCtx.ClusterSnapshot.Revert()
		return fmt.Errorf("couldn't commit booked capacity: %w", err)
	}
	return nil
}

func (p *provReqProcessor) bookCheckCapacityProvisioningRequest(autoscalingCtx *ca_context.AutoscalingContext, provReq *provreqwrapper.ProvisioningRequest) error {
	workload, err := p.simulationWorkloadBuilder.ForProvisioningRequest(provReq)
	if err != nil {
		p.markFailedToBookCapacity(provReq, fmt.Sprintf("Couldn't create simulation workload: %v", err))
		return nil
	}

	autoscalingCtx.ClusterSnapshot.Fork()
	if err := autoscalingCtx.ClusterSnapshot.DraSnapshot().AddClaims(workload.Claims); err != nil {
		autoscalingCtx.ClusterSnapshot.Revert()
		p.markFailedToBookCapacity(provReq, fmt.Sprintf("Couldn't add simulated ResourceClaims: %v", err))
		return nil
	}

	schedulingResult, err := p.injector.TrySchedulePods(context.Background(), autoscalingCtx.ClusterSnapshot, workload.Pods, false, clustersnapshot.SchedulingOptions{})
	if err != nil {
		autoscalingCtx.ClusterSnapshot.Revert()
		return fmt.Errorf("couldn't book capacity for ProvisioningRequest %s/%s: %w", provReq.Namespace, provReq.Name, err)
	}

	// Booking is best-effort: retain claims for Pods which scheduled and discard
	// claims belonging to virtual Pods which did not schedule. Match by UID because
	// the simulator may return statuses in a different order than the input.
	scheduledPods := make(map[types.UID]struct{}, len(schedulingResult.Statuses))
	for _, schedulingStatus := range schedulingResult.Statuses {
		if schedulingStatus.Pod != nil {
			scheduledPods[schedulingStatus.Pod.UID] = struct{}{}
		}
	}
	for _, pod := range workload.Pods {
		if _, found := scheduledPods[pod.UID]; !found {
			autoscalingCtx.ClusterSnapshot.DraSnapshot().RemovePodOwnedClaims(pod)
		}
	}

	if err := autoscalingCtx.ClusterSnapshot.Commit(); err != nil {
		autoscalingCtx.ClusterSnapshot.Revert()
		return fmt.Errorf("couldn't commit booked capacity for ProvisioningRequest %s/%s: %w", provReq.Namespace, provReq.Name, err)
	}
	return nil
}

func (p *provReqProcessor) markFailedToBookCapacity(provReq *provreqwrapper.ProvisioningRequest, message string) {
	conditions.AddOrUpdateCondition(provReq, v1.Failed, metav1.ConditionTrue, conditions.FailedToBookCapacityReason, message, metav1.Now())
	if _, err := p.client.UpdateProvisioningRequest(provReq.ProvisioningRequest); err != nil {
		klog.Errorf("failed to add Failed condition to ProvReq %s/%s, err: %v", provReq.Namespace, provReq.Name, err)
	}
}

// DeleteOldProvReqs delete ProvReq that have terminal state (Provisioned/Failed == True) more than a week.
func (p *provReqProcessor) DeleteOldProvReqs(provReqs []*provreqwrapper.ProvisioningRequest) {
	provReqQuota := klogx.NewLoggingQuota(30)
	for _, provReq := range provReqs {
		conditions := provReq.Status.Conditions
		provisioned := apimeta.FindStatusCondition(conditions, v1.Provisioned)
		failed := apimeta.FindStatusCondition(conditions, v1.Failed)
		if provisioned != nil && provisioned.LastTransitionTime.Add(defaultTerminalProvReqTTL).Before(p.now()) ||
			failed != nil && failed.LastTransitionTime.Add(defaultTerminalProvReqTTL).Before(p.now()) {
			klogx.V(4).UpTo(provReqQuota).Infof("Delete old ProvisioningRequest %s/%s", provReq.Namespace, provReq.Name)
			err := p.client.DeleteProvisioningRequest(provReq.ProvisioningRequest)
			if err != nil {
				klog.Warningf("Couldn't delete old %s/%s Provisioning Request, err: %v", provReq.Namespace, provReq.Name, err)
			}
		}
	}
}
