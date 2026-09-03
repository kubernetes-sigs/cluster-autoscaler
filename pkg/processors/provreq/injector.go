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
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/lru"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest"
	provreqconditions "sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/conditions"
	provreqpods "sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
)

// ProvisioningRequestPodsInjector creates in-memory pods from ProvisioningRequest and inject them to unscheduled pods list.
type ProvisioningRequestPodsInjector struct {
	initialRetryTime                   time.Duration
	maxBackoffTime                     time.Duration
	backoffDuration                    *lru.Cache
	clock                              clock.PassiveClock
	client                             *provreqclient.ProvisioningRequestClient
	lastProvisioningRequestProcessTime time.Time
	checkCapacityBatchProcessing       bool
	checkCapacityProcessorInstance     string
	bestEffortAtomicBatchProcessing    bool
	bestEffortAtomicMaxBatchSize       int
	maxConcurrentUpdates               int
}

// IsAvailableForProvisioning checks if the provisioning request is the correct state for processing and provisioning has not been attempted recently.
func (p *ProvisioningRequestPodsInjector) IsAvailableForProvisioning(pr *provreqwrapper.ProvisioningRequest) bool {
	conditions := pr.Status.Conditions
	if apimeta.IsStatusConditionTrue(conditions, v1.Failed) || apimeta.IsStatusConditionTrue(conditions, v1.Provisioned) {
		p.backoffDuration.Remove(key(pr))
		return false
	}
	provisioned := apimeta.FindStatusCondition(conditions, v1.Provisioned)
	if provisioned == nil {
		return true
	}
	if provisioned.Status != metav1.ConditionFalse {
		return false
	}
	return provisioned.LastTransitionTime.Add(p.retryTime(pr)).Before(p.clock.Now())
}

// recordProvisioningAttempt doubles the retry backoff of a previously failed ProvisioningRequest
// that is about to be processed again. It must be called at most once per iteration and only for
// requests that are actually going to be injected, otherwise the backoff grows too fast.
func (p *ProvisioningRequestPodsInjector) recordProvisioningAttempt(pr *provreqwrapper.ProvisioningRequest) {
	provisioned := apimeta.FindStatusCondition(pr.Status.Conditions, v1.Provisioned)
	if provisioned == nil || provisioned.Status != metav1.ConditionFalse {
		return
	}
	retryTime := p.retryTime(pr)
	p.backoffDuration.Remove(key(pr))
	p.backoffDuration.Add(key(pr), min(2*retryTime, p.maxBackoffTime))
}

// retryTime returns the current retry backoff of the ProvisioningRequest.
func (p *ProvisioningRequestPodsInjector) retryTime(pr *provreqwrapper.ProvisioningRequest) time.Duration {
	val, found := p.backoffDuration.Get(key(pr))
	retryTime, ok := val.(time.Duration)
	if !found || !ok {
		return p.initialRetryTime
	}
	return retryTime
}

// MarkAsAccepted marks the ProvisioningRequest as accepted.
func (p *ProvisioningRequestPodsInjector) MarkAsAccepted(ctx context.Context, pr *provreqwrapper.ProvisioningRequest) error {
	logger := klog.FromContext(ctx)
	if err := p.markAsAccepted(ctx, pr); err != nil {
		logger.Error(err, "failed add Accepted condition to ProvReq", "provReq", klog.KObj(pr))
		return err
	}
	p.UpdateLastProcessTime()
	return nil
}

// markAsAccepted adds the Accepted condition without touching any injector state, so that it can
// be called concurrently for several ProvisioningRequests.
func (p *ProvisioningRequestPodsInjector) markAsAccepted(ctx context.Context, pr *provreqwrapper.ProvisioningRequest) error {
	provreqconditions.AddOrUpdateCondition(ctx, pr, v1.Accepted, metav1.ConditionTrue, provreqconditions.AcceptedReason, provreqconditions.AcceptedMsg, metav1.NewTime(p.clock.Now()))
	_, err := p.client.UpdateProvisioningRequest(ctx, pr.ProvisioningRequest)
	return err
}

// MarkAsFailed marks the ProvisioningRequest as failed.
func (p *ProvisioningRequestPodsInjector) MarkAsFailed(ctx context.Context, pr *provreqwrapper.ProvisioningRequest, reason string, message string) {
	logger := klog.FromContext(ctx)
	provreqconditions.AddOrUpdateCondition(ctx, pr, v1.Failed, metav1.ConditionTrue, reason, message, metav1.NewTime(p.clock.Now()))
	if _, err := p.client.UpdateProvisioningRequest(ctx, pr.ProvisioningRequest); err != nil {
		logger.Error(err, "failed add Failed condition to ProvReq", "provReq", klog.KObj(pr))
	}
	p.UpdateLastProcessTime()
}

func (p *ProvisioningRequestPodsInjector) isSupportedClass(ctx context.Context, pr *provreqwrapper.ProvisioningRequest) bool {
	return provisioningrequest.SupportedProvisioningClass(ctx, pr.ProvisioningRequest, p.checkCapacityProcessorInstance)
}

func (p *ProvisioningRequestPodsInjector) isSupportedCheckCapacityClass(ctx context.Context, pr *provreqwrapper.ProvisioningRequest) bool {
	return provisioningrequest.SupportedCheckCapacityClass(ctx, pr.ProvisioningRequest, p.checkCapacityProcessorInstance)
}

func (p *ProvisioningRequestPodsInjector) isSupportedBestEffortAtomicClass(pr *provreqwrapper.ProvisioningRequest) bool {
	return provisioningrequest.SupportedBestEffortAtomicClass(pr.ProvisioningRequest, p.checkCapacityProcessorInstance)
}

func (p *ProvisioningRequestPodsInjector) shouldMarkAsAccepted(ctx context.Context, pr *provreqwrapper.ProvisioningRequest) bool {
	// Don't mark as accepted the check capacity ProvReq when batch processing is enabled.
	// It will be marked later, in parallel, during processing the requests.
	return !p.checkCapacityBatchProcessing || !p.isSupportedCheckCapacityClass(ctx, pr)
}

// GetPodsFromNextRequest picks one ProvisioningRequest meeting the condition passed using isSupportedClass function, marks it as accepted and returns pods from it.
func (p *ProvisioningRequestPodsInjector) GetPodsFromNextRequest(ctx context.Context) ([]*apiv1.Pod, error) {
	logger := klog.FromContext(ctx)
	provReqs, err := p.client.ProvisioningRequests(ctx)
	if err != nil {
		return nil, err
	}
	provreqwrapper.SortProvisioningRequests(provReqs)
	for _, pr := range provReqs {
		if !p.isSupportedClass(ctx, pr) {
			continue
		}

		// Inject pods if ProvReq wasn't scaled up before or it has Provisioned == False condition more than defaultRetryTime
		if !p.IsAvailableForProvisioning(pr) {
			continue
		}
		p.recordProvisioningAttempt(pr)

		podsFromProvReq, err := provreqpods.PodsForProvisioningRequest(pr)
		if err != nil {
			logger.Error(err, "Failed to get pods for ProvisioningRequest", "provReq", klog.KObj(pr))
			p.MarkAsFailed(ctx, pr, provreqconditions.FailedToCreatePodsReason, err.Error())
			continue
		}
		if p.shouldMarkAsAccepted(ctx, pr) {
			if err := p.MarkAsAccepted(ctx, pr); err != nil {
				continue
			}
			return podsFromProvReq, nil
		}
		p.UpdateLastProcessTime()
		return podsFromProvReq, nil
	}
	return nil, nil
}

// ProvisioningRequestWithPods contains a ProvisioningRequest Wrapper
// and its associated pods.
type ProvisioningRequestWithPods struct {
	PrWrapper *provreqwrapper.ProvisioningRequest
	Pods      []*apiv1.Pod
}

// GetCheckCapacityBatch returns up to the requested number of ProvisioningRequestWithPods.
// We do not mark the PRs as accepted here.
// If we fail to get the pods for a PR, we mark the PR as failed and issue an update.
func (p *ProvisioningRequestPodsInjector) GetCheckCapacityBatch(ctx context.Context, maxPrs int) ([]ProvisioningRequestWithPods, error) {
	return p.getBatch(ctx, maxPrs, func(pr *provreqwrapper.ProvisioningRequest) bool {
		return p.isSupportedCheckCapacityClass(ctx, pr)
	})
}

// GetBestEffortAtomicBatch returns up to the requested number of best-effort-atomic
// ProvisioningRequestWithPods. The PRs are not marked as accepted here; callers that
// inject the pods are expected to call MarkBatchAsAccepted.
func (p *ProvisioningRequestPodsInjector) GetBestEffortAtomicBatch(ctx context.Context, maxPrs int) ([]ProvisioningRequestWithPods, error) {
	return p.getBatch(ctx, maxPrs, p.isSupportedBestEffortAtomicClass)
}

// getBatch returns up to maxPrs eligible ProvisioningRequests matching isSupported, together
// with the in-memory pods generated for each of them. If pods cannot be generated for a PR, the
// PR is marked as failed and skipped rather than failing the whole batch.
func (p *ProvisioningRequestPodsInjector) getBatch(ctx context.Context, maxPrs int, isSupported func(*provreqwrapper.ProvisioningRequest) bool) ([]ProvisioningRequestWithPods, error) {
	provReqs, err := p.client.ProvisioningRequests(ctx)
	if err != nil {
		return nil, err
	}
	provreqwrapper.SortProvisioningRequests(provReqs)
	return p.collectBatch(ctx, provReqs, maxPrs, isSupported), nil
}

// collectBatch picks up to maxPrs eligible ProvisioningRequests matching isSupported from an
// already ordered list, and generates their pods. Only the requests that end up in the batch have
// their retry backoff advanced, so callers may inspect the list beforehand without side effects.
func (p *ProvisioningRequestPodsInjector) collectBatch(ctx context.Context, provReqs []*provreqwrapper.ProvisioningRequest, maxPrs int, isSupported func(*provreqwrapper.ProvisioningRequest) bool) []ProvisioningRequestWithPods {
	logger := klog.FromContext(ctx)
	prsWithPods := make([]ProvisioningRequestWithPods, 0, min(maxPrs, len(provReqs)))
	for _, pr := range provReqs {
		if len(prsWithPods) >= maxPrs {
			break
		}
		if !isSupported(pr) {
			continue
		}
		if !p.IsAvailableForProvisioning(pr) {
			continue
		}
		p.recordProvisioningAttempt(pr)

		pods, err := provreqpods.PodsForProvisioningRequest(pr)
		if err != nil {
			logger.Error(err, "Failed to get pods for ProvisioningRequest", "provReq", klog.KObj(pr))
			p.MarkAsFailed(ctx, pr, provreqconditions.FailedToCreatePodsReason, err.Error())
			continue
		}
		prsWithPods = append(prsWithPods, ProvisioningRequestWithPods{pr, pods})
	}
	return prsWithPods
}

// MarkBatchAsAccepted marks every ProvisioningRequest in the batch as accepted, in parallel,
// and returns the subset that was accepted successfully. Requests that could not be updated
// are dropped so that CA does not scale up for a request it failed to claim.
func (p *ProvisioningRequestPodsInjector) MarkBatchAsAccepted(ctx context.Context, batch []ProvisioningRequestWithPods) []ProvisioningRequestWithPods {
	accepted := make([]bool, len(batch))
	semaphore := make(chan struct{}, min(max(1, p.maxConcurrentUpdates), len(batch)))
	wg := sync.WaitGroup{}
	wg.Add(len(batch))
	for i, prWithPods := range batch {
		go func(index int, request ProvisioningRequestWithPods) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			if err := p.markAsAccepted(ctx, request.PrWrapper); err != nil {
				klog.FromContext(ctx).Error(err, "failed add Accepted condition to ProvReq", "provReq", klog.KObj(request.PrWrapper))
				return
			}
			accepted[index] = true
		}(i, prWithPods)
	}
	wg.Wait()
	p.UpdateLastProcessTime()

	result := make([]ProvisioningRequestWithPods, 0, len(batch))
	for i, prWithPods := range batch {
		if accepted[i] {
			result = append(result, prWithPods)
		}
	}
	return result
}

// Process picks ProvisioningRequests to handle in this iteration, updates their Accepted
// condition and injects their pods into the unscheduled pods list.
//
// By default a single ProvisioningRequest is picked per iteration. When best-effort-atomic
// batch processing is enabled and the next eligible request belongs to that class, a batch of
// such requests is injected instead. Batching is only applied when the next request in the
// regular ordering is best-effort-atomic, so that enabling it cannot starve check capacity
// requests.
func (p *ProvisioningRequestPodsInjector) Process(
	ctx context.Context,
	_ *ca_context.AutoscalingContext,
	unschedulablePods []*apiv1.Pod,
) ([]*apiv1.Pod, error) {
	if p.bestEffortAtomicBatchProcessing {
		podsFromBatch, handled, err := p.getPodsFromNextBestEffortAtomicBatch(ctx)
		if err != nil {
			return unschedulablePods, err
		}
		if handled {
			return append(unschedulablePods, podsFromBatch...), nil
		}
	}

	podsFromProvReq, err := p.GetPodsFromNextRequest(ctx)
	if err != nil {
		return unschedulablePods, err
	}

	return append(unschedulablePods, podsFromProvReq...), nil
}

// getPodsFromNextBestEffortAtomicBatch injects a batch of best-effort-atomic requests, but only
// if the next eligible ProvisioningRequest in the regular ordering belongs to that class. The
// returned bool reports whether the batch path handled this iteration; when it is false the
// caller should fall back to the regular single-request path.
func (p *ProvisioningRequestPodsInjector) getPodsFromNextBestEffortAtomicBatch(ctx context.Context) ([]*apiv1.Pod, bool, error) {
	logger := klog.FromContext(ctx)
	provReqs, err := p.client.ProvisioningRequests(ctx)
	if err != nil {
		return nil, false, err
	}
	provreqwrapper.SortProvisioningRequests(provReqs)

	// Fairness: only start a batch when the request that the regular path would pick next is
	// itself best-effort-atomic, so that enabling batching cannot starve the other classes.
	next := p.nextEligibleRequest(ctx, provReqs)
	if next == nil || !p.isSupportedBestEffortAtomicClass(next) {
		return nil, false, nil
	}

	batch := p.collectBatch(ctx, provReqs, p.bestEffortAtomicMaxBatchSize, p.isSupportedBestEffortAtomicClass)
	batch = p.MarkBatchAsAccepted(ctx, batch)
	if len(batch) == 0 {
		return nil, false, nil
	}

	var pods []*apiv1.Pod
	for _, prWithPods := range batch {
		pods = append(pods, prWithPods.Pods...)
	}
	logger.Info("Injecting pods from a batch of best-effort-atomic ProvisioningRequests", "batchSize", len(batch), "podsCount", len(pods))
	return pods, true, nil
}

// nextEligibleRequest returns the ProvisioningRequest that the regular, non-batched path would
// pick next from an ordered list. It has no side effects on the request.
func (p *ProvisioningRequestPodsInjector) nextEligibleRequest(ctx context.Context, provReqs []*provreqwrapper.ProvisioningRequest) *provreqwrapper.ProvisioningRequest {
	for _, pr := range provReqs {
		if p.isSupportedClass(ctx, pr) && p.IsAvailableForProvisioning(pr) {
			return pr
		}
	}
	return nil
}

// CleanUp cleans up the processor's internal structures.
func (p *ProvisioningRequestPodsInjector) CleanUp() {}

// NewProvisioningRequestPodsInjector creates a ProvisioningRequest filter processor.
func NewProvisioningRequestPodsInjector(client *provreqclient.ProvisioningRequestClient, initialBackoffTime, maxBackoffTime time.Duration, maxCacheSize int, checkCapacityBatchProcessing bool, checkCapacityProcessorInstance string, bestEffortAtomicBatchProcessing bool, bestEffortAtomicMaxBatchSize, kubeClientBurst int) *ProvisioningRequestPodsInjector {
	if !bestEffortAtomicBatchProcessing {
		bestEffortAtomicMaxBatchSize = 1
	}
	maxConcurrentUpdates := max(1, kubeClientBurst)
	return &ProvisioningRequestPodsInjector{
		initialRetryTime:                   initialBackoffTime,
		maxBackoffTime:                     maxBackoffTime,
		backoffDuration:                    lru.New(maxCacheSize),
		client:                             client,
		clock:                              clock.RealClock{},
		lastProvisioningRequestProcessTime: time.Now(),
		checkCapacityBatchProcessing:       checkCapacityBatchProcessing,
		checkCapacityProcessorInstance:     checkCapacityProcessorInstance,
		bestEffortAtomicBatchProcessing:    bestEffortAtomicBatchProcessing && bestEffortAtomicMaxBatchSize > 1,
		bestEffortAtomicMaxBatchSize:       bestEffortAtomicMaxBatchSize,
		maxConcurrentUpdates:               maxConcurrentUpdates,
	}
}

func key(pr *provreqwrapper.ProvisioningRequest) string {
	return string(pr.UID)
}

// LastProvisioningRequestProcessTime returns the time when the last provisioning request was processed.
func (p *ProvisioningRequestPodsInjector) LastProvisioningRequestProcessTime() time.Time {
	return p.lastProvisioningRequestProcessTime
}

// UpdateLastProcessTime updates the time we last processed a ProvisioningRequest
// to now. This time is used to skip waiting between loops if a request
// was processed in the last loop.
func (p *ProvisioningRequestPodsInjector) UpdateLastProcessTime() {
	p.lastProvisioningRequestProcessTime = p.clock.Now()
}
