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
	"slices"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/lru"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest"
	provreqconditions "sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/conditions"
	provreqpods "sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils"
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
	bestEffortAtomicBatchSizeTTL       time.Duration
	// settleBestEffortAtomicBatches, if set, settles the outcomes of earlier best-effort-atomic
	// batches, which can change the remembered batch sizes.
	settleBestEffortAtomicBatches func(ctx context.Context)
	maxConcurrentUpdates          int
	// awaitedConditions holds the best-effort-atomic requests whose Provisioned condition was just
	// written, until the informer cache shows it. The client already shows successful admissions
	// before the cache does; this also covers Provisioned=False conditions and failed writes.
	awaitedConditions *lru.Cache
	// batchRetryLimitsMu guards batchRetryLimits, which scale-up failures reported after a resize
	// was accepted can update from other goroutines.
	batchRetryLimitsMu sync.Mutex
	batchRetryLimits   []batchRetryLimit
}

// maxBatchRetryLimits bounds the memory used to remember batch sizes. When it's reached, the size
// remembered longest ago is forgotten first.
const maxBatchRetryLimits = 100

// batchRetryLimit caps how many pods of matching requests are batched together. Pods measure the
// capacity a batch needs better than requests do, because requests can ask for very different
// numbers of pods.
type batchRetryLimit struct {
	request   *provreqwrapper.ProvisioningRequest
	pods      int
	expiresAt time.Time
}

// RememberBestEffortAtomicBatchLimit lowers how many pods of compatible best-effort-atomic
// ProvisioningRequests later iterations batch together: to the size that succeeded after a batch
// had to shrink, or to the next smaller size when a search exhausted its retry budget or a resize
// failed. It only limits future selection; request retry delays and node-group backoff still apply.
func (p *ProvisioningRequestPodsInjector) RememberBestEffortAtomicBatchLimit(ctx context.Context, pr *provreqwrapper.ProvisioningRequest, pods int) {
	if !p.bestEffortAtomicBatchProcessing {
		return
	}
	p.batchRetryLimitsMu.Lock()
	defer p.batchRetryLimitsMu.Unlock()
	pods = max(1, pods)
	if index := p.batchRetryLimitIndex(pr); index >= 0 {
		limit := &p.batchRetryLimits[index]
		limit.pods = min(limit.pods, pods)
		limit.expiresAt = p.clock.Now().Add(p.bestEffortAtomicBatchSizeTTL)
		pods = limit.pods
	} else {
		if len(p.batchRetryLimits) == maxBatchRetryLimits {
			p.batchRetryLimits = slices.Delete(p.batchRetryLimits, 0, 1)
		}
		request := &provreqwrapper.ProvisioningRequest{ProvisioningRequest: pr.DeepCopy()}
		for _, template := range pr.PodTemplates {
			request.PodTemplates = append(request.PodTemplates, template.DeepCopy())
		}
		p.batchRetryLimits = append(p.batchRetryLimits, batchRetryLimit{
			request: request, pods: pods, expiresAt: p.clock.Now().Add(p.bestEffortAtomicBatchSizeTTL),
		})
	}
	klog.FromContext(ctx).V(2).Info("Remembering a smaller best-effort-atomic batch for the next iteration", "provReq", klog.KObj(pr), "batchPods", pods)
}

// RecordBestEffortAtomicBatchSuccess lets a remembered limit grow again as capacity recovers: when
// a batch with at least as many pods as the limit succeeds, the limit doubles, and it's dropped once
// it would cover a batch of the configured maximum number of requests like these.
func (p *ProvisioningRequestPodsInjector) RecordBestEffortAtomicBatchSuccess(ctx context.Context, pr *provreqwrapper.ProvisioningRequest, requests, pods int) {
	if !p.bestEffortAtomicBatchProcessing {
		return
	}
	p.batchRetryLimitsMu.Lock()
	defer p.batchRetryLimitsMu.Unlock()
	index := p.batchRetryLimitIndex(pr)
	if index < 0 || pods < p.batchRetryLimits[index].pods {
		return
	}
	logger := klog.FromContext(ctx)
	fullBatch := p.bestEffortAtomicMaxBatchSize * pods / max(1, requests)
	if grown := 2 * p.batchRetryLimits[index].pods; grown < fullBatch {
		p.batchRetryLimits[index].pods = grown
		logger.V(2).Info("Growing the remembered best-effort-atomic batch size", "provReq", klog.KObj(pr), "batchPods", grown)
		return
	}
	p.batchRetryLimits = slices.Delete(p.batchRetryLimits, index, index+1)
	logger.V(2).Info("Dropping the remembered best-effort-atomic batch size", "provReq", klog.KObj(pr), "maxBatchSize", p.bestEffortAtomicMaxBatchSize)
}

// batchRetryLimitIndex drops expired limits and returns the index of the limit that applies to
// requests with the same scheduling requirements as pr, or -1 if there is none. The caller must
// hold batchRetryLimitsMu.
func (p *ProvisioningRequestPodsInjector) batchRetryLimitIndex(pr *provreqwrapper.ProvisioningRequest) int {
	now := p.clock.Now()
	p.batchRetryLimits = slices.DeleteFunc(p.batchRetryLimits, func(limit batchRetryLimit) bool {
		return !now.Before(limit.expiresAt)
	})
	return slices.IndexFunc(p.batchRetryLimits, func(limit batchRetryLimit) bool {
		return sameSchedulingRequirements(limit.request, pr)
	})
}

// batchRetryLimit returns a copy of the limit that applies to requests with the same scheduling
// requirements as pr, or nil if there is none.
func (p *ProvisioningRequestPodsInjector) batchRetryLimit(pr *provreqwrapper.ProvisioningRequest) *batchRetryLimit {
	p.batchRetryLimitsMu.Lock()
	defer p.batchRetryLimitsMu.Unlock()
	if index := p.batchRetryLimitIndex(pr); index >= 0 {
		limit := p.batchRetryLimits[index]
		return &limit
	}
	return nil
}

// IsAvailableForProvisioning checks if the provisioning request is the correct state for processing and provisioning has not been attempted recently.
func (p *ProvisioningRequestPodsInjector) IsAvailableForProvisioning(pr *provreqwrapper.ProvisioningRequest) bool {
	conditions := pr.Status.Conditions
	if apimeta.IsStatusConditionTrue(conditions, v1.Failed) || apimeta.IsStatusConditionTrue(conditions, v1.Provisioned) {
		p.backoffDuration.Remove(key(pr))
		if p.awaitedConditions != nil {
			p.awaitedConditions.Remove(awaitKey(pr))
		}
		return false
	}
	provisioned := apimeta.FindStatusCondition(conditions, v1.Provisioned)
	if p.awaitingCondition(pr, provisioned) {
		return false
	}
	if provisioned == nil {
		return true
	}
	if provisioned.Status != metav1.ConditionFalse {
		return false
	}
	return provisioned.LastTransitionTime.Add(p.retryTime(pr)).Before(p.clock.Now())
}

// awaitedCondition is the Provisioned condition a request had when a new one was written for it.
type awaitedCondition struct {
	previous *metav1.Condition
	until    time.Time
}

// AwaitProvisionedCondition records that a Provisioned condition is being written for a
// best-effort-atomic ProvisioningRequest. Its retry delay only grows once the new condition shows
// in the informer cache, so the request isn't picked again until it does. If the write fails, the
// request becomes eligible again once its current retry delay has passed.
func (p *ProvisioningRequestPodsInjector) AwaitProvisionedCondition(pr *provreqwrapper.ProvisioningRequest) {
	if p.awaitedConditions == nil {
		return
	}
	awaited := awaitedCondition{until: p.clock.Now().Add(p.retryTime(pr))}
	if provisioned := apimeta.FindStatusCondition(pr.Status.Conditions, v1.Provisioned); provisioned != nil {
		awaited.previous = provisioned.DeepCopy()
	}
	p.awaitedConditions.Add(awaitKey(pr), awaited)
}

// awaitingCondition reports whether the Provisioned condition last written for the request isn't in
// the informer cache yet, and forgets the write once it is, or once the retry delay has passed.
func (p *ProvisioningRequestPodsInjector) awaitingCondition(pr *provreqwrapper.ProvisioningRequest, provisioned *metav1.Condition) bool {
	if p.awaitedConditions == nil {
		return false
	}
	val, found := p.awaitedConditions.Get(awaitKey(pr))
	if !found {
		return false
	}
	if awaited, ok := val.(awaitedCondition); ok && p.clock.Now().Before(awaited.until) && sameCondition(awaited.previous, provisioned) {
		return true
	}
	p.awaitedConditions.Remove(awaitKey(pr))
	return false
}

func sameCondition(a, b *metav1.Condition) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Status == b.Status && a.Reason == b.Reason && a.LastTransitionTime.Equal(&b.LastTransitionTime)
}

// awaitKey identifies a request across its versions, even when it has no UID yet.
func awaitKey(pr *provreqwrapper.ProvisioningRequest) string {
	return pr.Namespace + "/" + pr.Name + "/" + string(pr.UID)
}

// retryBackoff is the retry delay that applies after the failure recorded at failedAt.
type retryBackoff struct {
	delay    time.Duration
	failedAt time.Time
}

// retryTime returns how long a ProvisioningRequest waits after its latest Provisioned=False
// condition before it's retried. The delay starts at initialRetryTime and doubles with every
// further attempt. For best-effort-atomic requests that is every failure, that is every time the
// condition's transition time moves after the request was picked: picking a request for a batch
// doesn't change its delay, so one that is left out of the attempt that is finally made can be
// retried as soon as before. Other requests are attempted once picked, so picking them doubles it.
func (p *ProvisioningRequestPodsInjector) retryTime(pr *provreqwrapper.ProvisioningRequest) time.Duration {
	backoff, _ := p.currentBackoff(pr)
	return backoff.delay
}

// currentBackoff computes the retry backoff of a ProvisioningRequest from the record made when it
// was last picked, without changing the record. It reports false if the request hasn't failed.
func (p *ProvisioningRequestPodsInjector) currentBackoff(pr *provreqwrapper.ProvisioningRequest) (retryBackoff, bool) {
	provisioned := apimeta.FindStatusCondition(pr.Status.Conditions, v1.Provisioned)
	if provisioned == nil || provisioned.Status != metav1.ConditionFalse {
		return retryBackoff{delay: p.initialRetryTime}, false
	}
	backoff := retryBackoff{delay: p.initialRetryTime, failedAt: provisioned.LastTransitionTime.Time}
	if val, found := p.backoffDuration.Get(key(pr)); found {
		if recorded, ok := val.(retryBackoff); ok {
			switch {
			case !retriesAfterNewFailures(pr):
				backoff.delay = recorded.delay
			case recorded.failedAt.Equal(backoff.failedAt):
				return recorded, true
			default:
				backoff.delay = min(2*recorded.delay, p.maxBackoffTime)
			}
		}
	}
	return backoff, true
}

// recordProvisioningAttempt records the retry backoff of a previously failed ProvisioningRequest
// that is picked for another attempt, so that the delay doubles if the attempt fails too, or right
// away for requests that are attempted whenever they're picked. Only picked requests are recorded,
// so scanning the requests that wait for a retry doesn't evict other records from the bounded cache.
func (p *ProvisioningRequestPodsInjector) recordProvisioningAttempt(pr *provreqwrapper.ProvisioningRequest) {
	backoff, failed := p.currentBackoff(pr)
	if !failed {
		return
	}
	if !retriesAfterNewFailures(pr) {
		backoff.delay = min(2*backoff.delay, p.maxBackoffTime)
	}
	p.backoffDuration.Add(key(pr), backoff)
}

// retriesAfterNewFailures reports whether the retry delay of a request only grows when it fails
// again, rather than whenever it's picked. Best-effort-atomic batches pick requests that may then
// be left out of the attempt that is made, which mustn't delay them.
func retriesAfterNewFailures(pr *provreqwrapper.ProvisioningRequest) bool {
	return pr.Spec.ProvisioningClassName == v1.ProvisioningClassBestEffortAtomicScaleUp
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
// ProvisioningRequestWithPods sharing the oldest eligible request's scheduling requirements.
// A remembered batch limit can reduce the batch further.
// The PRs are not marked as accepted here; callers that inject the pods are expected to call
// MarkBatchAsAccepted.
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
// already ordered list, and generates their pods. A remembered best-effort-atomic batch limit caps
// the batch's pods, except that the oldest eligible request is always picked; the batch stays
// oldest first, so it ends at the first matching request that doesn't fit. Only picked requests
// have their retry backoff recorded, so callers may inspect the list beforehand without side effects.
func (p *ProvisioningRequestPodsInjector) collectBatch(ctx context.Context, provReqs []*provreqwrapper.ProvisioningRequest, maxPrs int, isSupported func(*provreqwrapper.ProvisioningRequest) bool) []ProvisioningRequestWithPods {
	logger := klog.FromContext(ctx)
	prsWithPods := make([]ProvisioningRequestWithPods, 0, min(maxPrs, len(provReqs)))
	podLimit, batchPods := 0, 0
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

		pods, err := provreqpods.PodsForProvisioningRequest(pr)
		if err != nil {
			logger.Error(err, "Failed to get pods for ProvisioningRequest", "provReq", klog.KObj(pr))
			p.MarkAsFailed(ctx, pr, provreqconditions.FailedToCreatePodsReason, err.Error())
			continue
		}
		if len(prsWithPods) > 0 && p.isSupportedBestEffortAtomicClass(pr) && !sameSchedulingRequirements(prsWithPods[0].PrWrapper, pr) {
			continue
		}
		if len(prsWithPods) == 0 && p.bestEffortAtomicBatchProcessing && p.isSupportedBestEffortAtomicClass(pr) {
			if limit := p.batchRetryLimit(pr); limit != nil {
				podLimit = limit.pods
			}
		} else if podLimit > 0 && batchPods+len(pods) > podLimit {
			break
		}
		p.recordProvisioningAttempt(pr)
		prsWithPods = append(prsWithPods, ProvisioningRequestWithPods{pr, pods})
		batchPods += len(pods)
	}
	return prsWithPods
}

// sameSchedulingRequirements reports whether two requests ask for interchangeable capacity: the
// same namespace and semantically equal pod specs for every pod set. Pod labels and annotations
// are ignored because workload controllers stamp per-workload identity on them (Kueue, for
// example, copies each Job's batch.kubernetes.io/job-name label into its request's PodTemplate),
// and the scale-up simulation evaluates every pod with its own labels anyway.
func sameSchedulingRequirements(first, second *provreqwrapper.ProvisioningRequest) bool {
	if first.Namespace != second.Namespace || len(first.PodTemplates) != len(second.PodTemplates) {
		return false
	}
	for index, firstTemplate := range first.PodTemplates {
		if !utils.PodSpecSemanticallyEqual(*firstTemplate.Template.Spec.DeepCopy(), *second.PodTemplates[index].Template.Spec.DeepCopy()) {
			return false
		}
	}
	return true
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
		if p.settleBestEffortAtomicBatches != nil {
			// Earlier batches whose nodes have arrived can let this iteration's batch grow.
			p.settleBestEffortAtomicBatches(ctx)
		}
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

// SetBestEffortAtomicBatchSettler sets a function that Process calls in every iteration before it
// selects a best-effort-atomic batch. It settles the outcomes of earlier batches, so that a batch
// size they change already applies to the batch selected in the same iteration.
func (p *ProvisioningRequestPodsInjector) SetBestEffortAtomicBatchSettler(settle func(ctx context.Context)) {
	p.settleBestEffortAtomicBatches = settle
}

// CleanUp cleans up the processor's internal structures.
func (p *ProvisioningRequestPodsInjector) CleanUp() {}

// PodsInjectorOptions configures ProvisioningRequest selection and batch processing.
type PodsInjectorOptions struct {
	InitialBackoffTime              time.Duration
	MaxBackoffTime                  time.Duration
	MaxBackoffCacheSize             int
	CheckCapacityBatchProcessing    bool
	CheckCapacityProcessorInstance  string
	BestEffortAtomicBatchProcessing bool
	BestEffortAtomicMaxBatchSize    int
	KubeClientBurst                 int
	BestEffortAtomicBatchSizeTTL    time.Duration
}

// NewProvisioningRequestPodsInjector creates a ProvisioningRequest filter processor with
// best-effort-atomic batching disabled.
//
// Deprecated: Use NewProvisioningRequestPodsInjectorWithOptions for batch configuration.
func NewProvisioningRequestPodsInjector(client *provreqclient.ProvisioningRequestClient, initialBackoffTime, maxBackoffTime time.Duration, maxCacheSize int, checkCapacityBatchProcessing bool, checkCapacityProcessorInstance string) *ProvisioningRequestPodsInjector {
	return NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
		InitialBackoffTime:             initialBackoffTime,
		MaxBackoffTime:                 maxBackoffTime,
		MaxBackoffCacheSize:            maxCacheSize,
		CheckCapacityBatchProcessing:   checkCapacityBatchProcessing,
		CheckCapacityProcessorInstance: checkCapacityProcessorInstance,
	})
}

// NewProvisioningRequestPodsInjectorWithOptions creates a ProvisioningRequest filter processor.
// A nonpositive batch-size TTL uses the default, and a nonpositive client burst serializes updates.
func NewProvisioningRequestPodsInjectorWithOptions(client *provreqclient.ProvisioningRequestClient, opts PodsInjectorOptions) *ProvisioningRequestPodsInjector {
	if !opts.BestEffortAtomicBatchProcessing {
		opts.BestEffortAtomicMaxBatchSize = 1
	}
	if opts.BestEffortAtomicBatchSizeTTL <= 0 {
		opts.BestEffortAtomicBatchSizeTTL = config.DefaultBestEffortAtomicProvisioningRequestBatchSizeTTL
	}
	return &ProvisioningRequestPodsInjector{
		initialRetryTime:                   opts.InitialBackoffTime,
		maxBackoffTime:                     opts.MaxBackoffTime,
		backoffDuration:                    lru.New(opts.MaxBackoffCacheSize),
		awaitedConditions:                  lru.New(opts.MaxBackoffCacheSize),
		client:                             client,
		clock:                              clock.RealClock{},
		lastProvisioningRequestProcessTime: time.Now(),
		checkCapacityBatchProcessing:       opts.CheckCapacityBatchProcessing,
		checkCapacityProcessorInstance:     opts.CheckCapacityProcessorInstance,
		bestEffortAtomicBatchProcessing:    opts.BestEffortAtomicBatchProcessing && opts.BestEffortAtomicMaxBatchSize > 1,
		bestEffortAtomicMaxBatchSize:       opts.BestEffortAtomicMaxBatchSize,
		bestEffortAtomicBatchSizeTTL:       opts.BestEffortAtomicBatchSizeTTL,
		maxConcurrentUpdates:               max(1, opts.KubeClientBurst),
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
