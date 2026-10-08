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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	clock "k8s.io/utils/clock/testing"
	"k8s.io/utils/lru"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
)

func TestProvisioningRequestPodsInjector(t *testing.T) {
	now := time.Now()
	minAgo := now.Add(-1 * time.Minute).Add(-1 * time.Second)
	hourAgo := now.Add(-1 * time.Hour)

	accepted := metav1.Condition{
		Type:               v1.Accepted,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(minAgo),
	}
	failed := metav1.Condition{
		Type:               v1.Failed,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(hourAgo),
	}
	provisioned := metav1.Condition{
		Type:               v1.Provisioned,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(hourAgo),
	}
	notProvisioned := metav1.Condition{
		Type:               v1.Provisioned,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(hourAgo),
	}
	unknownProvisioned := metav1.Condition{
		Type:               v1.Provisioned,
		Status:             metav1.ConditionUnknown,
		LastTransitionTime: metav1.NewTime(hourAgo),
	}
	notProvisionedRecently := metav1.Condition{
		Type:               v1.Provisioned,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(minAgo),
	}

	podsA := 10
	newProvReqA := testProvisioningRequestWithCondition("new", podsA, v1.ProvisioningClassCheckCapacity)
	newAcceptedProvReqA := testProvisioningRequestWithCondition("new-accepted", podsA, v1.ProvisioningClassCheckCapacity, accepted)
	newProvReqAWithInstance := testProvisioningRequestWithCondition("new-instance", podsA, v1.ProvisioningClassCheckCapacity)
	newProvReqAWithInstance.Spec.Parameters = map[string]v1.Parameter{
		provisioningrequest.CheckCapacityProcessorInstanceKey: "test-instance",
	}
	newProvReqAPrefixed := testProvisioningRequestWithCondition("new-prefixed", podsA, "test-prefix.check-capacity.autoscaling.x-k8s.io")

	podsB := 20
	notProvisionedAcceptedProvReqB := testProvisioningRequestWithCondition("provisioned-false-B", podsB, v1.ProvisioningClassBestEffortAtomicScaleUp, notProvisioned, accepted)
	provisionedAcceptedProvReqB := testProvisioningRequestWithCondition("provisioned-and-accepted", podsB, v1.ProvisioningClassBestEffortAtomicScaleUp, provisioned, accepted)
	failedProvReq := testProvisioningRequestWithCondition("failed", podsA, v1.ProvisioningClassBestEffortAtomicScaleUp, failed)
	notProvisionedRecentlyProvReqB := testProvisioningRequestWithCondition("provisioned-false-recently-B", podsB, v1.ProvisioningClassBestEffortAtomicScaleUp, notProvisionedRecently)
	unknownProvisionedProvReqB := testProvisioningRequestWithCondition("provisioned-unknown-B", podsB, v1.ProvisioningClassBestEffortAtomicScaleUp, unknownProvisioned)
	unknownClass := testProvisioningRequestWithCondition("new-accepted", podsA, "unknown-class", accepted)
	eligibleAtomicProvReq1 := testProvisioningRequestWithCondition("eligible-atomic-1", podsA, v1.ProvisioningClassBestEffortAtomicScaleUp)
	eligibleAtomicProvReq2 := testProvisioningRequestWithCondition("eligible-atomic-2", podsA, v1.ProvisioningClassBestEffortAtomicScaleUp)
	eligibleAtomicProvReq3 := testProvisioningRequestWithCondition("eligible-atomic-3", podsA, v1.ProvisioningClassBestEffortAtomicScaleUp)
	eligibleAtomicProvReqs := []*provreqwrapper.ProvisioningRequest{eligibleAtomicProvReq1, eligibleAtomicProvReq2, eligibleAtomicProvReq3}

	testCases := []struct {
		name                                 string
		provReqs                             []*provreqwrapper.ProvisioningRequest
		existingUnsUnschedulablePodCount     int
		checkCapacityBatchProcessing         bool
		checkCapacityProcessorInstance       string
		bestEffortAtomicBatchProcessing      bool
		bestEffortAtomicMaxBatchSize         int
		wantUnscheduledPodCount              int
		wantUpdatedConditionName             string
		expectedAcceptedProvisioningRequests []*provreqwrapper.ProvisioningRequest
	}{
		{
			name:                     "New ProvisioningRequest, pods are injected and Accepted condition is added",
			provReqs:                 []*provreqwrapper.ProvisioningRequest{newProvReqA, provisionedAcceptedProvReqB},
			wantUnscheduledPodCount:  podsA,
			wantUpdatedConditionName: newProvReqA.Name,
		},
		{
			name:                         "New check capacity ProvisioningRequest with batch processing, pods are injected and Accepted condition is not added",
			provReqs:                     []*provreqwrapper.ProvisioningRequest{newProvReqA, provisionedAcceptedProvReqB},
			checkCapacityBatchProcessing: true,
			wantUnscheduledPodCount:      podsA,
			wantUpdatedConditionName:     newProvReqA.Name,
		},
		{
			name:                     "New ProvisioningRequest, pods are injected and Accepted condition is updated",
			provReqs:                 []*provreqwrapper.ProvisioningRequest{newAcceptedProvReqA, provisionedAcceptedProvReqB},
			wantUnscheduledPodCount:  podsA,
			wantUpdatedConditionName: newAcceptedProvReqA.Name,
		},
		{
			name:     "New ProvisioningRequest with not matching custom prefix, no pods are injected",
			provReqs: []*provreqwrapper.ProvisioningRequest{newProvReqAPrefixed},
		},
		{
			name:                           "New ProvisioningRequest with not matching processor instance, no pods are injected",
			provReqs:                       []*provreqwrapper.ProvisioningRequest{newProvReqA, provisionedAcceptedProvReqB},
			checkCapacityProcessorInstance: "test-instance",
		},
		{
			name:                           "New check capacity ProvisioningRequest with matching processor instance, pods are injected and Accepted condition is added",
			provReqs:                       []*provreqwrapper.ProvisioningRequest{newProvReqAWithInstance, provisionedAcceptedProvReqB},
			checkCapacityProcessorInstance: "test-instance",
			wantUnscheduledPodCount:        podsA,
			wantUpdatedConditionName:       newProvReqAWithInstance.Name,
		},
		{
			name:                           "New ProvisioningRequest with not matching prefix, no pods are injected",
			provReqs:                       []*provreqwrapper.ProvisioningRequest{newProvReqA, provisionedAcceptedProvReqB},
			checkCapacityProcessorInstance: "test-prefix.",
		},
		{
			name:                           "New check capacity ProvisioningRequest with matching prefix, pods are injected and Accepted condition is added",
			provReqs:                       []*provreqwrapper.ProvisioningRequest{newProvReqAPrefixed, provisionedAcceptedProvReqB},
			checkCapacityProcessorInstance: "test-prefix.",
			wantUnscheduledPodCount:        podsA,
			wantUpdatedConditionName:       newProvReqAPrefixed.Name,
		},
		{
			name:                     "Provisioned=False, pods are injected",
			provReqs:                 []*provreqwrapper.ProvisioningRequest{notProvisionedAcceptedProvReqB, failedProvReq},
			wantUnscheduledPodCount:  podsB,
			wantUpdatedConditionName: notProvisionedAcceptedProvReqB.Name,
		},
		{
			name:     "Provisioned=True, no pods are injected",
			provReqs: []*provreqwrapper.ProvisioningRequest{provisionedAcceptedProvReqB, failedProvReq},
		},
		{
			name:     "Provisioned=False, ProvReq is backed off, no pods are injected",
			provReqs: []*provreqwrapper.ProvisioningRequest{notProvisionedRecentlyProvReqB},
		},
		{
			name:     "Provisioned=Unknown, no pods are injected",
			provReqs: []*provreqwrapper.ProvisioningRequest{unknownProvisionedProvReqB, failedProvReq},
		},
		{
			name:     "ProvisionedClass is unknown, no pods are injected",
			provReqs: []*provreqwrapper.ProvisioningRequest{unknownClass, failedProvReq},
		},
		{
			name:                                 "Multiple eligible best-effort-atomic ProvisioningRequests without batch processing, only one is injected",
			provReqs:                             eligibleAtomicProvReqs,
			wantUnscheduledPodCount:              podsA,
			expectedAcceptedProvisioningRequests: []*provreqwrapper.ProvisioningRequest{eligibleAtomicProvReq1},
		},
		{
			name:                                 "Multiple eligible best-effort-atomic ProvisioningRequests with batch processing, all are injected and Accepted",
			provReqs:                             eligibleAtomicProvReqs,
			bestEffortAtomicBatchProcessing:      true,
			bestEffortAtomicMaxBatchSize:         10,
			wantUnscheduledPodCount:              3 * podsA,
			expectedAcceptedProvisioningRequests: eligibleAtomicProvReqs,
		},
		{
			name:                                 "Best-effort-atomic batch is capped by the max batch size",
			provReqs:                             eligibleAtomicProvReqs,
			bestEffortAtomicBatchProcessing:      true,
			bestEffortAtomicMaxBatchSize:         2,
			wantUnscheduledPodCount:              2 * podsA,
			expectedAcceptedProvisioningRequests: []*provreqwrapper.ProvisioningRequest{eligibleAtomicProvReq1, eligibleAtomicProvReq2},
		},
		{
			name:                            "Best-effort-atomic batching does not starve an older check capacity request",
			provReqs:                        append([]*provreqwrapper.ProvisioningRequest{newProvReqA}, eligibleAtomicProvReqs...),
			bestEffortAtomicBatchProcessing: true,
			bestEffortAtomicMaxBatchSize:    10,
			wantUnscheduledPodCount:         podsA,
			wantUpdatedConditionName:        newProvReqA.Name,
		},
		{
			name:                             "Provisioned=False, pods are injected but unschedulable pod list is not overwriten",
			provReqs:                         []*provreqwrapper.ProvisioningRequest{newProvReqA},
			existingUnsUnschedulablePodCount: 50,
			wantUnscheduledPodCount:          podsA + 50,
			wantUpdatedConditionName:         newProvReqA.Name,
		},
	}
	for _, tc := range testCases {
		client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, tc.provReqs...)
		backoffTime := lru.New(100)
		backoffTime.Add(key(notProvisionedRecentlyProvReqB), retryBackoff{delay: 2 * time.Minute, failedAt: minAgo})
		injector := ProvisioningRequestPodsInjector{
			initialRetryTime:                   1 * time.Minute,
			maxBackoffTime:                     10 * time.Minute,
			backoffDuration:                    backoffTime,
			clock:                              clock.NewFakePassiveClock(now),
			client:                             client,
			lastProvisioningRequestProcessTime: now,
			checkCapacityBatchProcessing:       tc.checkCapacityBatchProcessing,
			checkCapacityProcessorInstance:     tc.checkCapacityProcessorInstance,
			bestEffortAtomicBatchProcessing:    tc.bestEffortAtomicBatchProcessing,
			bestEffortAtomicMaxBatchSize:       tc.bestEffortAtomicMaxBatchSize,
		}
		getUnscheduledPods, err := injector.Process(context.Background(), nil, provreqwrapper.BuildTestPods("ns", "pod", tc.existingUnsUnschedulablePodCount))
		if err != nil {
			t.Errorf("%s failed: injector.Process return error %v", tc.name, err)
		}
		if len(getUnscheduledPods) != tc.wantUnscheduledPodCount {
			t.Errorf("%s failed: injector.Process return %d unscheduled pods, want %d", tc.name, len(getUnscheduledPods), tc.wantUnscheduledPodCount)
		}
		for _, expected := range tc.expectedAcceptedProvisioningRequests {
			pr, err := client.ProvisioningRequestNoCache(expected.Namespace, expected.Name)
			if err != nil {
				t.Errorf("%s: failed to get ProvisioningRequest %s/%s: %v", tc.name, expected.Namespace, expected.Name, err)
				continue
			}
			if accepted := apimeta.FindStatusCondition(pr.Status.Conditions, v1.Accepted); accepted == nil {
				t.Errorf("%s: injector.Process hasn't added accepted condition for ProvisioningRequest %s/%s", tc.name, expected.Namespace, expected.Name)
			}
		}
		if tc.wantUpdatedConditionName == "" {
			continue
		}
		pr, _ := client.ProvisioningRequestNoCache("ns", tc.wantUpdatedConditionName)
		accepted := apimeta.FindStatusCondition(pr.Status.Conditions, v1.Accepted)
		if tc.checkCapacityBatchProcessing {
			if accepted != nil {
				t.Errorf("%s: injector.Process updated accepted condition for ProvisioningRequest %s, but shouldn't for batch processing", tc.name, tc.wantUpdatedConditionName)
			}
		} else {
			if accepted == nil || accepted.LastTransitionTime != metav1.NewTime(now) {
				t.Errorf("%s: injector.Process hasn't update accepted condition for ProvisioningRequest %s", tc.name, tc.wantUpdatedConditionName)
			}
		}
	}

}

func testProvisioningRequestWithCondition(name string, podCount int, class string, conditions ...metav1.Condition) *provreqwrapper.ProvisioningRequest {
	pr := provreqwrapper.BuildTestProvisioningRequest("ns", name, "10", "100", "", int32(podCount), false, time.Now(), class)
	pr.Status.Conditions = conditions
	return pr
}

// TestBestEffortAtomicBatchRetryBackoff checks that requests which are being retried are included
// in the batch they qualify for, and that their retry backoff advances exactly once per failure.
// Selecting a batch involves inspecting the queue before picking the requests, and neither the
// inspection nor the selection may delay a request: one that is left out of the attempt that is
// finally made must be retried as soon as before.
func TestBestEffortAtomicBatchRetryBackoff(t *testing.T) {
	now := time.Now()
	initialRetryTime := time.Minute
	podCount := 10

	// Retried long enough ago to be due for a retry, but not long enough to still be due if the
	// backoff were doubled twice in the same iteration.
	justDue := metav1.Condition{
		Type:               v1.Provisioned,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(now.Add(-90 * time.Second)),
	}

	provReqs := []*provreqwrapper.ProvisioningRequest{
		testProvisioningRequestWithCondition("retry-atomic-1", podCount, v1.ProvisioningClassBestEffortAtomicScaleUp, justDue),
		testProvisioningRequestWithCondition("retry-atomic-2", podCount, v1.ProvisioningClassBestEffortAtomicScaleUp, justDue),
		testProvisioningRequestWithCondition("retry-atomic-3", podCount, v1.ProvisioningClassBestEffortAtomicScaleUp, justDue),
	}
	// The backoff cache is keyed by UID, which the test builder leaves empty.
	for _, pr := range provReqs {
		pr.UID = types.UID(pr.Name)
	}

	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, provReqs...)
	injector := ProvisioningRequestPodsInjector{
		initialRetryTime:                   initialRetryTime,
		maxBackoffTime:                     10 * time.Minute,
		backoffDuration:                    lru.New(100),
		clock:                              clock.NewFakePassiveClock(now),
		client:                             client,
		lastProvisioningRequestProcessTime: now,
		bestEffortAtomicBatchProcessing:    true,
		bestEffortAtomicMaxBatchSize:       10,
	}

	unschedulablePods, err := injector.Process(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("injector.Process returned error %v", err)
	}
	if want := len(provReqs) * podCount; len(unschedulablePods) != want {
		t.Errorf("injector.Process injected %d pods, want %d", len(unschedulablePods), want)
	}
	for _, pr := range provReqs {
		if got := injector.retryTime(pr); got != initialRetryTime {
			t.Errorf("retry backoff for ProvisioningRequest %s is %v after selection, want %v", pr.Name, got, initialRetryTime)
		}
		if !injector.IsAvailableForProvisioning(pr) {
			t.Errorf("ProvisioningRequest %s is no longer eligible, although it hasn't failed again", pr.Name)
		}
	}

	// Another failure moves the condition's transition time, which doubles the backoff once,
	// however often it's checked.
	failedAgain := provReqs[0]
	apimeta.FindStatusCondition(failedAgain.Status.Conditions, v1.Provisioned).LastTransitionTime = metav1.NewTime(now)
	for i := 0; i < 3; i++ {
		if got, want := injector.retryTime(failedAgain), 2*initialRetryTime; got != want {
			t.Errorf("retry backoff after another failure is %v, want %v", got, want)
		}
	}
}

// TestRetryBackoffSurvivesScans checks that scanning requests which wait for a retry doesn't evict
// the retry backoff of other requests from the bounded cache, which would reset their delay.
func TestRetryBackoffSurvivesScans(t *testing.T) {
	now := time.Now()
	waiting := metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-30 * time.Second))}
	var requests []*provreqwrapper.ProvisioningRequest
	for i := 0; i < 4; i++ {
		pr := testProvisioningRequestWithCondition(fmt.Sprintf("waiting-%d", i), 1, v1.ProvisioningClassBestEffortAtomicScaleUp, waiting)
		pr.UID = types.UID(pr.Name)
		requests = append(requests, pr)
	}
	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, requests...)
	injector := NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             2,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    10,
		KubeClientBurst:                 1,
	})
	injector.clock = clock.NewFakePassiveClock(now)
	// The cache only has room for the backoff of the two requests that already failed repeatedly.
	for _, pr := range requests[:2] {
		injector.backoffDuration.Add(key(pr), retryBackoff{delay: 4 * time.Minute, failedAt: waiting.LastTransitionTime.Time})
	}
	for i := 0; i < 3; i++ {
		if pods, err := injector.Process(context.Background(), nil, nil); err != nil || len(pods) != 0 {
			t.Fatalf("injector.Process injected %d pods (err %v), want none while every request waits for a retry", len(pods), err)
		}
	}
	for _, pr := range requests[:2] {
		if got := injector.retryTime(pr); got != 4*time.Minute {
			t.Errorf("retry backoff for %s = %v after scans, want %v", pr.Name, got, 4*time.Minute)
		}
	}
}

// TestAwaitProvisionedCondition checks that a best-effort-atomic request whose Provisioned
// condition was just written isn't picked again before the informer cache shows the new condition,
// and that it becomes eligible again after its retry delay if the write never shows.
func TestAwaitProvisionedCondition(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	fakeClock := clock.NewFakePassiveClock(now)
	injector := NewProvisioningRequestPodsInjectorWithOptions(nil, PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             100,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    10,
		KubeClientBurst:                 1,
	})
	injector.clock = fakeClock
	request := func(name string, conditions ...metav1.Condition) *provreqwrapper.ProvisioningRequest {
		pr := testProvisioningRequestWithCondition(name, 1, v1.ProvisioningClassBestEffortAtomicScaleUp, conditions...)
		pr.UID = types.UID(name)
		return pr
	}
	failedAt := func(at time.Time) metav1.Condition {
		return metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionFalse, Reason: "CapacityIsNotFound", LastTransitionTime: metav1.NewTime(at)}
	}

	stale := request("failed", failedAt(now.Add(-time.Hour)))
	if !injector.IsAvailableForProvisioning(stale) {
		t.Fatal("a request that failed an hour ago isn't eligible")
	}
	injector.AwaitProvisionedCondition(stale)
	if injector.IsAvailableForProvisioning(stale) {
		t.Error("request is eligible before the cache shows the condition written for it")
	}
	failedAgain := request("failed", failedAt(now))
	if injector.IsAvailableForProvisioning(failedAgain) {
		t.Error("request is eligible right after failing again")
	}
	fakeClock.SetTime(now.Add(time.Minute + time.Second))
	if !injector.IsAvailableForProvisioning(failedAgain) {
		t.Error("request isn't eligible once its retry delay passed")
	}

	// A write that never shows, for example because it failed, only delays the request.
	injector.AwaitProvisionedCondition(stale)
	if injector.IsAvailableForProvisioning(stale) {
		t.Error("request is eligible before the cache shows the condition written for it")
	}
	fakeClock.SetTime(fakeClock.Now().Add(time.Minute))
	if !injector.IsAvailableForProvisioning(stale) {
		t.Error("request isn't eligible after its retry delay, although the written condition never showed")
	}

	// A request without a Provisioned condition waits for its first one.
	fresh := request("fresh")
	injector.AwaitProvisionedCondition(fresh)
	if injector.IsAvailableForProvisioning(fresh) {
		t.Error("request is eligible before the cache shows its first Provisioned condition")
	}
	admitted := request("fresh", metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(fakeClock.Now())})
	if injector.IsAvailableForProvisioning(admitted) {
		t.Error("an admitted request is eligible")
	}
	if _, found := injector.awaitedConditions.Get(awaitKey(fresh)); found {
		t.Error("the awaited condition wasn't forgotten once it showed")
	}
}

// TestCheckCapacityRetryDelayDoublesWhenPicked checks that check capacity requests, which are
// attempted whenever they're picked, double their retry delay when they're picked, unlike
// best-effort-atomic requests, which batches can pick and then leave out.
func TestCheckCapacityRetryDelayDoublesWhenPicked(t *testing.T) {
	now := time.Now()
	failed := metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-90 * time.Second))}
	checkCapacity := testProvisioningRequestWithCondition("check-capacity", 1, v1.ProvisioningClassCheckCapacity, failed)
	checkCapacity.UID = types.UID(checkCapacity.Name)
	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, checkCapacity)
	injector := NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
		InitialBackoffTime:           time.Minute,
		MaxBackoffTime:               10 * time.Minute,
		MaxBackoffCacheSize:          100,
		CheckCapacityBatchProcessing: true,
		BestEffortAtomicMaxBatchSize: 1,
		KubeClientBurst:              1,
	})
	injector.clock = clock.NewFakePassiveClock(now)
	batch, err := injector.GetCheckCapacityBatch(context.Background(), 10)
	if err != nil || len(batch) != 1 {
		t.Fatalf("GetCheckCapacityBatch returned %d requests (err %v), want 1", len(batch), err)
	}
	if got, want := injector.retryTime(checkCapacity), 2*time.Minute; got != want {
		t.Errorf("retry delay after picking = %v, want %v", got, want)
	}
	if injector.IsAvailableForProvisioning(checkCapacity) {
		t.Error("picking the request didn't delay its next attempt")
	}
}

func TestBestEffortAtomicBatchSchedulingRequirements(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*apiv1.PodTemplateSpec)
	}{
		{
			name: "CPU requests",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.Containers[0].Resources.Requests[apiv1.ResourceCPU] = resource.MustParse("5")
			},
		},
		{
			name: "memory requests",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.Containers[0].Resources.Requests[apiv1.ResourceMemory] = resource.MustParse("50")
			},
		},
		{
			name: "GPU requests",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("1")
				template.Spec.Containers[0].Resources.Limits["nvidia.com/gpu"] = resource.MustParse("1")
			},
		},
		{
			name: "init container resources",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.InitContainers = []apiv1.Container{{
					Name: "init", Image: "init",
					Resources: apiv1.ResourceRequirements{Requests: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("20")}},
				}}
			},
		},
		{
			name: "node selectors",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.NodeSelector = map[string]string{"hardware": "gpu"}
			},
		},
		{
			name: "node affinity",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.Affinity.NodeAffinity = &apiv1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{NodeSelectorTerms: []apiv1.NodeSelectorTerm{{
						MatchExpressions: []apiv1.NodeSelectorRequirement{{Key: "hardware", Operator: apiv1.NodeSelectorOpIn, Values: []string{"gpu"}}},
					}}},
				}
			},
		},
		{
			name: "pod anti-affinity",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.Affinity.PodAntiAffinity = &apiv1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []apiv1.PodAffinityTerm{{
						TopologyKey: "kubernetes.io/hostname", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test-app"}},
					}},
				}
			},
		},
		{
			name: "tolerations",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.Tolerations = []apiv1.Toleration{{Key: "nvidia.com/gpu", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule}}
			},
		},
		{
			name: "topology spread constraints",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.TopologySpreadConstraints = []apiv1.TopologySpreadConstraint{{
					MaxSkew: 1, TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: apiv1.DoNotSchedule,
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test-app"}},
				}}
			},
		},
		{
			name: "persistent volumes",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.Volumes = []apiv1.Volume{{Name: "data", VolumeSource: apiv1.VolumeSource{
					PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
				}}}
			},
		},
		{
			name: "resource claims",
			mutate: func(template *apiv1.PodTemplateSpec) {
				claimName := "gpu-claim"
				template.Spec.ResourceClaims = []apiv1.PodResourceClaim{{Name: "gpu", ResourceClaimName: &claimName}}
			},
		},
		{
			name: "host ports",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.Containers[0].Ports = []apiv1.ContainerPort{{ContainerPort: 8080, HostPort: 8080, Protocol: apiv1.ProtocolTCP}}
			},
		},
		{
			name: "scheduler name",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Spec.SchedulerName = "custom-scheduler"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now()
			initialRetryTime := time.Minute
			notProvisioned := metav1.Condition{
				Type:               v1.Provisioned,
				Status:             metav1.ConditionFalse,
				LastTransitionTime: metav1.NewTime(now.Add(-90 * time.Second)),
			}
			oldest := testProvisioningRequestWithCondition("oldest", 1, v1.ProvisioningClassBestEffortAtomicScaleUp, notProvisioned)
			incompatible := testProvisioningRequestWithCondition("incompatible", 2, v1.ProvisioningClassBestEffortAtomicScaleUp, notProvisioned)
			compatible := testProvisioningRequestWithCondition("compatible", 3, v1.ProvisioningClassBestEffortAtomicScaleUp, notProvisioned)
			requests := []*provreqwrapper.ProvisioningRequest{oldest, incompatible, compatible}
			for index, request := range requests {
				request.UID = types.UID(request.Name)
				request.CreationTimestamp = metav1.NewTime(now.Add(time.Duration(index-3) * time.Hour))
			}
			test.mutate(&incompatible.PodTemplates[0].Template)

			client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, compatible, incompatible, oldest)
			injector := NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
				InitialBackoffTime:              initialRetryTime,
				MaxBackoffTime:                  10 * time.Minute,
				MaxBackoffCacheSize:             100,
				BestEffortAtomicBatchProcessing: true,
				BestEffortAtomicMaxBatchSize:    2,
				KubeClientBurst:                 1,
			})
			injector.clock = clock.NewFakePassiveClock(now)

			pods, err := injector.Process(context.Background(), nil, nil)
			if err != nil {
				t.Fatalf("injector.Process returned error: %v", err)
			}
			wantNames := []string{oldest.Name, compatible.Name, compatible.Name, compatible.Name}
			var gotNames []string
			for _, pod := range pods {
				gotNames = append(gotNames, pod.Annotations[v1.ProvisioningRequestPodAnnotationKey])
			}
			if !reflect.DeepEqual(gotNames, wantNames) {
				t.Fatalf("injected requests %v, want %v", gotNames, wantNames)
			}
			for _, request := range requests {
				updated, err := client.ProvisioningRequestNoCache(request.Namespace, request.Name)
				if err != nil {
					t.Fatalf("failed to get ProvisioningRequest %s: %v", request.Name, err)
				}
				wantAccepted := request != incompatible
				if accepted := apimeta.IsStatusConditionTrue(updated.Status.Conditions, v1.Accepted); accepted != wantAccepted {
					t.Errorf("Accepted for %s = %t, want %t", request.Name, accepted, wantAccepted)
				}
				if !wantAccepted && !reflect.DeepEqual(updated.Status.Conditions, request.Status.Conditions) {
					t.Errorf("conditions changed for deferred request %s", request.Name)
				}
				if retryTime := injector.retryTime(request); retryTime != initialRetryTime {
					t.Errorf("retry backoff for %s = %v, want %v", request.Name, retryTime, initialRetryTime)
				}
			}
			if !injector.IsAvailableForProvisioning(incompatible) {
				t.Error("incompatible request is no longer eligible for provisioning")
			}

			// The first batch is admitted, so the next iteration can only pick the incompatible request.
			for _, request := range []*provreqwrapper.ProvisioningRequest{oldest, compatible} {
				updated, err := client.ProvisioningRequestNoCache(request.Namespace, request.Name)
				if err != nil {
					t.Fatalf("failed to get ProvisioningRequest %s: %v", request.Name, err)
				}
				apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
					Type: v1.Provisioned, Status: metav1.ConditionTrue, Reason: "CapacityIsProvisioned", LastTransitionTime: metav1.NewTime(now),
				})
				if _, err := client.UpdateProvisioningRequest(context.Background(), updated.ProvisioningRequest); err != nil {
					t.Fatalf("failed to admit ProvisioningRequest %s: %v", request.Name, err)
				}
			}
			for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
				cached, err := client.ProvisioningRequests(context.Background())
				admitted := 0
				for _, request := range cached {
					if apimeta.IsStatusConditionTrue(request.Status.Conditions, v1.Provisioned) {
						admitted++
					}
				}
				if err == nil && admitted == 2 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the informer cache didn't observe the admitted batch")
				}
			}

			nextPods, err := injector.Process(context.Background(), nil, nil)
			if err != nil {
				t.Fatalf("next injector.Process returned error: %v", err)
			}
			if len(nextPods) != incompatible.PodCount() {
				t.Fatalf("next iteration injected %d pods, want %d", len(nextPods), incompatible.PodCount())
			}
			for _, pod := range nextPods {
				if pod.Annotations[v1.ProvisioningRequestPodAnnotationKey] != incompatible.Name {
					t.Errorf("next iteration injected incompatible pod %s", pod.Name)
				}
			}
		})
	}
}

func TestBestEffortAtomicBatchMultiplePodSets(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*provreqwrapper.ProvisioningRequest)
		wantBatch int
	}{
		{
			name: "equivalent pod sets with different counts",
			mutate: func(request *provreqwrapper.ProvisioningRequest) {
				request.Spec.PodSets[0].Count = 3
				request.Spec.PodSets[1].Count = 4
			},
			wantBatch: 2,
		},
		{
			name: "different second pod set",
			mutate: func(request *provreqwrapper.ProvisioningRequest) {
				request.PodTemplates[1].Template.Spec.NodeSelector = map[string]string{"hardware": "gpu"}
			},
			wantBatch: 1,
		},
		{
			name: "different number of pod sets",
			mutate: func(request *provreqwrapper.ProvisioningRequest) {
				request.Spec.PodSets = request.Spec.PodSets[:1]
				request.PodTemplates = request.PodTemplates[:1]
			},
			wantBatch: 1,
		},
		{
			name: "different namespaces",
			mutate: func(request *provreqwrapper.ProvisioningRequest) {
				request.Namespace = "other-namespace"
				for _, template := range request.PodTemplates {
					template.Namespace = request.Namespace
				}
			},
			wantBatch: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldest := testProvisioningRequestWithCondition("oldest", 1, v1.ProvisioningClassBestEffortAtomicScaleUp)
			next := testProvisioningRequestWithCondition("next", 1, v1.ProvisioningClassBestEffortAtomicScaleUp)
			for _, request := range []*provreqwrapper.ProvisioningRequest{oldest, next} {
				extra := testProvisioningRequestWithCondition(request.Name+"-extra", 2, v1.ProvisioningClassBestEffortAtomicScaleUp)
				extra.PodTemplates[0].Template.Spec.Containers[0].Resources.Requests[apiv1.ResourceCPU] = resource.MustParse("5")
				request.Spec.PodSets = append(request.Spec.PodSets, extra.Spec.PodSets...)
				request.PodTemplates = append(request.PodTemplates, extra.PodTemplates...)
			}
			test.mutate(next)
			client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, next, oldest)
			injector := NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
				InitialBackoffTime:              time.Minute,
				MaxBackoffTime:                  10 * time.Minute,
				MaxBackoffCacheSize:             100,
				BestEffortAtomicBatchProcessing: true,
				BestEffortAtomicMaxBatchSize:    10,
				KubeClientBurst:                 1,
			})
			batch, err := injector.GetBestEffortAtomicBatch(context.Background(), 10)
			if err != nil {
				t.Fatalf("GetBestEffortAtomicBatch returned error: %v", err)
			}
			if len(batch) != test.wantBatch {
				t.Fatalf("batch contains %d requests, want %d", len(batch), test.wantBatch)
			}
			if batch[0].PrWrapper.Name != oldest.Name {
				t.Errorf("batch starts with %s, want %s", batch[0].PrWrapper.Name, oldest.Name)
			}
			for _, request := range batch {
				if len(request.Pods) != request.PrWrapper.PodCount() {
					t.Errorf("request %s was split: got %d pods, want %d", request.PrWrapper.Name, len(request.Pods), request.PrWrapper.PodCount())
				}
				updated, err := client.ProvisioningRequestNoCache(request.PrWrapper.Namespace, request.PrWrapper.Name)
				if err != nil {
					t.Fatalf("failed to get ProvisioningRequest: %v", err)
				}
				if apimeta.FindStatusCondition(updated.Status.Conditions, v1.Accepted) != nil {
					t.Errorf("GetBestEffortAtomicBatch accepted request %s", request.PrWrapper.Name)
				}
			}
		})
	}
}

func TestSameSchedulingRequirementsPreservesTemplates(t *testing.T) {
	first := testProvisioningRequestWithCondition("first", 1, v1.ProvisioningClassBestEffortAtomicScaleUp)
	second := testProvisioningRequestWithCondition("second", 3, v1.ProvisioningClassBestEffortAtomicScaleUp)
	for _, request := range []*provreqwrapper.ProvisioningRequest{first, second} {
		spec := &request.PodTemplates[0].Template.Spec
		spec.Hostname = request.Name
		spec.Volumes = []apiv1.Volume{{Name: request.Name, VolumeSource: apiv1.VolumeSource{
			Projected: &apiv1.ProjectedVolumeSource{},
		}}}
		spec.Containers[0].Env = []apiv1.EnvVar{{Name: "REQUEST", Value: request.Name}}
		spec.Containers[0].VolumeMounts = []apiv1.VolumeMount{{Name: request.Name, MountPath: "/token"}}
		spec.InitContainers = []apiv1.Container{{
			Name: "init", Image: "init",
			Env:          []apiv1.EnvVar{{Name: "REQUEST", Value: request.Name}},
			VolumeMounts: []apiv1.VolumeMount{{Name: request.Name, MountPath: "/token"}},
		}}
	}
	second.PodTemplates[0].Template.Spec.Containers[0].Resources.Requests[apiv1.ResourceCPU] = resource.MustParse("10000m")
	firstBefore := first.PodTemplates[0].DeepCopy()
	secondBefore := second.PodTemplates[0].DeepCopy()
	if !sameSchedulingRequirements(first, second) {
		t.Error("equivalent scheduling requirements were considered incompatible")
	}
	if !reflect.DeepEqual(firstBefore, first.PodTemplates[0]) || !reflect.DeepEqual(secondBefore, second.PodTemplates[0]) {
		t.Error("comparing scheduling requirements mutated the pod templates")
	}
}

func TestBestEffortAtomicBatchIgnoresWorkloadIdentity(t *testing.T) {
	// Kueue copies each Job's pod template, including its per-Job labels, into the
	// ProvisioningRequest's PodTemplate. Requests for identical Jobs must still share a batch.
	var requests []*provreqwrapper.ProvisioningRequest
	for i, job := range []string{"train-a", "train-b", "train-c"} {
		request := testProvisioningRequestWithCondition(job, i+1, v1.ProvisioningClassBestEffortAtomicScaleUp)
		request.UID = types.UID(job)
		request.PodTemplates[0].Template.Labels = map[string]string{"batch.kubernetes.io/job-name": job}
		request.PodTemplates[0].Template.Annotations = map[string]string{"example.com/workload": job}
		requests = append(requests, request)
	}
	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, requests...)
	injector := NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             100,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    10,
		KubeClientBurst:                 1,
	})
	batch, err := injector.GetBestEffortAtomicBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("GetBestEffortAtomicBatch returned error: %v", err)
	}
	if len(batch) != len(requests) {
		t.Fatalf("batch contains %d requests, want %d", len(batch), len(requests))
	}
	for _, request := range batch {
		for _, pod := range request.Pods {
			if pod.Labels["batch.kubernetes.io/job-name"] != request.PrWrapper.Name {
				t.Errorf("pod %s lost its own job-name label", pod.Name)
			}
		}
	}
}

func TestCheckCapacityBatchDifferentSchedulingRequirements(t *testing.T) {
	first := testProvisioningRequestWithCondition("first", 1, v1.ProvisioningClassCheckCapacity)
	second := testProvisioningRequestWithCondition("second", 2, v1.ProvisioningClassCheckCapacity)
	second.PodTemplates[0].Template.Spec.NodeSelector = map[string]string{"hardware": "gpu"}
	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, first, second)
	injector := NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             100,
		CheckCapacityBatchProcessing:    true,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    10,
		KubeClientBurst:                 1,
	})
	batch, err := injector.GetCheckCapacityBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("GetCheckCapacityBatch returned error: %v", err)
	}
	if len(batch) != 2 {
		t.Errorf("check-capacity batch contains %d requests, want 2", len(batch))
	}
}

func TestBestEffortAtomicLargeBatchInjection(t *testing.T) {
	const requestCount = 100
	provReqs := make([]*provreqwrapper.ProvisioningRequest, 0, requestCount)
	for i := 0; i < requestCount; i++ {
		pr := testProvisioningRequestWithCondition(fmt.Sprintf("atomic-%03d", i), 1, v1.ProvisioningClassBestEffortAtomicScaleUp)
		pr.UID = types.UID(pr.Name)
		provReqs = append(provReqs, pr)
	}

	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, provReqs...)
	injector := NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             1000,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    requestCount,
		KubeClientBurst:                 10,
	})

	unschedulablePods, err := injector.Process(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("injector.Process returned error: %v", err)
	}
	if len(unschedulablePods) != requestCount {
		t.Fatalf("injector.Process injected %d pods, want %d", len(unschedulablePods), requestCount)
	}

	updated, err := client.ProvisioningRequestsNoCache()
	if err != nil {
		t.Fatalf("failed to list updated ProvisioningRequests: %v", err)
	}
	accepted := 0
	for _, pr := range updated {
		if apimeta.IsStatusConditionTrue(pr.Status.Conditions, v1.Accepted) {
			accepted++
		}
	}
	if accepted != requestCount {
		t.Errorf("%d ProvisioningRequests have Accepted=True, want %d", accepted, requestCount)
	}
}

func TestProvisioningRequestUpdateConcurrency(t *testing.T) {
	tests := []struct {
		name            string
		kubeClientBurst int
		want            int
	}{
		{name: "configured burst", kubeClientBurst: 25, want: 25},
		{name: "zero burst falls back to serial updates", kubeClientBurst: 0, want: 1},
		{name: "negative burst falls back to serial updates", kubeClientBurst: -1, want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			injector := NewProvisioningRequestPodsInjectorWithOptions(nil, PodsInjectorOptions{
				InitialBackoffTime:              time.Minute,
				MaxBackoffTime:                  10 * time.Minute,
				MaxBackoffCacheSize:             1000,
				BestEffortAtomicBatchProcessing: true,
				BestEffortAtomicMaxBatchSize:    10,
				KubeClientBurst:                 tc.kubeClientBurst,
			})
			if injector.maxConcurrentUpdates != tc.want {
				t.Errorf("maxConcurrentUpdates = %d, want %d", injector.maxConcurrentUpdates, tc.want)
			}
		})
	}
}

func TestOriginalPodsInjectorConstructor(t *testing.T) {
	constructor := NewProvisioningRequestPodsInjector
	var original func(*provreqclient.ProvisioningRequestClient, time.Duration, time.Duration, int, bool, string) *ProvisioningRequestPodsInjector = constructor
	injector := original(nil, time.Minute, 10*time.Minute, 100, true, "processor")
	assert.Equal(t, time.Minute, injector.initialRetryTime)
	assert.Equal(t, 10*time.Minute, injector.maxBackoffTime)
	assert.True(t, injector.checkCapacityBatchProcessing)
	assert.Equal(t, "processor", injector.checkCapacityProcessorInstance)
	assert.False(t, injector.bestEffortAtomicBatchProcessing)
	assert.Equal(t, 1, injector.bestEffortAtomicMaxBatchSize)
	assert.Equal(t, 1, injector.maxConcurrentUpdates)
}

func TestBestEffortAtomicBatchSizeOneDisablesBatching(t *testing.T) {
	injector := NewProvisioningRequestPodsInjectorWithOptions(nil, PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             1000,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    1,
		KubeClientBurst:                 10,
	})
	if injector.bestEffortAtomicBatchProcessing {
		t.Error("best-effort-atomic batching enabled with a maximum batch size of one")
	}
	if injector.bestEffortAtomicMaxBatchSize != 1 {
		t.Errorf("bestEffortAtomicMaxBatchSize = %d, want 1", injector.bestEffortAtomicMaxBatchSize)
	}
}
