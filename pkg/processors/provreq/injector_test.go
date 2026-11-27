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
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
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

	testCases := []struct {
		name                             string
		provReqs                         []*provreqwrapper.ProvisioningRequest
		existingUnsUnschedulablePodCount int
		checkCapacityBatchProcessing     bool
		checkCapacityProcessorInstance   string
		wantUnscheduledPodCount          int
		wantUpdatedConditionName         string
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
		backoffTime.Add(key(notProvisionedRecentlyProvReqB), 2*time.Minute)
		injector := ProvisioningRequestPodsInjector{
			initialRetryTime:                   1 * time.Minute,
			maxBackoffTime:                     10 * time.Minute,
			backoffDuration:                    backoffTime,
			clock:                              clock.NewFakePassiveClock(now),
			client:                             client,
			lastProvisioningRequestProcessTime: now,
			checkCapacityBatchProcessing:       tc.checkCapacityBatchProcessing,
			checkCapacityProcessorInstance:     tc.checkCapacityProcessorInstance,
			releasedPicks:                      lru.New(100),
		}
		getUnscheduledPods, err := injector.Process(context.Background(), nil, provreqwrapper.BuildTestPods("ns", "pod", tc.existingUnsUnschedulablePodCount))
		if err != nil {
			t.Errorf("%s failed: injector.Process return error %v", tc.name, err)
		}
		if len(getUnscheduledPods) != tc.wantUnscheduledPodCount {
			t.Errorf("%s failed: injector.Process return %d unscheduled pods, want %d", tc.name, len(getUnscheduledPods), tc.wantUnscheduledPodCount)
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

func TestGetPodsFromNextRequestRepeatsUnevaluatedPick(t *testing.T) {
	now := time.Now()
	hourAgo := now.Add(-1 * time.Hour)
	backoffTime := now.Add(-90 * time.Second)
	failed := metav1.Condition{Type: v1.Failed, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(now)}
	provisioned := metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(now)}
	provisionedAtBackoff := metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(backoffTime)}
	notProvisioned := metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionFalse, LastTransitionTime: metav1.NewTime(now)}
	notProvisionedBeforeBackoff := metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionFalse, LastTransitionTime: metav1.NewTime(backoffTime)}

	testCases := []struct {
		name                    string
		class                   string
		batch                   bool
		firstPick               []metav1.Condition
		firstPickAfter          []metav1.Condition
		firstDeleted            bool
		firstPodTemplateDeleted bool
		loops                   int
		wantPick                string
		wantFirstFailed         bool
	}{
		{
			name:     "first pick not evaluated - first pick injected again",
			loops:    2,
			wantPick: "first",
		},
		{
			name:           "first pick got Provisioned=True - second injected",
			firstPickAfter: []metav1.Condition{provisioned},
			loops:          2,
			wantPick:       "second",
		},
		{
			name:           "first pick got Provisioned=False - second injected",
			firstPickAfter: []metav1.Condition{notProvisioned},
			loops:          2,
			wantPick:       "second",
		},
		{
			name:           "first pick failed - second injected",
			firstPickAfter: []metav1.Condition{failed},
			loops:          2,
			wantPick:       "second",
		},
		{
			name:         "first pick deleted - second injected",
			firstDeleted: true,
			loops:        2,
			wantPick:     "second",
		},
		{
			name:                    "first pick pod template deleted - first marked failed and second injected",
			firstPodTemplateDeleted: true,
			loops:                   2,
			wantPick:                "second",
			wantFirstFailed:         true,
		},
		{
			name:           "retried first pick got Provisioned=False again - second injected",
			firstPick:      []metav1.Condition{notProvisionedBeforeBackoff},
			firstPickAfter: []metav1.Condition{notProvisioned},
			loops:          2,
			wantPick:       "second",
		},
		{
			name:           "retried first pick got Provisioned=True at same transition time - second injected",
			firstPick:      []metav1.Condition{notProvisionedBeforeBackoff},
			firstPickAfter: []metav1.Condition{provisionedAtBackoff},
			loops:          2,
			wantPick:       "second",
		},
		{
			name:           "retried first pick not evaluated - first pick injected again despite doubled backoff",
			firstPick:      []metav1.Condition{notProvisionedBeforeBackoff},
			firstPickAfter: []metav1.Condition{notProvisionedBeforeBackoff},
			loops:          2,
			wantPick:       "first",
		},
		{
			name:                    "retried first pick pod template deleted - first marked failed and second injected",
			firstPick:               []metav1.Condition{notProvisionedBeforeBackoff},
			firstPickAfter:          []metav1.Condition{notProvisionedBeforeBackoff},
			firstPodTemplateDeleted: true,
			loops:                   2,
			wantPick:                "second",
			wantFirstFailed:         true,
		},
		{
			name:     "first pick not evaluated for max loops - first pick injected again",
			loops:    maxUnevaluatedPicks,
			wantPick: "first",
		},
		{
			name:     "first pick not evaluated over max loops - second injected",
			loops:    maxUnevaluatedPicks + 1,
			wantPick: "second",
		},
		{
			name:     "check capacity batch processing over max loops - first pick not held, injected again as longest waiting",
			class:    v1.ProvisioningClassCheckCapacity,
			batch:    true,
			loops:    maxUnevaluatedPicks + 1,
			wantPick: "first",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			class := v1.ProvisioningClassBestEffortAtomicScaleUp
			if tc.class != "" {
				class = tc.class
			}
			first := testProvisioningRequestCreatedAt("first", hourAgo, class, tc.firstPick...)
			second := testProvisioningRequestCreatedAt("second", now.Add(-1*time.Minute), class)
			injector := NewFakePodsInjector(provreqclient.NewFakeProvisioningRequestClient(t.Context(), t, first, second), clock.NewFakePassiveClock(now))
			injector.checkCapacityBatchProcessing = tc.batch
			if got := pickedProvisioningRequest(t, injector); got != "first" {
				t.Fatalf("first loop picked %q, want %q", got, "first")
			}

			firstAfter := testProvisioningRequestCreatedAt("first", hourAgo, class, tc.firstPickAfter...)
			if tc.firstPodTemplateDeleted {
				firstAfter.PodTemplates = nil
			}
			if tc.firstDeleted {
				firstAfter = nil
			}
			injector.client = provreqclient.NewFakeProvisioningRequestClient(t.Context(), t, firstAfter, second)
			var got string
			for loop := 2; loop <= tc.loops; loop++ {
				got = pickedProvisioningRequest(t, injector)
			}
			if got != tc.wantPick {
				t.Errorf("loop %d picked %q, want %q", tc.loops, got, tc.wantPick)
			}
			if got == "first" {
				updated, err := injector.client.ProvisioningRequestNoCache("ns", "first")
				if err != nil {
					t.Fatalf("ProvisioningRequestNoCache returned error: %v", err)
				}
				if apimeta.FindStatusCondition(updated.Status.Conditions, v1.Accepted) != nil {
					t.Errorf("repeat pick rewrote Accepted condition on first ProvisioningRequest: %v", updated.Status.Conditions)
				}
			}
			if tc.wantFirstFailed {
				updated, err := injector.client.ProvisioningRequestNoCache("ns", "first")
				if err != nil {
					t.Fatalf("ProvisioningRequestNoCache returned error: %v", err)
				}
				if !apimeta.IsStatusConditionTrue(updated.Status.Conditions, v1.Failed) {
					t.Errorf("first ProvisioningRequest was not marked Failed, conditions: %v", updated.Status.Conditions)
				}
			}
		})
	}
}

func TestGetPodsFromNextRequestPicksLongestWaiting(t *testing.T) {
	now := time.Now()
	hourAgo := now.Add(-1 * time.Hour)
	twoHoursAgo := now.Add(-2 * time.Hour)
	failed := metav1.Condition{Type: v1.Failed, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(now)}
	provisioned := metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(now)}
	notProvisionedHalfHourAgo := metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-30 * time.Minute))}
	notProvisionedTwoHoursAgo := metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionFalse, LastTransitionTime: metav1.NewTime(twoHoursAgo)}

	testCases := []struct {
		name                  string
		older                 []metav1.Condition
		newerAt               time.Time
		releaseOld            bool
		wantPick              string
		wantOldReleaseRemoved bool
	}{
		{
			name:     "never evaluated - older created injected",
			newerAt:  now,
			wantPick: "older",
		},
		{
			name:     "evaluated after newer was created - newer injected",
			older:    []metav1.Condition{notProvisionedHalfHourAgo},
			newerAt:  hourAgo,
			wantPick: "newer",
		},
		{
			name:     "evaluated before newer was created - older injected",
			older:    []metav1.Condition{notProvisionedTwoHoursAgo},
			newerAt:  hourAgo,
			wantPick: "older",
		},
		{
			name:       "older pick released - newer injected",
			newerAt:    hourAgo,
			releaseOld: true,
			wantPick:   "newer",
		},
		{
			name:                  "older pick released and then provisioned - newer injected and release entry removed",
			older:                 []metav1.Condition{provisioned},
			newerAt:               hourAgo,
			releaseOld:            true,
			wantPick:              "newer",
			wantOldReleaseRemoved: true,
		},
		{
			name:                  "older pick released and then failed - newer injected and release entry removed",
			older:                 []metav1.Condition{failed},
			newerAt:               hourAgo,
			releaseOld:            true,
			wantPick:              "newer",
			wantOldReleaseRemoved: true,
		},
		{
			name:     "same waiting time - first by name injected",
			newerAt:  twoHoursAgo,
			wantPick: "newer",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			older := testProvisioningRequestCreatedAt("older", twoHoursAgo, v1.ProvisioningClassBestEffortAtomicScaleUp, tc.older...)
			newer := testProvisioningRequestCreatedAt("newer", tc.newerAt, v1.ProvisioningClassBestEffortAtomicScaleUp)
			injector := NewFakePodsInjector(provreqclient.NewFakeProvisioningRequestClient(t.Context(), t, newer, older), clock.NewFakePassiveClock(now))
			if tc.releaseOld {
				injector.releasedPicks.Add(key(older), now)
			}
			if got := pickedProvisioningRequest(t, injector); got != tc.wantPick {
				t.Errorf("picked %q, want %q", got, tc.wantPick)
			}
			if _, found := injector.releasedPicks.Get(key(older)); tc.wantOldReleaseRemoved && found {
				t.Errorf("older still in releasedPicks after becoming terminal")
			}
		})
	}
}

func pickedProvisioningRequest(t *testing.T, injector *ProvisioningRequestPodsInjector) string {
	t.Helper()
	pods, err := injector.GetPodsFromNextRequest(t.Context())
	if err != nil {
		t.Fatalf("GetPodsFromNextRequest returned error: %v", err)
	}
	if len(pods) == 0 {
		return ""
	}
	return pods[0].Annotations[v1.ProvisioningRequestPodAnnotationKey]
}

func testProvisioningRequestCreatedAt(name string, created time.Time, class string, conditions ...metav1.Condition) *provreqwrapper.ProvisioningRequest {
	pr := provreqwrapper.BuildTestProvisioningRequest("ns", name, "10", "100", "", 1, false, created, class)
	pr.UID = types.UID(name)
	pr.Status.Conditions = conditions
	return pr
}
