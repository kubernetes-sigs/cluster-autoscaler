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
// in the batch they qualify for, and that their retry backoff is advanced exactly once per
// iteration. Selecting a batch involves inspecting the queue before picking the requests, and an
// inspection with side effects would both drop the request from its own batch and make the
// backoff grow twice as fast as configured.
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
		val, found := injector.backoffDuration.Get(key(pr))
		if !found {
			t.Errorf("no backoff recorded for ProvisioningRequest %s", pr.Name)
			continue
		}
		if got, want := val.(time.Duration), 2*initialRetryTime; got != want {
			t.Errorf("backoff for ProvisioningRequest %s is %v, want %v", pr.Name, got, want)
		}
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
		{
			name: "pod labels",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Labels["app"] = "other-app"
			},
		},
		{
			name: "pod annotations",
			mutate: func(template *apiv1.PodTemplateSpec) {
				template.Annotations = map[string]string{"example.com/scheduling-policy": "gpu"}
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
			injector := NewProvisioningRequestPodsInjector(client, initialRetryTime, 10*time.Minute, 100, false, "", true, 2, 1)
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
				wantRetryTime := initialRetryTime
				if wantAccepted {
					wantRetryTime *= 2
				}
				if retryTime := injector.retryTime(request); retryTime != wantRetryTime {
					t.Errorf("retry backoff for %s = %v, want %v", request.Name, retryTime, wantRetryTime)
				}
			}
			if !injector.IsAvailableForProvisioning(incompatible) {
				t.Error("incompatible request is no longer eligible for provisioning")
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
			injector := NewProvisioningRequestPodsInjector(client, time.Minute, 10*time.Minute, 100, false, "", true, 10, 1)
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

func TestCheckCapacityBatchDifferentSchedulingRequirements(t *testing.T) {
	first := testProvisioningRequestWithCondition("first", 1, v1.ProvisioningClassCheckCapacity)
	second := testProvisioningRequestWithCondition("second", 2, v1.ProvisioningClassCheckCapacity)
	second.PodTemplates[0].Template.Spec.NodeSelector = map[string]string{"hardware": "gpu"}
	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, first, second)
	injector := NewProvisioningRequestPodsInjector(client, time.Minute, 10*time.Minute, 100, true, "", true, 10, 1)
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
	injector := NewProvisioningRequestPodsInjector(client, time.Minute, 10*time.Minute, 1000, false, "", true, requestCount, 10)

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
			injector := NewProvisioningRequestPodsInjector(nil, time.Minute, 10*time.Minute, 1000, false, "", true, 10, tc.kubeClientBurst)
			if injector.maxConcurrentUpdates != tc.want {
				t.Errorf("maxConcurrentUpdates = %d, want %d", injector.maxConcurrentUpdates, tc.want)
			}
		})
	}
}

func TestBestEffortAtomicBatchSizeOneDisablesBatching(t *testing.T) {
	injector := NewProvisioningRequestPodsInjector(nil, time.Minute, 10*time.Minute, 1000, false, "", true, 1, 10)
	if injector.bestEffortAtomicBatchProcessing {
		t.Error("best-effort-atomic batching enabled with a maximum batch size of one")
	}
	if injector.bestEffortAtomicMaxBatchSize != 1 {
		t.Errorf("bestEffortAtomicMaxBatchSize = %d, want 1", injector.bestEffortAtomicMaxBatchSize)
	}
}
