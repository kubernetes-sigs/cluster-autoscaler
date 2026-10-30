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

package provreqclient

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	v1ac "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/applyconfiguration/autoscaling.x-k8s.io/v1"
	prfake "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/clientset/versioned/fake"
	prlisters "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/listers/autoscaling.x-k8s.io/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
)

func newAdmissionTestClient(t *testing.T) (*ProvisioningRequestClient, *prfake.Clientset, cache.Indexer, *v1.ProvisioningRequest) {
	t.Helper()
	request := ProvisioningRequestWrapperForTesting("ns", "request")
	request.UID = "request-uid"
	request.ResourceVersion = "initial"
	request.Spec.ProvisioningClassName = v1.ProvisioningClassBestEffortAtomicScaleUp
	requests := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	templates := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, requests.Add(request.DeepCopy()))
	for _, template := range request.PodTemplates {
		require.NoError(t, templates.Add(template.DeepCopy()))
	}
	api := prfake.NewSimpleClientset(request.DeepCopy())
	client := NewProvisioningRequestClient(api, prlisters.NewProvisioningRequestLister(requests), corelisters.NewPodTemplateLister(templates))
	return client, api, requests, request.ProvisioningRequest
}

func TestPendingAdmissionVisibleUntilReconciled(t *testing.T) {
	for _, write := range []string{"apply", "update"} {
		t.Run(write, func(t *testing.T) {
			client, _, indexer, request := newAdmissionTestClient(t)
			admitted := request.DeepCopy()
			admitted.ResourceVersion = "acknowledged"
			condition := metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionTrue, Reason: "CapacityIsFound", LastTransitionTime: metav1.Now()}
			apimeta.SetStatusCondition(&admitted.Status.Conditions, condition)
			if write == "apply" {
				_, err := client.ApplyProvisioningRequest(v1ac.ProvisioningRequest(request.Name, request.Namespace).
					WithResourceVersion(admitted.ResourceVersion).
					WithStatus(v1ac.ProvisioningRequestStatus().WithConditions(metav1ac.Condition().
						WithType(condition.Type).WithStatus(condition.Status).WithReason(condition.Reason).
						WithLastTransitionTime(condition.LastTransitionTime))), "cluster-autoscaler")
				require.NoError(t, err)
			} else {
				_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
				require.NoError(t, err)
			}

			got, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
			require.NoError(t, err)
			require.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, v1.Provisioned))
			// Neither a returned pending admission nor a cached object may be mutated by a caller.
			got.Status.Conditions = nil
			listed, err := client.ProvisioningRequests(t.Context())
			require.NoError(t, err)
			require.Len(t, listed, 1)
			assert.True(t, apimeta.IsStatusConditionTrue(listed[0].Status.Conditions, v1.Provisioned))
			require.Len(t, client.pendingAdmissions, 1)
			raw, _, err := indexer.GetByKey("ns/request")
			require.NoError(t, err)
			assert.Empty(t, raw.(*v1.ProvisioningRequest).Status.Conditions)

			require.NoError(t, indexer.Update(admitted))
			got, err = client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
			require.NoError(t, err)
			assert.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, v1.Provisioned))
			assert.Empty(t, client.pendingAdmissions)
			got.Status.Conditions = nil
			raw, _, err = indexer.GetByKey("ns/request")
			require.NoError(t, err)
			assert.True(t, apimeta.IsStatusConditionTrue(raw.(*v1.ProvisioningRequest).Status.Conditions, v1.Provisioned))
		})
	}
}

func TestPendingAdmissionReconciliation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*v1.ProvisioningRequest)
	}{
		{name: "observed admission", mutate: func(request *v1.ProvisioningRequest) {
			apimeta.SetStatusCondition(&request.Status.Conditions, metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()})
		}},
		{name: "failed", mutate: func(request *v1.ProvisioningRequest) {
			apimeta.SetStatusCondition(&request.Status.Conditions, metav1.Condition{Type: v1.Failed, Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()})
		}},
		{name: "booking expired", mutate: func(request *v1.ProvisioningRequest) {
			apimeta.SetStatusCondition(&request.Status.Conditions, metav1.Condition{Type: v1.BookingExpired, Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()})
		}},
		{name: "recreated", mutate: func(request *v1.ProvisioningRequest) { request.UID = "replacement" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _, indexer, request := newAdmissionTestClient(t)
			admitted := request.DeepCopy()
			admitted.ResourceVersion = "acknowledged"
			apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Minute))})
			_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
			require.NoError(t, err)
			cached := request.DeepCopy()
			cached.ResourceVersion = "newer-opaque-version"
			tc.mutate(cached)
			require.NoError(t, indexer.Update(cached))
			listed, err := client.ProvisioningRequests(t.Context())
			require.NoError(t, err)
			require.Len(t, listed, 1)
			assert.Equal(t, cached, listed[0].ProvisioningRequest)
			assert.Empty(t, client.pendingAdmissions)
		})
	}
	for _, read := range []string{"get", "list"} {
		t.Run("deleted/"+read, func(t *testing.T) {
			client, _, indexer, request := newAdmissionTestClient(t)
			admitted := request.DeepCopy()
			apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()})
			_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
			require.NoError(t, err)
			require.NoError(t, indexer.Delete(request))
			if read == "get" {
				_, err = client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
				require.Error(t, err)
			} else {
				listed, err := client.ProvisioningRequests(t.Context())
				require.NoError(t, err)
				assert.Empty(t, listed)
			}
			assert.Empty(t, client.pendingAdmissions)
		})
	}
}

func TestFailedAdmissionIsNotRemembered(t *testing.T) {
	client, api, _, request := newAdmissionTestClient(t)
	api.PrependReactor("*", "provisioningrequests", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("status write failed")
	})
	admitted := request.DeepCopy()
	apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()})
	_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
	require.Error(t, err)
	_, err = client.ApplyProvisioningRequest(v1ac.ProvisioningRequest(request.Name, request.Namespace).
		WithStatus(v1ac.ProvisioningRequestStatus().WithConditions(metav1ac.Condition().
			WithType(v1.Provisioned).WithStatus(metav1.ConditionTrue))), "cluster-autoscaler")
	require.Error(t, err)
	assert.Empty(t, client.pendingAdmissions)
	listed, err := client.ProvisioningRequests(t.Context())
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.False(t, apimeta.IsStatusConditionTrue(listed[0].Status.Conditions, v1.Provisioned))
}

func TestPendingAdmissionTerminalWriteAndFailure(t *testing.T) {
	client, api, indexer, request := newAdmissionTestClient(t)
	admitted := request.DeepCopy()
	apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{
		Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now(),
	})
	_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
	require.NoError(t, err)
	require.NoError(t, indexer.Update(admitted.DeepCopy()))
	// Booking is consumed before the informer observes that terminal write.
	consumed := admitted.DeepCopy()
	apimeta.SetStatusCondition(&consumed.Status.Conditions, metav1.Condition{
		Type: v1.BookingExpired, Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now(),
	})
	_, err = client.UpdateProvisioningRequest(t.Context(), consumed)
	require.NoError(t, err)
	got, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
	require.NoError(t, err)
	assert.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, v1.BookingExpired))
	assert.Len(t, client.pendingAdmissions, 1, "observing only the admission must not retire a pending terminal write")

	api.PrependReactor("update", "provisioningrequests", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("status write failed")
	})
	_, err = client.UpdateProvisioningRequest(t.Context(), request.DeepCopy())
	require.Error(t, err)
	got, err = client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
	require.NoError(t, err)
	assert.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, v1.BookingExpired), "a failed write must not discard an acknowledged status")
	require.NoError(t, indexer.Update(consumed))
	_, err = client.ProvisioningRequests(t.Context())
	require.NoError(t, err)
	assert.Empty(t, client.pendingAdmissions)
}

func TestPendingAdmissionNamespaceIsolationAndConcurrency(t *testing.T) {
	client, api, indexer, template := newAdmissionTestClient(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		request := template.DeepCopy()
		request.Namespace = fmt.Sprintf("ns-%d", i)
		request.UID = types.UID(request.Namespace)
		require.NoError(t, indexer.Add(request.DeepCopy()))
		require.NoError(t, api.Tracker().Add(request.DeepCopy()))
		wg.Add(1)
		go func() {
			defer wg.Done()
			apimeta.SetStatusCondition(&request.Status.Conditions, metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()})
			_, err := client.UpdateProvisioningRequest(t.Context(), request)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			listed, err := client.ProvisioningRequests(t.Context())
			assert.NoError(t, err)
			admitted := 0
			for _, request := range listed {
				if apimeta.IsStatusConditionTrue(request.Status.Conditions, v1.Provisioned) {
					admitted++
				}
			}
			assert.Equal(t, 20, admitted)
		}()
	}
	wg.Wait()
}

// TestPendingAdmissionSupersededByLaterState checks that a pending admission is retired by any
// cached state at least as recent as the write, whatever its status, and by no older state.
func TestPendingAdmissionSupersededByLaterState(t *testing.T) {
	written := metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
	provisioned := func(status metav1.ConditionStatus, reason string, transition time.Time) *metav1.Condition {
		return &metav1.Condition{Type: v1.Provisioned, Status: status, Reason: reason, LastTransitionTime: metav1.NewTime(transition)}
	}
	for _, tc := range []struct {
		name        string
		cached      *metav1.Condition
		wantRetired bool
	}{
		{name: "not shown yet"},
		{name: "earlier admission", cached: provisioned(metav1.ConditionTrue, "Earlier", written.Add(-time.Minute))},
		{name: "earlier failure", cached: provisioned(metav1.ConditionFalse, "Earlier", written.Add(-time.Minute))},
		// Transition times have a resolution of a second, so this can't be told apart from an
		// earlier failure.
		{name: "failure in the same second", cached: provisioned(metav1.ConditionFalse, "SameSecond", written.Time)},
		{name: "the admission", cached: provisioned(metav1.ConditionTrue, "Written", written.Time), wantRetired: true},
		// The cache skipped the write, for example when the informer relisted, and shows a later reset.
		{name: "later failure", cached: provisioned(metav1.ConditionFalse, "Later", written.Add(time.Second)), wantRetired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _, indexer, request := newAdmissionTestClient(t)
			admitted := request.DeepCopy()
			apimeta.SetStatusCondition(&admitted.Status.Conditions, *provisioned(metav1.ConditionTrue, "Written", written.Time))
			_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
			require.NoError(t, err)
			cached := request.DeepCopy()
			cached.ResourceVersion = "other-opaque-version"
			if tc.cached != nil {
				apimeta.SetStatusCondition(&cached.Status.Conditions, *tc.cached)
			}
			require.NoError(t, indexer.Update(cached))

			got, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
			require.NoError(t, err)
			condition := apimeta.FindStatusCondition(got.Status.Conditions, v1.Provisioned)
			require.NotNil(t, condition)
			if tc.wantRetired {
				assert.Equal(t, tc.cached.Status, condition.Status)
				assert.Equal(t, tc.cached.Reason, condition.Reason)
				assert.Empty(t, client.pendingAdmissions)
			} else {
				assert.Equal(t, metav1.ConditionTrue, condition.Status)
				assert.Equal(t, "Written", condition.Reason, "the cache shows an older state, so the admission is shown")
				assert.Len(t, client.pendingAdmissions, 1)
			}
		})
	}
}

func TestPendingAdmissionReconcilesAfterTTL(t *testing.T) {
	client, api, indexer, request := newAdmissionTestClient(t)
	now := time.Now()
	clock := clocktesting.NewFakePassiveClock(now)
	client.clock = clock
	admitted := request.DeepCopy()
	admitted.ResourceVersion = "acknowledged"
	apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(now)})
	_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
	require.NoError(t, err)
	api.ClearActions()

	clock.SetTime(now.Add(pendingAdmissionTTL - time.Nanosecond))
	got, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
	require.NoError(t, err)
	assert.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, v1.Provisioned), "the admission is shown until the TTL passes")
	assert.Len(t, client.pendingAdmissions, 1)
	assert.Empty(t, api.Actions(), "normal pending-admission reads need no API call")

	clock.SetTime(now.Add(pendingAdmissionTTL))
	listed, err := client.ProvisioningRequests(t.Context())
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.True(t, apimeta.IsStatusConditionTrue(listed[0].Status.Conditions, v1.Provisioned), "time must not erase an acknowledged admission")
	assert.Len(t, api.Actions(), 1)
	assert.Len(t, client.pendingAdmissions, 1, "subsequent reads must not fall back to the stale cache")
	got, err = client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
	require.NoError(t, err)
	assert.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, v1.Provisioned))
	assert.Len(t, api.Actions(), 1, "the API is read at most once per check interval")
	clock.SetTime(clock.Now().Add(pendingAdmissionCheckInterval))
	got, err = client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
	require.NoError(t, err)
	assert.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, v1.Provisioned))
	assert.Len(t, api.Actions(), 2)

	// The reconciled admission still follows the ordinary booking-expiry path.
	clock.SetTime(clock.Now().Add(time.Second))
	apimeta.SetStatusCondition(&got.Status.Conditions, metav1.Condition{
		Type: v1.BookingExpired, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(clock.Now()),
	})
	got.ResourceVersion = "booking-expired"
	_, err = client.UpdateProvisioningRequest(t.Context(), got.ProvisioningRequest)
	require.NoError(t, err)
	listed, err = client.ProvisioningRequests(t.Context())
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.True(t, apimeta.IsStatusConditionTrue(listed[0].Status.Conditions, v1.Provisioned))
	assert.True(t, apimeta.IsStatusConditionTrue(listed[0].Status.Conditions, v1.BookingExpired))

	require.NoError(t, indexer.Update(got.ProvisioningRequest))
	_, err = client.ProvisioningRequests(t.Context())
	require.NoError(t, err)
	assert.Empty(t, client.pendingAdmissions)
}

func TestPendingAdmissionReconciliationUsesLatestAPIState(t *testing.T) {
	for _, state := range []string{"unchanged", "false", "removed", "terminal", "replacement"} {
		t.Run(state, func(t *testing.T) {
			client, api, indexer, request := newAdmissionTestClient(t)
			clock := clocktesting.NewFakePassiveClock(time.Now().Truncate(time.Second))
			client.clock = clock
			admitted := request.DeepCopy()
			admitted.ResourceVersion = "admitted"
			apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{
				Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(clock.Now()),
			})
			_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
			require.NoError(t, err)
			current := admitted.DeepCopy()
			current.ResourceVersion = "authoritative"
			switch state {
			case "false":
				// Even a reset within the same timestamp second is authoritative when read live.
				apimeta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
					Type: v1.Provisioned, Status: metav1.ConditionFalse, LastTransitionTime: metav1.NewTime(clock.Now()),
				})
			case "removed":
				current.Status.Conditions = nil
			case "terminal":
				apimeta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
					Type: v1.BookingExpired, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(clock.Now()),
				})
			case "replacement":
				current.UID = "replacement"
				current.Status.Conditions = nil
			}
			require.NoError(t, api.Tracker().Update(v1.SchemeGroupVersion.WithResource("provisioningrequests"), current, current.Namespace))
			clock.SetTime(clock.Now().Add(pendingAdmissionTTL))
			got, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
			require.NoError(t, err)
			assert.Equal(t, current, got.ProvisioningRequest)
			// Observing an intermediate admission must not hide a newer reset or replacement.
			require.NoError(t, indexer.Update(admitted))
			for i := 0; i < 2; i++ {
				listed, err := client.ProvisioningRequests(t.Context())
				require.NoError(t, err)
				require.Len(t, listed, 1)
				assert.Equal(t, current, listed[0].ProvisioningRequest)
				assert.Len(t, client.pendingAdmissions, 1)
			}
			if !apimeta.IsStatusConditionTrue(current.Status.Conditions, v1.Provisioned) {
				// A subsequent Accepted write must not allow the old cached admission to reappear.
				_, err = client.UpdateProvisioningRequest(t.Context(), current)
				require.NoError(t, err)
				got, err = client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
				require.NoError(t, err)
				assert.Equal(t, current, got.ProvisioningRequest)
			}
			require.NoError(t, indexer.Update(current))
			_, err = client.ProvisioningRequests(t.Context())
			require.NoError(t, err)
			assert.Empty(t, client.pendingAdmissions)
			api.ClearActions()
			_, err = client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
			require.NoError(t, err)
			assert.Empty(t, api.Actions(), "return to informer reads once its version matches")
		})
	}
}

func TestPendingAdmissionReconciliationDeletion(t *testing.T) {
	client, api, indexer, request := newAdmissionTestClient(t)
	clock := clocktesting.NewFakePassiveClock(time.Now())
	client.clock = clock
	admitted := request.DeepCopy()
	// The API server gives every change a new resource version; the fake API doesn't.
	admitted.ResourceVersion = "admitted"
	apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{
		Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(clock.Now()),
	})
	_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
	require.NoError(t, err)
	require.NoError(t, api.Tracker().Delete(v1.SchemeGroupVersion.WithResource("provisioningrequests"), request.Namespace, request.Name))
	clock.SetTime(clock.Now().Add(pendingAdmissionTTL))
	for i := 0; i < 2; i++ {
		_, err = client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
		require.True(t, apierrors.IsNotFound(err), "authoritative deletion should remain NotFound: %v", err)
		listed, err := client.ProvisioningRequests(t.Context())
		require.NoError(t, err)
		assert.Empty(t, listed, "do not return the stale cached object after confirmed deletion")
		assert.Len(t, client.pendingAdmissions, 1)
	}
	require.NoError(t, indexer.Delete(request))
	_, err = client.ProvisioningRequests(t.Context())
	require.NoError(t, err)
	assert.Empty(t, client.pendingAdmissions)
}

// TestPendingAdmissionReconciliationFailure checks that failing to read the API doesn't fail
// reads, which show the newest state known instead, and that the API is read again after the
// check interval.
func TestPendingAdmissionReconciliationFailure(t *testing.T) {
	client, api, _, request := newAdmissionTestClient(t)
	clock := clocktesting.NewFakePassiveClock(time.Now())
	client.clock = clock
	admitted := request.DeepCopy()
	admitted.ResourceVersion = "admitted"
	apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{
		Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(clock.Now()),
	})
	_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
	require.NoError(t, err)
	key := types.NamespacedName{Namespace: request.Namespace, Name: request.Name}
	clock.SetTime(clock.Now().Add(pendingAdmissionTTL))

	// A read whose context is done doesn't read the API, and leaves the request due for a check.
	api.ClearActions()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, cancelExpired := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancelExpired()
	for _, ctx := range []context.Context{cancelled, expired} {
		got, err := client.ProvisioningRequest(ctx, request.Namespace, request.Name)
		require.NoError(t, err)
		assert.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, v1.Provisioned))
		listed, err := client.ProvisioningRequests(ctx)
		require.NoError(t, err)
		require.Len(t, listed, 1)
		assert.True(t, apimeta.IsStatusConditionTrue(listed[0].Status.Conditions, v1.Provisioned))
	}
	assert.Empty(t, api.Actions())
	assert.True(t, client.pendingAdmissions[key].checked.IsZero())

	readErr := apierrors.NewServiceUnavailable("API unavailable")
	fail := true
	api.PrependReactor("get", "provisioningrequests", func(clienttesting.Action) (bool, runtime.Object, error) {
		if fail {
			return true, nil, readErr
		}
		return false, nil, nil
	})
	for i := 0; i < 2; i++ {
		got, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
		require.NoError(t, err, "failing to read the API doesn't fail the read")
		assert.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, v1.Provisioned), "the admission is shown while the API can't be read")
		listed, err := client.ProvisioningRequests(t.Context())
		require.NoError(t, err)
		require.Len(t, listed, 1)
		assert.True(t, apimeta.IsStatusConditionTrue(listed[0].Status.Conditions, v1.Provisioned))
	}
	assert.Len(t, api.Actions(), 1, "a failed API read is only retried after the check interval")
	assert.Len(t, client.pendingAdmissions, 1)

	// A later write that isn't an admission is shown, rather than the stale cache, until the API
	// is read again.
	rejected := admitted.DeepCopy()
	apimeta.SetStatusCondition(&rejected.Status.Conditions, metav1.Condition{
		Type: v1.Provisioned, Status: metav1.ConditionFalse, Reason: "Rejected", LastTransitionTime: metav1.NewTime(clock.Now()),
	})
	_, err = client.UpdateProvisioningRequest(t.Context(), rejected)
	require.NoError(t, err)
	got, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
	require.NoError(t, err)
	assert.Equal(t, "Rejected", apimeta.FindStatusCondition(got.Status.Conditions, v1.Provisioned).Reason)

	fail = false
	reset := rejected.DeepCopy()
	reset.ResourceVersion = "reset"
	apimeta.SetStatusCondition(&reset.Status.Conditions, metav1.Condition{
		Type: v1.Provisioned, Status: metav1.ConditionFalse, Reason: "Reset", LastTransitionTime: metav1.NewTime(clock.Now()),
	})
	require.NoError(t, api.Tracker().Update(v1.SchemeGroupVersion.WithResource("provisioningrequests"), reset, reset.Namespace))
	clock.SetTime(clock.Now().Add(pendingAdmissionCheckInterval))
	got, err = client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
	require.NoError(t, err)
	assert.Equal(t, reset, got.ProvisioningRequest, "once the API can be read, it decides what is shown")
	assert.Len(t, client.pendingAdmissions, 1, "until the cache catches up")
}

// TestPendingAdmissionReconciliationKeepsConcurrentWrites checks that an API read that started
// before a write was acknowledged doesn't replace that write.
func TestPendingAdmissionReconciliationKeepsConcurrentWrites(t *testing.T) {
	client, api, indexer, request := newAdmissionTestClient(t)
	clock := clocktesting.NewFakePassiveClock(time.Now())
	client.clock = clock
	admitted := request.DeepCopy()
	admitted.ResourceVersion = "admitted"
	apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{
		Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(clock.Now()),
	})
	_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
	require.NoError(t, err)
	clock.SetTime(clock.Now().Add(pendingAdmissionTTL))
	entered := make(chan struct{})
	release := make(chan struct{})
	api.PrependReactor("get", "provisioningrequests", func(clienttesting.Action) (bool, runtime.Object, error) {
		close(entered)
		<-release
		return true, admitted.DeepCopy(), nil
	})
	readDone := make(chan error, 1)
	go func() {
		_, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
		readDone <- err
	}()
	<-entered
	// The fake API serializes calls, so the write is recorded the way UpdateProvisioningRequest
	// records it once the API server acknowledges it, while the read still waits for the API.
	consumed := admitted.DeepCopy()
	consumed.ResourceVersion = "consumed"
	apimeta.SetStatusCondition(&consumed.Status.Conditions, metav1.Condition{
		Type: v1.BookingExpired, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(clock.Now()),
	})
	client.rememberAdmission(consumed)
	close(release)
	require.NoError(t, <-readDone)
	got, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
	require.NoError(t, err)
	assert.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, v1.BookingExpired), "an older reconciliation must not overwrite the concurrent acknowledged write")
	require.NoError(t, indexer.Update(consumed))
	_, err = client.ProvisioningRequests(t.Context())
	require.NoError(t, err)
	assert.Empty(t, client.pendingAdmissions)
}

// TestPendingAdmissionCheckDoesNotBlockReads checks that while the API is read for one request,
// other reads neither wait for it nor read the API again, and show the admission meanwhile.
func TestPendingAdmissionCheckDoesNotBlockReads(t *testing.T) {
	client, api, indexer, request := newAdmissionTestClient(t)
	other := request.DeepCopy()
	other.Name, other.UID = "other", "other-uid"
	require.NoError(t, indexer.Add(other))
	clock := clocktesting.NewFakePassiveClock(time.Now())
	client.clock = clock
	admitted := request.DeepCopy()
	// The API server gives every change a new resource version; the fake API doesn't.
	admitted.ResourceVersion = "admitted"
	apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{
		Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(clock.Now()),
	})
	_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
	require.NoError(t, err)
	clock.SetTime(clock.Now().Add(pendingAdmissionTTL))
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	api.PrependReactor("get", "provisioningrequests", func(clienttesting.Action) (bool, runtime.Object, error) {
		close(entered)
		<-release
		return false, nil, nil
	})
	checkDone := make(chan error, 1)
	go func() {
		_, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
		checkDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the admission wasn't checked with the API")
	}

	var otherErr, listErr error
	var listed []*provreqwrapper.ProvisioningRequest
	readsDone := make(chan struct{})
	go func() {
		defer close(readsDone)
		_, otherErr = client.ProvisioningRequest(t.Context(), other.Namespace, other.Name)
		listed, listErr = client.ProvisioningRequests(t.Context())
	}()
	select {
	case <-readsDone:
	case <-time.After(10 * time.Second):
		t.Fatal("reads waited for an API read made for another read")
	}
	require.NoError(t, otherErr)
	require.NoError(t, listErr)
	require.Len(t, listed, 2)
	for _, pr := range listed {
		if pr.Name == request.Name {
			assert.True(t, apimeta.IsStatusConditionTrue(pr.Status.Conditions, v1.Provisioned), "the admission is shown while it's checked")
		}
	}
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-checkDone)
}

// TestPendingAdmissionConcurrentReaderRetirement checks that when another read retires an
// admission while the API is being read for it, the first read shows the newer cache as well,
// not its own older snapshot, which may not show the admission or a deletion.
func TestPendingAdmissionConcurrentReaderRetirement(t *testing.T) {
	for _, method := range []string{"get", "list"} {
		for _, state := range []string{"admitted", "deleted"} {
			t.Run(method+"/"+state, func(t *testing.T) {
				client, api, indexer, request := newAdmissionTestClient(t)
				clock := clocktesting.NewFakePassiveClock(time.Now())
				client.clock = clock
				admitted := request.DeepCopy()
				admitted.ResourceVersion = "admitted"
				apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{
					Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(clock.Now()),
				})
				_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
				require.NoError(t, err)
				clock.SetTime(clock.Now().Add(pendingAdmissionTTL))
				entered := make(chan struct{})
				release := make(chan struct{})
				var releaseOnce sync.Once
				defer releaseOnce.Do(func() { close(release) })
				api.PrependReactor("get", "provisioningrequests", func(clienttesting.Action) (bool, runtime.Object, error) {
					close(entered)
					<-release
					return true, admitted.DeepCopy(), nil
				})
				read := func() ([]*provreqwrapper.ProvisioningRequest, error) {
					if method == "list" {
						return client.ProvisioningRequests(t.Context())
					}
					pr, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
					if err != nil {
						return nil, err
					}
					return []*provreqwrapper.ProvisioningRequest{pr}, nil
				}
				type result struct {
					requests []*provreqwrapper.ProvisioningRequest
					err      error
				}
				done := make(chan result, 1)
				go func() {
					prs, err := read()
					done <- result{requests: prs, err: err}
				}()
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					t.Fatal("the first reader didn't reach API reconciliation")
				}
				if state == "deleted" {
					require.NoError(t, indexer.Delete(request))
				} else {
					require.NoError(t, indexer.Update(admitted.DeepCopy()))
				}
				// A second reader observes the changed informer and retires the overlay.
				prs, err := read()
				if state == "deleted" && method == "get" {
					require.True(t, apierrors.IsNotFound(err), "expected informer deletion, got %v", err)
				} else {
					require.NoError(t, err)
				}
				if state == "deleted" {
					require.Empty(t, prs)
				} else {
					require.Len(t, prs, 1)
					require.Equal(t, admitted, prs[0].ProvisioningRequest)
				}
				require.Empty(t, client.pendingAdmissions)
				releaseOnce.Do(func() { close(release) })
				var first result
				select {
				case first = <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("the first reader didn't finish reconciliation")
				}
				if state == "deleted" && method == "get" {
					require.True(t, apierrors.IsNotFound(first.err), "the first reader must observe deletion too: %v", first.err)
				} else {
					require.NoError(t, first.err)
				}
				if state == "deleted" {
					assert.Empty(t, first.requests, "a stale API result must not resurrect the deleted request")
				} else {
					require.Len(t, first.requests, 1)
					assert.Equal(t, admitted, first.requests[0].ProvisioningRequest, "retirement must not expose the pre-admission informer snapshot")
				}
			})
		}
	}
}

// TestPendingAdmissionRefreshedReadFailure checks that if reading the cache again after the API
// calls fails, the read fails rather than use its older snapshot, the lock is released, and the
// admission is still shown by later reads.
func TestPendingAdmissionRefreshedReadFailure(t *testing.T) {
	client, _, _, request := newAdmissionTestClient(t)
	clock := clocktesting.NewFakePassiveClock(time.Now())
	client.clock = clock
	admitted := request.DeepCopy()
	admitted.ResourceVersion = "admitted"
	apimeta.SetStatusCondition(&admitted.Status.Conditions, metav1.Condition{
		Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(clock.Now()),
	})
	_, err := client.UpdateProvisioningRequest(t.Context(), admitted)
	require.NoError(t, err)
	clock.SetTime(clock.Now().Add(pendingAdmissionTTL))
	readErr := fmt.Errorf("informer read failed after reconciliation")
	reads := 0
	got, err := client.withPendingAdmissions(t.Context(), func() ([]*v1.ProvisioningRequest, error) {
		reads++
		if reads == 1 {
			return []*v1.ProvisioningRequest{request}, nil
		}
		return nil, readErr
	})
	require.ErrorIs(t, err, readErr)
	assert.Nil(t, got, "do not fall back to the original snapshot if refreshing it fails")
	assert.Equal(t, 2, reads)
	require.True(t, client.admissionsMu.TryLock(), "a failed refresh must release the mutex")
	client.admissionsMu.Unlock()
	gotRequest, err := client.ProvisioningRequest(t.Context(), request.Namespace, request.Name)
	require.NoError(t, err)
	assert.Equal(t, admitted, gotRequest.ProvisioningRequest, "a failed informer refresh must not discard the admission")
}

// TestPendingAdmissionChecksAreBounded checks that a single read only reads the API for a few
// requests, and that later reads check the others.
func TestPendingAdmissionChecksAreBounded(t *testing.T) {
	client, api, indexer, template := newAdmissionTestClient(t)
	clock := clocktesting.NewFakePassiveClock(time.Now())
	client.clock = clock
	count := maxPendingAdmissionChecks + 1
	for i := 0; i < count; i++ {
		request := template.DeepCopy()
		request.Name, request.UID = fmt.Sprintf("request-%d", i), types.UID(fmt.Sprintf("uid-%d", i))
		require.NoError(t, indexer.Add(request.DeepCopy()))
		require.NoError(t, api.Tracker().Add(request.DeepCopy()))
		// The API server gives every change a new resource version; the fake API doesn't.
		request.ResourceVersion = "admitted"
		apimeta.SetStatusCondition(&request.Status.Conditions, metav1.Condition{
			Type: v1.Provisioned, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(clock.Now()),
		})
		_, err := client.UpdateProvisioningRequest(t.Context(), request)
		require.NoError(t, err)
	}
	clock.SetTime(clock.Now().Add(pendingAdmissionTTL))
	api.ClearActions()
	for _, want := range []int{maxPendingAdmissionChecks, count, count} {
		listed, err := client.ProvisioningRequests(t.Context())
		require.NoError(t, err)
		assert.Len(t, listed, count+1)
		assert.Len(t, api.Actions(), want)
	}
}
