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

package provreq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
)

func TestBatchRetryLimitTTL(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ttl     time.Duration
		wantTTL time.Duration
	}{
		{name: "default", wantTTL: config.DefaultBestEffortAtomicProvisioningRequestBatchSizeTTL},
		{name: "configured", ttl: 5 * time.Minute, wantTTL: 5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			clock := clocktesting.NewFakePassiveClock(now)
			injector := NewProvisioningRequestPodsInjectorWithOptions(nil, PodsInjectorOptions{
				InitialBackoffTime:              time.Minute,
				MaxBackoffTime:                  10 * time.Minute,
				MaxBackoffCacheSize:             100,
				BestEffortAtomicBatchProcessing: true,
				BestEffortAtomicMaxBatchSize:    10,
				KubeClientBurst:                 1,
				BestEffortAtomicBatchSizeTTL:    tc.ttl,
			})
			injector.clock = clock
			request := testProvisioningRequestWithCondition("request", 1, v1.ProvisioningClassBestEffortAtomicScaleUp)
			injector.RememberBestEffortAtomicBatchLimit(t.Context(), request, 2)
			clock.SetTime(now.Add(tc.wantTTL - time.Nanosecond))
			assert.NotNil(t, injector.batchRetryLimit(request), "the remembered size lasts until the TTL passes")
			clock.SetTime(now.Add(tc.wantTTL))
			assert.Nil(t, injector.batchRetryLimit(request), "the remembered size expires after the TTL")
		})
	}
}

func TestBatchRetryLimitSelection(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	clock := clocktesting.NewFakePassiveClock(now)
	var requests []*provreqwrapper.ProvisioningRequest
	for i := 0; i < 6; i++ {
		request := testProvisioningRequestWithCondition(fmt.Sprintf("request-%d", i), i+1, v1.ProvisioningClassBestEffortAtomicScaleUp,
			metav1.Condition{Type: v1.Provisioned, Status: metav1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-90 * time.Second))})
		request.UID = types.UID(request.Name)
		request.CreationTimestamp = metav1.NewTime(now.Add(time.Duration(i) * time.Second))
		request.PodTemplates[0].Template.Labels = map[string]string{"job-name": request.Name}
		requests = append(requests, request)
	}
	client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, requests...)
	injector := NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             100,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    6,
		KubeClientBurst:                 1,
	})
	injector.clock = clock
	// Request i asks for i+1 pods, so three pods fit the first two requests.
	injector.RememberBestEffortAtomicBatchLimit(ctx, requests[0], 3)
	batch, err := injector.GetBestEffortAtomicBatch(ctx, 6)
	require.NoError(t, err)
	require.Len(t, batch, 2)
	for i, request := range requests {
		if i < len(batch) {
			assert.Equal(t, request.Name, batch[i].PrWrapper.Name)
			assert.Len(t, batch[i].Pods, request.PodCount(), "never split a request")
		}
		_, recorded := injector.backoffDuration.Get(key(request))
		assert.Equal(t, i < len(batch), recorded)
		assert.Equal(t, time.Minute, injector.retryTime(request), "selection must not advance retry delay")
	}
	// A newer matching request inherits the limit even when counts and workload labels differ.
	injector.RememberBestEffortAtomicBatchLimit(ctx, requests[2], 4)
	batch, err = injector.GetBestEffortAtomicBatch(ctx, 6)
	require.NoError(t, err)
	assert.Len(t, batch, 2, "do not discard progress while the limit is live")
	clock.SetTime(now.Add(injector.bestEffortAtomicBatchSizeTTL))
	batch, err = injector.GetBestEffortAtomicBatch(ctx, 6)
	require.NoError(t, err)
	assert.Len(t, batch, 6, "allow larger batches again after expiry")
}

func TestBatchRetryLimitIsolationAndBounds(t *testing.T) {
	ctx := t.Context()
	clock := clocktesting.NewFakePassiveClock(time.Now())
	injector := NewProvisioningRequestPodsInjectorWithOptions(nil, PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             100,
		CheckCapacityBatchProcessing:    true,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    10,
		KubeClientBurst:                 1,
	})
	injector.clock = clock
	first := testProvisioningRequestWithCondition("first", 1, v1.ProvisioningClassBestEffortAtomicScaleUp)
	compatible := testProvisioningRequestWithCondition("next", 5, v1.ProvisioningClassBestEffortAtomicScaleUp)
	injector.RememberBestEffortAtomicBatchLimit(ctx, first, 100)
	require.Equal(t, 100, injector.batchRetryLimit(compatible).pods, "a limit in pods isn't capped by the maximum number of requests")
	injector.RememberBestEffortAtomicBatchLimit(ctx, first, 0)
	require.Equal(t, 1, injector.batchRetryLimit(compatible).pods)
	first.PodTemplates[0].Template.Spec.NodeSelector = map[string]string{"hardware": "gpu"}
	assert.Nil(t, injector.batchRetryLimit(first), "template edits must not mutate the stored cohort")
	assert.Equal(t, 1, injector.batchRetryLimit(compatible).pods)
	compatible.Namespace = "other"
	assert.Nil(t, injector.batchRetryLimit(compatible))

	for i := 0; i < maxBatchRetryLimits+1; i++ {
		request := testProvisioningRequestWithCondition(fmt.Sprintf("cohort-%d", i), 1, v1.ProvisioningClassBestEffortAtomicScaleUp)
		request.Namespace = request.Name
		injector.RememberBestEffortAtomicBatchLimit(ctx, request, 2)
	}
	assert.Len(t, injector.batchRetryLimits, maxBatchRetryLimits)
	clock.SetTime(clock.Now().Add(injector.bestEffortAtomicBatchSizeTTL))
	assert.Nil(t, injector.batchRetryLimit(first))
	assert.Empty(t, injector.batchRetryLimits)
}

func TestBatchRetryLimitDoesNotAffectCheckCapacity(t *testing.T) {
	ctx := t.Context()
	atomic := testProvisioningRequestWithCondition("atomic", 1, v1.ProvisioningClassBestEffortAtomicScaleUp)
	checkA := testProvisioningRequestWithCondition("check-a", 1, v1.ProvisioningClassCheckCapacity)
	checkB := testProvisioningRequestWithCondition("check-b", 1, v1.ProvisioningClassCheckCapacity)
	client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, atomic, checkA, checkB)
	injector := NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             100,
		CheckCapacityBatchProcessing:    true,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    10,
		KubeClientBurst:                 1,
	})
	injector.RememberBestEffortAtomicBatchLimit(ctx, atomic, 1)
	batch, err := injector.GetCheckCapacityBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, batch, 2)
	injector.bestEffortAtomicBatchProcessing = false
	injector.batchRetryLimits = nil
	injector.RememberBestEffortAtomicBatchLimit(ctx, atomic, 1)
	assert.Empty(t, injector.batchRetryLimits, "disabled batching must not record limits")
}

func TestBatchRetryLimitGrowsBack(t *testing.T) {
	ctx := t.Context()
	injector := NewProvisioningRequestPodsInjectorWithOptions(nil, PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             100,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    10,
		KubeClientBurst:                 1,
	})
	injector.clock = clocktesting.NewFakePassiveClock(time.Now())
	first := testProvisioningRequestWithCondition("first", 1, v1.ProvisioningClassBestEffortAtomicScaleUp)
	compatible := testProvisioningRequestWithCondition("compatible", 3, v1.ProvisioningClassBestEffortAtomicScaleUp)

	injector.RecordBestEffortAtomicBatchSuccess(ctx, first, 10, 10)
	assert.Nil(t, injector.batchRetryLimit(first), "a success without a limit doesn't create one")

	injector.RememberBestEffortAtomicBatchLimit(ctx, first, 1)
	for _, step := range []struct{ succeeded, want int }{
		{succeeded: 1, want: 2},
		// A batch smaller than the limit says nothing about larger ones.
		{succeeded: 1, want: 2},
		{succeeded: 2, want: 4},
		{succeeded: 4, want: 8},
	} {
		injector.RecordBestEffortAtomicBatchSuccess(ctx, compatible, step.succeeded, step.succeeded)
		require.NotNil(t, injector.batchRetryLimit(first))
		assert.Equal(t, step.want, injector.batchRetryLimit(first).pods, "after a batch of %d single-pod requests succeeded", step.succeeded)
	}
	injector.RecordBestEffortAtomicBatchSuccess(ctx, compatible, 8, 8)
	assert.Nil(t, injector.batchRetryLimit(first), "a limit that would cover the configured maximum of such requests is dropped")

	// With ten pods per request, a batch of the configured maximum of ten requests has 100 pods.
	injector.RememberBestEffortAtomicBatchLimit(ctx, first, 10)
	for _, want := range []int{20, 40, 80} {
		injector.RecordBestEffortAtomicBatchSuccess(ctx, compatible, want/20, want/2)
		assert.Equal(t, want, injector.batchRetryLimit(first).pods)
	}
	injector.RecordBestEffortAtomicBatchSuccess(ctx, compatible, 8, 80)
	assert.Nil(t, injector.batchRetryLimit(first), "a limit that would cover the configured maximum of such requests is dropped")

	injector.RememberBestEffortAtomicBatchLimit(ctx, first, 2)
	injector.bestEffortAtomicBatchProcessing = false
	injector.RecordBestEffortAtomicBatchSuccess(ctx, first, 2, 2)
	assert.Equal(t, 2, injector.batchRetryLimit(first).pods, "disabled batching doesn't grow limits")
}

// TestBatchRetryLimitCountsPods checks that a remembered limit caps the pods of a batch, that the
// batch stays oldest first, and that the oldest request is picked even if it alone exceeds the limit.
func TestBatchRetryLimitCountsPods(t *testing.T) {
	for _, tc := range []struct {
		name      string
		podCounts []int
		limit     int
		want      []string
	}{
		{name: "requests up to the limit", podCounts: []int{1, 3, 1, 1}, limit: 5, want: []string{"request-0", "request-1", "request-2"}},
		{name: "a smaller request doesn't jump ahead", podCounts: []int{1, 3, 1, 1}, limit: 2, want: []string{"request-0"}},
		{name: "the oldest request exceeds the limit on its own", podCounts: []int{4, 1, 1}, limit: 1, want: []string{"request-0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			now := time.Now()
			var requests []*provreqwrapper.ProvisioningRequest
			for i, pods := range tc.podCounts {
				request := testProvisioningRequestWithCondition(fmt.Sprintf("request-%d", i), pods, v1.ProvisioningClassBestEffortAtomicScaleUp)
				request.UID = types.UID(request.Name)
				request.CreationTimestamp = metav1.NewTime(now.Add(time.Duration(i) * time.Second))
				requests = append(requests, request)
			}
			client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, requests...)
			injector := NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
				InitialBackoffTime:              time.Minute,
				MaxBackoffTime:                  10 * time.Minute,
				MaxBackoffCacheSize:             100,
				BestEffortAtomicBatchProcessing: true,
				BestEffortAtomicMaxBatchSize:    10,
				KubeClientBurst:                 1,
			})
			injector.RememberBestEffortAtomicBatchLimit(ctx, requests[0], tc.limit)
			batch, err := injector.GetBestEffortAtomicBatch(ctx, 10)
			require.NoError(t, err)
			var names []string
			for _, request := range batch {
				names = append(names, request.PrWrapper.Name)
			}
			assert.Equal(t, tc.want, names)
		})
	}
}

// TestBatchSettledBeforeSelection checks that Process settles earlier batches before it selects a
// batch, so that a remembered size they let grow already applies to the batch it selects.
func TestBatchSettledBeforeSelection(t *testing.T) {
	for _, tc := range []struct {
		name     string
		settle   bool
		wantPods int
	}{
		{name: "without a settler", wantPods: 1},
		{name: "with a settler", settle: true, wantPods: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			now := time.Now()
			var requests []*provreqwrapper.ProvisioningRequest
			for i := 0; i < 4; i++ {
				request := testProvisioningRequestWithCondition(fmt.Sprintf("request-%d", i), 1, v1.ProvisioningClassBestEffortAtomicScaleUp)
				request.UID = types.UID(request.Name)
				request.CreationTimestamp = metav1.NewTime(now.Add(time.Duration(i) * time.Second))
				requests = append(requests, request)
			}
			client := provreqclient.NewFakeProvisioningRequestClient(ctx, t, requests...)
			injector := NewProvisioningRequestPodsInjectorWithOptions(client, PodsInjectorOptions{
				InitialBackoffTime:              time.Minute,
				MaxBackoffTime:                  10 * time.Minute,
				MaxBackoffCacheSize:             100,
				BestEffortAtomicBatchProcessing: true,
				BestEffortAtomicMaxBatchSize:    4,
				KubeClientBurst:                 1,
			})
			injector.RememberBestEffortAtomicBatchLimit(ctx, requests[0], 1)
			settled := 0
			if tc.settle {
				injector.SetBestEffortAtomicBatchSettler(func(ctx context.Context) {
					settled++
					// The nodes of an earlier batch of the remembered size have arrived.
					injector.RecordBestEffortAtomicBatchSuccess(ctx, requests[0], 1, 1)
				})
			}
			pods, err := injector.Process(ctx, nil, nil)
			require.NoError(t, err)
			assert.Len(t, pods, tc.wantPods)
			if tc.settle {
				assert.Equal(t, 1, settled)
			}
		})
	}
}
