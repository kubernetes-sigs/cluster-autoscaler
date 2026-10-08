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

package controller

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	v1beta1ac "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/applyconfiguration/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/testutil"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func startTestReadyReplicasController(t *testing.T, controller *ReadyReplicasController) func() {
	t.Helper()
	mgr, err := manager.New(&rest.Config{}, manager.Options{
		Metrics: metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{
			// SkipNameValidation allows registering the same controller name across multiple test cases.
			SkipNameValidation: new(true),
			// UsePriorityQueue defaults to true in controller-runtime v0.23+, but its background
			// handleReadyItems goroutine can remain blocked on a channel send if the manager stops
			// while items are still queued, causing synctest.Test to fail with a leaked-goroutine panic.
			UsePriorityQueue: new(false),
		},
	})
	assert.NoError(t, err)
	assert.NoError(t, controller.SetupWithManager(mgr))

	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Go(func() {
		assert.NoError(t, mgr.Start(ctx))
	})
	// Wait until the manager and controller workers have started and are parked waiting for events.
	synctest.Wait()
	return func() {
		// Stop the manager and wait for all controller-runtime goroutines in the synctest bubble to exit.
		cancel()
		wg.Wait()
		synctest.Wait()
	}
}

func TestReadyReplicasControllerSkipsIntermediateUpdates(t *testing.T) {
	const applyLatency = 100 * time.Millisecond

	testCases := []struct {
		name           string
		initial        *int32
		inFlightUpdate *int32
		queuedUpdates  []int32
		wantApplied    []int32
	}{
		{
			name:          "skips pending intermediate updates while waiting in queue",
			queuedUpdates: []int32{1, 2, 3, 4},
			wantApplied:   []int32{4},
		},
		{
			name:          "skips API call completely if queued update reverts to current status",
			initial:       new(int32(5)),
			queuedUpdates: []int32{1, 2, 5},
			wantApplied:   nil,
		},
		{
			name:           "skips intermediate updates arriving while an update is in flight",
			inFlightUpdate: new(int32(1)),
			queuedUpdates:  []int32{2, 3, 4},
			wantApplied:    []int32{1, 4},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var applied []int32
			kubeClient := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
				SubResourceApply: func(_ context.Context, _ client.Client, subResourceName string, obj runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
					time.Sleep(applyLatency)
					assert.Equal(t, "status", subResourceName)
					applyCfg, ok := obj.(*v1beta1ac.CapacityBufferApplyConfiguration)
					assert.True(t, ok, "unexpected apply configuration type %T", obj)
					applied = append(applied, *applyCfg.Status.ReadyReplicas)
					return nil
				},
			}).Build()

			var bufferOpts []testutil.BufferOption
			if tc.initial != nil {
				bufferOpts = append(bufferOpts, testutil.WithStatusReadyReplicas(*tc.initial))
			}
			buffer := testutil.NewBuffer(bufferOpts...)

			// synctest.Test runs the closure in an isolated "bubble" with a fake clock that only
			// advances when all goroutines in the bubble are durably blocked (e.g. on time.Sleep
			// or channel operations). synctest.Wait() blocks the caller until all other goroutines
			// in the bubble are durably blocked, allowing deterministic coordination without real sleeps.
			synctest.Test(t, func(t *testing.T) {
				controller := NewReadyReplicasController(kubeClient)
				var stop func()

				if tc.inFlightUpdate != nil {
					// Start the controller first, enqueue the initial update, and call synctest.Wait()
					// so the worker picks up the event and blocks inside SubResourceApply's time.Sleep(applyLatency)
					// while virtual time is still at T=0.
					stop = startTestReadyReplicasController(t, controller)
					controller.Update(buffer, *tc.inFlightUpdate)
					synctest.Wait()
				}

				// Enqueue the sequence of updates at T=0. If an update is already in flight, these
				// arrive while the worker is mid-Apply; otherwise they accumulate before the worker starts.
				for _, desired := range tc.queuedUpdates {
					controller.Update(buffer, desired)
				}

				if stop == nil {
					stop = startTestReadyReplicasController(t, controller)
				}

				// Advance the fake clock by 2*applyLatency so both any in-flight Apply and the
				// subsequent coalesced Apply can complete, then wait for workers to settle.
				time.Sleep(2 * applyLatency)
				synctest.Wait()
				stop()

				assert.Equal(t, tc.wantApplied, applied)
				assert.Empty(t, controller.targets)
			})
		})
	}
}

func TestReadyReplicasControllerConcurrencyAndTimeout(t *testing.T) {
	// Applies are executed asynchronously in rounds of maxConcurrentStatusUpdates
	// concurrent calls, with each individual reconciliation bounded by statusUpdateTimeout.
	testCases := []struct {
		name         string
		buffersCount int
		applyLatency time.Duration
		waitDuration time.Duration
		wantCalls    int32
		wantApplied  int32
	}{
		{
			name:         "all buffers updated in parallel within per-request timeout",
			buffersCount: 3 * maxConcurrentStatusUpdates,
			// 3 rounds complete after 3/4 of statusUpdateTimeout.
			applyLatency: statusUpdateTimeout / 4,
			waitDuration: 3 * (statusUpdateTimeout / 4),
			wantCalls:    3 * maxConcurrentStatusUpdates,
			wantApplied:  3 * maxConcurrentStatusUpdates,
		},
		{
			name:         "shutdown cancels in-flight calls and skips remaining queued buffers",
			buffersCount: 4 * maxConcurrentStatusUpdates,
			// 2 rounds complete at 4/5 of waitDuration; the 3rd round is in flight
			// when the controller stops after waitDuration, and the 4th round is skipped.
			applyLatency: statusUpdateTimeout * 2 / 5,
			waitDuration: statusUpdateTimeout,
			wantCalls:    3 * maxConcurrentStatusUpdates,
			wantApplied:  2 * maxConcurrentStatusUpdates,
		},
		{
			name:         "unresponsive API server times out each request after statusUpdateTimeout",
			buffersCount: maxConcurrentStatusUpdates,
			applyLatency: 2 * statusUpdateTimeout,
			waitDuration: statusUpdateTimeout,
			wantCalls:    maxConcurrentStatusUpdates,
			wantApplied:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buffers := make([]*v1beta1.CapacityBuffer, tc.buffersCount)
			for i := range tc.buffersCount {
				buffers[i] = testutil.NewBuffer(testutil.WithName(fmt.Sprintf("b%d", i)))
			}

			var calls, applied atomic.Int32
			kubeClient := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
				SubResourceApply: func(ctx context.Context, _ client.Client, _ string, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					calls.Add(1)
					select {
					case <-time.After(tc.applyLatency):
						applied.Add(1)
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			}).Build()

			synctest.Test(t, func(t *testing.T) {
				controller := NewReadyReplicasController(kubeClient)
				stop := startTestReadyReplicasController(t, controller)

				start := time.Now()
				for _, buffer := range buffers {
					controller.Update(buffer, 1)
				}

				// Enqueueing must return immediately without blocking on API calls.
				assert.Zero(t, time.Since(start))
				assert.Zero(t, applied.Load())

				time.Sleep(tc.waitDuration)
				synctest.Wait()
				stop()

				assert.Equal(t, tc.wantCalls, calls.Load())
				assert.Equal(t, tc.wantApplied, applied.Load())
			})
		})
	}
}

func TestReadyReplicasControllerRetryOnFailure(t *testing.T) {
	testCases := []struct {
		name         string
		firstCallErr error
		wantCalls    int32
		wantApplied  []int32
	}{
		{
			name:         "transient error is retried after backoff",
			firstCallErr: fmt.Errorf("transient API error"),
			wantCalls:    2,
			wantApplied:  []int32{3},
		},
		{
			name:         "not found error clears target without retrying",
			firstCallErr: apierrors.NewNotFound(schema.GroupResource{Group: "autoscaling.x-k8s.io", Resource: "capacitybuffers"}, "b1"),
			wantCalls:    1,
			wantApplied:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buffer := testutil.NewBuffer(
				testutil.WithName("b1"),
				testutil.WithStatusReadyReplicas(2),
			)

			var applied []int32
			var calls atomic.Int32
			kubeClient := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
				SubResourceApply: func(_ context.Context, _ client.Client, _ string, obj runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
					if calls.Add(1) == 1 {
						return tc.firstCallErr
					}
					applyCfg := obj.(*v1beta1ac.CapacityBufferApplyConfiguration)
					applied = append(applied, *applyCfg.Status.ReadyReplicas)
					return nil
				},
			}).Build()

			synctest.Test(t, func(t *testing.T) {
				controller := NewReadyReplicasController(kubeClient)
				stop := startTestReadyReplicasController(t, controller)

				controller.Update(buffer, 3)
				synctest.Wait()
				assert.Equal(t, int32(1), calls.Load())

				// Advance the fake clock by 1s (well past controller-runtime's 5ms default initial
				// exponential backoff delay) so the first retry can execute if the error was retriable.
				time.Sleep(time.Second)
				synctest.Wait()
				stop()

				assert.Equal(t, tc.wantCalls, calls.Load())
				assert.Equal(t, tc.wantApplied, applied)
				assert.Empty(t, controller.targets)
			})
		})
	}
}
