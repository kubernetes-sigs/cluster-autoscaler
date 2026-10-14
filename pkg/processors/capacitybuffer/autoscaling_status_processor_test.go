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

package capacitybufferpodlister

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	v1beta1ac "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/applyconfiguration/autoscaling.x-k8s.io/v1beta1"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/testutil"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestCapacityBufferAutoscalingStatusProcessor(t *testing.T) {
	const namespace = "default"

	existingNode := BuildTestNode("existing", 1000, 1000)
	upcomingNode := BuildTestNode("upcoming", 1000, 1000)
	upcomingNode.Annotations = map[string]string{annotations.NodeUpcomingAnnotation: "true"}

	type fakePodSpec struct {
		name     string
		nodeName string
		buffer   string
	}

	testCases := []struct {
		name        string
		buffers     []*v1beta1.CapacityBuffer
		processed   []string
		fakePods    []fakePodSpec
		wantApplied map[string]int32
	}{
		{
			name: "empty registry, no updates",
			buffers: []*v1beta1.CapacityBuffer{
				testutil.NewBuffer(
					testutil.WithName("b1"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b1-uid"),
					testutil.WithStatusReadyReplicas(3),
				),
			},
			wantApplied: map[string]int32{},
		},
		{
			name: "fake pods on existing node are counted",
			buffers: []*v1beta1.CapacityBuffer{
				testutil.NewBuffer(
					testutil.WithName("b1"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b1-uid"),
				),
			},
			processed: []string{"b1"},
			fakePods: []fakePodSpec{
				{name: "p1", nodeName: "existing", buffer: "b1"},
				{name: "p2", nodeName: "existing", buffer: "b1"},
			},
			wantApplied: map[string]int32{"b1": 2},
		},
		{
			name: "fake pods on upcoming node are not counted",
			buffers: []*v1beta1.CapacityBuffer{
				testutil.NewBuffer(
					testutil.WithName("b1"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b1-uid"),
				),
			},
			fakePods: []fakePodSpec{
				{name: "p1", nodeName: "existing", buffer: "b1"},
				{name: "p2", nodeName: "upcoming", buffer: "b1"},
			},
			wantApplied: map[string]int32{"b1": 1},
		},
		{
			name: "processed buffer without fake pods gets 0",
			buffers: []*v1beta1.CapacityBuffer{
				testutil.NewBuffer(
					testutil.WithName("b1"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b1-uid"),
					testutil.WithStatusReadyReplicas(2),
				),
			},
			processed:   []string{"b1"},
			wantApplied: map[string]int32{"b1": 0},
		},
		{
			name: "processed buffer with fake pods only on upcoming nodes gets 0",
			buffers: []*v1beta1.CapacityBuffer{
				testutil.NewBuffer(
					testutil.WithName("b1"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b1-uid"),
					testutil.WithStatusReadyReplicas(2),
				),
			},
			fakePods: []fakePodSpec{
				{name: "p1", nodeName: "upcoming", buffer: "b1"},
			},
			wantApplied: map[string]int32{"b1": 0},
		},
		{
			name: "unprocessed buffer is left unchanged",
			buffers: []*v1beta1.CapacityBuffer{
				testutil.NewBuffer(
					testutil.WithName("b1"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b1-uid"),
				),
				testutil.NewBuffer(
					testutil.WithName("unprocessed"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("unprocessed-uid"),
					testutil.WithStatusReadyReplicas(5),
				),
			},
			processed: []string{"b1"},
			fakePods: []fakePodSpec{
				{name: "p1", nodeName: "existing", buffer: "b1"},
			},
			wantApplied: map[string]int32{"b1": 1},
		},
		{
			name: "unchanged ready replicas are not applied",
			buffers: []*v1beta1.CapacityBuffer{
				testutil.NewBuffer(
					testutil.WithName("b1"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b1-uid"),
					testutil.WithStatusReadyReplicas(1),
				),
			},
			processed: []string{"b1"},
			fakePods: []fakePodSpec{
				{name: "p1", nodeName: "existing", buffer: "b1"},
			},
			wantApplied: map[string]int32{},
		},
		{
			name: "unchanged zero ready replicas are not applied",
			buffers: []*v1beta1.CapacityBuffer{
				testutil.NewBuffer(
					testutil.WithName("b1"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b1-uid"),
					testutil.WithStatusReadyReplicas(0),
				),
			},
			processed:   []string{"b1"},
			wantApplied: map[string]int32{},
		},
		{
			name: "multiple buffers",
			buffers: []*v1beta1.CapacityBuffer{
				testutil.NewBuffer(
					testutil.WithName("b1"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b1-uid"),
				),
				testutil.NewBuffer(
					testutil.WithName("b2"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b2-uid"),
					testutil.WithStatusReadyReplicas(4),
				),
				testutil.NewBuffer(
					testutil.WithName("b3"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b3-uid"),
					testutil.WithStatusReadyReplicas(1),
				),
			},
			processed: []string{"b3"},
			fakePods: []fakePodSpec{
				{name: "p1", nodeName: "existing", buffer: "b1"},
				{name: "p2", nodeName: "existing", buffer: "b2"},
				{name: "p3", nodeName: "existing", buffer: "b2"},
				{name: "p4", nodeName: "upcoming", buffer: "b2"},
			},
			wantApplied: map[string]int32{"b1": 1, "b2": 2, "b3": 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buffersByName := make(map[string]*v1beta1.CapacityBuffer, len(tc.buffers))
			for _, buffer := range tc.buffers {
				buffersByName[buffer.Name] = buffer
			}

			registry := fakepods.NewRegistry(nil)
			for _, name := range tc.processed {
				registry.MarkProcessed(buffersByName[name])
			}
			var pods []*apiv1.Pod
			for _, fp := range tc.fakePods {
				pod := BuildTestPod(fp.name, 100, 100, WithNamespace(namespace), WithNodeName(fp.nodeName))
				registry.SetCapacityBuffer(pod.UID, buffersByName[fp.buffer])
				pods = append(pods, pod)
			}

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			err := snapshot.SetClusterState(context.Background(), []*apiv1.Node{existingNode, upcomingNode}, pods, nil, nil)
			assert.NoError(t, err)

			// Applies are executed concurrently, so access to the map must be synchronized.
			var mu sync.Mutex
			applied := map[string]int32{}
			kubeClient := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
				SubResourceApply: func(_ context.Context, _ client.Client, subResourceName string, obj runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
					assert.Equal(t, "status", subResourceName)
					applyCfg, ok := obj.(*v1beta1ac.CapacityBufferApplyConfiguration)
					assert.True(t, ok, "unexpected apply configuration type %T", obj)
					assert.NotNil(t, applyCfg.Status)
					assert.NotNil(t, applyCfg.Status.ReadyReplicas)
					mu.Lock()
					defer mu.Unlock()
					applied[*applyCfg.GetName()] = *applyCfg.Status.ReadyReplicas
					return nil
				},
			}).Build()

			processor := NewCapacityBufferAutoscalingStatusProcessor(kubeClient, registry)
			autoscalingCtx := &ca_context.AutoscalingContext{ClusterSnapshot: snapshot}
			err = processor.Process(t.Context(), autoscalingCtx, nil, time.Now())
			assert.NoError(t, err)

			assert.Equal(t, tc.wantApplied, applied)
		})
	}
}

func TestCapacityBufferAutoscalingStatusProcessorTimeout(t *testing.T) {
	// Applies are executed in rounds of maxConcurrentStatusUpdates concurrent calls,
	// and the whole phase is bounded by statusUpdatesTimeout.
	testCases := []struct {
		name         string
		buffersCount int
		applyLatency time.Duration
		wantCalls    int32
		wantApplied  int32
	}{
		{
			name:         "all buffers updated within the timeout",
			buffersCount: 3 * maxConcurrentStatusUpdates,
			// 3 rounds complete after 3/4 of the timeout.
			applyLatency: statusUpdatesTimeout / 4,
			wantCalls:    3 * maxConcurrentStatusUpdates,
			wantApplied:  3 * maxConcurrentStatusUpdates,
		},
		{
			name:         "slow API server, in-flight calls are canceled and remaining buffers are skipped",
			buffersCount: 4 * maxConcurrentStatusUpdates,
			// 2 rounds complete, the 3rd one is in flight when the timeout expires.
			applyLatency: statusUpdatesTimeout * 2 / 5,
			wantCalls:    3 * maxConcurrentStatusUpdates,
			wantApplied:  2 * maxConcurrentStatusUpdates,
		},
		{
			name:         "unresponsive API server, only the first round of calls is attempted",
			buffersCount: 2 * maxConcurrentStatusUpdates,
			applyLatency: 2 * statusUpdatesTimeout,
			wantCalls:    maxConcurrentStatusUpdates,
			wantApplied:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			registry := fakepods.NewRegistry(nil)
			for i := range tc.buffersCount {
				name := fmt.Sprintf("b%d", i)
				registry.MarkProcessed(testutil.NewBuffer(testutil.WithName(name), testutil.WithUID[*v1beta1.CapacityBuffer](types.UID(name+"-uid"))))
			}

			var calls, applied atomic.Int32
			kubeClient := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
				SubResourceApply: func(ctx context.Context, _ client.Client, _ string, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
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

			processor := NewCapacityBufferAutoscalingStatusProcessor(kubeClient, registry)
			// The snapshot has to be created outside of the bubble, as it starts background goroutines that never exit.
			autoscalingCtx := &ca_context.AutoscalingContext{ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t)}

			synctest.Test(t, func(t *testing.T) {
				err := processor.Process(t.Context(), autoscalingCtx, nil, time.Now())
				assert.NoError(t, err)

				assert.Equal(t, tc.wantCalls, calls.Load())
				assert.Equal(t, tc.wantApplied, applied.Load())
			})
		})
	}
}
