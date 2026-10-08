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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	v1beta1ac "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/applyconfiguration/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/client-go/rest"
	cbctrl "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/controller"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/testutil"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
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
		wantUpdated map[string]int32
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
			wantUpdated: map[string]int32{},
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
			wantUpdated: map[string]int32{"b1": 2},
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
			wantUpdated: map[string]int32{"b1": 1},
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
			wantUpdated: map[string]int32{"b1": 0},
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
			wantUpdated: map[string]int32{"b1": 0},
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
			wantUpdated: map[string]int32{"b1": 1},
		},
		{
			name: "unchanged ready replicas are not enqueued",
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
			wantUpdated: map[string]int32{},
		},
		{
			name: "unchanged zero ready replicas are not enqueued",
			buffers: []*v1beta1.CapacityBuffer{
				testutil.NewBuffer(
					testutil.WithName("b1"),
					testutil.WithUID[*v1beta1.CapacityBuffer]("b1-uid"),
					testutil.WithStatusReadyReplicas(0),
				),
			},
			processed:   []string{"b1"},
			wantUpdated: map[string]int32{},
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
			wantUpdated: map[string]int32{"b1": 1, "b2": 2, "b3": 0},
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

			var mu sync.Mutex
			gotUpdated := make(map[string]int32)
			kubeClient := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
				SubResourceApply: func(_ context.Context, _ client.Client, _ string, obj runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
					applyCfg := obj.(*v1beta1ac.CapacityBufferApplyConfiguration)
					mu.Lock()
					gotUpdated[*applyCfg.Name] = *applyCfg.Status.ReadyReplicas
					mu.Unlock()
					return nil
				},
			}).Build()

			synctest.Test(t, func(t *testing.T) {
				controller := cbctrl.NewReadyReplicasController(kubeClient)
				// Start a controller-runtime manager so ReadyReplicasController's background
				// worker is running and reconciles events enqueued by processor.Process.
				mgr, err := manager.New(&rest.Config{}, manager.Options{
					Metrics: metricsserver.Options{BindAddress: "0"},
					Controller: config.Controller{
						// SkipNameValidation allows registering the same controller name across multiple test cases.
						SkipNameValidation: new(true),
						// UsePriorityQueue is disabled in synctest unit tests so priorityqueue's background
						// handleReadyItems goroutine does not remain blocked inside the synctest bubble on shutdown.
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

				processor := NewCapacityBufferAutoscalingStatusProcessor(controller, registry)
				autoscalingCtx := &ca_context.AutoscalingContext{ClusterSnapshot: snapshot}
				err = processor.Process(t.Context(), autoscalingCtx, nil, time.Now())
				assert.NoError(t, err)

				// Wait for the controller workers to drain all enqueued events and finish calling SubResourceApply.
				synctest.Wait()
				// Stop the manager and wait for all controller-runtime goroutines in the synctest bubble to exit.
				cancel()
				wg.Wait()
				synctest.Wait()

				assert.Equal(t, tc.wantUpdated, gotUpdated)
			})
		})
	}
}
