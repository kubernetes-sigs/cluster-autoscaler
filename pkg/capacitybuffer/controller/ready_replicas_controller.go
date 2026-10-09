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
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	v1beta1ac "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/applyconfiguration/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	readyReplicasControllerName = "capacitybuffer-ready-replicas-controller"
	maxConcurrentStatusUpdates  = 10
	eventChannelBufferSize      = 1024

	// statusUpdateTimeout bounds the duration of a single status apply reconciliation.
	statusUpdateTimeout = 15 * time.Second
)

// ReadyReplicasController reconciles CapacityBuffer .status.readyReplicas updates
// asynchronously in parallel using controller-runtime, skipping pending intermediate
// updates per buffer.
type ReadyReplicasController struct {
	kubeClient client.Client
	events     chan event.TypedGenericEvent[reconcile.Request]

	mu sync.Mutex
	// targets holds only pending (unapplied) readyReplicas values keyed by buffer NamespacedName.
	// Entries are removed once applied (or canceled when desired reverts to current status)
	// to avoid stale state across buffer recreation and memory leaks.
	targets map[types.NamespacedName]int32
}

// NewReadyReplicasController returns a new ReadyReplicasController.
func NewReadyReplicasController(kubeClient client.Client) *ReadyReplicasController {
	return &ReadyReplicasController{
		kubeClient: kubeClient,
		events:     make(chan event.TypedGenericEvent[reconcile.Request], eventChannelBufferSize),
		targets:    make(map[types.NamespacedName]int32),
	}
}

// SetupWithManager registers the controller with the passed manager.
func (c *ReadyReplicasController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(readyReplicasControllerName).
		WatchesRawSource(source.TypedChannel(
			c.events,
			handler.TypedFuncs[reconcile.Request, reconcile.Request]{
				GenericFunc: func(_ context.Context, e event.TypedGenericEvent[reconcile.Request], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
					q.Add(e.Object)
				},
			},
		)).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrentStatusUpdates,
			ReconciliationTimeout:   statusUpdateTimeout,
		}).
		Complete(c)
}

// Update sets the target ReadyReplicas state for the given buffer and enqueues
// an asynchronous reconciliation if needed. If the buffer already has a pending
// update in the queue, its target state is overwritten so intermediate updates
// are skipped.
func (c *ReadyReplicasController) Update(buffer *v1beta1.CapacityBuffer, readyReplicas int32) {
	if buffer == nil {
		return
	}
	key := client.ObjectKeyFromObject(buffer)
	if c.setTarget(key, buffer.Status.ReadyReplicas, readyReplicas) {
		select {
		case c.events <- event.TypedGenericEvent[reconcile.Request]{Object: reconcile.Request{NamespacedName: key}}:
		default:
			c.clearTarget(key, readyReplicas)
			klog.Warningf("CapacityBuffer ReadyReplicas event channel is full, dropping status update for %s (will retry in next loop)", key)
		}
	}
}

// Reconcile applies the latest desired ReadyReplicas to the CapacityBuffer.
func (c *ReadyReplicasController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	desired, ok := c.getTarget(req.NamespacedName)
	if !ok {
		return reconcile.Result{}, nil
	}
	applyCfg := readyReplicasApplyConfiguration(req.NamespacedName, desired)
	if err := c.kubeClient.Status().Apply(ctx, applyCfg, client.FieldOwner(readyReplicasControllerName), client.ForceOwnership); err != nil {
		if apierrors.IsNotFound(err) {
			c.clearTarget(req.NamespacedName, desired)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	c.clearTarget(req.NamespacedName, desired)
	return reconcile.Result{}, nil
}

func (c *ReadyReplicasController) setTarget(key types.NamespacedName, current *int32, desired int32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ptr.Equal(current, &desired) {
		delete(c.targets, key)
		return false
	}
	if prev, ok := c.targets[key]; ok && prev == desired {
		return false
	}
	c.targets[key] = desired
	return true
}

func (c *ReadyReplicasController) getTarget(key types.NamespacedName) (int32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	desired, ok := c.targets[key]
	return desired, ok
}

func (c *ReadyReplicasController) clearTarget(key types.NamespacedName, applied int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if desired, ok := c.targets[key]; ok && desired == applied {
		delete(c.targets, key)
	}
}

func readyReplicasApplyConfiguration(key types.NamespacedName, readyReplicas int32) *v1beta1ac.CapacityBufferApplyConfiguration {
	return v1beta1ac.CapacityBuffer(key.Name, key.Namespace).
		WithStatus(v1beta1ac.CapacityBufferStatus().WithReadyReplicas(readyReplicas))
}
