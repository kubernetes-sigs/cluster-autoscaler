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
	"errors"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	metav1apply "k8s.io/client-go/applyconfigurations/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	cbapply "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/applyconfiguration/autoscaling.x-k8s.io/v1beta1"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	scalableobject "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/translators/scalable_objects"
)

const (
	dynamicWatchControllerName = "capacitybuffer-scalableref-watcher"

	// maxDynamicWatches specify how many unique kinds can be watched.
	// This is a purely defensive measure to ensure there is an upper bound
	// of maintained watches, as each watch costs CPU, memory, and API server load.
	maxDynamicWatches = 50

	// dynamicWatchSyncTimeout bounds how long a single reconciliation waits for a freshly
	// created informer to sync.
	dynamicWatchSyncTimeout = 5 * time.Second

	dynamicWatchMaxConcurrentReconciles = 2

	dynamicWatchMinRetryDelay = time.Second
	dynamicWatchMaxRetryDelay = 5 * time.Minute

	scalableRefGroupKindIndex = "spec.scalableRef.groupKind"
	scalableRefObjectIndex    = "spec.scalableRef.object"
)

var (
	errWatchLimitExceeded = fmt.Errorf("the maximum of %d dynamically watched kinds is already reached", maxDynamicWatches)
	errWatchNotSynced     = errors.New("the watch has been established but not yet synced. Double check if the Cluster Autoscaler has watch, list and get permissions to the referenced kind. Will retry after backoff")
)

// dynamicWatcher establishes watches for the custom scalable kinds referenced by
// CapacityBuffers, and feeds their events back into the namespace reconciliation queue.
//
// It is a controller of its own rather than part of bufferController because establishing
// a watch needs API discovery and a cache sync, and neither is allowed to delay or fail
// namespace reconciliation done by CapacityBuffer controller.
type dynamicWatcher struct {
	cache            ctrlcache.Cache
	client           ctrlclient.Client
	mapper           meta.RESTMapper
	enqueueNamespace func(namespace string)

	// ctx is the context the whole controller lives under, not a request scoped one.
	// Informer event handlers are called by client-go, whose ResourceEventHandler
	// interface has no context parameter. If we passed ctx from Reconcile, it would
	// get canceled after the reconciliation, closing the event handlers.
	ctx context.Context

	maxWatches  int
	syncTimeout time.Duration

	mu      sync.Mutex
	watches map[schema.GroupKind]ctrlcache.Informer
}

// dynamicWatcherOptions configures a dynamicWatcher.
type dynamicWatcherOptions struct {
	// Context bounds the lifetime of the watches and of the event handlers on them.
	Context context.Context
	// Cache creates and destroys the informers backing the dynamic watches.
	Cache ctrlcache.Cache
	// Client reads CapacityBuffers from an indexed cache and writes their conditions.
	Client ctrlclient.Client
	// Mapper resolves referenced group kinds to a concrete resource.
	Mapper meta.RESTMapper
	// EnqueueNamespace schedules a namespace for reconciliation.
	EnqueueNamespace func(namespace string)
}

// newDynamicWatcher creates a dynamicWatcher.
func newDynamicWatcher(opts dynamicWatcherOptions) *dynamicWatcher {
	return &dynamicWatcher{
		cache:            opts.Cache,
		client:           opts.Client,
		mapper:           opts.Mapper,
		enqueueNamespace: opts.EnqueueNamespace,
		ctx:              opts.Context,
		maxWatches:       maxDynamicWatches,
		syncTimeout:      dynamicWatchSyncTimeout,
		watches:          map[schema.GroupKind]ctrlcache.Informer{},
	}
}

// SetupWithManager registers the dynamic watch controller with the passed manager.
func (w *dynamicWatcher) SetupWithManager(mgr ctrl.Manager) error {
	indexer := mgr.GetFieldIndexer()
	if err := indexer.IndexField(w.ctx, &v1.CapacityBuffer{}, scalableRefGroupKindIndex, indexScalableRefGroupKind); err != nil {
		return fmt.Errorf("failed to index capacity buffers by scalable ref group kind: %w", err)
	}
	if err := indexer.IndexField(w.ctx, &v1.CapacityBuffer{}, scalableRefObjectIndex, indexScalableRefObject); err != nil {
		return fmt.Errorf("failed to index capacity buffers by scalable ref object: %w", err)
	}

	return builder.TypedControllerManagedBy[schema.GroupKind](mgr).
		Named(dynamicWatchControllerName).
		WatchesRawSource(source.TypedKind(mgr.GetCache(), &v1.CapacityBuffer{},
			handler.TypedEnqueueRequestsFromMapFunc(bufferToGroupKinds))).
		WithOptions(controller.TypedOptions[schema.GroupKind]{
			MaxConcurrentReconciles: dynamicWatchMaxConcurrentReconciles,
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[schema.GroupKind](
				dynamicWatchMinRetryDelay, dynamicWatchMaxRetryDelay),
		}).
		Complete(w)
}

// Reconcile ensures a watch exists for the passed group kind
// and reports the outcome on the referencing buffers.
func (w *dynamicWatcher) Reconcile(ctx context.Context, groupKind schema.GroupKind) (reconcile.Result, error) {
	logger := klog.FromContext(ctx)

	buffers, err := w.buffersReferencing(ctx, groupKind)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to list buffers referencing %s: %w", groupKind, err)
	}

	if len(buffers) == 0 {
		return reconcile.Result{}, nil
	}

	informer, found := w.lookup(groupKind)
	if !found {
		// activeCount() can exceed maxWatches if separate reconcile requests
		// are processed in parallel. This is acceptable, as maxWatches is purely
		// a defensive measure, not a hard limit.
		if w.activeCount() >= w.maxWatches {
			w.reportFailure(ctx, groupKind, buffers, capacitybuffer.WatchLimitExceededReason, errWatchLimitExceeded)
			return reconcile.Result{}, errWatchLimitExceeded
		}

		mapping, err := w.mapper.RESTMapping(groupKind)
		if err != nil {
			w.reportFailure(ctx, groupKind, buffers, capacitybuffer.UnknownKindReason, err)
			return reconcile.Result{}, fmt.Errorf("failed to resolve kind %s: %w", groupKind, err)
		}

		if informer, err = w.startWatch(ctx, groupKind, mapping.GroupVersionKind); err != nil {
			w.reportFailure(ctx, groupKind, buffers, capacitybuffer.WatchNotSyncedReason, err)
			return reconcile.Result{}, err
		}
		logger.V(4).Info("Watching custom scalable kind", "groupKind", groupKind)
	}

	if !w.waitForSync(ctx, informer) {
		w.reportFailure(ctx, groupKind, buffers, capacitybuffer.WatchNotSyncedReason, errWatchNotSynced)
		return reconcile.Result{}, errWatchNotSynced
	}

	w.reportEstablished(ctx, groupKind, buffers)
	return reconcile.Result{}, nil
}

// startWatch creates the informer backing the watch of the passed kind and records it.
func (w *dynamicWatcher) startWatch(ctx context.Context, groupKind schema.GroupKind, gvk schema.GroupVersionKind) (ctrlcache.Informer, error) {
	// Metadata only. The watch is a trigger: the replica counts the buffers need are
	// read through the scale subresource.
	object := &metav1.PartialObjectMetadata{}
	object.SetGroupVersionKind(gvk)

	informer, err := w.cache.GetInformer(ctx, object, ctrlcache.BlockUntilSynced(false))
	if err != nil {
		return nil, fmt.Errorf("failed to create informer for %s: %w", gvk, err)
	}

	if _, err := informer.AddEventHandler(w.eventHandler(groupKind)); err != nil {
		if removeErr := w.cache.RemoveInformer(ctx, object); removeErr != nil {
			utilruntime.HandleErrorWithContext(ctx, removeErr, "Failed to drop the informer of a watch that could not be handled", "groupKind", groupKind)
		}
		return nil, fmt.Errorf("failed to add event handler for %s: %w", gvk, err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.watches[groupKind] = informer
	return informer, nil
}

// waitForSync waits for the passed informer's cache to be populated, and reports whether it is.
//
// Initial sync might take a long time for kinds with many object, so the function times out
// after w.syncTimeout to avoid blocking the queue for too long. Wait will be retried after
// a backoff.
func (w *dynamicWatcher) waitForSync(ctx context.Context, informer ctrlcache.Informer) bool {
	if informer.HasSynced() {
		return true
	}

	syncCtx, cancel := context.WithTimeout(ctx, w.syncTimeout)
	defer cancel()
	return toolscache.WaitForCacheSync(syncCtx.Done(), informer.HasSynced)
}

func (w *dynamicWatcher) eventHandler(groupKind schema.GroupKind) toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			w.enqueueReferencingNamespace(groupKind, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldObject, oldErr := objectMeta(oldObj)
			newObject, newErr := objectMeta(newObj)
			if oldErr == nil && newErr == nil && oldObject.GetResourceVersion() == newObject.GetResourceVersion() {
				return
			}
			w.enqueueReferencingNamespace(groupKind, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			w.enqueueReferencingNamespace(groupKind, obj)
		},
	}
}

// enqueueReferencingNamespace schedules the namespace of the passed object for
// reconciliation, if any buffer in it references the object.
func (w *dynamicWatcher) enqueueReferencingNamespace(groupKind schema.GroupKind, obj interface{}) {
	ctx := w.ctx

	object, err := objectMeta(obj)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Received an event for an object without metadata", "groupKind", groupKind)
		return
	}

	var buffers v1.CapacityBufferList
	err = w.client.List(ctx, &buffers,
		ctrlclient.InNamespace(object.GetNamespace()),
		ctrlclient.MatchingFields{scalableRefObjectIndex: objectIndexKey(groupKind, object.GetName())})
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed to look up buffers referencing a watched object",
			"groupKind", groupKind, "object", klog.KObj(object))
		return
	}
	if len(buffers.Items) > 0 {
		w.enqueueNamespace(object.GetNamespace())
	}
}

// buffersReferencing returns the buffers whose scalable ref points at the passed group kind.
func (w *dynamicWatcher) buffersReferencing(ctx context.Context, groupKind schema.GroupKind) ([]v1.CapacityBuffer, error) {
	var buffers v1.CapacityBufferList
	if err := w.client.List(ctx, &buffers, ctrlclient.MatchingFields{scalableRefGroupKindIndex: groupKindIndexKey(groupKind)}); err != nil {
		return nil, err
	}
	return buffers.Items, nil
}

func (w *dynamicWatcher) lookup(groupKind schema.GroupKind) (ctrlcache.Informer, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	informer, found := w.watches[groupKind]
	return informer, found
}

func (w *dynamicWatcher) activeCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.watches)
}

func (w *dynamicWatcher) reportEstablished(ctx context.Context, groupKind schema.GroupKind, buffers []v1.CapacityBuffer) {
	message := fmt.Sprintf("Watching %s for changes", groupKind)

	for i := range buffers {
		w.applyWatchCondition(ctx, &buffers[i], metav1.ConditionTrue, capacitybuffer.WatchEstablishedReason, message)
	}
}

func (w *dynamicWatcher) reportFailure(ctx context.Context, groupKind schema.GroupKind, buffers []v1.CapacityBuffer, reason string, cause error) {
	message := fmt.Sprintf("Cannot watch %s referenced by spec.scalableRef: %v", groupKind, cause)

	for i := range buffers {
		w.applyWatchCondition(ctx, &buffers[i], metav1.ConditionFalse, reason, message)
	}
}

func (w *dynamicWatcher) applyWatchCondition(ctx context.Context, buffer *v1.CapacityBuffer, status metav1.ConditionStatus, reason, message string) bool {
	if watchConditionUpToDate(buffer, status, reason, message) {
		return false
	}

	configuration := cbapply.CapacityBuffer(buffer.Name, buffer.Namespace).
		WithStatus(cbapply.CapacityBufferStatus().
			WithConditions(metav1apply.Condition().
				WithType(capacitybuffer.ScalableRefWatchedCondition).
				WithStatus(status).
				WithReason(reason).
				WithMessage(message).
				WithObservedGeneration(buffer.Generation).
				WithLastTransitionTime(metav1.Now())))

	err := w.client.Status().Apply(ctx, configuration,
		ctrlclient.FieldOwner(dynamicWatchControllerName), ctrlclient.ForceOwnership)
	if err != nil {
		klog.FromContext(ctx).Error(err, "Failed to report the scalable ref watch status",
			"buffer", klog.KObj(buffer), "condition", capacitybuffer.ScalableRefWatchedCondition)
		return false
	}
	return true
}

func watchConditionUpToDate(buffer *v1.CapacityBuffer, status metav1.ConditionStatus, reason, message string) bool {
	current := meta.FindStatusCondition(buffer.Status.Conditions, capacitybuffer.ScalableRefWatchedCondition)
	return current != nil &&
		current.Status == status &&
		current.Reason == reason &&
		current.Message == message &&
		current.ObservedGeneration == buffer.Generation
}

func bufferToGroupKinds(_ context.Context, buffer *v1.CapacityBuffer) []schema.GroupKind {
	groupKind, needsWatch := customScalableRefGroupKind(buffer)
	if !needsWatch {
		return nil
	}
	return []schema.GroupKind{groupKind}
}

// customScalableRefGroupKind returns the group kind referenced by the passed buffer, and
// whether it needs a dynamically established watch.
func customScalableRefGroupKind(buffer *v1.CapacityBuffer) (schema.GroupKind, bool) {
	ref := buffer.Spec.ScalableRef
	if ref == nil || ref.Kind == "" {
		return schema.GroupKind{}, false
	}
	if scalableobject.IsStaticallyWatched(ref.APIGroup, ref.Kind) {
		return schema.GroupKind{}, false
	}
	return schema.GroupKind{Group: ref.APIGroup, Kind: ref.Kind}, true
}

func indexScalableRefGroupKind(object ctrlclient.Object) []string {
	buffer, isBuffer := object.(*v1.CapacityBuffer)
	if !isBuffer {
		return nil
	}
	groupKind, needsWatch := customScalableRefGroupKind(buffer)
	if !needsWatch {
		return nil
	}
	return []string{groupKindIndexKey(groupKind)}
}

func indexScalableRefObject(object ctrlclient.Object) []string {
	buffer, isBuffer := object.(*v1.CapacityBuffer)
	if !isBuffer {
		return nil
	}
	groupKind, needsWatch := customScalableRefGroupKind(buffer)
	if !needsWatch {
		return nil
	}
	return []string{objectIndexKey(groupKind, buffer.Spec.ScalableRef.Name)}
}

func groupKindIndexKey(groupKind schema.GroupKind) string {
	return groupKind.String()
}

func objectIndexKey(groupKind schema.GroupKind, name string) string {
	return groupKind.String() + "/" + name
}

// objectMeta returns the metadata of a watched object, unwrapping tombstones.
func objectMeta(obj interface{}) (metav1.Object, error) {
	if tombstone, isTombstone := obj.(toolscache.DeletedFinalStateUnknown); isTombstone {
		if tombstone.Obj == nil {
			return nil, errors.New("received a deletion tombstone with no object")
		}
		obj = tombstone.Obj
	}
	return meta.Accessor(obj)
}
