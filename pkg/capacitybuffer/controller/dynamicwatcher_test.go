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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllertest"

	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/testutil"
	scalableobject "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/translators/scalable_objects"
)

// testVersion is the version every kind the rest mapper knows about is served under.
const testVersion = "v1"

var (
	customGroupKind = schema.GroupKind{Group: "testing.x-k8s.io", Kind: "TestScalable"}
	otherGroupKind  = schema.GroupKind{Group: "testing.x-k8s.io", Kind: "OtherScalable"}

	// scalableObject is the object the buffer under test references.
	scalableObject = &metav1.PartialObjectMetadata{
		ObjectMeta: metav1.ObjectMeta{Name: "scalable", Namespace: "default"},
	}
)

// newScalableRefBuffer returns a buffer referencing scalableObject.
func newScalableRefBuffer() *v1.CapacityBuffer {
	return testutil.NewBuffer(
		testutil.WithName("buffer"),
		testutil.WithNamespace[*v1.CapacityBuffer]("default"),
		testutil.WithScalableRef(customGroupKind.Group, customGroupKind.Kind, scalableObject.Name),
	)
}

func TestDynamicWatcherReconcile(t *testing.T) {
	buffer := newScalableRefBuffer()

	tests := []struct {
		name            string
		buffers         []*v1.CapacityBuffer
		knownKinds      []schema.GroupKind
		informersSynced bool
		watchedKinds    []schema.GroupKind
		maxWatches      int

		wantErr     bool
		wantWatched []schema.GroupKind
	}{
		{
			name:            "establishes a watch for a referenced custom kind",
			buffers:         []*v1.CapacityBuffer{buffer},
			knownKinds:      []schema.GroupKind{customGroupKind},
			informersSynced: true,
			wantWatched:     []schema.GroupKind{customGroupKind},
		},
		{
			name:            "keeps an already established watch",
			buffers:         []*v1.CapacityBuffer{buffer},
			knownKinds:      []schema.GroupKind{customGroupKind},
			informersSynced: true,
			watchedKinds:    []schema.GroupKind{customGroupKind},
			wantWatched:     []schema.GroupKind{customGroupKind},
		},
		{
			name:       "retries a kind that cannot be resolved",
			buffers:    []*v1.CapacityBuffer{buffer},
			knownKinds: nil,
			wantErr:    true,
		},
		{
			name:            "retries a kind whose watch does not sync",
			buffers:         []*v1.CapacityBuffer{buffer},
			knownKinds:      []schema.GroupKind{customGroupKind},
			informersSynced: false,
			wantErr:         true,
			wantWatched:     []schema.GroupKind{customGroupKind},
		},
		{
			name:            "refuses to exceed the watch limit",
			buffers:         []*v1.CapacityBuffer{buffer},
			knownKinds:      []schema.GroupKind{customGroupKind},
			informersSynced: true,
			watchedKinds:    []schema.GroupKind{otherGroupKind},
			maxWatches:      1,
			wantErr:         true,
			wantWatched:     []schema.GroupKind{otherGroupKind},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache := newMetadataCache(test.informersSynced)
			watcher := newTestDynamicWatcher(t, cache, test.buffers, test.knownKinds)
			if test.maxWatches > 0 {
				watcher.maxWatches = test.maxWatches
			}
			for _, groupKind := range test.watchedKinds {
				watcher.watches[groupKind] = newInformer(test.informersSynced)
			}

			result, err := watcher.Reconcile(t.Context(), customGroupKind)

			if test.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.True(t, result.IsZero())
			assert.ElementsMatch(t, test.wantWatched, watchedKinds(watcher))
		})
	}
}

// TestDynamicWatcherHandlesEventsOnce checks that repeated reconciliations of a kind
// leave a single event handler on its informer.
func TestDynamicWatcherHandlesEventsOnce(t *testing.T) {
	cache := newMetadataCache(true)
	watcher := newTestDynamicWatcher(t, cache, []*v1.CapacityBuffer{newScalableRefBuffer()}, []schema.GroupKind{customGroupKind})
	var enqueued []string
	watcher.enqueueNamespace = func(namespace string) { enqueued = append(enqueued, namespace) }

	for range 2 {
		_, err := watcher.Reconcile(t.Context(), customGroupKind)
		assert.NoError(t, err)
	}

	informer, found := cache.InformersByGVK[customGroupKind.WithVersion(testVersion)]
	assert.True(t, found, "expected an informer to have been created")
	informer.(*controllertest.FakeInformer).Add(scalableObject)

	assert.Equal(t, []string{scalableObject.Namespace}, enqueued)
}

func TestDynamicWatcherEventHandler(t *testing.T) {
	withResourceVersion := func(resourceVersion string) *metav1.PartialObjectMetadata {
		object := scalableObject.DeepCopy()
		object.ResourceVersion = resourceVersion
		return object
	}

	tests := []struct {
		name         string
		fire         func(handler toolscache.ResourceEventHandler)
		wantEnqueued []string
	}{
		{
			name:         "add",
			fire:         func(handler toolscache.ResourceEventHandler) { handler.OnAdd(scalableObject, false) },
			wantEnqueued: []string{scalableObject.Namespace},
		},
		{
			name: "update",
			fire: func(handler toolscache.ResourceEventHandler) {
				handler.OnUpdate(withResourceVersion("1"), withResourceVersion("2"))
			},
			wantEnqueued: []string{scalableObject.Namespace},
		},
		{
			name: "resync",
			fire: func(handler toolscache.ResourceEventHandler) {
				handler.OnUpdate(withResourceVersion("1"), withResourceVersion("1"))
			},
		},
		{
			name:         "delete",
			fire:         func(handler toolscache.ResourceEventHandler) { handler.OnDelete(scalableObject) },
			wantEnqueued: []string{scalableObject.Namespace},
		},
		{
			name: "tombstone received",
			fire: func(handler toolscache.ResourceEventHandler) {
				handler.OnDelete(toolscache.DeletedFinalStateUnknown{Key: "default/scalable", Obj: scalableObject})
			},
			wantEnqueued: []string{scalableObject.Namespace},
		},
		{
			name: "delete of a tombstone that lost its object",
			fire: func(handler toolscache.ResourceEventHandler) {
				handler.OnDelete(toolscache.DeletedFinalStateUnknown{Key: "default/scalable"})
			},
		},
		{
			name: "add of an object no buffer references",
			fire: func(handler toolscache.ResourceEventHandler) {
				handler.OnAdd(&metav1.PartialObjectMetadata{
					ObjectMeta: metav1.ObjectMeta{Name: "unreferenced", Namespace: "default"},
				}, false)
			},
		},
		{
			name: "add of an object in another namespace",
			fire: func(handler toolscache.ResourceEventHandler) {
				handler.OnAdd(&metav1.PartialObjectMetadata{
					ObjectMeta: metav1.ObjectMeta{Name: scalableObject.Name, Namespace: "other"},
				}, false)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			watcher := newTestDynamicWatcher(t, newMetadataCache(true), []*v1.CapacityBuffer{newScalableRefBuffer()}, []schema.GroupKind{customGroupKind})
			var enqueued []string
			watcher.enqueueNamespace = func(namespace string) { enqueued = append(enqueued, namespace) }

			test.fire(watcher.eventHandler(customGroupKind))

			assert.Equal(t, test.wantEnqueued, enqueued)
		})
	}
}

func TestCustomScalableRefGroupKind(t *testing.T) {
	tests := []struct {
		name          string
		buffer        *v1.CapacityBuffer
		wantGroupKind schema.GroupKind
		wantWatch     bool
	}{
		{
			name:   "no scalable ref",
			buffer: testutil.NewBuffer(testutil.WithPodTemplateRef("template")),
		},
		{
			name:   "empty kind",
			buffer: testutil.NewBuffer(testutil.WithScalableRef(scalableobject.ApiGroupApps, "", "name")),
		},
		{
			name:   "statically watched deployment",
			buffer: testutil.NewBuffer(testutil.WithScalableRef(scalableobject.ApiGroupApps, "Deployment", "name")),
		},
		{
			name:   "statically watched job",
			buffer: testutil.NewBuffer(testutil.WithScalableRef(scalableobject.ApiGroupBatch, "Job", "name")),
		},
		{
			name:   "statically watched replication controller in the core group",
			buffer: testutil.NewBuffer(testutil.WithScalableRef(scalableobject.ApiGroupCore, "ReplicationController", "name")),
		},
		{
			name:          "custom kind",
			buffer:        testutil.NewBuffer(testutil.WithScalableRef("testing.x-k8s.io", "TestScalable", "name")),
			wantGroupKind: customGroupKind,
			wantWatch:     true,
		},
		{
			name:          "custom kind in a statically watched group",
			buffer:        testutil.NewBuffer(testutil.WithScalableRef("apps", "CustomSet", "name")),
			wantGroupKind: schema.GroupKind{Group: "apps", Kind: "CustomSet"},
			wantWatch:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			groupKind, needsWatch := customScalableRefGroupKind(test.buffer)
			assert.Equal(t, test.wantWatch, needsWatch)
			assert.Equal(t, test.wantGroupKind, groupKind)

			var wantMapped []schema.GroupKind
			if test.wantWatch {
				wantMapped = []schema.GroupKind{test.wantGroupKind}
			}
			assert.Equal(t, wantMapped, bufferToGroupKinds(t.Context(), test.buffer))
		})
	}
}

func newTestDynamicWatcher(t *testing.T, cache *metadataCache, buffers []*v1.CapacityBuffer, knownKinds []schema.GroupKind) *dynamicWatcher {
	t.Helper()

	scheme := runtime.NewScheme()
	assert.NoError(t, v1.AddToScheme(scheme))

	objects := make([]ctrlclient.Object, 0, len(buffers))
	for _, buffer := range buffers {
		objects = append(objects, buffer)
	}
	client := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&v1.CapacityBuffer{}).
		WithIndex(&v1.CapacityBuffer{}, scalableRefGroupKindIndex, indexScalableRefGroupKind).
		WithIndex(&v1.CapacityBuffer{}, scalableRefObjectIndex, indexScalableRefObject).
		Build()

	// The default rest mapper only resolves a kind without a version if the kind's group
	// version is one of its defaults.
	groupVersions := make([]schema.GroupVersion, 0, len(knownKinds))
	for _, groupKind := range knownKinds {
		groupVersions = append(groupVersions, schema.GroupVersion{Group: groupKind.Group, Version: testVersion})
	}
	mapper := apimeta.NewDefaultRESTMapper(groupVersions)
	for _, groupKind := range knownKinds {
		mapper.Add(groupKind.WithVersion(testVersion), apimeta.RESTScopeNamespace)
	}

	watcher := newDynamicWatcher(dynamicWatcherOptions{
		Context:          t.Context(),
		Cache:            cache,
		Client:           client,
		Mapper:           mapper,
		EnqueueNamespace: func(string) {},
	})
	watcher.syncTimeout = 5 * time.Millisecond
	return watcher
}

// newInformer stands in for a watch established by an earlier reconciliation.
func newInformer(synced bool) *controllertest.FakeInformer {
	if synced {
		return controllertest.NewFakeInformer(controllertest.Synced)
	}
	return controllertest.NewFakeInformer()
}

func watchedKinds(watcher *dynamicWatcher) []schema.GroupKind {
	kinds := make([]schema.GroupKind, 0, len(watcher.watches))
	for groupKind := range watcher.watches {
		kinds = append(kinds, groupKind)
	}
	return kinds
}

type metadataCache struct {
	*informertest.FakeInformers

	informersSynced bool
}

func newMetadataCache(informersSynced bool) *metadataCache {
	return &metadataCache{
		FakeInformers: &informertest.FakeInformers{
			InformersByGVK: map[schema.GroupVersionKind]toolscache.SharedIndexInformer{},
		},
		informersSynced: informersSynced,
	}
}

// GetInformer overrides FakeInformers.GetInformer to support metadata-only informers.
func (c *metadataCache) GetInformer(_ context.Context, object ctrlclient.Object, _ ...ctrlcache.InformerGetOption) (ctrlcache.Informer, error) {
	gvk := object.GetObjectKind().GroupVersionKind()

	informer, found := c.InformersByGVK[gvk]
	if !found {
		informer = newInformer(c.informersSynced)
		c.InformersByGVK[gvk] = informer
	}
	return informer, nil
}
