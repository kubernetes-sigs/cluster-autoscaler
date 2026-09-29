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

package fakepods

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
)

// Registry tracks the relationship between fake pods (created for capacity buffers)
// and their originating CapacityBuffer objects. It also tracks the set of buffers
// processed in the current autoscaling loop, including those for which no fake pods
// were created.
type Registry struct {
	fakePodsUIDToBuffer map[types.UID]*v1beta1.CapacityBuffer
	processedBuffers    map[types.UID]*v1beta1.CapacityBuffer
	mutex               sync.RWMutex
}

// NewRegistry returns a new instance of Registry.
// If fakePodsToBuffers is nil, it initializes an empty registry.
// All buffers from fakePodsToBuffers are marked as processed.
func NewRegistry(fakePodsToBuffers map[types.UID]*v1beta1.CapacityBuffer) *Registry {
	if fakePodsToBuffers == nil {
		fakePodsToBuffers = make(map[types.UID]*v1beta1.CapacityBuffer)
	}
	processedBuffers := make(map[types.UID]*v1beta1.CapacityBuffer)
	for _, buffer := range fakePodsToBuffers {
		processedBuffers[buffer.UID] = buffer
	}
	return &Registry{fakePodsUIDToBuffer: fakePodsToBuffers, processedBuffers: processedBuffers}
}

// GetCapacityBuffer returns the CapacityBuffer associated with the given fake pod UID.
// Returns nil if no such mapping exists.
func (r *Registry) GetCapacityBuffer(fakePodUID types.UID) *v1beta1.CapacityBuffer {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.fakePodsUIDToBuffer[fakePodUID]
}

// SetCapacityBuffer registers a mapping between a fake pod's UID and the CapacityBuffer it was created from.
// The buffer is also marked as processed.
func (r *Registry) SetCapacityBuffer(fakePodUID types.UID, buffer *v1beta1.CapacityBuffer) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.fakePodsUIDToBuffer[fakePodUID] = buffer
	r.processedBuffers[buffer.UID] = buffer
}

// UnsetCapacityBuffer removes the mapping for the specified fake pod UID.
// It doesn't unmark the buffer as processed.
func (r *Registry) UnsetCapacityBuffer(fakePodUID types.UID) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.fakePodsUIDToBuffer, fakePodUID)
}

// MarkProcessed marks the buffer as processed in the current autoscaling loop,
// regardless of whether any fake pods were created for it.
func (r *Registry) MarkProcessed(buffer *v1beta1.CapacityBuffer) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.processedBuffers[buffer.UID] = buffer
}

// ProcessedBuffers returns all buffers marked as processed.
func (r *Registry) ProcessedBuffers() []*v1beta1.CapacityBuffer {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	buffers := make([]*v1beta1.CapacityBuffer, 0, len(r.processedBuffers))
	for _, buffer := range r.processedBuffers {
		buffers = append(buffers, buffer)
	}
	return buffers
}

// Clear removes all mappings and processed buffers from the registry, effectively resetting it.
func (r *Registry) Clear() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	clear(r.fakePodsUIDToBuffer)
	clear(r.processedBuffers)
}
