/*
Copyright 2025 The Kubernetes Authors.

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

package common

import (
	"iter"
	"maps"
)

// PatchSet manages a stack of patches, allowing for fork/revert/commit operations.
// It provides a view of the data as if all patches were applied sequentially.
//
// Alongside the patches, PatchSet maintains the effective map: the key/value pairs
// resulting from applying all the patches in order. The effective map is kept up to
// date by every operation, so read operations are served from it directly and never
// need to traverse the patches.
//
// Time Complexities:
//   - Fork(): O(1).
//   - Commit(): O(P), where P is the number of modified/deleted entries
//     in the current patch or no-op for PatchSet with a single patch.
//   - Revert(): O(P * N), where P is the number of modified/deleted entries
//     in the topmost patch or no-op for PatchSet with a single patch. Each of
//     the P keys is re-resolved from the remaining patches.
//   - FindValue(key), SetCurrent(key, value), DeleteCurrent(key), InCurrentPatch(key), Len(): O(1).
//   - AsMap(), ValuesSlice(), Values(): O(C), where C is the number of effective key/value pairs.
//
// Variables used in complexity analysis:
//   - N: The number of patch layers in the PatchSet.
//   - P: The number of modified/deleted entries in a single patch layer
//
// Read operations never modify the PatchSet, so they can safely run concurrently with each
// other. The PatchSet must not be modified concurrently with any other operation.
//
// Values are stored and returned by value, so V should be cheap to copy (e.g. a pointer).
type PatchSet[K comparable, V any] struct {
	// patches is a stack of individual modification layers. The base data is
	// at index 0, and subsequent modifications are layered on top.
	patches []*Patch[K, V]

	// effective holds the effective key/value pairs - the result of applying
	// all the patches in order. It's always up to date.
	effective map[K]V
}

// NewPatchSet creates a new PatchSet, initializing it with the provided base patches.
func NewPatchSet[K comparable, V any](patches ...*Patch[K, V]) *PatchSet[K, V] {
	p := &PatchSet[K, V]{
		patches:   patches,
		effective: make(map[K]V, totalKeyCount(patches)),
	}
	for _, patch := range patches {
		maps.Copy(p.effective, patch.modified)
		for key := range patch.deleted {
			delete(p.effective, key)
		}
	}
	return p
}

// Fork adds a new, empty patch layer to the top of the stack.
// Subsequent modifications will be recorded in this new layer.
func (p *PatchSet[K, V]) Fork() {
	p.patches = append(p.patches, NewPatch[K, V]())
}

// Commit merges the topmost patch layer into the one below it.
// If there's only one layer (or none), it's a no-op.
func (p *PatchSet[K, V]) Commit() {
	if len(p.patches) < 2 {
		return
	}

	currentPatch := p.patches[len(p.patches)-1]
	previousPatch := p.patches[len(p.patches)-2]
	mergePatchesInPlace(previousPatch, currentPatch)
	p.patches = p.patches[:len(p.patches)-1]
}

// Revert removes the topmost patch layer.
// Any modifications or deletions recorded in that layer are discarded.
func (p *PatchSet[K, V]) Revert() {
	if len(p.patches) <= 1 {
		return
	}

	currentPatch := p.patches[len(p.patches)-1]
	p.patches = p.patches[:len(p.patches)-1]

	// Only keys modified or deleted in the reverted patch could have changed
	// their effective value - re-resolve them from the remaining patches.
	for key := range currentPatch.modified {
		p.repairEffectiveKey(key)
	}
	for key := range currentPatch.deleted {
		p.repairEffectiveKey(key)
	}
}

// repairEffectiveKey recomputes the effective value of the key from the patches.
func (p *PatchSet[K, V]) repairEffectiveKey(key K) {
	for i := len(p.patches) - 1; i >= 0; i-- {
		patch := p.patches[i]
		if patch.IsDeleted(key) {
			break
		}
		if value, found := patch.Get(key); found {
			p.effective[key] = value
			return
		}
	}
	delete(p.effective, key)
}

// FindValue returns the effective value of the key and true if found, or the zero value
// and false if the key is deleted or not present in any patch.
func (p *PatchSet[K, V]) FindValue(key K) (value V, found bool) {
	value, found = p.effective[key]
	return value, found
}

// AsMap returns a newly allocated map with all the effective key/value pairs.
func (p *PatchSet[K, V]) AsMap() map[K]V {
	return maps.Clone(p.effective)
}

// ValuesSlice returns a newly allocated slice with all the effective values
// of the PatchSet, in no particular order.
func (p *PatchSet[K, V]) ValuesSlice() []V {
	values := make([]V, 0, len(p.effective))
	for _, value := range p.effective {
		values = append(values, value)
	}
	return values
}

// Values returns an iterator over all the effective values of the PatchSet,
// in no particular order. The PatchSet must not be modified while iterating.
func (p *PatchSet[K, V]) Values() iter.Seq[V] {
	return maps.Values(p.effective)
}

// Len returns the number of effective key/value pairs in the PatchSet.
func (p *PatchSet[K, V]) Len() int {
	return len(p.effective)
}

// SetCurrent adds or updates a key-value pair in the topmost patch layer.
func (p *PatchSet[K, V]) SetCurrent(key K, value V) {
	if len(p.patches) == 0 {
		p.Fork()
	}

	currentPatch := p.patches[len(p.patches)-1]
	currentPatch.Set(key, value)
	p.effective[key] = value
}

// DeleteCurrent marks a key as deleted in the topmost patch layer.
func (p *PatchSet[K, V]) DeleteCurrent(key K) {
	if len(p.patches) == 0 {
		p.Fork()
	}

	currentPatch := p.patches[len(p.patches)-1]
	currentPatch.Delete(key)
	delete(p.effective, key)
}

// InCurrentPatch checks if the key is available in the topmost patch layer.
func (p *PatchSet[K, V]) InCurrentPatch(key K) bool {
	if len(p.patches) == 0 {
		return false
	}

	currentPatch := p.patches[len(p.patches)-1]
	_, found := currentPatch.Get(key)
	return found
}

// totalKeyCount calculates an approximate total number of key-value
// pairs across all patches taking the highest number of records possible
// this calculation does not consider deleted records - thus it is likely
// to be not accurate
func totalKeyCount[K comparable, V any](patches []*Patch[K, V]) int {
	count := 0
	for _, patch := range patches {
		count += len(patch.modified)
	}

	return count
}

// ClonePatchSet creates a deep copy of a PatchSet object with the same patch layers
// structure, while copying keys and values using cloneKey and cloneValue functions
// provided.
//
// This function is intended for testing purposes only.
func ClonePatchSet[K comparable, V any](ps *PatchSet[K, V], cloneKey func(K) K, cloneValue func(V) V) *PatchSet[K, V] {
	if ps == nil {
		return nil
	}

	clonedPatches := make([]*Patch[K, V], 0, len(ps.patches))
	for _, patch := range ps.patches {
		clonedPatch := NewPatch[K, V]()
		for key, value := range patch.modified {
			clonedKey, clonedValue := cloneKey(key), cloneValue(value)
			clonedPatch.Set(clonedKey, clonedValue)
		}

		for key := range patch.deleted {
			clonedKey := cloneKey(key)
			clonedPatch.Delete(clonedKey)
		}

		clonedPatches = append(clonedPatches, clonedPatch)
	}

	// The effective map is built from the cloned patches, so it holds the cloned values.
	return NewPatchSet(clonedPatches...)
}
