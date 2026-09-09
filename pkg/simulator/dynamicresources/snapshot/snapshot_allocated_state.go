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

package snapshot

import (
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/structured"
)

// allocatedStateTracker maintains the set of devices consumed by the ResourceClaims in a
// Snapshot. The scheduler asks for this state in every PreFilter, that is once per pod
// placement attempt, and recomputing it means walking every claim and re-deriving every
// DeviceID. Between two consecutive attempts only a handful of claims change, so the state
// is instead maintained as claims are written and served directly.
//
// Time Complexities:
//   - apply(claimId, claim): O(D).
//   - withdraw(claimId): O(D), over the devices the claim contributed when applied.
//   - refresh(claimIds, lookup): O(P * D), driven by Snapshot.Revert.
//   - ensureBuilt(walkClaims): O(C * D) plus the cost of listing the claims on the
//     first read, O(1) on every read after that.
//   - allocatedState(): O(1) - the collections are handed over as they are.
//
// Variables used in complexity analysis:
//   - C: The number of ResourceClaims effective in the Snapshot.
//   - D: The number of allocated devices in a single ResourceClaim.
//   - P: The number of modified/deleted entries in a single patch layer.
//
// The work moved onto a write is bounded by the claim being written, never by the size of
// the Snapshot, and it takes the O(C * D) walk off every read but the first. Snapshot.Revert
// is the one operation where the tracker adds work proportional to something other than a
// single claim, and PatchSet.Revert is already O(P), so the refresh keeps it in the same
// complexity class with a larger constant.
//
// Withdrawing a claim cannot work by looking at its previous value: Snapshot mutates claims
// in place (see Snapshot.ensureClaimWritable, which hands back the stored object whenever the
// claim already lives in the current patch layer), so by the time a write reaches the tracker
// the old allocation may already be gone. The tracker therefore records what each claim
// contributed when it was applied, and withdraws exactly that.
//
// That in-place mutation is itself the subject of two open bugs - kubernetes-sigs/cluster-autoscaler#33,
// where a Pod referencing one claim through two aliases leaves the base layer dirty past a
// Revert, and #37, where the base layer holds the informer's own objects. Recording
// contributions rather than re-deriving them keeps the tracker consistent with whatever the
// claims currently say, so it stays correct however those two are eventually resolved.
//
// Entries are reference counted. A device should only ever be consumed by one claim, but
// refcounting means a duplicate does not make the device disappear from the state when just
// one of the claims goes away.
type allocatedStateTracker struct {
	// devices holds the devices claimed exclusively, matching what GatherAllocatedState
	// reports when the DRAConsumableCapacity feature is enabled.
	devices refCountedSet[structured.DeviceID]
	// allDevices holds every allocated device, including the shared ones. This is what
	// ListAllAllocatedDevices reports, and what GatherAllocatedState reports as allocated
	// when the DRAConsumableCapacity feature is disabled.
	allDevices refCountedSet[structured.DeviceID]
	// sharedDeviceIDs holds the (device, share) pairs of the shared devices.
	sharedDeviceIDs refCountedSet[structured.SharedDeviceID]
	// capacity aggregates the consumed capacity of the shared devices.
	capacity structured.ConsumedCapacityCollection

	// contributions records what each claim currently adds to the collections above.
	contributions map[ResourceClaimId]*claimContribution

	// built is false until the state has been populated from a full pass over the claims.
	// Writes are ignored while it is false, because that first pass picks them up anyway.
	built bool
}

// claimContribution is what a single ResourceClaim adds to the tracked state.
//
// The devices of shared results are kept apart from the dedicated ones rather than in a
// second combined slice: allDevices is the union of the two, and claims using shared devices
// are rare, so the common claim only needs the one slice.
type claimContribution struct {
	// devices are the devices the claim consumes exclusively.
	devices []structured.DeviceID
	// sharedDevices are the devices behind sharedDeviceIDs. They count towards allDevices
	// but not towards devices.
	sharedDevices   []structured.DeviceID
	sharedDeviceIDs []structured.SharedDeviceID
	capacity        []structured.DeviceConsumedCapacity
}

func newAllocatedStateTracker() *allocatedStateTracker {
	return &allocatedStateTracker{
		devices:         newRefCountedSet[structured.DeviceID](),
		allDevices:      newRefCountedSet[structured.DeviceID](),
		sharedDeviceIDs: newRefCountedSet[structured.SharedDeviceID](),
		capacity:        structured.NewConsumedCapacityCollection(),
		contributions:   map[ResourceClaimId]*claimContribution{},
	}
}

// isBuilt reports whether the state has been populated. While it hasn't, updates are
// skipped, because the first full pass picks them up anyway.
func (t *allocatedStateTracker) isBuilt() bool {
	return t.built
}

// ensureBuilt populates the state from a full pass over the claims, unless that has already
// happened. walkClaims has to visit every ResourceClaim effective in the Snapshot.
func (t *allocatedStateTracker) ensureBuilt(walkClaims func(func(*resourceapi.ResourceClaim) bool)) {
	if t.built {
		return
	}

	// Mark as built first - apply is a no-op until it is.
	t.built = true
	walkClaims(func(claim *resourceapi.ResourceClaim) bool {
		t.apply(GetClaimId(claim), claim)
		return true
	})
}

// apply records the claim as the current contribution for claimId, withdrawing whatever the
// claim contributed before. A nil claim only withdraws.
func (t *allocatedStateTracker) apply(claimId ResourceClaimId, claim *resourceapi.ResourceClaim) {
	if !t.built {
		return
	}

	t.withdraw(claimId)
	if claim == nil {
		return
	}

	// Both device set variants are maintained, because which one a caller wants depends on
	// the DRAConsumableCapacity feature gate, and that can be flipped after the Snapshot was
	// created. Deriving them in one pass keeps the DeviceID interning to a single call.
	contribution := &claimContribution{}
	forEachAllocatedResult(claim, func(deviceID structured.DeviceID, result *resourceapi.DeviceRequestAllocationResult) {
		if result.ShareID != nil {
			contribution.sharedDevices = append(contribution.sharedDevices, deviceID)
			contribution.sharedDeviceIDs = append(contribution.sharedDeviceIDs, structured.MakeSharedDeviceID(deviceID, result.ShareID))
			if result.ConsumedCapacity != nil {
				contribution.capacity = append(contribution.capacity,
					ownedDeviceConsumedCapacity(deviceID, result.ConsumedCapacity))
			}
			return
		}

		contribution.devices = append(contribution.devices, deviceID)
	})

	if contribution.isEmpty() {
		// Unallocated claims are the common case - don't keep an entry for them.
		return
	}

	for _, deviceID := range contribution.devices {
		t.devices.insert(deviceID)
		t.allDevices.insert(deviceID)
	}
	for _, deviceID := range contribution.sharedDevices {
		t.allDevices.insert(deviceID)
	}
	for _, sharedDeviceID := range contribution.sharedDeviceIDs {
		t.sharedDeviceIDs.insert(sharedDeviceID)
	}
	for _, deviceCapacity := range contribution.capacity {
		t.capacity.Insert(deviceCapacity)
	}
	t.contributions[claimId] = contribution
}

// ownedDeviceConsumedCapacity is structured.NewDeviceConsumedCapacity with a deep copy.
//
// The contribution outlives the call that builds it, and withdraw has to subtract exactly
// what apply added, so it needs capacity values the claim cannot reach. The upstream
// constructor does not give that: it stores &quantity of its range variable, which copies
// the resource.Quantity struct but not the *inf.Dec a quantity carries when ParseQuantity
// could not land it in the scaled-int64 form. It therefore aliases the source for exactly
// those values and copies the rest, which is the awkward half of the two. Since the Snapshot
// rewrites claims in place, an aliased contribution would let a later update move a number
// already recorded against it, and the aggregate is never recomputed.
//
// Which values those are is not obvious, and the cost here follows the same split, so it is
// worth stating: DeepCopy is a plain struct copy when the quantity is in the int64 form and
// allocates a fresh inf.Dec when it is not. It is not a question of magnitude - "1e20" is in
// the int64 form and free, while "9223372036854775807" is not and allocates. The case that
// matters for DRA is that ANY fractional binary-suffixed value goes to inf.Dec, including
// "0.5Gi", which is a whole number of bytes. Consumed capacity written as a fraction of a
// device - the natural way to express a partial GPU - therefore pays the allocation, while
// "4Gi", "512Mi" and "1Ti" do not. Measured at +1.6%..+4.8% time and +8.7%..+12.5% allocs on
// the write path for the inf.Dec case, and no measurable difference for int64.
//
// Deliberately not NewDeviceConsumedCapacity(...).Clone(). That reaches the same result by
// allocating a map of pointers and immediately discarding it for a second one, measured at
// ~13% slower and ~23% more allocations on this path than the single pass below.
//
// This does duplicate the shape of the upstream constructor. The better fix is for
// schedulerapi.NewDeviceConsumedCapacity to stop aliasing, which would remove both the
// duplication here and the trap for every other caller - schedulerapi is declared a
// scheduler/autoscaler contract, so that is a change Cluster Autoscaler can propose.
func ownedDeviceConsumedCapacity(deviceID structured.DeviceID, capacity map[resourceapi.QualifiedName]resource.Quantity) structured.DeviceConsumedCapacity {
	owned := make(structured.ConsumedCapacity, len(capacity))
	for name, quantity := range capacity {
		q := quantity.DeepCopy()
		owned[name] = &q
	}
	return structured.DeviceConsumedCapacity{DeviceID: deviceID, ConsumedCapacity: owned}
}

// withdraw removes whatever the claim with the given id currently contributes.
func (t *allocatedStateTracker) withdraw(claimId ResourceClaimId) {
	if !t.built {
		return
	}

	contribution, found := t.contributions[claimId]
	if !found {
		return
	}

	for _, deviceID := range contribution.devices {
		t.devices.remove(deviceID)
		t.allDevices.remove(deviceID)
	}
	for _, deviceID := range contribution.sharedDevices {
		t.allDevices.remove(deviceID)
	}
	for _, sharedDeviceID := range contribution.sharedDeviceIDs {
		t.sharedDeviceIDs.remove(sharedDeviceID)
	}
	for _, deviceCapacity := range contribution.capacity {
		t.capacity.Remove(deviceCapacity)
	}
	delete(t.contributions, claimId)
}

// refresh re-reads the given claims and updates their contributions. It is used after a
// Revert, where the effective value of the reverted layer's keys changes without any
// individual write going through the tracker. lookup returns the claim now effective for an
// id, or false if there is none.
func (t *allocatedStateTracker) refresh(claimIds []ResourceClaimId, lookup func(ResourceClaimId) (*resourceapi.ResourceClaim, bool)) {
	if !t.built {
		return
	}

	for _, claimId := range claimIds {
		claim, found := lookup(claimId)
		if !found {
			t.withdraw(claimId)
			continue
		}
		t.apply(claimId, claim)
	}
}

// allocatedState returns the tracked state. The returned collections are the tracker's own
// and must not be modified - see snapshotClaimTracker.GatherAllocatedState.
func (t *allocatedStateTracker) allocatedState() *structured.AllocatedState {
	return &structured.AllocatedState{
		AllocatedDevices:         t.devices.set,
		AllocatedSharedDeviceIDs: t.sharedDeviceIDs.set,
		AggregatedCapacity:       t.capacity,
	}
}

func (c *claimContribution) isEmpty() bool {
	return len(c.devices) == 0 && len(c.sharedDevices) == 0 && len(c.sharedDeviceIDs) == 0 && len(c.capacity) == 0
}

// refCountedSet is a set that only drops an element once every insert of it has been removed.
// The set itself is kept materialized so that it can be handed to the scheduler without
// copying.
type refCountedSet[T comparable] struct {
	set sets.Set[T]
	// refCounts only holds entries inserted more than once, which should not normally
	// happen - a device is expected to be consumed by a single claim.
	refCounts map[T]int
}

func newRefCountedSet[T comparable]() refCountedSet[T] {
	return refCountedSet[T]{set: sets.New[T](), refCounts: map[T]int{}}
}

func (s refCountedSet[T]) insert(item T) {
	if s.set.Has(item) {
		s.refCounts[item]++
		return
	}
	s.set.Insert(item)
}

func (s refCountedSet[T]) remove(item T) {
	if count := s.refCounts[item]; count > 0 {
		if count == 1 {
			delete(s.refCounts, item)
		} else {
			s.refCounts[item] = count - 1
		}
		return
	}
	s.set.Delete(item)
}
