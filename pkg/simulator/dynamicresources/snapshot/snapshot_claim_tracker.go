/*
Copyright 2024 The Kubernetes Authors.

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
	"fmt"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/kubernetes/pkg/features"
)

type snapshotClaimTracker struct {
	snapshot *Snapshot
}

func (ct snapshotClaimTracker) List() ([]*resourceapi.ResourceClaim, error) {
	return ct.snapshot.listResourceClaims(), nil
}

func (ct snapshotClaimTracker) Get(namespace, claimName string) (*resourceapi.ResourceClaim, error) {
	claimId := ResourceClaimId{Name: claimName, Namespace: namespace}
	claim, found := ct.snapshot.getResourceClaim(claimId)
	if !found {
		return nil, fmt.Errorf("claim %s/%s not found", namespace, claimName)
	}
	return claim, nil
}

// Borrowed collections
//
// ListAllAllocatedDevices and GatherAllocatedState hand back the snapshot's own live
// collections instead of copies. Copying them would put an O(allocated devices) walk back on
// a path the scheduler takes once per pod placement attempt, which is the cost this whole
// mechanism exists to remove, so the borrow is deliberate. It comes with two conditions on
// the caller:
//
//   - Read only. Writing through a returned collection corrupts the snapshot's derived
//     state, and nothing recomputes it.
//   - Valid until the next write. Any change to the snapshot's ResourceClaims - including
//     Fork, Commit and Revert - may mutate the collection in place. Callers that need a
//     value across a write have to call again afterwards, or copy it themselves.
//
// fwk.ResourceClaimTracker states neither condition, so this is a contract Cluster
// Autoscaler adds on top of the interface rather than one the interface grants. That is only
// sound because every caller is known and checked:
//
//   - k8s.io/dynamic-resource-allocation/structured, which is where the DRA scheduler plugin
//     sends the result. All three allocator variants (stable, incubating, experimental) and
//     structured.IsDeviceAllocated only ever read - Set.Has, len, range, map index.
//   - Cluster Autoscaler's own code, which does not call either method outside tests.
//
// TestSnapshotClaimTrackerBorrowedCollectionsAreReadOnly pins the first bullet down against
// the real allocator, so a Kubernetes bump that starts writing through the alias fails there
// instead of silently skewing the snapshot.
//
// Be aware of what that leaves standing. Nothing detects a violation at runtime: a consumer
// that writes through one of these collections corrupts the snapshot's derived state
// silently and permanently, because nothing recomputes it. The test above covers the
// consumer that exists today, so a violation should surface as a test failure on a
// Kubernetes bump rather than in a cluster - but that is the whole of the protection, and it
// only covers callers the test drives. A cheap runtime check is not available either: the
// collections are maps, so the affordable signal is their size, and that misses both a
// balanced insert-and-delete and a resource.Quantity edited in place inside an
// AggregatedCapacity entry that already exists. Anything that caught those would need a deep
// comparison on every write, which costs exactly what handing the collections out saves.
//
// If a future caller needs these to be safe rather than merely known-safe, the fix is to
// state the contract in k8s.io/dynamic-resource-allocation/structured/schedulerapi, whose
// types are declared a scheduler/autoscaler contract that Cluster Autoscaler has approval
// rights over, rather than to add a partial guard here.

// ListAllAllocatedDevices returns every device consumed by the ResourceClaims in the
// snapshot. The returned set is borrowed - see "Borrowed collections" above.
func (ct snapshotClaimTracker) ListAllAllocatedDevices() (sets.Set[structured.DeviceID], error) {
	tracker := ct.snapshot.allocationTracker()
	tracker.ensureBuilt(ct.snapshot.walkResourceClaims)
	return tracker.allDevices.set, nil
}

// GatherAllocatedState returns the state of all devices consumed by the ResourceClaims in
// the snapshot. The collections behind the returned AllocatedState are borrowed - see
// "Borrowed collections" above.
//
// The scheduler calls this once per PreFilter, that is once per pod scheduling attempt, so
// the state is maintained as ResourceClaims change rather than recomputed here.
func (ct snapshotClaimTracker) GatherAllocatedState() (*structured.AllocatedState, error) {
	tracker := ct.snapshot.allocationTracker()
	tracker.ensureBuilt(ct.snapshot.walkResourceClaims)

	// The AllocatedState wrapper is built fresh on every call, so the fields below can be
	// reassigned without disturbing the tracker or an earlier caller's copy of the struct.
	state := tracker.allocatedState()
	if !utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity) {
		// With the feature off, shared devices are reported as ordinary allocated ones and
		// sharing is not reported at all. The two empty collections are freshly allocated
		// rather than borrowed, but callers should not rely on that distinction.
		state.AllocatedDevices = tracker.allDevices.set
		state.AllocatedSharedDeviceIDs = sets.New[structured.SharedDeviceID]()
		state.AggregatedCapacity = structured.NewConsumedCapacityCollection()
	}
	return state, nil
}

func (ct snapshotClaimTracker) SignalClaimPendingAllocation(claimUid types.UID, allocatedClaim *resourceapi.ResourceClaim) error {
	// The DRA scheduler plugin calls this at the end of the scheduling phase, in Reserve. Then, the allocation is persisted via an API
	// call during the binding phase.
	//
	// In Cluster Autoscaler only the scheduling phase is run, so SignalClaimPendingAllocation() is used to obtain the allocation
	// and persist it in-memory in the snapshot.
	claimId := ResourceClaimId{Name: allocatedClaim.Name, Namespace: allocatedClaim.Namespace}
	claim, found := ct.snapshot.getResourceClaim(claimId)
	if !found {
		return fmt.Errorf("claim %s/%s not found", allocatedClaim.Namespace, allocatedClaim.Name)
	}
	if claim.UID != claimUid {
		return fmt.Errorf("claim %s/%s: snapshot has UID %q, allocation came for UID %q - shouldn't happenn", allocatedClaim.Namespace, allocatedClaim.Name, claim.UID, claimUid)
	}

	ct.snapshot.configureResourceClaim(allocatedClaim)
	return nil
}

func (ct snapshotClaimTracker) GetPendingAllocation(claimUid types.UID) *resourceapi.AllocationResult {
	// The DRA scheduler plugin calls this at the beginning of Filter, and fails the filter if true is returned to handle race conditions.
	//
	// In the scheduler implementation, GetPendingAllocation() starts returning the allocation result after SignalClaimPendingAllocation()
	// is called at the end of the scheduling phase, until RemoveClaimPendingAllocation() is called after the allocation API call
	// is made in the asynchronous bind phase.
	//
	// In Cluster Autoscaler only the scheduling phase is run, and SignalClaimPendingAllocation() synchronously persists the allocation
	// in-memory. So the race conditions don't apply, and this should always return nil not to block the filter.
	return nil
}

func (ct snapshotClaimTracker) MaybeRemoveClaimPendingAllocation(claimUID types.UID, forceRemove bool) (deleted bool) {
	// This method is only called during the Bind phase of scheduler framework, which is never run by CA. We need to implement
	// it to satisfy the interface, but it should never be called.
	panic("snapshotClaimTracker.MaybeRemoveClaimPendingAllocation() was called - this should never happen")
}

func (ct snapshotClaimTracker) AssumeClaimAfterAPICall(claim *resourceapi.ResourceClaim) error {
	// This method is only called during the Bind phase of scheduler framework, which is never run by CA. We need to implement
	// it to satisfy the interface, but it should never be called.
	panic("snapshotClaimTracker.AssumeClaimAfterAPICall() was called - this should never happen")
}

func (ct snapshotClaimTracker) AssumedClaimRestore(namespace, claimName string) {
	// This method is only called during the Bind phase of scheduler framework, which is never run by CA. We need to implement
	// it to satisfy the interface, but it should never be called.
	panic("snapshotClaimTracker.AssumedClaimRestore() was called - this should never happen")
}

// forEachAllocatedResult invokes the provided callback for each allocation result in the
// claim that counts as consuming its device, along with the DeviceID derived from it.
//
// This replaces foreachAllocatedDevice, which was a fork of the corresponding scheduler
// logic. Splitting the devices between dedicated and shared now happens in
// allocatedStateTracker.apply, which is the only place that needs it.
func forEachAllocatedResult(claim *resourceapi.ResourceClaim, callback func(structured.DeviceID, *resourceapi.DeviceRequestAllocationResult)) {
	if claim.Status.Allocation == nil {
		return
	}
	for resultIndex := range claim.Status.Allocation.Devices.Results {
		result := &claim.Status.Allocation.Devices.Results[resultIndex]
		callback(structured.MakeDeviceID(result.Driver, result.Pool, result.Device), result)
	}
}
