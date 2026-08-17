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

// ListAllAllocatedDevices returns every device consumed by the ResourceClaims in the
// snapshot.
//
// The returned set is the snapshot's own rather than a copy: it must not be modified, and
// it is only valid until the next change to the snapshot's ResourceClaims. Handing it over
// directly is what keeps the per-attempt walk over every claim off this path. The scheduler
// honours that - the DRA plugin folds the set into a structured.AllocatedState and passes it
// to structured.NewAllocator, which only ever reads it (Set.Has, range, map lookup).
func (ct snapshotClaimTracker) ListAllAllocatedDevices() (sets.Set[structured.DeviceID], error) {
	tracker := ct.snapshot.allocationTracker()
	tracker.ensureBuilt(ct.snapshot.walkResourceClaims)
	return tracker.allDevices.set, nil
}

// GatherAllocatedState returns the state of all devices consumed by the ResourceClaims in
// the snapshot.
//
// The scheduler calls this once per PreFilter, that is once per pod scheduling attempt, so
// the state is maintained as ResourceClaims change rather than recomputed here.
//
// The returned collections are the snapshot's own rather than copies: they must not be
// modified, and they are only valid until the next change to the snapshot's ResourceClaims.
// The DRA scheduler plugin dereferences the AllocatedState into structured.NewAllocator, and
// the allocator only ever reads the collections (Set.Has, range, map lookup), so nothing
// downstream writes through the alias. The AllocatedState wrapper itself is built fresh on
// every call, so reassigning its fields below is safe.
func (ct snapshotClaimTracker) GatherAllocatedState() (*structured.AllocatedState, error) {
	tracker := ct.snapshot.allocationTracker()
	tracker.ensureBuilt(ct.snapshot.walkResourceClaims)

	state := tracker.allocatedState()
	if !utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity) {
		// Without the feature the shared devices are reported as ordinary allocated ones,
		// matching what foreachAllocatedDevice does when told sharing is disabled.
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

// foreachAllocatedDevice invokes the provided callback for each
// device in the claim's allocation result which was allocated
// exclusively for the claim.
//
// This method is a fork of a corresponding scheduler logic
func foreachAllocatedDevice(claim *resourceapi.ResourceClaim,
	dedicatedDeviceCallback func(deviceID structured.DeviceID),
	enabledConsumableCapacity bool,
	sharedDeviceCallback func(structured.SharedDeviceID),
	consumedCapacityCallback func(structured.DeviceConsumedCapacity)) {
	forEachAllocatedResult(claim, func(deviceID structured.DeviceID, result *resourceapi.DeviceRequestAllocationResult) {

		// Execute sharedDeviceCallback and consumedCapacityCallback correspondingly
		// if DRAConsumableCapacity feature is enabled
		if enabledConsumableCapacity {
			shared := result.ShareID != nil
			if shared {
				sharedDeviceID := structured.MakeSharedDeviceID(deviceID, result.ShareID)
				sharedDeviceCallback(sharedDeviceID)
				if result.ConsumedCapacity != nil {
					deviceConsumedCapacity := structured.NewDeviceConsumedCapacity(deviceID, result.ConsumedCapacity)
					consumedCapacityCallback(deviceConsumedCapacity)
				}
				return
			}
		}

		// Otherwise, execute dedicatedDeviceCallback
		dedicatedDeviceCallback(deviceID)
	})
}

// forEachAllocatedResult invokes the provided callback for each allocation result in the
// claim that counts as consuming its device, along with the DeviceID derived from it.
//
// This is the single place where an allocation result is turned into a DeviceID, so that
// foreachAllocatedDevice and allocatedStateTracker cannot drift apart on which devices count
// as allocated.
func forEachAllocatedResult(claim *resourceapi.ResourceClaim, callback func(structured.DeviceID, *resourceapi.DeviceRequestAllocationResult)) {
	if claim.Status.Allocation == nil {
		return
	}
	for resultIndex := range claim.Status.Allocation.Devices.Results {
		result := &claim.Status.Allocation.Devices.Results[resultIndex]
		callback(structured.MakeDeviceID(result.Driver, result.Pool, result.Device), result)
	}
}
