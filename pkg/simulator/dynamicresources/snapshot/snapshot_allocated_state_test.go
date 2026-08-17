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
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"

	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuretesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
	drautils "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/utils"

	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// referenceAllocatedState recomputes the allocated state from scratch, the way
// GatherAllocatedState used to before the state was maintained incrementally. The
// incremental tracker has to agree with this at all times.
func referenceAllocatedState(snapshot *Snapshot, enabledConsumableCapacity bool) *structured.AllocatedState {
	devices := sets.New[structured.DeviceID]()
	sharedDeviceIDs := sets.New[structured.SharedDeviceID]()
	capacity := structured.NewConsumedCapacityCollection()

	snapshot.walkResourceClaims(func(claim *resourceapi.ResourceClaim) bool {
		foreachAllocatedDevice(claim,
			func(deviceID structured.DeviceID) { devices.Insert(deviceID) },
			enabledConsumableCapacity,
			func(sharedDeviceID structured.SharedDeviceID) { sharedDeviceIDs.Insert(sharedDeviceID) },
			func(deviceCapacity structured.DeviceConsumedCapacity) { capacity.Insert(deviceCapacity) },
		)
		return true
	})

	return &structured.AllocatedState{
		AllocatedDevices:         devices,
		AllocatedSharedDeviceIDs: sharedDeviceIDs,
		AggregatedCapacity:       capacity,
	}
}

func assertStateMatchesReference(t *testing.T, snapshot *Snapshot, step string) {
	t.Helper()

	enabledConsumableCapacity := utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity)

	got, err := snapshot.ResourceClaims().GatherAllocatedState()
	if err != nil {
		t.Fatalf("%s: GatherAllocatedState(): unexpected error: %v", step, err)
	}
	want := referenceAllocatedState(snapshot, enabledConsumableCapacity)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("%s: GatherAllocatedState(): unexpected output (-want +got): %s", step, diff)
	}

	gotDevices, err := snapshot.ResourceClaims().ListAllAllocatedDevices()
	if err != nil {
		t.Fatalf("%s: ListAllAllocatedDevices(): unexpected error: %v", step, err)
	}
	wantDevices := referenceAllocatedState(snapshot, false).AllocatedDevices
	if diff := cmp.Diff(wantDevices, gotDevices); diff != "" {
		t.Fatalf("%s: ListAllAllocatedDevices(): unexpected output (-want +got): %s", step, diff)
	}
}

func allocationForDevices(devices ...resourceapi.DeviceRequestAllocationResult) *resourceapi.AllocationResult {
	// Only Devices.Results matters here - that is all the allocated state is derived from.
	return &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{Results: devices},
	}
}

func dedicatedResult(driver, pool, device string) resourceapi.DeviceRequestAllocationResult {
	return resourceapi.DeviceRequestAllocationResult{Request: "req", Driver: driver, Pool: pool, Device: device}
}

func sharedResult(driver, pool, device, shareID string, capacity int64) resourceapi.DeviceRequestAllocationResult {
	result := dedicatedResult(driver, pool, device)
	result.ShareID = ptr.To(types.UID(shareID))
	if capacity > 0 {
		result.ConsumedCapacity = map[resourceapi.QualifiedName]resource.Quantity{
			"memory": *resource.NewQuantity(capacity, resource.DecimalSI),
		}
	}
	return result
}

func allocatedStateTestClaim(namespace, name string, allocation *resourceapi.AllocationResult) *resourceapi.ResourceClaim {
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID(namespace + "/" + name)},
	}
	if allocation != nil {
		claim = drautils.TestClaimWithAllocation(claim, allocation)
	}
	return claim
}

// TestAllocatedStateTrackerMatchesReference walks the tracker through the individual
// operations that change ResourceClaims, checking against a from-scratch recomputation after
// every one of them.
func TestAllocatedStateTrackerMatchesReference(t *testing.T) {
	for _, consumableCapacity := range []bool{true, false} {
		t.Run(fmt.Sprintf("DRAConsumableCapacity=%v", consumableCapacity), func(t *testing.T) {
			featuretesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAConsumableCapacity, consumableCapacity)

			allocated := allocatedStateTestClaim("default", "allocated", allocationForDevices(dedicatedResult("driver", "pool", "dev-1")))
			shared := allocatedStateTestClaim("default", "shared", allocationForDevices(
				sharedResult("driver", "pool", "dev-2", "share-a", 10),
				sharedResult("driver", "pool", "dev-2", "share-b", 5),
			))
			unallocated := allocatedStateTestClaim("default", "unallocated", nil)

			snapshot := NewSnapshot(
				map[ResourceClaimId]*resourceapi.ResourceClaim{
					GetClaimId(allocated): allocated,
					GetClaimId(shared):    shared,
				}, nil, nil, nil)
			assertStateMatchesReference(t, snapshot, "initial")

			// Adding an unallocated claim contributes nothing.
			if err := snapshot.AddClaims([]*resourceapi.ResourceClaim{unallocated}); err != nil {
				t.Fatalf("AddClaims(): %v", err)
			}
			assertStateMatchesReference(t, snapshot, "after adding unallocated claim")

			// Adding an allocated claim contributes its devices.
			extra := allocatedStateTestClaim("default", "extra", allocationForDevices(dedicatedResult("driver", "pool", "dev-3")))
			if err := snapshot.AddClaims([]*resourceapi.ResourceClaim{extra}); err != nil {
				t.Fatalf("AddClaims(): %v", err)
			}
			assertStateMatchesReference(t, snapshot, "after adding allocated claim")

			// Allocating a previously unallocated claim.
			snapshot.Fork()
			nowAllocated := allocatedStateTestClaim("default", "unallocated", allocationForDevices(dedicatedResult("driver", "pool", "dev-4")))
			snapshot.configureResourceClaim(nowAllocated)
			assertStateMatchesReference(t, snapshot, "after allocating in a fork")

			// Reverting has to bring back the unallocated version.
			snapshot.Revert()
			assertStateMatchesReference(t, snapshot, "after reverting the allocation")

			// The same allocation, committed this time.
			snapshot.Fork()
			snapshot.configureResourceClaim(nowAllocated)
			snapshot.Commit()
			assertStateMatchesReference(t, snapshot, "after committing the allocation")

			// Deleting a claim withdraws its devices.
			snapshot.Fork()
			snapshot.deleteResourceClaim(GetClaimId(extra))
			assertStateMatchesReference(t, snapshot, "after deleting a claim")

			snapshot.Revert()
			assertStateMatchesReference(t, snapshot, "after reverting the deletion")
		})
	}
}

// TestAllocatedStateTrackerRandomizedOperations is the main guard on the incremental
// bookkeeping: it drives a Snapshot through a long random sequence of the operations that
// mutate ResourceClaims, and after every step asserts the maintained state still equals a
// from-scratch recomputation. Pod reservations are included because they rewrite claims in
// place, which is exactly the case the tracker's contribution bookkeeping exists for.
func TestAllocatedStateTrackerRandomizedOperations(t *testing.T) {
	for _, consumableCapacity := range []bool{true, false} {
		t.Run(fmt.Sprintf("DRAConsumableCapacity=%v", consumableCapacity), func(t *testing.T) {
			featuretesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAConsumableCapacity, consumableCapacity)

			const claimCount = 12
			const steps = 400
			// Fixed seed - the sequence has to be reproducible when it finds a problem.
			random := rand.New(rand.NewSource(1))

			claimIds := make([]ResourceClaimId, 0, claimCount)
			initial := map[ResourceClaimId]*resourceapi.ResourceClaim{}
			pods := make([]*apiv1.Pod, 0, claimCount)
			for i := 0; i < claimCount; i++ {
				claim := allocatedStateTestClaim("default", fmt.Sprintf("claim-%d", i), nil)
				pod := BuildTestPod(fmt.Sprintf("pod-%d", i), 1, 1, WithResourceClaim(claim.Name, claim.Name, ""))
				claim = drautils.TestClaimWithPodOwnership(pod, claim)
				initial[GetClaimId(claim)] = claim
				claimIds = append(claimIds, GetClaimId(claim))
				pods = append(pods, pod)
			}

			snapshot := NewSnapshot(initial, nil, nil, nil)
			assertStateMatchesReference(t, snapshot, "initial")

			depth := 0
			for step := 0; step < steps; step++ {
				index := random.Intn(claimCount)
				claimId := claimIds[index]
				pod := pods[index]

				switch random.Intn(8) {
				case 0: // allocate a dedicated device
					claim, found := snapshot.getResourceClaim(claimId)
					if !found {
						break
					}
					updated := drautils.TestClaimWithAllocation(claim.DeepCopy(), allocationForDevices(
						dedicatedResult("driver", fmt.Sprintf("pool-%d", random.Intn(3)), fmt.Sprintf("dev-%d", random.Intn(5))),
					))
					snapshot.configureResourceClaim(updated)
				case 1: // allocate shared devices with consumed capacity
					claim, found := snapshot.getResourceClaim(claimId)
					if !found {
						break
					}
					updated := drautils.TestClaimWithAllocation(claim.DeepCopy(), allocationForDevices(
						sharedResult("driver", "pool-shared", fmt.Sprintf("dev-%d", random.Intn(3)), fmt.Sprintf("share-%d", random.Intn(3)), int64(random.Intn(10)+1)),
					))
					snapshot.configureResourceClaim(updated)
				case 2: // reserve for the owning pod - rewrites the claim in place
					if err := snapshot.ReservePodClaims(pod); err != nil {
						// Expected when the claim isn't allocated or is gone.
						break
					}
				case 3: // unreserve - can deallocate the claim in place
					if err := snapshot.UnreservePodClaims(pod); err != nil {
						break
					}
				case 4: // remove the pod-owned claim
					snapshot.RemovePodOwnedClaims(context.Background(), pod)
				case 5: // re-add a removed claim
					if _, found := snapshot.getResourceClaim(claimId); found {
						break
					}
					readded := allocatedStateTestClaim(claimId.Namespace, claimId.Name, nil)
					readded = drautils.TestClaimWithPodOwnership(pod, readded)
					if err := snapshot.AddClaims([]*resourceapi.ResourceClaim{readded}); err != nil {
						t.Fatalf("step %d: AddClaims(): %v", step, err)
					}
				case 6: // fork
					snapshot.Fork()
					depth++
				case 7: // commit or revert
					if depth == 0 {
						break
					}
					depth--
					if random.Intn(2) == 0 {
						snapshot.Commit()
					} else {
						snapshot.Revert()
					}
				}

				assertStateMatchesReference(t, snapshot, fmt.Sprintf("step %d", step))
			}
		})
	}
}

// TestAllocatedStateTrackerBuildsLazily checks that mutations applied before the state is
// ever read are still reflected, since the tracker skips updates until its first full pass.
func TestAllocatedStateTrackerBuildsLazily(t *testing.T) {
	snapshot := NewSnapshot(nil, nil, nil, nil)

	claim := allocatedStateTestClaim("default", "claim", allocationForDevices(dedicatedResult("driver", "pool", "dev-1")))
	if err := snapshot.AddClaims([]*resourceapi.ResourceClaim{claim}); err != nil {
		t.Fatalf("AddClaims(): %v", err)
	}

	// First read of the state - has to pick up the claim added before any read.
	assertStateMatchesReference(t, snapshot, "after first read")

	state, err := snapshot.ResourceClaims().GatherAllocatedState()
	if err != nil {
		t.Fatalf("GatherAllocatedState(): %v", err)
	}
	if want := structured.MakeDeviceID("driver", "pool", "dev-1"); !state.AllocatedDevices.Has(want) {
		t.Errorf("GatherAllocatedState(): device %v missing from %v", want, state.AllocatedDevices)
	}
}

// TestAllocatedStateTrackerDuplicateDevice checks the reference counting: a device claimed by
// two claims only leaves the state once both are gone.
func TestAllocatedStateTrackerDuplicateDevice(t *testing.T) {
	first := allocatedStateTestClaim("default", "first", allocationForDevices(dedicatedResult("driver", "pool", "dev-1")))
	second := allocatedStateTestClaim("default", "second", allocationForDevices(dedicatedResult("driver", "pool", "dev-1")))
	snapshot := NewSnapshot(map[ResourceClaimId]*resourceapi.ResourceClaim{
		GetClaimId(first):  first,
		GetClaimId(second): second,
	}, nil, nil, nil)

	deviceID := structured.MakeDeviceID("driver", "pool", "dev-1")
	state, err := snapshot.ResourceClaims().GatherAllocatedState()
	if err != nil {
		t.Fatalf("GatherAllocatedState(): %v", err)
	}
	if !state.AllocatedDevices.Has(deviceID) {
		t.Fatalf("device %v should be allocated", deviceID)
	}

	snapshot.deleteResourceClaim(GetClaimId(first))
	if !state.AllocatedDevices.Has(deviceID) {
		t.Errorf("device %v should still be allocated while the second claim holds it", deviceID)
	}

	snapshot.deleteResourceClaim(GetClaimId(second))
	if state.AllocatedDevices.Has(deviceID) {
		t.Errorf("device %v should be released once no claim holds it", deviceID)
	}
}

// TestAllocatedStateTrackerAliasedClaim covers a Pod that reaches the same ResourceClaim
// through two names. Every claim write then hits the tracker twice for one claimId, and on
// the second hit Snapshot.ensureClaimWritable hands back the already-stored object, so the
// tracker cannot recover the previous allocation from the claim itself. Recording each
// claim's contribution is what keeps the refcounts balanced here, and a stray increment
// would strand the device in the state after the claim goes away.
//
// This is the shape reported in kubernetes-sigs/cluster-autoscaler#33. The base-layer leak
// described there is a Snapshot bug and is deliberately not asserted on; what this pins down
// is that the derived state stays in agreement with the claims either way.
func TestAllocatedStateTrackerAliasedClaim(t *testing.T) {
	pod := BuildTestPod("alias-pod", 1, 1,
		WithResourceClaim("first", "shared-claim", ""),
		WithResourceClaim("second", "shared-claim", ""),
	)
	claim := allocatedStateTestClaim("default", "shared-claim", allocationForDevices(dedicatedResult("driver", "pool", "dev-1")))
	snapshot := NewSnapshot(map[ResourceClaimId]*resourceapi.ResourceClaim{GetClaimId(claim): claim}, nil, nil, nil)

	// Build the state before forking, so the writes below actually reach the tracker.
	assertStateMatchesReference(t, snapshot, "initial")

	snapshot.Fork()
	if err := snapshot.ReservePodClaims(pod); err != nil {
		t.Fatalf("ReservePodClaims(): %v", err)
	}
	assertStateMatchesReference(t, snapshot, "after ReservePodClaims")

	if err := snapshot.UnreservePodClaims(pod); err != nil {
		t.Fatalf("UnreservePodClaims(): %v", err)
	}
	assertStateMatchesReference(t, snapshot, "after UnreservePodClaims")

	snapshot.Revert()
	assertStateMatchesReference(t, snapshot, "after Revert")

	// The device was reached through two aliases, so a leaked refcount would keep it in the
	// state past the removal of the only claim holding it.
	snapshot.deleteResourceClaim(GetClaimId(claim))
	assertStateMatchesReference(t, snapshot, "after delete")

	state, err := snapshot.ResourceClaims().GatherAllocatedState()
	if err != nil {
		t.Fatalf("GatherAllocatedState(): %v", err)
	}
	if deviceID := structured.MakeDeviceID("driver", "pool", "dev-1"); state.AllocatedDevices.Has(deviceID) {
		t.Errorf("device %v should be released once the aliased claim is gone", deviceID)
	}
}
