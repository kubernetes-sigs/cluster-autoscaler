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
	"maps"
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
//
// It deliberately shares no traversal code with the implementation it checks: it walks the
// allocation results itself rather than going through forEachAllocatedResult, and lists the
// claims with listResourceClaims rather than the Snapshot.walkResourceClaims wrapper the
// tracker builds itself from. Both of those are part of what is under test here, and a
// reference that called them would agree with a broken tracker about a skipped or
// double-counted result.
func referenceAllocatedState(snapshot *Snapshot, enabledConsumableCapacity bool) *structured.AllocatedState {
	devices := sets.New[structured.DeviceID]()
	sharedDeviceIDs := sets.New[structured.SharedDeviceID]()
	capacity := structured.NewConsumedCapacityCollection()

	for _, claim := range snapshot.listResourceClaims() {
		if claim.Status.Allocation == nil {
			continue
		}
		for _, result := range claim.Status.Allocation.Devices.Results {
			deviceID := structured.MakeDeviceID(result.Driver, result.Pool, result.Device)
			if enabledConsumableCapacity && result.ShareID != nil {
				sharedDeviceIDs.Insert(structured.MakeSharedDeviceID(deviceID, result.ShareID))
				if result.ConsumedCapacity != nil {
					capacity.Insert(structured.NewDeviceConsumedCapacity(deviceID, result.ConsumedCapacity))
				}
				continue
			}
			devices.Insert(deviceID)
		}
	}

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

// TestAllocatedStateRevertWithoutForkRefreshesNothing checks that a Revert with no Fork
// under it does not refresh anything, and that a Revert with one still does.
//
// PatchSet.Revert keeps the bottom layer, so at that depth it discards nothing and no
// claim's effective value changes. The topmost layer is then the base layer holding every
// claim in the snapshot, so a refresh driven off it would re-read the whole cluster's worth
// of claims to conclude nothing moved - turning a no-op into the O(claims) walk this
// mechanism exists to avoid.
//
// A refresh replaces a claim's contribution with a freshly allocated one, so contribution
// identity is an exact witness for whether a claim was re-read. That lets this drive the
// real Snapshot.Revert rather than a copy of its logic, which a guard in the wrong place
// would otherwise satisfy.
func TestAllocatedStateRevertWithoutForkRefreshesNothing(t *testing.T) {
	newSnapshotWithClaims := func(t *testing.T) *Snapshot {
		t.Helper()
		claims := map[ResourceClaimId]*resourceapi.ResourceClaim{}
		for i := 0; i < 3; i++ {
			claim := allocatedStateTestClaim("default", fmt.Sprintf("claim-%d", i),
				allocationForDevices(dedicatedResult("driver", "pool", fmt.Sprintf("dev-%d", i))))
			claims[GetClaimId(claim)] = claim
		}
		snapshot := NewSnapshot(claims, nil, nil, nil)
		// Build the state, so the contributions below exist.
		assertStateMatchesReference(t, snapshot, "initial")
		if got := len(snapshot.allocatedState.contributions); got != 3 {
			t.Fatalf("expected a contribution per claim, got %d", got)
		}
		return snapshot
	}

	refreshedClaims := func(before map[ResourceClaimId]*claimContribution, snapshot *Snapshot) []ResourceClaimId {
		var refreshed []ResourceClaimId
		for claimId, contribution := range before {
			if snapshot.allocatedState.contributions[claimId] != contribution {
				refreshed = append(refreshed, claimId)
			}
		}
		return refreshed
	}

	t.Run("withoutFork", func(t *testing.T) {
		snapshot := newSnapshotWithClaims(t)
		before := maps.Clone(snapshot.allocatedState.contributions)

		snapshot.Revert()

		if refreshed := refreshedClaims(before, snapshot); len(refreshed) != 0 {
			t.Errorf("Revert with no Fork under it re-read %d claims (%v), want none", len(refreshed), refreshed)
		}
		assertStateMatchesReference(t, snapshot, "after Revert without Fork")
	})

	// The control: without this, a guard that suppressed every refresh would also pass.
	t.Run("withFork", func(t *testing.T) {
		snapshot := newSnapshotWithClaims(t)
		snapshot.Fork()
		changed := allocatedStateTestClaim("default", "claim-0",
			allocationForDevices(dedicatedResult("driver", "pool", "dev-changed")))
		snapshot.configureResourceClaim(changed)
		before := maps.Clone(snapshot.allocatedState.contributions)

		snapshot.Revert()

		refreshed := refreshedClaims(before, snapshot)
		if len(refreshed) != 1 || refreshed[0] != GetClaimId(changed) {
			t.Errorf("Revert with a Fork under it re-read %v, want just %v", refreshed, GetClaimId(changed))
		}
		assertStateMatchesReference(t, snapshot, "after Revert with Fork")
	})
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

	if want := structured.MakeDeviceID("driver", "pool", "dev-1"); !deviceAllocated(t, snapshot, want) {
		t.Errorf("GatherAllocatedState(): device %v missing from the allocated state", want)
	}
}

// TestAllocatedStateTrackerConsumedCapacityIsCopied checks that a claim's consumed capacity
// is copied into the contribution rather than referenced.
//
// structured.NewDeviceConsumedCapacity only shallow-copies each resource.Quantity out of the
// claim, so the copy still points at the same inf.Dec whenever the quantity is held in
// decimal form - which is what a value too large for int64 gets parsed into. The Snapshot
// rewrites claims in place, so arithmetic reaching that quantity would move the number the
// contribution recorded, and withdraw would then subtract something other than what apply
// added. Nothing recomputes the aggregate, so the skew would be permanent.
func TestAllocatedStateTrackerConsumedCapacityIsCopied(t *testing.T) {
	featuretesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAConsumableCapacity, true)

	// A fractional binary quantity. Any of these lands in resource.Quantity's inf.Dec
	// form rather than its int64 form - that is a property of the fractional binary
	// suffix, not of the magnitude, and it holds for "0.5Gi" too even though that is a
	// whole number of bytes. The inf.Dec sits behind a pointer, and that pointer is what
	// the contribution would otherwise share with the claim.
	const decFormQuantity = "0.1Gi"

	result := dedicatedResult("driver", "pool", "dev-1")
	result.ShareID = ptr.To(types.UID("share-a"))
	result.ConsumedCapacity = map[resourceapi.QualifiedName]resource.Quantity{
		"memory": resource.MustParse(decFormQuantity),
	}
	claim := allocatedStateTestClaim("default", "shared", allocationForDevices(result))

	snapshot := NewSnapshot(map[ResourceClaimId]*resourceapi.ResourceClaim{GetClaimId(claim): claim}, nil, nil, nil)
	// Build the state, so the claim's capacity is recorded as a contribution.
	assertStateMatchesReference(t, snapshot, "initial")

	// Mutate the claim's quantity in place, the way an update rewriting the claim would.
	// Quantity.Add on a decimal-form value writes through the shared inf.Dec pointer.
	stored, found := snapshot.getResourceClaim(GetClaimId(claim))
	if !found {
		t.Fatalf("getResourceClaim(): claim not found")
	}
	capacity := stored.Status.Allocation.Devices.Results[0].ConsumedCapacity
	quantity := capacity["memory"]
	quantity.Add(resource.MustParse(decFormQuantity))
	capacity["memory"] = quantity

	// Withdrawing has to remove exactly what was added, leaving nothing behind.
	snapshot.deleteResourceClaim(GetClaimId(claim))

	state, err := snapshot.ResourceClaims().GatherAllocatedState()
	if err != nil {
		t.Fatalf("GatherAllocatedState(): %v", err)
	}
	if len(state.AggregatedCapacity) != 0 {
		t.Errorf("AggregatedCapacity should be empty once the only claim is gone, got %v", state.AggregatedCapacity)
	}
}

// deviceAllocated reads the currently allocated devices out of the snapshot and reports
// whether deviceID is among them.
//
// The state has to be re-read after every write: GatherAllocatedState borrows the snapshot's
// own collections, so a value fetched before a write is not a live view of what comes after
// it, and holding one across a write is exactly what the method documents callers must not
// do.
func deviceAllocated(t *testing.T, snapshot *Snapshot, deviceID structured.DeviceID) bool {
	t.Helper()

	state, err := snapshot.ResourceClaims().GatherAllocatedState()
	if err != nil {
		t.Fatalf("GatherAllocatedState(): %v", err)
	}
	return state.AllocatedDevices.Has(deviceID)
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
	if !deviceAllocated(t, snapshot, deviceID) {
		t.Fatalf("device %v should be allocated", deviceID)
	}

	snapshot.deleteResourceClaim(GetClaimId(first))
	if !deviceAllocated(t, snapshot, deviceID) {
		t.Errorf("device %v should still be allocated while the second claim holds it", deviceID)
	}

	snapshot.deleteResourceClaim(GetClaimId(second))
	if deviceAllocated(t, snapshot, deviceID) {
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

	if deviceID := structured.MakeDeviceID("driver", "pool", "dev-1"); deviceAllocated(t, snapshot, deviceID) {
		t.Errorf("device %v should be released once the aliased claim is gone", deviceID)
	}
}

// TestAllocatedStateTrackerExpectedStates pins the derived state down against expectations
// written out by hand, for each shape an allocation result can take.
//
// The randomized and reference-comparison tests above check the tracker against a
// recomputation, which establishes that the incremental path agrees with a full pass but not
// that either is right. These cases say what the answer should be. They also cover shapes a
// random walk reaches rarely or not at all - two claims holding the same share of a device,
// two shares of one device in a single claim, and a shared result carrying no capacity.
func TestAllocatedStateTrackerExpectedStates(t *testing.T) {
	dev1 := structured.MakeDeviceID("driver", "pool", "dev-1")
	dev2 := structured.MakeDeviceID("driver", "pool", "dev-2")
	shareA, shareB := ptr.To(types.UID("share-a")), ptr.To(types.UID("share-b"))

	// capacityOf builds an expected AggregatedCapacity holding one device's total. The
	// totals in the cases below are worked out by hand, not derived from the claims.
	capacityOf := func(deviceID structured.DeviceID, total int64) structured.ConsumedCapacityCollection {
		collection := structured.NewConsumedCapacityCollection()
		collection.Insert(structured.NewDeviceConsumedCapacity(deviceID, map[resourceapi.QualifiedName]resource.Quantity{
			"memory": *resource.NewQuantity(total, resource.DecimalSI),
		}))
		return collection
	}

	for _, tc := range []struct {
		name               string
		consumableCapacity bool
		claims             []*resourceapi.ResourceClaim
		wantDevices        sets.Set[structured.DeviceID]
		wantShared         sets.Set[structured.SharedDeviceID]
		wantCapacity       structured.ConsumedCapacityCollection
		wantAllDevices     sets.Set[structured.DeviceID]
	}{
		{
			name:               "dedicated",
			consumableCapacity: true,
			claims: []*resourceapi.ResourceClaim{allocatedStateTestClaim("default", "a", allocationForDevices(
				dedicatedResult("driver", "pool", "dev-1"),
				dedicatedResult("driver", "pool", "dev-2"),
			))},
			wantDevices:    sets.New(dev1, dev2),
			wantShared:     sets.New[structured.SharedDeviceID](),
			wantCapacity:   structured.NewConsumedCapacityCollection(),
			wantAllDevices: sets.New(dev1, dev2),
		},
		{
			// With the gate off, sharing is not reported and the device counts as
			// ordinarily allocated.
			name:               "shared/gateOff",
			consumableCapacity: false,
			claims: []*resourceapi.ResourceClaim{allocatedStateTestClaim("default", "a", allocationForDevices(
				sharedResult("driver", "pool", "dev-1", "share-a", 4),
			))},
			wantDevices:    sets.New(dev1),
			wantShared:     sets.New[structured.SharedDeviceID](),
			wantCapacity:   structured.NewConsumedCapacityCollection(),
			wantAllDevices: sets.New(dev1),
		},
		{
			// A shared device is not an exclusively allocated one, so it stays out of
			// AllocatedDevices while still counting towards ListAllAllocatedDevices.
			name:               "shared",
			consumableCapacity: true,
			claims: []*resourceapi.ResourceClaim{allocatedStateTestClaim("default", "a", allocationForDevices(
				sharedResult("driver", "pool", "dev-1", "share-a", 4),
			))},
			wantDevices:    sets.New[structured.DeviceID](),
			wantShared:     sets.New(structured.MakeSharedDeviceID(dev1, shareA)),
			wantCapacity:   capacityOf(dev1, 4),
			wantAllDevices: sets.New(dev1),
		},
		{
			// Two claims holding the same share of one device: one shared entry, and the
			// capacities add up. This is what the refcounting exists for - the entry has
			// to survive one of the claims going away.
			name:               "duplicateShare",
			consumableCapacity: true,
			claims: []*resourceapi.ResourceClaim{
				allocatedStateTestClaim("default", "a", allocationForDevices(
					sharedResult("driver", "pool", "dev-1", "share-a", 4))),
				allocatedStateTestClaim("default", "b", allocationForDevices(
					sharedResult("driver", "pool", "dev-1", "share-a", 4))),
			},
			wantDevices:    sets.New[structured.DeviceID](),
			wantShared:     sets.New(structured.MakeSharedDeviceID(dev1, shareA)),
			wantCapacity:   capacityOf(dev1, 8),
			wantAllDevices: sets.New(dev1),
		},
		{
			// Two distinct shares of one device inside a single claim: two shared entries,
			// one device, capacities summed.
			name:               "twoSharesOfOneDevice",
			consumableCapacity: true,
			claims: []*resourceapi.ResourceClaim{allocatedStateTestClaim("default", "a", allocationForDevices(
				sharedResult("driver", "pool", "dev-1", "share-a", 4),
				sharedResult("driver", "pool", "dev-1", "share-b", 6),
			))},
			wantDevices: sets.New[structured.DeviceID](),
			wantShared: sets.New(
				structured.MakeSharedDeviceID(dev1, shareA),
				structured.MakeSharedDeviceID(dev1, shareB),
			),
			wantCapacity:   capacityOf(dev1, 10),
			wantAllDevices: sets.New(dev1),
		},
		{
			// A share can carry no consumed capacity at all, which must not create an
			// AggregatedCapacity entry.
			name:               "sharedWithoutCapacity",
			consumableCapacity: true,
			claims: []*resourceapi.ResourceClaim{allocatedStateTestClaim("default", "a", allocationForDevices(
				sharedResult("driver", "pool", "dev-1", "share-a", 0),
			))},
			wantDevices:    sets.New[structured.DeviceID](),
			wantShared:     sets.New(structured.MakeSharedDeviceID(dev1, shareA)),
			wantCapacity:   structured.NewConsumedCapacityCollection(),
			wantAllDevices: sets.New(dev1),
		},
		{
			// Dedicated and shared together: AllocatedDevices holds only the dedicated
			// one, ListAllAllocatedDevices holds both.
			name:               "dedicatedAndShared",
			consumableCapacity: true,
			claims: []*resourceapi.ResourceClaim{
				allocatedStateTestClaim("default", "a", allocationForDevices(
					dedicatedResult("driver", "pool", "dev-1"))),
				allocatedStateTestClaim("default", "b", allocationForDevices(
					sharedResult("driver", "pool", "dev-2", "share-a", 3))),
			},
			wantDevices:    sets.New(dev1),
			wantShared:     sets.New(structured.MakeSharedDeviceID(dev2, shareA)),
			wantCapacity:   capacityOf(dev2, 3),
			wantAllDevices: sets.New(dev1, dev2),
		},
		{
			name:               "unallocated",
			consumableCapacity: true,
			claims:             []*resourceapi.ResourceClaim{allocatedStateTestClaim("default", "a", nil)},
			wantDevices:        sets.New[structured.DeviceID](),
			wantShared:         sets.New[structured.SharedDeviceID](),
			wantCapacity:       structured.NewConsumedCapacityCollection(),
			wantAllDevices:     sets.New[structured.DeviceID](),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			featuretesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAConsumableCapacity, tc.consumableCapacity)

			claimMap := map[ResourceClaimId]*resourceapi.ResourceClaim{}
			for _, claim := range tc.claims {
				claimMap[GetClaimId(claim)] = claim
			}
			snapshot := NewSnapshot(claimMap, nil, nil, nil)

			want := &structured.AllocatedState{
				AllocatedDevices:         tc.wantDevices,
				AllocatedSharedDeviceIDs: tc.wantShared,
				AggregatedCapacity:       tc.wantCapacity,
			}
			got, err := snapshot.ResourceClaims().GatherAllocatedState()
			if err != nil {
				t.Fatalf("GatherAllocatedState(): %v", err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("GatherAllocatedState(): unexpected output (-want +got): %s", diff)
			}

			gotAll, err := snapshot.ResourceClaims().ListAllAllocatedDevices()
			if err != nil {
				t.Fatalf("ListAllAllocatedDevices(): %v", err)
			}
			if diff := cmp.Diff(tc.wantAllDevices, gotAll); diff != "" {
				t.Errorf("ListAllAllocatedDevices(): unexpected output (-want +got): %s", diff)
			}
		})
	}
}
