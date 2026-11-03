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
	"math/rand/v2"
	"testing"

	"github.com/google/go-cmp/cmp"

	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
	drautils "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/utils"

	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// byteReader hands out the bytes of a fuzz input one by one, and zeros once
// it's exhausted.
type byteReader struct {
	data []byte
}

func (r *byteReader) next() byte {
	if len(r.data) == 0 {
		return 0
	}
	b := r.data[0]
	r.data = r.data[1:]
	return b
}

// Operation codes of the FuzzAllocatedStateTracker input (taken modulo allocatedStateOpCount).
const (
	// opAllocate sets a claim's allocation through SignalClaimPendingAllocation, the way the
	// DRA plugin does in Reserve. The allocation may be empty, which deallocates the claim.
	opAllocate = iota
	// opReservePodClaims, opUnreservePodClaims and opRemovePodOwnedClaims rewrite claims in
	// place through ensureClaimWritable - the case the tracker's recorded contributions exist for.
	opReservePodClaims
	opUnreservePodClaims
	opRemovePodOwnedClaims
	// opAddClaim re-adds a claim missing from the snapshot, possibly already allocated.
	opAddClaim
	opFork
	opCommit
	opRevert
	// opRead reads the allocated state. The tracker is built lazily on the first read, so
	// where this lands decides how many writes happen before the tracker exists.
	opRead
	allocatedStateOpCount
)

// allocatedStateClaimSpace is small on purpose, so that operations frequently hit the same
// claims. The device space (see fuzzAllocation) is small for the same reason - so that
// claims frequently collide on devices and shares, which is what refcounting is for.
const allocatedStateClaimSpace = 8

// fuzzAllocation decodes an allocation from the input:
//   - 1 byte: the kind (mod 4) - unallocated, one dedicated result, one shared result, or
//     a dedicated and a shared result.
//   - 1 byte per result, selecting the device (and for shared results the share and the
//     consumed capacity).
//
// Dedicated devices come from pool-0/pool-1 x dev-0..3 and shared ones from pool-0 x dev-0..2,
// so a device can also be held dedicated by one claim and shared by another.
func fuzzAllocation(r *byteReader) *resourceapi.AllocationResult {
	dedicated := func() resourceapi.DeviceRequestAllocationResult {
		b := r.next()
		return dedicatedResult("driver", fmt.Sprintf("pool-%d", b%2), fmt.Sprintf("dev-%d", (b/2)%4))
	}
	shared := func() resourceapi.DeviceRequestAllocationResult {
		b := r.next()
		result := dedicatedResult("driver", "pool-0", fmt.Sprintf("dev-%d", b%3))
		result.ShareID = ptr.To(types.UID(fmt.Sprintf("share-%d", (b/3)%3)))
		// "0.5Gi" can't be represented in the scaled int64 form, so it's backed by an *inf.Dec -
		// the case ownedDeviceConsumedCapacity has to deep-copy.
		switch (b / 9) % 4 {
		case 1:
			result.ConsumedCapacity = map[resourceapi.QualifiedName]resource.Quantity{"memory": resource.MustParse("4")}
		case 2:
			result.ConsumedCapacity = map[resourceapi.QualifiedName]resource.Quantity{"memory": resource.MustParse("0.5Gi")}
		case 3:
			result.ConsumedCapacity = map[resourceapi.QualifiedName]resource.Quantity{"memory": resource.MustParse("1Gi")}
		}
		return result
	}

	switch r.next() % 4 {
	case 1:
		return allocationForDevices(dedicated())
	case 2:
		return allocationForDevices(shared())
	case 3:
		return allocationForDevices(dedicated(), shared())
	default:
		return nil
	}
}

// verifyAllocatedState checks everything the tracker maintains against a from-scratch
// recomputation, returning a description of the first mismatch or "" if there is none.
//
// The tracker maintains the state for both values of the DRAConsumableCapacity gate at once,
// so both are checked directly, without flipping the gate. The public methods are checked
// too, for whatever value the gate has.
func verifyAllocatedState(snapshot *Snapshot) string {
	tracker := snapshot.allocatedState

	// The gate-on view: dedicated devices, shares and aggregated capacity.
	if diff := cmp.Diff(referenceAllocatedState(snapshot, true), tracker.allocatedState()); diff != "" {
		return fmt.Sprintf("tracked state (DRAConsumableCapacity=true) mismatch (-want +got): %s", diff)
	}
	// The gate-off view: every allocated device, shared or not.
	if diff := cmp.Diff(referenceAllocatedState(snapshot, false).AllocatedDevices, tracker.allDevices.set); diff != "" {
		return fmt.Sprintf("tracked allDevices mismatch (-want +got): %s", diff)
	}

	enabledConsumableCapacity := utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity)
	gotState, err := snapshot.ResourceClaims().GatherAllocatedState()
	if err != nil {
		return fmt.Sprintf("GatherAllocatedState(): unexpected error: %v", err)
	}
	if diff := cmp.Diff(referenceAllocatedState(snapshot, enabledConsumableCapacity), gotState); diff != "" {
		return fmt.Sprintf("GatherAllocatedState() (DRAConsumableCapacity=%v) mismatch (-want +got): %s", enabledConsumableCapacity, diff)
	}
	gotDevices, err := snapshot.ResourceClaims().ListAllAllocatedDevices()
	if err != nil {
		return fmt.Sprintf("ListAllAllocatedDevices(): unexpected error: %v", err)
	}
	if diff := cmp.Diff(referenceAllocatedState(snapshot, false).AllocatedDevices, gotDevices); diff != "" {
		return fmt.Sprintf("ListAllAllocatedDevices() mismatch (-want +got): %s", diff)
	}

	// The sets agreeing with the reference doesn't mean the refcounts behind them are right -
	// an off-by-one there only shows up later, when the last holder of a device goes away.
	// So count how many times each element should have been inserted, and check that against
	// the refcounts exactly.
	wantDevices := map[structured.DeviceID]int{}
	wantAllDevices := map[structured.DeviceID]int{}
	wantShared := map[structured.SharedDeviceID]int{}
	wantContributions := 0
	for _, claim := range snapshot.listResourceClaims() {
		if claim.Status.Allocation == nil || len(claim.Status.Allocation.Devices.Results) == 0 {
			if _, found := tracker.contributions[GetClaimId(claim)]; found {
				return fmt.Sprintf("unallocated claim %v has a recorded contribution", GetClaimId(claim))
			}
			continue
		}
		wantContributions++
		if _, found := tracker.contributions[GetClaimId(claim)]; !found {
			return fmt.Sprintf("allocated claim %v has no recorded contribution", GetClaimId(claim))
		}
		for _, result := range claim.Status.Allocation.Devices.Results {
			deviceID := structured.MakeDeviceID(result.Driver, result.Pool, result.Device)
			wantAllDevices[deviceID]++
			if result.ShareID != nil {
				wantShared[structured.MakeSharedDeviceID(deviceID, result.ShareID)]++
			} else {
				wantDevices[deviceID]++
			}
		}
	}
	// Every allocated claim has a contribution (checked above), so a count mismatch means
	// contributions left behind for claims no longer in the snapshot.
	if got := len(tracker.contributions); got != wantContributions {
		return fmt.Sprintf("got %d recorded contributions, want %d", got, wantContributions)
	}
	if msg := verifyRefCounts("devices", tracker.devices, wantDevices); msg != "" {
		return msg
	}
	if msg := verifyRefCounts("allDevices", tracker.allDevices, wantAllDevices); msg != "" {
		return msg
	}
	return verifyRefCounts("sharedDeviceIDs", tracker.sharedDeviceIDs, wantShared)
}

// verifyRefCounts checks that s holds exactly the elements of want, each with the number of
// inserts recorded there: present in the set, plus count-1 extra references in refCounts.
func verifyRefCounts[T comparable](name string, s refCountedSet[T], want map[T]int) string {
	if len(s.set) != len(want) {
		return fmt.Sprintf("%s: got %d elements, want %d", name, len(s.set), len(want))
	}
	wantExtra := 0
	for item, count := range want {
		if !s.set.Has(item) {
			return fmt.Sprintf("%s: %v missing", name, item)
		}
		if got := s.refCounts[item]; got != count-1 {
			return fmt.Sprintf("%s: %v has %d extra references, want %d", name, item, got, count-1)
		}
		if count > 1 {
			wantExtra++
		}
	}
	if len(s.refCounts) != wantExtra {
		return fmt.Sprintf("%s: refCounts has %d entries, want %d (stale entries for elements inserted once)", name, len(s.refCounts), wantExtra)
	}
	return ""
}

// FuzzAllocatedStateTracker interprets the input as a sequence of the operations that mutate
// ResourceClaims, runs them against a Snapshot, and after every step checks the incrementally
// maintained allocated state - including its refcounts - against a from-scratch recomputation.
//
// The Snapshot holds up to allocatedStateClaimSpace claims; claim i is owned by owner pod i.
// Pod operations act either on owner pod i, or on alias pod i, which references claim i
// through two aliases and claim i+1 through a third, without owning either - so its
// reservations are rewritten in place rather than the claims deleted.
//
// Input format:
//   - 1 byte: a bitmask of the claims present in the initial Snapshot, followed by an
//     allocation (see fuzzAllocation) for each of them.
//   - The rest: operations, each 2 bytes (operation code, target), plus an allocation for
//     opAllocate and opAddClaim. The target selects claim target%allocatedStateClaimSpace,
//     and for pod operations the alias pod if (target/allocatedStateClaimSpace) is odd.
//
// `go test` runs the seed corpus below. To search for new failures, run e.g.:
//
//	go test -run '^$' -fuzz FuzzAllocatedStateTracker -fuzztime=30s ./pkg/simulator/dynamicresources/snapshot/
func FuzzAllocatedStateTracker(f *testing.F) {
	const (
		dedicated  = 1 // allocation kinds, see fuzzAllocation
		shared     = 2
		both       = 3
		alias      = allocatedStateClaimSpace // target offset selecting the alias pod
		half       = 2 * 9                    // shared result byte: dev-0, share-0, "0.5Gi"
		sameDevice = 0                        // dedicated result byte: pool-0/dev-0
	)
	f.Add([]byte{})
	// Read first, then: allocate in a fork, reserve, revert; allocate again and commit.
	f.Add([]byte{
		0b10, 0,
		opRead, 0,
		opFork, 0, opAllocate, 1, dedicated, sameDevice, opReservePodClaims, 1, opRevert, 0,
		opFork, 0, opAllocate, 1, both, sameDevice, half, opCommit, 0, opRead, 0,
	})
	// Writes before the first read - inside nested forks, a Commit and Reverts - all of which
	// the lazy build has to pick up from the claims rather than from the writes.
	f.Add([]byte{
		0b00000111, dedicated, sameDevice, shared, half, 0,
		opFork, 0, opAllocate, 2, shared, half, opFork, 0, opRemovePodOwnedClaims, 0,
		opCommit, 0, opFork, 0, opAddClaim, 0, both, sameDevice, half, opRevert, 0,
		opRead, 0, opRevert, 0, opRead, 0,
	})
	// Several claims holding the same share of the same device, released one by one through
	// every path - the refcounting edge case - with alias pods rewriting them in place.
	f.Add([]byte{
		0b00001111, shared, half, shared, half, shared, half, both, sameDevice, half,
		opRead, 0,
		opFork, 0, opReservePodClaims, alias + 0, opReservePodClaims, alias + 1, opReservePodClaims, 2,
		opUnreservePodClaims, alias + 0, opRemovePodOwnedClaims, 1, opFork, 0, opAllocate, 2, 0,
		opRemovePodOwnedClaims, alias + 3, opRevert, 0, opCommit, 0, opUnreservePodClaims, 2,
		opRevert, 0, opRevert, 0, opRemovePodOwnedClaims, 3,
	})
	// A few long pseudo-random sequences, so that plain `go test` also exercises long
	// interleavings of writes, forks, commits and reverts.
	for seed := range uint64(8) {
		rng := rand.New(rand.NewPCG(seed, 0xa110c))
		data := make([]byte, 3000)
		for i := range data {
			data[i] = byte(rng.UintN(256))
		}
		f.Add(data)
	}

	ownerPods := make([]*apiv1.Pod, allocatedStateClaimSpace)
	aliasPods := make([]*apiv1.Pod, allocatedStateClaimSpace)
	claimName := func(i int) string { return fmt.Sprintf("claim-%d", i) }
	for i := range allocatedStateClaimSpace {
		ownerPods[i] = BuildTestPod(fmt.Sprintf("owner-%d", i), 1, 1, WithResourceClaim("own", claimName(i), ""))
		aliasPods[i] = BuildTestPod(fmt.Sprintf("alias-%d", i), 1, 1,
			WithResourceClaim("a", claimName(i), ""),
			WithResourceClaim("b", claimName(i), ""),
			WithResourceClaim("next", claimName((i+1)%allocatedStateClaimSpace), ""),
		)
	}
	newClaim := func(i int, allocation *resourceapi.AllocationResult) *resourceapi.ResourceClaim {
		claim := drautils.TestClaimWithPodOwnership(ownerPods[i], allocatedStateTestClaim("default", claimName(i), nil))
		claim.Status.Allocation = allocation
		return claim
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		r := &byteReader{data: data}

		initial := map[ResourceClaimId]*resourceapi.ResourceClaim{}
		mask := r.next()
		for i := range allocatedStateClaimSpace {
			if mask&(1<<i) != 0 {
				claim := newClaim(i, fuzzAllocation(r))
				initial[GetClaimId(claim)] = claim
			}
		}
		snapshot := NewSnapshot(initial, nil, nil, nil)

		var history []string
		fail := func(format string, args ...any) {
			t.Helper()
			const maxHistory = 30
			recent := history[max(0, len(history)-maxHistory):]
			t.Fatalf("after %d ops (last %d: %v): "+format, append([]any{len(history), len(recent), recent}, args...)...)
		}

		read := false
		for len(r.data) > 0 {
			op, target := r.next()%allocatedStateOpCount, int(r.next())
			index := target % allocatedStateClaimSpace
			claimId := ResourceClaimId{Namespace: "default", Name: claimName(index)}
			pod, podName := ownerPods[index], fmt.Sprintf("owner-%d", index)
			if (target/allocatedStateClaimSpace)%2 == 1 {
				pod, podName = aliasPods[index], fmt.Sprintf("alias-%d", index)
			}

			switch op {
			case opAllocate:
				allocation := fuzzAllocation(r)
				history = append(history, fmt.Sprintf("Allocate(%s, %d results)", claimId.Name, len(allocationResults(allocation))))
				claim, found := snapshot.getResourceClaim(claimId)
				if !found {
					break
				}
				allocated := claim.DeepCopy()
				allocated.Status.Allocation = allocation
				if err := snapshot.ResourceClaims().SignalClaimPendingAllocation(claim.UID, allocated); err != nil {
					fail("SignalClaimPendingAllocation(%s): unexpected error: %v", claimId.Name, err)
				}
			case opReservePodClaims:
				history = append(history, fmt.Sprintf("ReservePodClaims(%s)", podName))
				// Fails when a referenced claim is missing - nothing to check then.
				_ = snapshot.ReservePodClaims(pod)
			case opUnreservePodClaims:
				history = append(history, fmt.Sprintf("UnreservePodClaims(%s)", podName))
				_ = snapshot.UnreservePodClaims(pod)
			case opRemovePodOwnedClaims:
				history = append(history, fmt.Sprintf("RemovePodOwnedClaims(%s)", podName))
				snapshot.RemovePodOwnedClaims(context.Background(), pod)
			case opAddClaim:
				allocation := fuzzAllocation(r)
				history = append(history, fmt.Sprintf("AddClaim(%s, %d results)", claimId.Name, len(allocationResults(allocation))))
				if _, found := snapshot.getResourceClaim(claimId); found {
					break
				}
				if err := snapshot.AddClaims([]*resourceapi.ResourceClaim{newClaim(index, allocation)}); err != nil {
					fail("AddClaims(%s): unexpected error: %v", claimId.Name, err)
				}
			case opFork:
				history = append(history, "Fork")
				snapshot.Fork()
			case opCommit:
				history = append(history, "Commit")
				snapshot.Commit()
			case opRevert:
				history = append(history, "Revert")
				snapshot.Revert()
			case opRead:
				history = append(history, "Read")
				if _, err := snapshot.ResourceClaims().GatherAllocatedState(); err != nil {
					fail("GatherAllocatedState(): unexpected error: %v", err)
				}
			}

			if op == opRead {
				read = true
			}
			built := snapshot.allocatedState != nil && snapshot.allocatedState.isBuilt()
			if built != read {
				// The tracker has to be built by the first read, and not a moment earlier.
				fail("tracker built = %v, want %v (built lazily on the first read)", built, read)
			}
			if !built {
				continue
			}
			if msg := verifyAllocatedState(snapshot); msg != "" {
				fail("%s", msg)
			}
		}
	})
}

func allocationResults(allocation *resourceapi.AllocationResult) []resourceapi.DeviceRequestAllocationResult {
	if allocation == nil {
		return nil
	}
	return allocation.Devices.Results
}
