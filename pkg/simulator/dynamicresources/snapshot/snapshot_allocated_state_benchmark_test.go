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
	"fmt"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	drautils "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/utils"
)

// These benchmarks exist to find where maintaining the allocated state costs
// more than recomputing it would. Every benchmark elsewhere in the tree is a
// case the incremental tracker is good at - claims read far more often than they
// are written - so they say nothing about the cases it is bad at.
//
// The tracker trades work on writes for work on reads. It pays, per claim write,
// for deriving the claim's contribution and recording it; it saves, per read, a
// walk over every claim. So it loses whenever writes outnumber reads, whenever
// the claim set is small enough that recomputing was already trivial, or
// whenever the state is read once and thrown away.

const benchDriver = "driver.example.com"

// Deliberately self-contained: this file has to compile both with and without
// the incremental tracker, so it cannot lean on helpers that arrive alongside it.
func benchClaim(namespace, name string, devices ...string) *resourceapi.ResourceClaim {
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID(namespace + "/" + name)},
	}
	if len(devices) == 0 {
		return claim
	}
	results := make([]resourceapi.DeviceRequestAllocationResult, 0, len(devices))
	for _, device := range devices {
		results = append(results, resourceapi.DeviceRequestAllocationResult{
			Request: "req", Driver: benchDriver, Pool: "pool", Device: device,
		})
	}
	return drautils.TestClaimWithAllocation(claim, &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{Results: results},
	})
}

func benchAllocatedClaim(index int) *resourceapi.ResourceClaim {
	return benchClaim("default", fmt.Sprintf("claim-%d", index), fmt.Sprintf("dev-%d", index))
}

func benchSnapshotWithClaims(count int) (*Snapshot, []ResourceClaimId) {
	claims := map[ResourceClaimId]*resourceapi.ResourceClaim{}
	ids := make([]ResourceClaimId, 0, count)
	for i := 0; i < count; i++ {
		claim := benchAllocatedClaim(i)
		claims[GetClaimId(claim)] = claim
		ids = append(ids, GetClaimId(claim))
	}
	return NewSnapshot(claims, nil, nil, nil), ids
}

// BenchmarkAllocatedStateSingleRead is the worst case for maintaining state: the
// snapshot is built, the allocated state is read exactly once, and everything is
// discarded. Recomputing would do one walk; the tracker does the same walk and
// additionally records what every claim contributed, for a second read that
// never comes.
func BenchmarkAllocatedStateSingleRead(b *testing.B) {
	for _, claims := range []int{100, 5000, 20000} {
		b.Run(fmt.Sprintf("claims=%d", claims), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				snapshot, _ := benchSnapshotWithClaims(claims)
				b.StartTimer()

				if _, err := snapshot.ResourceClaims().GatherAllocatedState(); err != nil {
					b.Fatalf("GatherAllocatedState(): %v", err)
				}
			}
		})
	}
}

// BenchmarkAllocatedStateWriteHeavy inverts the usual ratio: many claim writes
// against a single read. Every write pays the bookkeeping the tracker needs to
// make reads cheap, and here that investment is never recovered.
func BenchmarkAllocatedStateWriteHeavy(b *testing.B) {
	for _, writes := range []int{100, 1000} {
		b.Run(fmt.Sprintf("writes=%d", writes), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				snapshot, ids := benchSnapshotWithClaims(writes)
				// Force the state to exist so writes are actually tracked;
				// otherwise the lazy build would skip all of them.
				if _, err := snapshot.ResourceClaims().GatherAllocatedState(); err != nil {
					b.Fatalf("GatherAllocatedState(): %v", err)
				}
				b.StartTimer()

				snapshot.Fork()
				for j, id := range ids {
					updated := benchClaim(id.Namespace, id.Name, fmt.Sprintf("dev-%d-v2", j))
					snapshot.configureResourceClaim(updated)
				}
				if _, err := snapshot.ResourceClaims().GatherAllocatedState(); err != nil {
					b.Fatalf("GatherAllocatedState(): %v", err)
				}
				snapshot.Revert()
			}
		})
	}
}

// BenchmarkAllocatedStateForkRevertChurn hammers Fork/Revert. Reverting a layer
// makes the tracker re-read every claim the layer touched, where recomputing
// would simply drop the derived state and rebuild it on demand. With a small
// claim set that rebuild is trivial, so the bookkeeping has nothing to beat.
func BenchmarkAllocatedStateForkRevertChurn(b *testing.B) {
	for _, claims := range []int{0, 10, 1000} {
		b.Run(fmt.Sprintf("claims=%d", claims), func(b *testing.B) {
			snapshot, ids := benchSnapshotWithClaims(claims)
			if _, err := snapshot.ResourceClaims().GatherAllocatedState(); err != nil {
				b.Fatalf("GatherAllocatedState(): %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snapshot.Fork()
				// Touch one claim so the reverted layer is not empty - an empty
				// layer is a special case both implementations short-circuit.
				if len(ids) > 0 {
					id := ids[i%len(ids)]
					updated := benchClaim(id.Namespace, id.Name, "dev-churn")
					snapshot.configureResourceClaim(updated)
				}
				snapshot.Revert()
			}
		})
	}
}

// BenchmarkAllocatedStateUnallocatedClaims fills the snapshot with claims that
// have no allocation. Recomputing over these is nearly free - the derivation
// returns immediately on a nil allocation - so there is very little for the
// tracker to save, while it still pays to walk them once at build time.
func BenchmarkAllocatedStateUnallocatedClaims(b *testing.B) {
	for _, claims := range []int{5000, 20000} {
		b.Run(fmt.Sprintf("claims=%d", claims), func(b *testing.B) {
			claimMap := map[ResourceClaimId]*resourceapi.ResourceClaim{}
			for i := 0; i < claims; i++ {
				claim := benchClaim("default", fmt.Sprintf("claim-%d", i))
				claimMap[GetClaimId(claim)] = claim
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				snapshot := NewSnapshot(claimMap, nil, nil, nil)
				b.StartTimer()

				// Two reads, the pattern the tracker is meant to win at. Even
				// here there is almost nothing to win.
				for r := 0; r < 2; r++ {
					if _, err := snapshot.ResourceClaims().GatherAllocatedState(); err != nil {
						b.Fatalf("GatherAllocatedState(): %v", err)
					}
				}
			}
		})
	}
}

// BenchmarkSnapshotForkRevertNoDRA covers a snapshot that never touches DRA at
// all, which is what every non-DRA cluster looks like. Nothing should be derived
// and nothing should be allocated; this guards against the maintenance
// machinery leaking cost into clusters that do not use the feature.
func BenchmarkSnapshotForkRevertNoDRA(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		snapshot := NewSnapshot(nil, nil, nil, nil)
		b.StartTimer()

		for f := 0; f < 100; f++ {
			snapshot.Fork()
			snapshot.Revert()
		}
	}
}
