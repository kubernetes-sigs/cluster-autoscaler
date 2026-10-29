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
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuretesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured"
	schedulerinterface "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

var (
	claim1 = &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-1", UID: "claim-1", Namespace: "default"}}
	claim2 = &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-2", UID: "claim-2", Namespace: "default"}}
	claim3 = &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-3", UID: "claim-3", Namespace: "default"}}

	allocatedClaim1 = &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-1", UID: "claim-1", Namespace: "default"},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{Request: "req-1", Driver: "driver.example.com", Pool: "pool-1", Device: "device-1"},
						{Request: "req-2", Driver: "driver.example.com", Pool: "pool-1", Device: "device-2"},
					},
				},
			},
		},
	}
	allocatedClaim2 = &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-2", UID: "claim-2", Namespace: "default"},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{Request: "req-1", Driver: "driver.example.com", Pool: "pool-1", Device: "device-3"},
						{Request: "req-2", Driver: "driver2.example.com", Pool: "pool-2", Device: "device-1"},
					},
				},
			},
		},
	}
)

func TestSnapshotClaimTrackerList(t *testing.T) {
	for _, tc := range []struct {
		testName   string
		claims     map[ResourceClaimId]*resourceapi.ResourceClaim
		wantClaims []*resourceapi.ResourceClaim
	}{
		{
			testName:   "no claims in snapshot",
			wantClaims: []*resourceapi.ResourceClaim{},
		},
		{
			testName: "claims present in snapshot",
			claims: map[ResourceClaimId]*resourceapi.ResourceClaim{
				GetClaimId(claim1): claim1,
				GetClaimId(claim2): claim2,
				GetClaimId(claim3): claim3,
			},
			wantClaims: []*resourceapi.ResourceClaim{claim1, claim2, claim3},
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			snapshot := NewSnapshot(tc.claims, nil, nil, nil)
			var resourceClaimTracker schedulerinterface.ResourceClaimTracker = snapshot.ResourceClaims()
			claims, err := resourceClaimTracker.List()
			if err != nil {
				t.Fatalf("snapshotClaimTracker.List(): got unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.wantClaims, claims, cmpopts.EquateEmpty(), test.IgnoreObjectOrder[*resourceapi.ResourceClaim]()); diff != "" {
				t.Errorf("snapshotClaimTracker.List(): unexpected output (-want +got): %s", diff)
			}
		})
	}
}

func TestSnapshotClaimTrackerGet(t *testing.T) {
	for _, tc := range []struct {
		testName       string
		claimName      string
		claimNamespace string
		wantClaim      *resourceapi.ResourceClaim
		wantErr        error
	}{
		{
			testName:       "claim present in snapshot",
			claimName:      "claim-2",
			claimNamespace: "default",
			wantClaim:      claim2,
		},
		{
			testName:       "claim not present in snapshot (wrong name)",
			claimName:      "claim-1337",
			claimNamespace: "default",
			wantErr:        cmpopts.AnyError,
		},
		{
			testName:       "claim not present in snapshot (wrong namespace)",
			claimName:      "claim-2",
			claimNamespace: "non-default",
			wantErr:        cmpopts.AnyError,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			claims := map[ResourceClaimId]*resourceapi.ResourceClaim{
				GetClaimId(claim1): claim1,
				GetClaimId(claim2): claim2,
				GetClaimId(claim3): claim3,
			}
			snapshot := NewSnapshot(claims, nil, nil, nil)
			var resourceClaimTracker schedulerinterface.ResourceClaimTracker = snapshot.ResourceClaims()

			claim, err := resourceClaimTracker.Get(tc.claimNamespace, tc.claimName)
			if diff := cmp.Diff(tc.wantErr, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("snapshotClaimTracker.Get(): unexpected error (-want +got): %s", diff)
			}
			if diff := cmp.Diff(tc.wantClaim, claim); diff != "" {
				t.Errorf("snapshotClaimTracker.Get(): unexpected output (-want +got): %s", diff)
			}
		})
	}
}

func TestSnapshotClaimTrackerListAllAllocatedDevices(t *testing.T) {
	for _, tc := range []struct {
		testName    string
		claims      map[ResourceClaimId]*resourceapi.ResourceClaim
		wantDevices sets.Set[structured.DeviceID]
	}{
		{
			testName:    "no claims in snapshot",
			wantDevices: sets.New[structured.DeviceID](),
		},
		{
			testName: "claims present in snapshot, all unallocated",
			claims: map[ResourceClaimId]*resourceapi.ResourceClaim{
				GetClaimId(claim1): claim1,
				GetClaimId(claim2): claim2,
				GetClaimId(claim3): claim3,
			},
			wantDevices: sets.New[structured.DeviceID](),
		},
		{
			testName: "claims present in snapshot, some allocated",
			claims: map[ResourceClaimId]*resourceapi.ResourceClaim{
				GetClaimId(allocatedClaim1): allocatedClaim1,
				GetClaimId(allocatedClaim2): allocatedClaim2,
				GetClaimId(claim3):          claim3,
			},
			wantDevices: sets.New(
				structured.MakeDeviceID("driver.example.com", "pool-1", "device-1"),
				structured.MakeDeviceID("driver.example.com", "pool-1", "device-2"),
				structured.MakeDeviceID("driver.example.com", "pool-1", "device-3"),
				structured.MakeDeviceID("driver2.example.com", "pool-2", "device-1"),
			),
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			snapshot := NewSnapshot(tc.claims, nil, nil, nil)
			var resourceClaimTracker schedulerinterface.ResourceClaimTracker = snapshot.ResourceClaims()
			devices, err := resourceClaimTracker.ListAllAllocatedDevices()
			if err != nil {
				t.Fatalf("snapshotClaimTracker.ListAllAllocatedDevices(): got unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.wantDevices, devices, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("snapshotClaimTracker.ListAllAllocatedDevices(): unexpected output (-want +got): %s", diff)
			}
		})
	}
}

func TestSnapshotClaimTrackerSignalClaimPendingAllocation(t *testing.T) {
	for _, tc := range []struct {
		testName       string
		claimUid       types.UID
		allocatedClaim *resourceapi.ResourceClaim
		wantErr        error
	}{
		{
			testName:       "claim not present in snapshot",
			claimUid:       "bad-name",
			allocatedClaim: &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: "bad-name", UID: "bad-name", Namespace: "default"}},
			wantErr:        cmpopts.AnyError,
		},
		{
			testName:       "provided UIDs don't match",
			claimUid:       "bad-name",
			allocatedClaim: allocatedClaim2,
			wantErr:        cmpopts.AnyError,
		},
		{
			testName:       "claim correctly modified",
			claimUid:       "claim-2",
			allocatedClaim: allocatedClaim2,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			claims := map[ResourceClaimId]*resourceapi.ResourceClaim{
				GetClaimId(claim1): claim1,
				GetClaimId(claim2): claim2,
				GetClaimId(claim3): claim3,
			}
			snapshot := NewSnapshot(claims, nil, nil, nil)
			var resourceClaimTracker schedulerinterface.ResourceClaimTracker = snapshot.ResourceClaims()

			err := resourceClaimTracker.SignalClaimPendingAllocation(tc.claimUid, tc.allocatedClaim)
			if diff := cmp.Diff(tc.wantErr, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("snapshotClaimTracker.SignalClaimPendingAllocation(): unexpected error (-want +got): %s", diff)
			}
			if tc.wantErr != nil {
				return
			}

			claimAfterSignal, err := resourceClaimTracker.Get(tc.allocatedClaim.Namespace, tc.allocatedClaim.Name)
			if err != nil {
				t.Fatalf("snapshotClaimTracker.Get(): got unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.allocatedClaim, claimAfterSignal); diff != "" {
				t.Errorf("Claim in unexpected state after snapshotClaimTracker.SignalClaimPendingAllocation() (-want +got): %s", diff)
			}
		})
	}
}

// TestSnapshotClaimTrackerBorrowedCollectionsAreReadOnly guards the "Borrowed collections"
// contract documented on ListAllAllocatedDevices and GatherAllocatedState.
//
// Both hand back the snapshot's own live collections, which is only safe because the code
// they are handed to - the structured allocator behind the DRA scheduler plugin - only reads
// them. fwk.ResourceClaimTracker does not promise that, so nothing but this test stops a
// Kubernetes bump from quietly starting to write through the alias and skewing a snapshot
// that is never recomputed.
//
// So: hand the real allocator the real borrowed collections, let it allocate, and check that
// it did not touch them.
func TestSnapshotClaimTrackerBorrowedCollectionsAreReadOnly(t *testing.T) {
	const (
		driver    = "driver.example.com"
		className = "example-class"
		nodeName  = "node-1"
		poolName  = "pool-1"
	)

	// Both allocator variants that can be reached through structured.NewAllocator: the
	// stable one takes only AllocatedDevices, the incubating one takes the whole
	// AllocatedState including the shared devices and the aggregated capacity.
	//
	// GatherAllocatedState only fills the latter two in when Kubernetes'
	// DRAConsumableCapacity gate is on, so the gate has to move with the allocator variant -
	// otherwise the incubating case would be handed two empty collections and the
	// assertions on them would hold no matter what the allocator did.
	for _, tc := range []struct {
		name               string
		allocatorFeatures  structured.Features
		consumableCapacity bool
	}{
		{name: "stable", allocatorFeatures: structured.Features{}},
		{name: "incubating/ConsumableCapacity", allocatorFeatures: structured.Features{ConsumableCapacity: true}, consumableCapacity: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			featuretesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAConsumableCapacity, tc.consumableCapacity)
			ctx := context.Background()

			devices := make([]resourceapi.Device, 4)
			for i := range devices {
				devices[i] = resourceapi.Device{Name: fmt.Sprintf("device-%d", i)}
			}
			slice := &resourceapi.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-1", UID: "slice-1"},
				Spec: resourceapi.ResourceSliceSpec{
					NodeName: ptr.To(nodeName),
					Driver:   driver,
					Pool:     resourceapi.ResourcePool{Name: poolName, ResourceSliceCount: 1},
					Devices:  devices,
				},
			}
			deviceClass := &resourceapi.DeviceClass{
				ObjectMeta: metav1.ObjectMeta{Name: className},
				Spec: resourceapi.DeviceClassSpec{
					Selectors: []resourceapi.DeviceSelector{
						{CEL: &resourceapi.CELDeviceSelector{Expression: fmt.Sprintf("device.driver == %q", driver)}},
					},
				},
			}

			// An already-allocated claim, so the borrowed collections are not empty and the
			// allocator has something to read out of them.
			allocated := &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "allocated", UID: "allocated", Namespace: "default"},
				Status: resourceapi.ResourceClaimStatus{
					Allocation: &resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{Request: "req", Driver: driver, Pool: poolName, Device: "device-0"},
							},
						},
					},
				},
			}

			// A claim holding a share of a device, so AllocatedSharedDeviceIDs and
			// AggregatedCapacity are populated too and the assertions on them are not
			// comparing one empty collection to another.
			shared := &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", UID: "shared", Namespace: "default"},
				Status: resourceapi.ResourceClaimStatus{
					Allocation: &resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req", Driver: driver, Pool: poolName, Device: "device-1",
									ShareID: ptr.To(types.UID("share-a")),
									ConsumedCapacity: map[resourceapi.QualifiedName]resource.Quantity{
										"memory": *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							},
						},
					},
				},
			}

			snapshot := NewSnapshot(
				map[ResourceClaimId]*resourceapi.ResourceClaim{
					GetClaimId(allocated): allocated,
					GetClaimId(shared):    shared,
				},
				map[string][]*resourceapi.ResourceSlice{nodeName: {slice}},
				nil,
				map[string]*resourceapi.DeviceClass{className: deviceClass},
			)

			state, err := snapshot.ResourceClaims().GatherAllocatedState()
			if err != nil {
				t.Fatalf("GatherAllocatedState(): %v", err)
			}
			borrowedDevices, err := snapshot.ResourceClaims().ListAllAllocatedDevices()
			if err != nil {
				t.Fatalf("ListAllAllocatedDevices(): %v", err)
			}

			wantDevices := state.AllocatedDevices.Clone()
			wantSharedDeviceIDs := state.AllocatedSharedDeviceIDs.Clone()
			wantCapacity := state.AggregatedCapacity.Clone()
			wantAllDevices := borrowedDevices.Clone()

			// An assertion that a collection did not change is worthless if the collection
			// was empty to begin with, so check the fixture actually filled them.
			if len(wantDevices) == 0 || len(wantAllDevices) == 0 {
				t.Fatalf("fixture produced no allocated devices: %d dedicated, %d total", len(wantDevices), len(wantAllDevices))
			}
			if tc.consumableCapacity && (len(wantSharedDeviceIDs) == 0 || len(wantCapacity) == 0) {
				t.Fatalf("fixture produced no sharing to guard: %d shared device IDs, %d capacity entries",
					len(wantSharedDeviceIDs), len(wantCapacity))
			}

			pending := &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "pending", UID: "pending", Namespace: "default"},
				Spec: resourceapi.ResourceClaimSpec{
					Devices: resourceapi.DeviceClaim{
						Requests: []resourceapi.DeviceRequest{
							{
								Name: "req",
								Exactly: &resourceapi.ExactDeviceRequest{
									DeviceClassName: className,
									AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
									Count:           1,
								},
							},
						},
					},
				},
			}

			allocator, err := structured.NewAllocator(
				ctx,
				tc.allocatorFeatures,
				*state,
				snapshot.DeviceClasses(),
				[]*resourceapi.ResourceSlice{slice},
				cel.NewCache(10, cel.Features{EnableConsumableCapacity: tc.allocatorFeatures.ConsumableCapacity}),
			)
			if err != nil {
				t.Fatalf("structured.NewAllocator(): %v", err)
			}

			results, err := allocator.Allocate(ctx, test.BuildTestNode(nodeName, 1000, 1000), []*resourceapi.ResourceClaim{pending})
			if err != nil {
				t.Fatalf("Allocate(): %v", err)
			}
			// A no-op allocation would read nothing and pass the assertions below for the
			// wrong reason.
			if len(results) != 1 {
				t.Fatalf("Allocate(): got %d results, want 1 - the allocation has to actually run", len(results))
			}

			if diff := cmp.Diff(wantDevices, state.AllocatedDevices); diff != "" {
				t.Errorf("the allocator modified the borrowed AllocatedDevices (-before +after): %s", diff)
			}
			if diff := cmp.Diff(wantSharedDeviceIDs, state.AllocatedSharedDeviceIDs); diff != "" {
				t.Errorf("the allocator modified the borrowed AllocatedSharedDeviceIDs (-before +after): %s", diff)
			}
			if diff := cmp.Diff(wantCapacity, state.AggregatedCapacity); diff != "" {
				t.Errorf("the allocator modified the borrowed AggregatedCapacity (-before +after): %s", diff)
			}
			if diff := cmp.Diff(wantAllDevices, borrowedDevices); diff != "" {
				t.Errorf("the allocator modified the borrowed ListAllAllocatedDevices set (-before +after): %s", diff)
			}
		})
	}
}
