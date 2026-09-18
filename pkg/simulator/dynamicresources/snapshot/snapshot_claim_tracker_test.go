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
	"k8s.io/dynamic-resource-allocation/structured"
	schedulerinterface "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
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

func TestSnapshotClaimTrackerGatherAllocatedState(t *testing.T) {
	makeClaim := func(name, device, share, capacity string) *resourceapi.ResourceClaim {
		result := resourceapi.DeviceRequestAllocationResult{
			Request: "request",
			Driver:  "driver.example.com",
			Pool:    "pool",
			Device:  device,
		}
		if share != "" {
			shareID := types.UID(share)
			result.ShareID = &shareID
		}
		if capacity != "" {
			result.ConsumedCapacity = map[resourceapi.QualifiedName]resource.Quantity{
				"capacity": resource.MustParse(capacity),
			}
		}
		return &resourceapi.ResourceClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name)},
			Status: resourceapi.ResourceClaimStatus{
				Allocation: &resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: []resourceapi.DeviceRequestAllocationResult{result},
					},
				},
			},
		}
	}

	for _, tc := range []struct {
		testName                  string
		enabledConsumableCapacity bool
	}{
		{testName: "consumable capacity enabled", enabledConsumableCapacity: true},
		{testName: "consumable capacity disabled", enabledConsumableCapacity: false},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			featuretesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAConsumableCapacity, tc.enabledConsumableCapacity)
			firstShare := makeClaim("first", "device-1", "share-1", "2")
			secondShare := makeClaim("second", "device-1", "share-2", "3")
			shareWithoutCapacity := makeClaim("no-capacity", "device-2", "share-3", "")
			dedicated := makeClaim("dedicated", "device-3", "", "")
			claims := map[ResourceClaimId]*resourceapi.ResourceClaim{}
			for _, claim := range []*resourceapi.ResourceClaim{firstShare, secondShare, shareWithoutCapacity, dedicated, claim3} {
				claims[GetClaimId(claim)] = claim
			}
			snapshot := NewSnapshot(claims, nil, nil, nil)
			device1 := structured.MakeDeviceID("driver.example.com", "pool", "device-1")
			device2 := structured.MakeDeviceID("driver.example.com", "pool", "device-2")
			device3 := structured.MakeDeviceID("driver.example.com", "pool", "device-3")

			expectState := func(shared sets.Set[structured.DeviceID], capacity string) *structured.AllocatedState {
				t.Helper()
				want := &structured.AllocatedState{
					AllocatedDevices:         sets.New(device3),
					AllocatedSharedDeviceIDs: shared,
					AggregatedCapacity:       structured.NewConsumedCapacityCollection(),
				}
				if !tc.enabledConsumableCapacity {
					want.AllocatedDevices = want.AllocatedDevices.Union(shared)
					want.AllocatedSharedDeviceIDs = sets.New[structured.DeviceID]()
				} else if capacity != "" {
					want.AggregatedCapacity.Insert(structured.NewDeviceConsumedCapacity(device1,
						map[resourceapi.QualifiedName]resource.Quantity{"capacity": resource.MustParse(capacity)}))
				}
				got, err := snapshot.ResourceClaims().GatherAllocatedState()
				if err != nil {
					t.Fatalf("snapshotClaimTracker.GatherAllocatedState(): got unexpected error: %v", err)
				}
				if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
					t.Fatalf("snapshotClaimTracker.GatherAllocatedState(): unexpected output (-want +got): %s", diff)
				}
				return got
			}

			original := expectState(sets.New(device1, device2), "5")
			snapshot.Fork()
			snapshot.resourceClaims.DeleteCurrent(GetClaimId(firstShare))
			expectState(sets.New(device1, device2), "3")
			if tc.enabledConsumableCapacity && original.AggregatedCapacity[device1]["capacity"].Cmp(resource.MustParse("5")) != 0 {
				t.Fatal("a later gather mutated the previously returned allocation state")
			}
			snapshot.Revert()
			expectState(sets.New(device1, device2), "5")

			snapshot.Fork()
			snapshot.resourceClaims.DeleteCurrent(GetClaimId(firstShare))
			snapshot.resourceClaims.DeleteCurrent(GetClaimId(secondShare))
			expectState(sets.New(device2), "")
			snapshot.Commit()
			expectState(sets.New(device2), "")
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
