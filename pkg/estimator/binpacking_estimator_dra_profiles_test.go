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

package estimator

import (
	"fmt"
	"testing"
	"time"

	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/util/feature"
	featuretesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	drasnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/snapshot"
	drautils "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/utils"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"

	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// The profiles below are modelled on how DRA is actually deployed rather than on
// what is convenient to generate. Three things drive the shapes:
//
//   - A node usually runs more than one driver. A GKE GPU node can publish
//     ResourceSlices for gpu.nvidia.com, dra.net (NICs) and
//     compute-domain.nvidia.com at the same time, each with its own pool and
//     DeviceClass, so "one driver, one class" is the unrealistic case.
//   - Devices are split across ResourceSlices at ResourceSliceMaxDevices (128),
//     so a node exposing many small devices - MIG partitions being the obvious
//     case - publishes several slices rather than one.
//   - Claims are not uniform. Exclusive whole-GPU requests, fractional MIG
//     requests, whole-node multi-GPU training requests and claims shared by
//     several pods all occur, and they exercise different paths in the snapshot.
//
// Each profile is a cluster shape; the benchmark runs one scale-up estimation
// against it, which is what Cluster Autoscaler does per candidate node group.

// draDriverSpec is one driver publishing devices on every node.
type draDriverSpec struct {
	driver         string
	deviceClass    string
	devicesPerNode int
	attributesPer  int // extra attributes per device, to give CEL selectors something to match
}

// draProfile is a cluster shape plus the workload waiting to be scheduled onto it.
type draProfile struct {
	name  string
	nodes int

	drivers []draDriverSpec

	// Existing workload: claims already allocated against the fleet.
	allocatedClaimsPerNode int

	// Pending workload. Pods either own a claim each (the ResourceClaimTemplate
	// pattern) or share one claim between podsPerSharedClaim pods.
	pendingPods        int
	devicesPerClaim    int
	podsPerSharedClaim int

	// unallocatedExisting leaves the existing claims unallocated. Deriving from
	// them costs almost nothing, so there is correspondingly little to save.
	unallocatedExisting bool
	// pendingPodsWithoutClaims adds pods that use no DRA at all. The scheduler
	// skips the DRA PreFilter for them entirely.
	pendingPodsWithoutClaims int
}

const (
	gpuDriver     = "gpu.nvidia.com"
	gpuClass      = "gpu.nvidia.com"
	migClass      = "mig.nvidia.com"
	netDriver     = "dra.net"
	netClass      = "dra.net"
	cdDriver      = "compute-domain.nvidia.com"
	cdChannelName = "compute-domain-default-channel.nvidia.com"
)

func draProfiles() []draProfile {
	return []draProfile{
		{
			// The common inference shape: 8 whole GPUs per node, one per pod.
			name:                   "gpu8/exclusive",
			nodes:                  500,
			drivers:                []draDriverSpec{{driver: gpuDriver, deviceClass: gpuClass, devicesPerNode: 8, attributesPer: 4}},
			allocatedClaimsPerNode: 4,
			pendingPods:            300,
			devicesPerClaim:        1,
		},
		{
			// Distributed training: a pod takes every GPU on a node. Fewer, much
			// larger allocations.
			name:                   "gpu8/wholeNode",
			nodes:                  500,
			drivers:                []draDriverSpec{{driver: gpuDriver, deviceClass: gpuClass, devicesPerNode: 8, attributesPer: 4}},
			allocatedClaimsPerNode: 1,
			pendingPods:            200,
			devicesPerClaim:        8,
		},
		{
			// MIG: each of 8 GPUs partitioned 7 ways is 56 devices per node, which
			// crosses no slice boundary yet but multiplies the device count by 7.
			name:                   "mig56/fractional",
			nodes:                  300,
			drivers:                []draDriverSpec{{driver: gpuDriver, deviceClass: migClass, devicesPerNode: 56, attributesPer: 5}},
			allocatedClaimsPerNode: 20,
			pendingPods:            300,
			devicesPerClaim:        1,
		},
		{
			// More devices than fit in one ResourceSlice, so the node publishes
			// several - the shape a large MIG or NIC fleet produces.
			name:                   "devices256/multiSlice",
			nodes:                  200,
			drivers:                []draDriverSpec{{driver: gpuDriver, deviceClass: migClass, devicesPerNode: 256, attributesPer: 3}},
			allocatedClaimsPerNode: 40,
			pendingPods:            200,
			devicesPerClaim:        1,
		},
		{
			// What a GKE GPU node actually looks like: three drivers, four device
			// classes, three pools per node.
			name:  "multiDriver/gpu+net+computeDomain",
			nodes: 400,
			drivers: []draDriverSpec{
				{driver: gpuDriver, deviceClass: gpuClass, devicesPerNode: 8, attributesPer: 4},
				{driver: netDriver, deviceClass: netClass, devicesPerNode: 4, attributesPer: 6},
				{driver: cdDriver, deviceClass: cdChannelName, devicesPerNode: 2, attributesPer: 2},
			},
			allocatedClaimsPerNode: 4,
			pendingPods:            250,
			devicesPerClaim:        1,
		},
		{
			// Several pods against one claim - inference servers with sidecars, or
			// anything coordinating over a shared device.
			name:                   "gpu8/sharedClaims",
			nodes:                  400,
			drivers:                []draDriverSpec{{driver: gpuDriver, deviceClass: gpuClass, devicesPerNode: 8, attributesPer: 4}},
			allocatedClaimsPerNode: 4,
			pendingPods:            240,
			devicesPerClaim:        1,
			podsPerSharedClaim:     4,
		},

		// --- shapes where the maintained state is expected to gain little ---
		{
			// A large claim set read once or twice: the estimator places only a
			// handful of pods, so recomputing would have walked the claims about
			// as many times as the tracker builds them.
			name:                   "noGain/manyClaimsFewPods",
			nodes:                  400,
			drivers:                []draDriverSpec{{driver: gpuDriver, deviceClass: gpuClass, devicesPerNode: 8, attributesPer: 4}},
			allocatedClaimsPerNode: 30,
			pendingPods:            3,
			devicesPerClaim:        1,
		},
		{
			// Claims that are present but unallocated. foreachAllocatedDevice
			// returns immediately on a nil allocation, so the walk the tracker
			// removes was nearly free to begin with.
			name:                   "noGain/existingUnallocated",
			nodes:                  400,
			drivers:                []draDriverSpec{{driver: gpuDriver, deviceClass: gpuClass, devicesPerNode: 8, attributesPer: 4}},
			allocatedClaimsPerNode: 30,
			unallocatedExisting:    true,
			pendingPods:            200,
			devicesPerClaim:        1,
		},
		{
			// A DRA fleet whose pending workload does not use DRA. The DRA
			// PreFilter short-circuits for pods without claims, so the allocated
			// state is never asked for.
			name:                     "noGain/draFleetNonDraPods",
			nodes:                    400,
			drivers:                  []draDriverSpec{{driver: gpuDriver, deviceClass: gpuClass, devicesPerNode: 8, attributesPer: 4}},
			allocatedClaimsPerNode:   30,
			pendingPodsWithoutClaims: 300,
		},
	}
}

func draProfileDevice(driver string, index, extraAttributes int) resourceapi.Device {
	attrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
		"driverVersion": {StringValue: ptr.To("560.35.03")},
		"index":         {IntValue: ptr.To(int64(index))},
	}
	for a := 0; a < extraAttributes; a++ {
		attrs[resourceapi.QualifiedName(fmt.Sprintf("attr%d", a))] = resourceapi.DeviceAttribute{
			StringValue: ptr.To(fmt.Sprintf("value-%d", a)),
		}
	}
	return resourceapi.Device{Name: fmt.Sprintf("%s-dev-%d", driver, index), Attributes: attrs}
}

// draProfileSlices builds the ResourceSlices one driver publishes for one node,
// splitting at ResourceSliceMaxDevices the way a real driver does.
func draProfileSlices(spec draDriverSpec, nodeName string) []*resourceapi.ResourceSlice {
	devices := make([]resourceapi.Device, spec.devicesPerNode)
	for i := range devices {
		devices[i] = draProfileDevice(spec.driver, i, spec.attributesPer)
	}

	poolName := fmt.Sprintf("%s-%s", nodeName, spec.driver)
	var slices []*resourceapi.ResourceSlice
	for start := 0; start < len(devices); start += resourceapi.ResourceSliceMaxDevices {
		end := min(start+resourceapi.ResourceSliceMaxDevices, len(devices))
		name := fmt.Sprintf("%s-slice-%d", poolName, len(slices))
		slices = append(slices, &resourceapi.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name)},
			Spec: resourceapi.ResourceSliceSpec{
				NodeName: ptr.To(nodeName),
				Driver:   spec.driver,
				Pool:     resourceapi.ResourcePool{Name: poolName, ResourceSliceCount: 1},
				Devices:  devices[start:end],
			},
		})
	}
	for _, s := range slices {
		s.Spec.Pool.ResourceSliceCount = int64(len(slices))
	}
	return slices
}

// draProfileClaim builds an unallocated claim asking for count devices of a class,
// with a CEL selector so the allocator has to evaluate one per candidate device.
func draProfileClaim(name, deviceClass, driver string, count int) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name)},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{Requests: []resourceapi.DeviceRequest{{
				Name: "req",
				Exactly: &resourceapi.ExactDeviceRequest{
					DeviceClassName: deviceClass,
					Selectors: []resourceapi.DeviceSelector{{CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf(`device.driver == %q`, driver),
					}}},
					AllocationMode: resourceapi.DeviceAllocationModeExactCount,
					Count:          int64(count),
				},
			}}},
		},
	}
}

func draProfileAllocated(claim *resourceapi.ResourceClaim, nodeName, driver, pool string, deviceIndexes []int) *resourceapi.ResourceClaim {
	results := make([]resourceapi.DeviceRequestAllocationResult, 0, len(deviceIndexes))
	for _, idx := range deviceIndexes {
		results = append(results, resourceapi.DeviceRequestAllocationResult{
			Request: "req", Driver: driver, Pool: pool, Device: fmt.Sprintf("%s-dev-%d", driver, idx),
		})
	}
	return drautils.TestClaimWithAllocation(claim, &resourceapi.AllocationResult{
		NodeSelector: &apiv1.NodeSelector{NodeSelectorTerms: []apiv1.NodeSelectorTerm{{
			MatchFields: []apiv1.NodeSelectorRequirement{{
				Key: "metadata.name", Operator: apiv1.NodeSelectorOpIn, Values: []string{nodeName},
			}},
		}}},
		Devices: resourceapi.DeviceAllocationResult{Results: results},
	})
}

// BenchmarkBinpackingEstimateDRAProfiles runs one scale-up estimation against a
// range of realistic DRA cluster shapes. BenchmarkBinpackingEstimateDRA varies
// scale on a single shape; this varies the shape itself, because the cost of the
// DRA paths depends on the number of devices and slices and device classes as
// much as it does on the number of claims.
func BenchmarkBinpackingEstimateDRAProfiles(b *testing.B) {
	featuretesting.SetFeatureGateDuringTest(b, feature.DefaultFeatureGate, features.DynamicResourceAllocation, true)

	for _, profile := range draProfiles() {
		b.Run(profile.name, func(b *testing.B) {
			deviceClasses := map[string]*resourceapi.DeviceClass{}
			for _, d := range profile.drivers {
				deviceClasses[d.deviceClass] = &resourceapi.DeviceClass{
					ObjectMeta: metav1.ObjectMeta{Name: d.deviceClass, UID: types.UID(d.deviceClass)},
					Spec: resourceapi.DeviceClassSpec{Selectors: []resourceapi.DeviceSelector{{
						CEL: &resourceapi.CELDeviceSelector{Expression: fmt.Sprintf(`device.driver == %q`, d.driver)},
					}}},
				}
			}
			primary := profile.drivers[0]

			existingNodes := make([]*framework.NodeInfo, profile.nodes)
			var allocatedClaims []*resourceapi.ResourceClaim
			for n := 0; n < profile.nodes; n++ {
				nodeName := fmt.Sprintf("node-%d", n)
				var slices []*resourceapi.ResourceSlice
				for _, d := range profile.drivers {
					slices = append(slices, draProfileSlices(d, nodeName)...)
				}
				existingNodes[n] = framework.NewNodeInfo(makeNode(64000, 256000, 110, nodeName, "zone-a"), slices)

				// Existing allocations consume devices of the primary driver.
				for c := 0; c < profile.allocatedClaimsPerNode; c++ {
					claim := draProfileClaim(fmt.Sprintf("running-%d-%d", n, c), primary.deviceClass, primary.driver, 1)
					if profile.unallocatedExisting {
						allocatedClaims = append(allocatedClaims, claim)
						continue
					}
					allocatedClaims = append(allocatedClaims,
						draProfileAllocated(claim, nodeName, primary.driver, fmt.Sprintf("%s-%s", nodeName, primary.driver), []int{c % primary.devicesPerNode}))
				}
			}

			nodeTemplate := func() *framework.NodeInfo {
				var slices []*resourceapi.ResourceSlice
				for _, d := range profile.drivers {
					slices = append(slices, draProfileSlices(d, "template")...)
				}
				return framework.NewNodeInfo(makeNode(64000, 256000, 110, "template", "zone-a"), slices)
			}()

			// Pending workload. With podsPerSharedClaim > 0 several pods reference
			// one claim; otherwise each pod owns its own.
			var pendingPods []*apiv1.Pod
			var pendingClaims []*resourceapi.ResourceClaim
			if profile.podsPerSharedClaim > 0 {
				shared := (profile.pendingPods + profile.podsPerSharedClaim - 1) / profile.podsPerSharedClaim
				for s := 0; s < shared; s++ {
					claim := draProfileClaim(fmt.Sprintf("shared-%d", s), primary.deviceClass, primary.driver, profile.devicesPerClaim)
					pendingClaims = append(pendingClaims, claim)
					for p := 0; p < profile.podsPerSharedClaim && len(pendingPods) < profile.pendingPods; p++ {
						pendingPods = append(pendingPods, BuildTestPod(
							fmt.Sprintf("pending-%d-%d", s, p), 500, 1000,
							WithNamespace("default"), WithResourceClaim(claim.Name, claim.Name, "")))
					}
				}
			} else {
				for p := 0; p < profile.pendingPods; p++ {
					claim := draProfileClaim(fmt.Sprintf("pending-%d", p), primary.deviceClass, primary.driver, profile.devicesPerClaim)
					pod := BuildTestPod(fmt.Sprintf("pending-%d", p), 500, 1000,
						WithNamespace("default"), WithResourceClaim(claim.Name, claim.Name, ""))
					pendingClaims = append(pendingClaims, drautils.TestClaimWithPodOwnership(pod, claim))
					pendingPods = append(pendingPods, pod)
				}
			}
			for p := 0; p < profile.pendingPodsWithoutClaims; p++ {
				pendingPods = append(pendingPods,
					BuildTestPod(fmt.Sprintf("plain-%d", p), 500, 1000, WithNamespace("default")))
			}
			podEquivalenceGroups := []PodEquivalenceGroup{{Pods: pendingPods}}

			var estimatedNodes int
			var estimatedPods []*apiv1.Pod

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				fwHandle, err := framework.NewTestFrameworkHandle()
				if err != nil {
					b.Fatalf("NewTestFrameworkHandle(): unexpected error: %v", err)
				}
				clusterSnapshot := predicate.NewPredicateSnapshot(store.NewBasicSnapshotStore(), fwHandle, true, draBenchmarkPredicateWorker, false, 0 /* schedulerVerbosityOffset */)

				draSnapshot := drasnapshot.NewSnapshot(nil, nil, nil, deviceClasses)
				if err := draSnapshot.AddClaims(allocatedClaims); err != nil {
					b.Fatalf("AddClaims(): unexpected error for allocated claims: %v", err)
				}
				if err := draSnapshot.AddClaims(pendingClaims); err != nil {
					b.Fatalf("AddClaims(): unexpected error for pending claims: %v", err)
				}
				if err := clusterSnapshot.SetClusterState(nil, nil, draSnapshot, nil); err != nil {
					b.Fatalf("SetClusterState(): unexpected error: %v", err)
				}
				for _, nodeInfo := range existingNodes {
					if err := clusterSnapshot.AddNodeInfo(nodeInfo.DeepCopy()); err != nil {
						b.Fatalf("AddNodeInfo(): unexpected error: %v", err)
					}
				}

				limiter := NewThresholdBasedEstimationLimiter([]Threshold{NewStaticThreshold(1000, time.Duration(0))})
				estimator := NewBinpackingNodeEstimator(clusterSnapshot, limiter, NewDecreasingPodOrderer(), nil /* EstimationContext */, nil /* EstimationAnalyserFunc */, false)
				b.StartTimer()

				estimatedNodes, estimatedPods = estimator.Estimate(podEquivalenceGroups, nodeTemplate, nil)
			}
			b.StopTimer()

			if estimatedNodes == 0 || len(estimatedPods) == 0 {
				b.Fatalf("Estimate(): expected pods to be placed on new nodes, got %d nodes and %d pods", estimatedNodes, len(estimatedPods))
			}
		})
	}
}
