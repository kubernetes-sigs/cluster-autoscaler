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
	"context"
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
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	drasnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/snapshot"
	drautils "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/utils"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"

	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	draBenchmarkDriver          = "driver.example.com"
	draBenchmarkDeviceClass     = "gpu"
	draBenchmarkDevicesPerNode  = 8
	draBenchmarkPredicateWorker = 4 // Matches the --predicate-parallelism default.
)

// draBenchmarkSlice builds the ResourceSlice advertising a node's devices.
func draBenchmarkSlice(nodeName string, devices int) *resourceapi.ResourceSlice {
	deviceList := make([]resourceapi.Device, devices)
	for i := 0; i < devices; i++ {
		deviceList[i] = resourceapi.Device{Name: fmt.Sprintf("gpu-%d", i)}
	}
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "slice-" + nodeName, UID: types.UID("slice-" + nodeName)},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Driver:   draBenchmarkDriver,
			Pool:     resourceapi.ResourcePool{Name: nodeName, ResourceSliceCount: 1},
			Devices:  deviceList,
		},
	}
}

// draBenchmarkClaim builds an unallocated ResourceClaim asking for a single device.
func draBenchmarkClaim(name string) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name)},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{Requests: []resourceapi.DeviceRequest{{
				Name: "req",
				Exactly: &resourceapi.ExactDeviceRequest{
					DeviceClassName: draBenchmarkDeviceClass,
					Selectors: []resourceapi.DeviceSelector{{CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf(`device.driver == "%s"`, draBenchmarkDriver),
					}}},
					AllocationMode: resourceapi.DeviceAllocationModeExactCount,
					Count:          1,
				},
			}}},
		},
	}
}

// draBenchmarkAllocatedClaim marks the claim as already holding one device of the node's pool,
// standing in for a claim of a pod that is already running.
func draBenchmarkAllocatedClaim(claim *resourceapi.ResourceClaim, nodeName string, deviceIndex int) *resourceapi.ResourceClaim {
	return drautils.TestClaimWithAllocation(claim, &resourceapi.AllocationResult{
		NodeSelector: &apiv1.NodeSelector{NodeSelectorTerms: []apiv1.NodeSelectorTerm{{
			MatchFields: []apiv1.NodeSelectorRequirement{{
				Key: "metadata.name", Operator: apiv1.NodeSelectorOpIn, Values: []string{nodeName},
			}},
		}}},
		Devices: resourceapi.DeviceAllocationResult{Results: []resourceapi.DeviceRequestAllocationResult{{
			Request: "req", Driver: draBenchmarkDriver, Pool: nodeName, Device: fmt.Sprintf("gpu-%d", deviceIndex),
		}}},
	})
}

// BenchmarkBinpackingEstimateDRA measures one scale-up estimation - what Cluster Autoscaler
// runs for a single node group in ComputeExpansionOption - against a cluster that already
// carries a lot of DRA workload.
//
// The cluster is shaped like a GPU fleet: every node advertises devices through a
// ResourceSlice, and part of the fleet is already consumed by allocated ResourceClaims, so the
// DRA snapshot holds nodes*claimsPerNode of them.
//
// The pending pods deliberately own *unallocated* claims. That is what makes this benchmark
// representative: with pre-allocated claims the scheduler reuses the existing allocation and
// never runs the structured allocator, so the whole PreFilter path that gathers the allocated
// state of every claim is skipped and the benchmark stops measuring the interesting part.
//
// Note that a real Cluster Autoscaler loop pays this once per candidate node group, so the
// numbers here have to be multiplied by the number of node groups being considered.
func BenchmarkBinpackingEstimateDRA(b *testing.B) {
	featuretesting.SetFeatureGateDuringTest(b, feature.DefaultFeatureGate, features.DynamicResourceAllocation, true)

	for _, scenario := range []struct {
		name          string
		nodes         int
		claimsPerNode int
		pendingPods   int
	}{
		{"nodes=1000/claims=4000/pendingPods=200", 1000, 4, 200},
		{"nodes=5000/claims=20000/pendingPods=500", 5000, 4, 500},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			deviceClasses := map[string]*resourceapi.DeviceClass{
				draBenchmarkDeviceClass: {ObjectMeta: metav1.ObjectMeta{Name: draBenchmarkDeviceClass, UID: "gpu-class"}},
			}

			// The existing fleet, with part of its devices already handed out.
			existingNodes := make([]*framework.NodeInfo, scenario.nodes)
			allocatedClaims := make([]*resourceapi.ResourceClaim, 0, scenario.nodes*scenario.claimsPerNode)
			for nodeIndex := 0; nodeIndex < scenario.nodes; nodeIndex++ {
				nodeName := fmt.Sprintf("node-%d", nodeIndex)
				node := makeNode(16000, 64000, 110, nodeName, "zone-a")
				slice := draBenchmarkSlice(nodeName, draBenchmarkDevicesPerNode)
				existingNodes[nodeIndex] = framework.NewNodeInfo(node, []*resourceapi.ResourceSlice{slice})

				for claimIndex := 0; claimIndex < scenario.claimsPerNode; claimIndex++ {
					claim := draBenchmarkClaim(fmt.Sprintf("running-%d-%d", nodeIndex, claimIndex))
					allocatedClaims = append(allocatedClaims, draBenchmarkAllocatedClaim(claim, nodeName, claimIndex))
				}
			}

			// The template of the node group being scaled up. The estimator sanitizes it per
			// added node, so each new node gets its own device pool.
			nodeTemplate := framework.NewNodeInfo(
				makeNode(16000, 64000, 110, "template", "zone-a"),
				[]*resourceapi.ResourceSlice{draBenchmarkSlice("template", draBenchmarkDevicesPerNode)},
			)

			// The pods waiting for capacity, each owning an unallocated claim.
			pendingPods := make([]*apiv1.Pod, scenario.pendingPods)
			pendingClaims := make([]*resourceapi.ResourceClaim, scenario.pendingPods)
			for podIndex := 0; podIndex < scenario.pendingPods; podIndex++ {
				claim := draBenchmarkClaim(fmt.Sprintf("pending-%d", podIndex))
				pod := BuildTestPod(
					fmt.Sprintf("pending-pod-%d", podIndex), 500, 1000,
					WithNamespace("default"),
					WithResourceClaim(claim.Name, claim.Name, ""),
				)
				pendingClaims[podIndex] = drautils.TestClaimWithPodOwnership(pod, claim)
				pendingPods[podIndex] = pod
			}
			podEquivalenceGroups := []PodEquivalenceGroup{{Pods: pendingPods}}

			var estimatedNodes int
			var estimatedPods []*apiv1.Pod

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Building the snapshot is not what's being measured here, and the estimation
				// mutates it, so every iteration gets a fresh one with the timer stopped.
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
				if err := clusterSnapshot.SetClusterState(context.Background(), nil, nil, draSnapshot, nil); err != nil {
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

				estimatedNodes, estimatedPods = estimator.Estimate(context.Background(), podEquivalenceGroups, nodeTemplate, nil)
			}
			b.StopTimer()

			// A run that places nothing would measure the wrong thing entirely.
			if estimatedNodes == 0 || len(estimatedPods) == 0 {
				b.Fatalf("Estimate(): expected pods to be placed on new nodes, got %d nodes and %d pods", estimatedNodes, len(estimatedPods))
			}
		})
	}
}
