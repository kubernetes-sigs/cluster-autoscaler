/*
Copyright The Kubernetes Authors.

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

package inmemory

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	fakecloudprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	// minSize is the min size of the node group in the min size tests.
	minSize = 2
	// extraInstances is the number of instances above the healthy nodes that the reconciliation operation
	// in flight removes from the node group in the min size tests.
	extraInstances       = 2
	maxNodeProvisionTime = 10 * time.Second
	cloudOperationDelay  = 10 * time.Minute
)

// reconciliationOperation is a background reconciliation operation a node group can have in flight.
type reconciliationOperation struct {
	name string
	// setUp adds the node group with healthyNodes healthy nodes, each pinned by a pod, and extraInstances
	// instances for the operation to remove. Its cloud provider operations take cloudOperationDelay. It
	// returns the node group and the pinning pods.
	setUp func(fakes *integration.FakeSet, healthyNodes int) (*fakecloudprovider.NodeGroup, []*apiv1.Pod)
	// extraInstancesExist tells whether the extra instances exist in the cloud provider until the operation
	// removes them, as opposed to the target size being inflated without them.
	extraInstancesExist bool
	// startsAfter is how long after the autoscaler first observes the node group the operation starts.
	startsAfter time.Duration
}

// instancesBeforeCompletion returns the number of instances in a node group with healthyNodes healthy nodes
// before the operation completes.
func (op reconciliationOperation) instancesBeforeCompletion(healthyNodes int) int {
	if op.extraInstancesExist {
		return healthyNodes + extraInstances
	}
	return healthyNodes
}

var reconciliationOperations = []reconciliationOperation{
	{
		name: "target size decrease",
		setUp: func(fakes *integration.FakeSet, healthyNodes int) (*fakecloudprovider.NodeGroup, []*apiv1.Pod) {
			ng, pins := addNodeGroupWithPinnedNodes(fakes, "ng", healthyNodes, minSize, fakecloudprovider.WithCloudOperationDelay(cloudOperationDelay))
			// The target size exceeds the number of instances, e.g. because the cloud provider failed to create
			// them without reporting it.
			ng.SetTargetSize(healthyNodes + extraInstances)
			return ng, pins
		},
		startsAfter: maxNodeProvisionTime,
	},
	{
		name: "removal of unregistered nodes",
		setUp: func(fakes *integration.FakeSet, healthyNodes int) (*fakecloudprovider.NodeGroup, []*apiv1.Pod) {
			ng, pins := addNodeGroupWithPinnedNodes(fakes, "ng", healthyNodes+extraInstances, minSize, fakecloudprovider.WithCloudOperationDelay(cloudOperationDelay))
			// extraInstances nodes disappear from k8s while their instances stay in the cloud provider, i.e. the
			// instances become unregistered.
			for _, pin := range pins[healthyNodes:] {
				fakes.K8s.DeletePod(pin.Namespace, pin.Name)
				fakes.K8s.DeleteNode(pin.Spec.NodeName)
			}
			return ng, pins[:healthyNodes]
		},
		extraInstancesExist: true,
		startsAfter:         maxNodeProvisionTime,
	},
	{
		name: "deletion of nodes with create errors",
		setUp: func(fakes *integration.FakeSet, healthyNodes int) (*fakecloudprovider.NodeGroup, []*apiv1.Pod) {
			return addNodeGroupWithPinnedNodes(fakes, "ng", healthyNodes, minSize, fakecloudprovider.WithCloudOperationDelay(cloudOperationDelay), fakecloudprovider.WithFailedInstances(extraInstances))
		},
		extraInstancesExist: true,
		// Nodes with create errors are deleted as soon as they're observed.
		startsAfter: 0,
	},
}

// scaleDownTiming tells when the healthy nodes of the node group become removable, relative to the
// iteration that starts the reconciliation operation.
type scaleDownTiming struct {
	name         string
	unneededTime time.Duration
}

var scaleDownTimings = []scaleDownTiming{
	// The healthy nodes become removable in the iteration that starts the operation.
	{name: "scale-down in the same iteration", unneededTime: 0},
	// The healthy nodes become removable in a later iteration, while the operation is still in flight.
	{name: "scale-down in a later iteration", unneededTime: time.Minute},
}

// TestScaleDown_RespectsMinSizeWhileNodeGroupOperationInFlight verifies that scale-down doesn't take a
// node group below its min size while a background reconciliation operation on the node group - a
// target size decrease (fixNodeGroupSize), a removal of long unregistered nodes or a deletion of nodes
// with create errors - is in flight: until the operation completes, the node group's target size still
// counts the instances the operation is about to remove, which must not be mistaken for room to remove
// healthy nodes. It also verifies that only the min size is protected: the operation doesn't hold up
// the removal of the unneeded nodes above it.
func TestScaleDown_RespectsMinSizeWhileNodeGroupOperationInFlight(t *testing.T) {
	for _, op := range reconciliationOperations {
		for _, sd := range scaleDownTimings {
			// surplusNodes is the number of healthy nodes above the min size, which are unneeded and removable.
			for _, surplusNodes := range []int{0, 1} {
				t.Run(fmt.Sprintf("%s, %s, %d surplus nodes", op.name, sd.name, surplusNodes), func(t *testing.T) {
					testScaleDownWhileNodeGroupOperationInFlight(t, op, sd, surplusNodes)
				})
			}
		}
	}
}

// testScaleDownWhileNodeGroupOperationInFlight runs one case of
// TestScaleDown_RespectsMinSizeWhileNodeGroupOperationInFlight: a node group with minSize+surplusNodes
// healthy nodes, which become unneeded right before op starts, has to end up at its min size, with the
// surplus nodes removed while op is in flight.
func testScaleDownWhileNodeGroupOperationInFlight(t *testing.T, op reconciliationOperation, sd scaleDownTiming, surplusNodes int) {
	options := integration.NewTestConfig().WithOverrides(
		integration.WithMaxNodeProvisionTime(maxNodeProvisionTime),
		integration.WithScaleDownUnneededTime(sd.unneededTime),
	).ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes
	// Unregistered nodes have no Node objects, CA identifies them by their instance IDs only.
	fakes.CloudProvider.ConfigureNodeGroupForNodeBehavior(true)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := test.GetTestContextWithCancel(t)
		defer synctestutils.TearDown(cancel)
		// Let the cloud provider operations possibly started by the last iteration complete before tearing
		// down, so that a failure doesn't leave the bubble deadlocked.
		defer synctestutils.AdvanceTime(cloudOperationDelay + time.Second)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		require.NoError(t, err)

		healthyNodeCount := minSize + surplusNodes
		ng, pins := op.setUp(fakes, healthyNodeCount)
		healthyNodes := pinnedNodeNames(pins)

		if op.startsAfter > 0 {
			// The autoscaler observes the node group, the operation starts once the state persists for
			// op.startsAfter.
			synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		}
		// All the healthy nodes become empty, and thus unneeded, right before the operation starts.
		unpin(fakes, pins)

		// The operation starts in the background and stays in flight for cloudOperationDelay. The node
		// group's target size still exceeds its min size by extraInstances on top of the surplus nodes,
		// which scale-down must not mistake for room to remove more than the surplus nodes.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, op.startsAfter+time.Second)
		if sd.unneededTime > 0 {
			synctestutils.MustRunOnceAfter(ctx, t, autoscaler, sd.unneededTime+time.Second)
		}
		assertNodeGroupSize(ctx, t, ng, healthyNodeCount+extraInstances, op.instancesBeforeCompletion(healthyNodeCount), "the operation should still be in flight")
		assertNodesBeingDeleted(t, fakes, healthyNodes, surplusNodes, "only the surplus nodes should be removed while the operation is in flight")

		// The operation, and the removal of the surplus nodes, complete.
		synctestutils.AdvanceTime(cloudOperationDelay + time.Second)
		assertNodeGroupSize(ctx, t, ng, minSize, minSize, "the operation and the removal of the surplus nodes should have completed")
		assertSomeOfNodes(t, fakes, healthyNodes, minSize, "only the surplus nodes should be removed")

		// The node group stays at its min size.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assertNodeGroupSize(ctx, t, ng, minSize, minSize, "the node group should stay at its min size")
		assertSomeOfNodes(t, fakes, healthyNodes, minSize, "the node group should stay at its min size")
	})
}

// TestScaleDown_RespectsMinSizeWhileScaleDownDeletionAndNodeGroupOperationInFlight verifies that the
// min size check of scale-down counts the scale-down deletions in flight in a node group and the
// background reconciliation operation in flight on it together: with the node group's target size
// minus both at the min size, one more node becoming unneeded must stay. (The operation is a target
// size decrease, its kind doesn't matter here: the kinds are covered individually by
// TestScaleDown_RespectsMinSizeWhileNodeGroupOperationInFlight.)
func TestScaleDown_RespectsMinSizeWhileScaleDownDeletionAndNodeGroupOperationInFlight(t *testing.T) {
	const (
		unneededTime = time.Second
		// phantomInstances is the excess of the node group's target size over its instances, which the
		// target size decrease in flight removes.
		phantomInstances = 1
	)
	options := integration.NewTestConfig().WithOverrides(
		integration.WithMaxNodeProvisionTime(maxNodeProvisionTime),
		integration.WithScaleDownUnneededTime(unneededTime),
	).ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := test.GetTestContextWithCancel(t)
		defer synctestutils.TearDown(cancel)
		// Let the cloud provider operations possibly started by the last iteration complete before tearing
		// down, so that a failure doesn't leave the bubble deadlocked.
		defer synctestutils.AdvanceTime(cloudOperationDelay + time.Second)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		require.NoError(t, err)

		// The node group has one healthy node above its min size, which is unneeded, and a target size
		// exceeding its instances by phantomInstances, e.g. because the cloud provider failed to create them
		// without reporting it.
		ng, pins := addNodeGroupWithPinnedNodes(fakes, "ng", minSize+1, minSize, fakecloudprovider.WithCloudOperationDelay(cloudOperationDelay))
		ng.SetTargetSize(minSize + 1 + phantomInstances)
		allNodes := pinnedNodeNames(pins)
		nodesToKeep := allNodes[:minSize]
		unpin(fakes, pins[minSize:])

		// The autoscaler observes the node group, and removes the unneeded node once it's unneeded for
		// unneededTime. The deletion stays in flight for cloudOperationDelay.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, unneededTime+time.Second)
		assertNodeGroupSize(ctx, t, ng, minSize+1+phantomInstances, minSize+1, "the scale-down deletion should be in flight")
		assertNodesBeingDeleted(t, fakes, allNodes, 1, "the unneeded node should be deleted")
		assertNodesIntact(t, fakes, nodesToKeep, "only the unneeded node should be deleted")

		// One of the remaining nodes becomes unneeded too, and the target size decrease starts once the
		// incorrect target size persists for maxNodeProvisionTime. The node group's target size minus the
		// deletion and the decrease in flight is its min size, so the node must stay, both in the iteration
		// that starts the decrease and in the later ones.
		unpin(fakes, pins[:1])
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, maxNodeProvisionTime)
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assertNodeGroupSize(ctx, t, ng, minSize+1+phantomInstances, minSize+1, "the scale-down deletion and the target size decrease should be in flight")
		assertNodesBeingDeleted(t, fakes, allNodes, 1, "no other node should be deleted while the scale-down deletion and the target size decrease are in flight")
		assertNodesIntact(t, fakes, nodesToKeep, "no other node should be deleted while the scale-down deletion and the target size decrease are in flight")

		// The deletion and the decrease complete.
		synctestutils.AdvanceTime(cloudOperationDelay + time.Second)
		assertNodeGroupSize(ctx, t, ng, minSize, minSize, "the scale-down deletion and the target size decrease should have completed")
		assertOnlyNodes(t, fakes, nodesToKeep, "only the unneeded node should be deleted")

		// The node group stays at its min size.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assertNodeGroupSize(ctx, t, ng, minSize, minSize, "the node group should stay at its min size")
		assertOnlyNodes(t, fakes, nodesToKeep, "the node group should stay at its min size")
	})
}

// addNodeGroupWithPinnedNodes adds a node group with nodeCount ready nodes, each running a pod that keeps
// it from being unneeded, and returns the node group and the pinning pods, in node order.
func addNodeGroupWithPinnedNodes(fakes *integration.FakeSet, id string, nodeCount, minSize int, opts ...fakecloudprovider.NodeGroupOption) (*fakecloudprovider.NodeGroup, []*apiv1.Pod) {
	template := test.BuildTestNode(id+"-node", 1000, 1000, test.IsReady(true))
	opts = append([]fakecloudprovider.NodeGroupOption{fakecloudprovider.WithNodes(template, nodeCount), fakecloudprovider.WithNGSize(minSize, 10)}, opts...)
	ng := fakes.CloudProvider.AddNodeGroup(id, opts...)
	var pins []*apiv1.Pod
	for i := range nodeCount {
		pin := test.BuildScheduledTestPod(fmt.Sprintf("%s-pin-%d", id, i), 600, 100, fmt.Sprintf("%s-node-%d", id, i))
		fakes.K8s.AddPod(pin)
		pins = append(pins, pin)
	}
	return ng, pins
}

// unpin deletes the given pinning pods, making their nodes empty and thus unneeded.
func unpin(fakes *integration.FakeSet, pins []*apiv1.Pod) {
	for _, pin := range pins {
		fakes.K8s.DeletePod(pin.Namespace, pin.Name)
	}
}

// pinnedNodeNames returns the names of the nodes the given pods are pinning.
func pinnedNodeNames(pins []*apiv1.Pod) []string {
	var names []string
	for _, pin := range pins {
		names = append(names, pin.Spec.NodeName)
	}
	return names
}

// nodeGroupSize returns the target size and the number of instances of the node group.
func nodeGroupSize(ctx context.Context, t *testing.T, ng *fakecloudprovider.NodeGroup) (targetSize, instances int) {
	t.Helper()
	targetSize, err := ng.TargetSize(ctx)
	require.NoError(t, err)
	nodes, err := ng.Nodes(ctx)
	require.NoError(t, err)
	return targetSize, len(nodes)
}

// assertNodeGroupSize asserts the target size and the number of instances of the node group.
func assertNodeGroupSize(ctx context.Context, t *testing.T, ng *fakecloudprovider.NodeGroup, wantTargetSize, wantInstances int, msg string) {
	t.Helper()
	targetSize, instances := nodeGroupSize(ctx, t, ng)
	assert.Equal(t, wantTargetSize, targetSize, "%s: target size", msg)
	assert.Equal(t, wantInstances, instances, "%s: instances", msg)
}

// assertNodesIntact asserts that the given nodes exist and aren't being deleted by the autoscaler.
func assertNodesIntact(t *testing.T, fakes *integration.FakeSet, want []string, msg string) {
	t.Helper()
	existing, beingDeleted := nodesByDeletionState(fakes)
	for _, name := range want {
		assert.Contains(t, existing, name, "%s: node %s should exist", msg, name)
		assert.NotContains(t, beingDeleted, name, "%s: node %s shouldn't be deleted", msg, name)
	}
}

// assertOnlyNodes asserts that exactly the given nodes exist and none of them is being deleted by the
// autoscaler.
func assertOnlyNodes(t *testing.T, fakes *integration.FakeSet, want []string, msg string) {
	t.Helper()
	existing, beingDeleted := nodesByDeletionState(fakes)
	want = slices.Sorted(slices.Values(want))
	assert.Equal(t, want, existing, "%s: nodes", msg)
	assert.Empty(t, beingDeleted, "%s: nodes being deleted", msg)
}

// assertSomeOfNodes asserts that wantCount of the given nodes, and no other nodes, exist, and that none of
// them is being deleted by the autoscaler.
func assertSomeOfNodes(t *testing.T, fakes *integration.FakeSet, candidates []string, wantCount int, msg string) {
	t.Helper()
	existing, beingDeleted := nodesByDeletionState(fakes)
	assert.Subset(t, candidates, existing, "%s: nodes", msg)
	assert.Len(t, existing, wantCount, "%s: nodes", msg)
	assert.Empty(t, beingDeleted, "%s: nodes being deleted", msg)
}

// assertNodesBeingDeleted asserts that all the given nodes exist and that exactly wantCount of them are
// being deleted by the autoscaler.
func assertNodesBeingDeleted(t *testing.T, fakes *integration.FakeSet, nodes []string, wantCount int, msg string) {
	t.Helper()
	existing, beingDeleted := nodesByDeletionState(fakes)
	assert.Subset(t, existing, nodes, "%s: nodes", msg)
	assert.Subset(t, nodes, beingDeleted, "%s: nodes being deleted", msg)
	assert.Len(t, beingDeleted, wantCount, "%s: nodes being deleted", msg)
}

// nodesByDeletionState returns the sorted names of all nodes, and of the nodes being deleted by the
// autoscaler (tainted with ToBeDeletedByClusterAutoscaler).
func nodesByDeletionState(fakes *integration.FakeSet) (existing, beingDeleted []string) {
	for _, node := range fakes.K8s.Nodes().Items {
		existing = append(existing, node.Name)
		if taints.HasToBeDeletedTaint(&node) {
			beingDeleted = append(beingDeleted, node.Name)
		}
	}
	slices.Sort(existing)
	slices.Sort(beingDeleted)
	return existing, beingDeleted
}
