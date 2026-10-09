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
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgotesting "k8s.io/client-go/testing"
	fakecloudprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	plannerTestUnneededTime = 1 * time.Minute
	plannerStepDuration     = 10 * time.Second
)

// TestPlanner_AtomicAndNonAtomicScaleDown verifies that the scaledown planner
// correctly evaluates and processes both atomic and non-atomic node groups.
func TestPlanner_AtomicAndNonAtomicScaleDown(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithScaleDownUnneededTime(plannerTestUnneededTime),
		)

	options := testConfig.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		// Create non-atomic node group with 2 nodes.
		naTemplate := test.BuildTestNode("node-na", 1000, 1000, test.IsReady(true))
		fakes.CloudProvider.AddNodeGroup("ng-nonatomic", fakecloudprovider.WithNodes(naTemplate, 2))

		// Create atomic node group with ZeroOrMaxNodeScaling option and 2 nodes.
		aTemplate := test.BuildTestNode("node-a", 1000, 1000, test.IsReady(true))
		atomicOpts := &config.NodeGroupAutoscalingOptions{
			ZeroOrMaxNodeScaling: true,
		}
		fakes.CloudProvider.AddNodeGroup("ng-atomic",
			fakecloudprovider.WithNodes(aTemplate, 2),
			fakecloudprovider.WithNGOptions(atomicOpts),
		)

		// Initial size check
		sizeNonAtomic, _ := fakes.CloudProvider.GetNodeGroup("ng-nonatomic").TargetSize(ctx)
		assert.Equal(t, 2, sizeNonAtomic)
		sizeAtomic, _ := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
		assert.Equal(t, 2, sizeAtomic)

		// Run CA loop once to mark empty nodes as unneeded.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerStepDuration)

		// Advance time past unneededTime to allow scale down actuation.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerTestUnneededTime+time.Second)

		// Run loop again to complete removal of all unneeded nodes.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerStepDuration)

		// Verify both non-atomic and atomic empty nodes are scaled down.
		finalSizeNonAtomic, _ := fakes.CloudProvider.GetNodeGroup("ng-nonatomic").TargetSize(ctx)
		assert.Equal(t, 0, finalSizeNonAtomic, "Non-atomic empty nodes should be scaled down")

		finalSizeAtomic, _ := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
		assert.Equal(t, 0, finalSizeAtomic, "Atomic empty nodes should be scaled down")
	})
}

// TestPlanner_AtomicNodesProcessedAfterNonAtomicLimit verifies that when non-atomic simulation
// hits the unneededNodesLimit, remaining non-atomic nodes are skipped, but atomic nodes are still
// evaluated, i.e. atomic node groups are not subject to the limit.
func TestPlanner_AtomicNodesProcessedAfterNonAtomicLimit(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithScaleDownUnneededTime(plannerTestUnneededTime),
			func(o *config.AutoscalingOptions) {
				// Set MaxScaleDownParallelism to 1 so unneededNodesLimit() is 2 in the first loop.
				o.MaxScaleDownParallelism = 1
			},
		)

	options := testConfig.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		registerEvictionReactor(fakes)

		// Create 4 empty non-atomic nodes (exceeding limit of 2) in ng-nonatomic.
		naTemplate := test.BuildTestNode("node-na", 1000, 1000, test.IsReady(true))
		fakes.CloudProvider.AddNodeGroup("ng-nonatomic", fakecloudprovider.WithNodes(naTemplate, 4))

		// Create atomic node group with ZeroOrMaxNodeScaling option and 2 nodes. The options are
		// based on the node group defaults, so that non-empty atomic nodes aren't filtered out as
		// not underutilized.
		aTemplate := test.BuildTestNode("node-a", 1000, 1000, test.IsReady(true))
		atomicOpts := options.NodeGroupDefaults
		atomicOpts.ZeroOrMaxNodeScaling = true
		fakes.CloudProvider.AddNodeGroup("ng-atomic",
			fakecloudprovider.WithNodes(aTemplate, 2),
			fakecloudprovider.WithNGOptions(&atomicOpts),
		)

		// Destination node, not part of any node group, with room for the pods from atomic nodes.
		fakes.K8s.AddNode(test.BuildTestNode("dest-node", 1000, 1000, test.IsReady(true)))

		// Atomic nodes are not empty, so EmptySorting orders them after all empty non-atomic nodes.
		// This way the non-atomic nodes exhaust the limit before the atomic group is simulated.
		fakes.K8s.AddPod(test.SetRSPodSpec(test.BuildScheduledTestPod("pod-a-0", 100, 100, "ng-atomic-node-0"), "rs-a-0"))
		fakes.K8s.AddPod(test.SetRSPodSpec(test.BuildScheduledTestPod("pod-a-1", 100, 100, "ng-atomic-node-1"), "rs-a-1"))

		// Initial verification of target sizes
		sizeNA, _ := fakes.CloudProvider.GetNodeGroup("ng-nonatomic").TargetSize(ctx)
		assert.Equal(t, 4, sizeNA)
		sizeA, _ := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
		assert.Equal(t, 2, sizeA)

		// Run loop to mark unneeded.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerStepDuration)

		// Advance past unneeded time and run scale down loop. Only nodes marked as unneeded in
		// the first loop are unneeded for long enough to be scaled down.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerTestUnneededTime+time.Second)

		// Run loop to actuate removal. It's too early for nodes marked as unneeded in the
		// previous loop to be scaled down.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerStepDuration)

		// Atomic nodes were marked as unneeded in the first loop, even though the limit was
		// already reached by non-atomic nodes.
		finalSizeAtomic, _ := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
		assert.Equal(t, 0, finalSizeAtomic, "Atomic nodes should be evaluated and scaled down even when non-atomic limit is reached")
	})
}

// TestPlanner_SimulationTimeoutSkipsAtomicAndNonAtomic verifies that a simulation timeout
// during non-atomic loop properly skips remaining non-atomic nodes and all atomic nodes.
func TestPlanner_SimulationTimeoutSkipsAtomicAndNonAtomic(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithScaleDownUnneededTime(plannerTestUnneededTime),
			func(o *config.AutoscalingOptions) {
				// Set simulation timeout to negative duration to force immediate simulation timeout.
				o.ScaleDownSimulationTimeout = -1 * time.Second
			},
		)

	options := testConfig.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		// Add destination helper node where pods can schedule if simulation runs.
		destNode := test.BuildTestNode("dest-node", 10000, 10000, test.IsReady(true))
		fakes.K8s.AddNode(destNode)

		naTemplate := test.BuildTestNode("node-na", 1000, 1000, test.IsReady(true))
		fakes.CloudProvider.AddNodeGroup("ng-nonatomic", fakecloudprovider.WithNodes(naTemplate, 2))

		aTemplate := test.BuildTestNode("node-a", 1000, 1000, test.IsReady(true))
		atomicOpts := &config.NodeGroupAutoscalingOptions{
			ZeroOrMaxNodeScaling: true,
		}
		fakes.CloudProvider.AddNodeGroup("ng-atomic",
			fakecloudprovider.WithNodes(aTemplate, 2),
			fakecloudprovider.WithNGOptions(atomicOpts),
		)

		// Add scheduled pods to nodes so scale down simulation is required.
		fakes.K8s.AddPod(test.BuildScheduledTestPod("pod-na-0", 100, 100, "ng-nonatomic-node-0"))
		fakes.K8s.AddPod(test.BuildScheduledTestPod("pod-na-1", 100, 100, "ng-nonatomic-node-1"))
		fakes.K8s.AddPod(test.BuildScheduledTestPod("pod-a-0", 100, 100, "ng-atomic-node-0"))
		fakes.K8s.AddPod(test.BuildScheduledTestPod("pod-a-1", 100, 100, "ng-atomic-node-1"))

		// Run CA loop. With simulation timeout <= 0, nodes should be skipped.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerStepDuration)
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerTestUnneededTime+time.Second)

		// Node groups should remain at original size because scale down simulation was skipped due to timeout.
		sizeNA, _ := fakes.CloudProvider.GetNodeGroup("ng-nonatomic").TargetSize(ctx)
		assert.Equal(t, 2, sizeNA)
		sizeA, _ := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
		assert.Equal(t, 2, sizeA)
	})
}

// TestPlanner_AtomicNodePdbHandling verifies that an atomic node group doesn't scale down when
// its nodes host pods protected by a PDB that doesn't allow any disruptions before the simulation.
func TestPlanner_AtomicNodePdbHandling(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithScaleDownUnneededTime(plannerTestUnneededTime),
		)

	options := testConfig.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		registerEvictionReactor(fakes)

		destNode := test.BuildTestNode("dest-node", 10000, 10000, test.IsReady(true))
		fakes.K8s.AddNode(destNode)

		// The options are based on the node group defaults, so that non-empty atomic nodes aren't
		// filtered out as not underutilized before the simulation.
		aTemplate := test.BuildTestNode("node-a", 1000, 1000, test.IsReady(true))
		atomicOpts := options.NodeGroupDefaults
		atomicOpts.ZeroOrMaxNodeScaling = true
		fakes.CloudProvider.AddNodeGroup("ng-atomic",
			fakecloudprovider.WithNodes(aTemplate, 2),
			fakecloudprovider.WithNGOptions(&atomicOpts),
		)

		// Create PDB that doesn't allow any disruptions.
		maxUnavailable := intstr.FromInt32(1)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-pdb",
				Namespace: "default",
			},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &maxUnavailable,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "test"},
				},
			},
			Status: policyv1.PodDisruptionBudgetStatus{
				DisruptionsAllowed: 0,
			},
		}
		_, err = fakes.KubeClient.PolicyV1().PodDisruptionBudgets("default").Create(ctx, pdb, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Pods are replicated, so the PDB is the only thing blocking their eviction.
		pod0 := test.SetRSPodSpec(test.BuildScheduledTestPod("pod-a-0", 100, 100, "ng-atomic-node-0"), "rs-a")
		pod0.Labels = map[string]string{"app": "test"}
		pod1 := test.SetRSPodSpec(test.BuildScheduledTestPod("pod-a-1", 100, 100, "ng-atomic-node-1"), "rs-a")
		pod1.Labels = map[string]string{"app": "test"}
		fakes.K8s.AddPod(pod0)
		fakes.K8s.AddPod(pod1)

		// Run CA loop once to mark unneeded and run simulation.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerStepDuration)
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerTestUnneededTime+time.Second)

		// Run loop to actuate.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerStepDuration)

		sizeA, _ := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
		assert.Equal(t, 2, sizeA, "Atomic node group should not scale down when PDB doesn't allow any disruptions")
	})
}

// TestPlanner_IncompleteAtomicNodeGroupPrefiltered verifies that an atomic node group
// with fewer unneeded nodes than its target size is prefiltered out before simulation,
// and its target size remains unchanged.
func TestPlanner_IncompleteAtomicNodeGroupPrefiltered(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithScaleDownUnneededTime(plannerTestUnneededTime),
			func(o *config.AutoscalingOptions) {
				// Enable soft tainting of unneeded nodes (flag defaults), so that it's observable
				// which nodes were marked as unneeded.
				o.MaxBulkSoftTaintCount = 10
				o.MaxBulkSoftTaintTime = 3 * time.Second
			},
		)

	options := testConfig.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		// Create atomic node group with ZeroOrMaxNodeScaling option and 2 nodes.
		aTemplate := test.BuildTestNode("node", 1000, 1000, test.IsReady(true))
		atomicOpts := &config.NodeGroupAutoscalingOptions{
			ZeroOrMaxNodeScaling: true,
		}
		fakes.CloudProvider.AddNodeGroup("ng-atomic",
			fakecloudprovider.WithNodes(aTemplate, 2),
			fakecloudprovider.WithNGOptions(atomicOpts),
		)

		// Add a non-removable pod to node 0 of ng-atomic so node 0 is NOT unneeded.
		blockingPod := test.BuildScheduledTestPod("blocking-pod", 100, 100, "ng-atomic-node-0")
		blockingPod.Annotations = map[string]string{
			"cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
		}
		fakes.K8s.AddPod(blockingPod)

		// Unneeded nodes get the deletion candidate soft taint. Without prefiltering, node 1 would
		// be simulated as a single node group, marked as unneeded and tainted. The size of the node
		// group alone doesn't prove prefiltering, since actuation doesn't delete incomplete atomic
		// node groups anyway.
		assertNoDeletionCandidateTaint := func() {
			t.Helper()
			node, err := fakes.KubeClient.CoreV1().Nodes().Get(ctx, "ng-atomic-node-1", metav1.GetOptions{})
			if assert.NoError(t, err) {
				assert.False(t, taints.HasDeletionCandidateTaint(node), "Node from incomplete atomic node group shouldn't be marked as unneeded")
			}
		}

		// Run CA loop once to process candidates.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerStepDuration)
		assertNoDeletionCandidateTaint()

		// Advance past unneeded time and run scale down loop.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerTestUnneededTime+time.Second)
		assertNoDeletionCandidateTaint()

		// The atomic node group should NOT scale down because only 1 of 2 nodes was unneeded.
		sizeA, _ := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
		assert.Equal(t, 2, sizeA, "Incomplete atomic node group should be prefiltered and remain at target size 2")
	})
}

// TestPlanner_AtomicNodeGroupUnremovable_DestinationCapacityRollback verifies that when one of the nodes
// of an atomic node group is unremovable (e.g. because destination capacity is exhausted), the destination
// capacity consumed by its sibling nodes in the simulation is rolled back, and subsequent candidates can use it.
func TestPlanner_AtomicNodeGroupUnremovable_DestinationCapacityRollback(t *testing.T) {
	// The atomic node group should not scale down because one of its nodes is unremovable,
	// while the non-atomic node group should scale down using the rolled back resources.
	runAtomicAndNonAtomicScaleDownTest(t, 3, 0, func(ctx context.Context, fakes *integration.FakeSet) {
		// All movable pods can only be scheduled on dest-node, which has capacity for exactly 1 pod
		// of 100 CPU. This way neither the non-atomic node nor sibling atomic nodes can be used as
		// destinations, and dest-node capacity alone decides the outcome of the simulation.
		destSelector := map[string]string{"pool": "dest"}
		destNode := test.BuildTestNode("dest-node", 100, 1000, test.IsReady(true))
		destNode.Labels = destSelector
		fakes.K8s.AddNode(destNode)

		newDestOnlyPod := func(name, nodeName, rsName string) *apiv1.Pod {
			pod := test.SetRSPodSpec(test.BuildScheduledTestPod(name, 100, 100, nodeName), rsName)
			pod.Spec.NodeSelector = destSelector
			return pod
		}

		// ng-atomic-node-0 is left EMPTY so EmptySorting orders it first in scaleDownCandidates,
		// which makes the atomic node group simulated before the non-atomic node.
		// ng-atomic-node-1 has a pod that consumes the whole dest-node capacity.
		fakes.K8s.AddPod(newDestOnlyPod("pod-a-1", "ng-atomic-node-1", "rs-a-1"))
		// ng-atomic-node-2 has a pod that cannot fit on dest-node anymore, making the atomic node group unremovable.
		fakes.K8s.AddPod(newDestOnlyPod("pod-a-2", "ng-atomic-node-2", "rs-a-2"))
		// ng-nonatomic-node-0 has a pod that fits on dest-node only once ng-atomic simulation is rolled back.
		fakes.K8s.AddPod(newDestOnlyPod("pod-na-0", "ng-nonatomic-node-0", "rs-na-0"))
	})
}

// TestPlanner_AtomicNodeGroupPdbConsumedBySiblings verifies that a PDB allowing a single disruption
// doesn't block atomic scale down when draining all nodes of the group at once would exceed the budget.
// Nodes whose pods exceed the remaining budget are drained sequentially (marked as risky) instead.
// The budget consumed by the atomic group is not rolled back, so a non-atomic candidate sharing
// the PDB has to wait.
func TestPlanner_AtomicNodeGroupPdbConsumedBySiblings(t *testing.T) {
	runAtomicAndNonAtomicScaleDownTest(t, 0, 1, func(ctx context.Context, fakes *integration.FakeSet) {
		// Destination helper node with plenty of capacity for pods.
		destNode := test.BuildTestNode("dest-node", 10000, 10000, test.IsReady(true))
		fakes.K8s.AddNode(destNode)

		// Create PDB allowing at most 1 pod disruption.
		maxUnavailable := intstr.FromInt32(1)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "shared-pdb",
				Namespace: "default",
			},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &maxUnavailable,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "shared"},
				},
			},
			Status: policyv1.PodDisruptionBudgetStatus{
				DisruptionsAllowed: 1,
			},
		}
		_, err := fakes.KubeClient.PolicyV1().PodDisruptionBudgets("default").Create(ctx, pdb, metav1.CreateOptions{})
		assert.NoError(t, err)

		// ng-atomic-node-0 is EMPTY, so EmptySorting orders it first and the atomic group is simulated first.
		// ng-atomic-node-1 has a pod matching shared-pdb, consuming the 1 allowed disruption.
		podA1 := test.SetRSPodSpec(test.BuildScheduledTestPod("pod-a-1", 100, 100, "ng-atomic-node-1"), "rs-a-1")
		podA1.Labels = map[string]string{"app": "shared"}
		fakes.K8s.AddPod(podA1)

		// ng-atomic-node-2 has a pod matching shared-pdb. The budget is already consumed by its sibling,
		// so the node is removable, but has to be drained sequentially.
		podA2 := test.SetRSPodSpec(test.BuildScheduledTestPod("pod-a-2", 100, 100, "ng-atomic-node-2"), "rs-a-2")
		podA2.Labels = map[string]string{"app": "shared"}
		fakes.K8s.AddPod(podA2)

		// ng-nonatomic-node-0 has a pod matching shared-pdb. The budget is consumed by the atomic group.
		podNA := test.SetRSPodSpec(test.BuildScheduledTestPod("pod-na-0", 100, 100, "ng-nonatomic-node-0"), "rs-na-0")
		podNA.Labels = map[string]string{"app": "shared"}
		fakes.K8s.AddPod(podNA)
	})
}

// runAtomicAndNonAtomicScaleDownTest sets up an atomic node group (3 nodes) and a non-atomic
// node group (1 node), invokes setupCluster to configure pods/resources, runs scale down and
// verifies the final target sizes of both node groups.
// The atomic node group options are based on the configured node group defaults (e.g. the scale
// down utilization threshold), so that non-empty atomic nodes aren't filtered out as not underutilized.
func runAtomicAndNonAtomicScaleDownTest(t *testing.T, wantAtomicSize, wantNonAtomicSize int, setupCluster func(ctx context.Context, fakes *integration.FakeSet)) {
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithScaleDownUnneededTime(plannerTestUnneededTime),
		)

	options := testConfig.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		registerEvictionReactor(fakes)

		// Create atomic node group with 3 nodes.
		aTemplate := test.BuildTestNode("node-a", 1000, 1000, test.IsReady(true))
		atomicOpts := options.NodeGroupDefaults
		atomicOpts.ZeroOrMaxNodeScaling = true
		fakes.CloudProvider.AddNodeGroup("ng-atomic",
			fakecloudprovider.WithNodes(aTemplate, 3),
			fakecloudprovider.WithNGOptions(&atomicOpts),
		)

		// Create non-atomic node group with 1 node.
		naTemplate := test.BuildTestNode("node-na", 1000, 1000, test.IsReady(true))
		fakes.CloudProvider.AddNodeGroup("ng-nonatomic",
			fakecloudprovider.WithNodes(naTemplate, 1),
		)

		setupCluster(ctx, fakes)

		// Initial sizes
		sizeA, _ := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
		assert.Equal(t, 3, sizeA)
		sizeNA, _ := fakes.CloudProvider.GetNodeGroup("ng-nonatomic").TargetSize(ctx)
		assert.Equal(t, 1, sizeNA)

		// Run CA loop once to mark unneeded and run simulation.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerStepDuration)

		// Advance past unneeded time and run scale down loop.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerTestUnneededTime+time.Second)

		// Run loop to actuate removal.
		synctestutils.MustRunOnceAfter(t, autoscaler, plannerStepDuration)

		finalSizeA, _ := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
		assert.Equal(t, wantAtomicSize, finalSizeA, "Unexpected atomic node group size")

		finalSizeNA, _ := fakes.CloudProvider.GetNodeGroup("ng-nonatomic").TargetSize(ctx)
		assert.Equal(t, wantNonAtomicSize, finalSizeNA, "Unexpected non-atomic node group size")
	})
}

// registerEvictionReactor registers a reactor on the fake kube client that automatically
// deletes evicted pods from the object tracker so that node draining succeeds.
func registerEvictionReactor(fakes *integration.FakeSet) {
	fakes.KubeClient.Fake.PrependReactor("create", "pods", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "eviction" {
			if createAction, ok := action.(clientgotesting.CreateAction); ok {
				var podName string
				if eviction, ok := createAction.GetObject().(*policyv1beta1.Eviction); ok {
					podName = eviction.Name
				} else if evictionV1, ok := createAction.GetObject().(*policyv1.Eviction); ok {
					podName = evictionV1.Name
				}
				if podName != "" {
					_ = fakes.KubeClient.Tracker().Delete(
						schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
						action.GetNamespace(),
						podName,
					)
					return true, createAction.GetObject(), nil
				}
			}
		}
		return false, nil, nil
	})
}
