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

package inmemory

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	fakecloudprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	unneededTime = 1 * time.Minute
)

func TestStaticAutoscaler_FullLifecycle(t *testing.T) {
	stepDuration := 10 * time.Second
	config := integration.NewTestConfig().
		WithOverrides(
			integration.WithCloudProviderName("gce"),
			integration.WithScaleDownUnneededTime(unneededTime),
		)

	options := config.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := test.GetTestContextWithCancel(t)
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		// Configure a test NodeGroup with a single Node.
		n := test.BuildTestNode("ng1-node-0", 1000, 1000, test.IsReady(true))
		fakes.CloudProvider.AddNodeGroup("ng1", fakecloudprovider.WithNode(n))
		fakes.K8s.AddPod(test.BuildScheduledTestPod("p1", 600, 100, n.Name))

		// Create a pending Pod that doesn't fit on the single existing Node.
		p := test.BuildTestPod("p2", 600, 100, test.MarkUnschedulable())
		fakes.K8s.AddPod(p)

		// Run a loop, CA should scale up a single Node for the pending Pod.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, stepDuration)
		tg1, _ := fakes.CloudProvider.GetNodeGroup("ng1").TargetSize(ctx)
		assert.Equal(t, 2, tg1)
		assert.Equal(t, 2, len(fakes.K8s.Nodes().Items))

		// Delete the Pod that was pending, the second Node should be empty now.
		fakes.K8s.DeletePod(p.Namespace, p.Name)

		// Run CA loop once to mark the Node as unneeded.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, stepDuration)
		// Run another CA loop after the unneeded time elapses, CA should delete the Node.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, unneededTime+time.Nanosecond)

		finalSize, _ := fakes.CloudProvider.GetNodeGroup("ng1").TargetSize(ctx)
		assert.Equal(t, 1, finalSize)
	})
}

func TestScaleUp_ResourceLimits(t *testing.T) {
	config := integration.NewTestConfig()

	options := config.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := test.GetTestContextWithCancel(t)
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		n := test.BuildTestNode("ng-node-0", 1000, 1000, test.IsReady(true))
		fakes.CloudProvider.AddNodeGroup("ng", fakecloudprovider.WithNode(n))
		// The first pod can fit on the existing node
		fakes.K8s.AddPod(test.BuildTestPod("pod1", 600, 100, test.MarkUnschedulable()))
		// The second pod should trigger the scaleup, if a resource request allows it
		fakes.K8s.AddPod(test.BuildTestPod("pod2", 600, 100, test.MarkUnschedulable()))

		// Scale-up should be blocked.
		fakes.CloudProvider.SetResourceLimit(cloudprovider.ResourceNameCores, 0, 1)

		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, unneededTime)
		size, _ := fakes.CloudProvider.GetNodeGroup("ng").TargetSize(ctx)
		assert.Equal(t, 1, size, "Should not scale up when max cores limit is reached")

		// Scale-up should succeed.
		fakes.CloudProvider.SetResourceLimit(cloudprovider.ResourceNameCores, 0, 2)

		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, unneededTime)
		newSize, _ := fakes.CloudProvider.GetNodeGroup("ng").TargetSize(ctx)
		assert.Equal(t, 2, newSize, "Should scale up after resource limit is increased")
	})
}

func TestScaleUp_EnforceMinimumResourceLimits(t *testing.T) {
	t.Run("groups share the remaining cores", func(t *testing.T) {
		testConfig := integration.NewTestConfig().
			WithOverrides(
				integration.WithEnforceNodeGroupMinSize(),
			)

		options := testConfig.ResolveOptions()
		infra := integration.SetupInfrastructure(t)
		fakes := infra.Fakes

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := test.GetTestContextWithCancel(t)
			defer synctestutils.TearDown(cancel)

			autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
			assert.NoError(t, err)

			// Configure two groups below their minimum sizes with one core left.
			template := test.BuildTestNode("template", 1000, 1000, test.IsReady(true))
			for _, id := range []string{"ng1", "ng2"} {
				fakes.CloudProvider.AddNodeGroup(id, fakecloudprovider.WithNodes(template, 1), fakecloudprovider.WithNGSize(3, 4))
				fakes.K8s.AddPod(test.BuildScheduledTestPod(id+"-pod", 600, 100, id+"-node-0"))
			}
			fakes.CloudProvider.SetResourceLimit(cloudprovider.ResourceNameCores, 0, 3)

			// Run a loop, CA should share the remaining core between the groups.
			synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)
			assert.Equal(t, 3, len(fakes.K8s.Nodes().Items), "new nodes must fit the resource limit")
		})
	})
}

func TestScaleUp_EnforceMinimumMaxNodesTotal(t *testing.T) {
	t.Run("one group", func(t *testing.T) {
		testConfig := integration.NewTestConfig().
			WithOverrides(
				integration.WithEnforceNodeGroupMinSize(),
				integration.WithMaxNodesTotal(3),
			)

		options := testConfig.ResolveOptions()
		infra := integration.SetupInfrastructure(t)
		fakes := infra.Fakes

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := test.GetTestContextWithCancel(t)
			defer synctestutils.TearDown(cancel)

			autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
			assert.NoError(t, err)

			// Configure a group below its minimum size and an unmanaged Node.
			n := test.BuildTestNode("ng1-node-0", 1000, 1000, test.IsReady(true))
			fakes.CloudProvider.AddNodeGroup("ng1", fakecloudprovider.WithNode(n), fakecloudprovider.WithNGSize(3, 4))
			fakes.K8s.AddPod(test.BuildScheduledTestPod("p1", 600, 100, n.Name))
			fakes.K8s.AddNode(test.BuildTestNode("unmanaged", 1000, 1000, test.IsReady(true)))

			// Run a loop, CA should count the unmanaged Node against the cluster limit.
			synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)
			assert.Equal(t, 3, len(fakes.K8s.Nodes().Items), "new nodes must fit the cluster allowance")
		})
	})

	t.Run("groups share the allowance", func(t *testing.T) {
		testConfig := integration.NewTestConfig().
			WithOverrides(
				integration.WithEnforceNodeGroupMinSize(),
				integration.WithMaxNodesTotal(3),
			)

		options := testConfig.ResolveOptions()
		infra := integration.SetupInfrastructure(t)
		fakes := infra.Fakes

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := test.GetTestContextWithCancel(t)
			defer synctestutils.TearDown(cancel)

			autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
			assert.NoError(t, err)

			// Configure two groups below their minimum sizes with one Node left.
			template := test.BuildTestNode("template", 1000, 1000, test.IsReady(true))
			for _, id := range []string{"ng1", "ng2"} {
				fakes.CloudProvider.AddNodeGroup(id, fakecloudprovider.WithNodes(template, 1), fakecloudprovider.WithNGSize(3, 4))
				fakes.K8s.AddPod(test.BuildScheduledTestPod(id+"-pod", 600, 100, id+"-node-0"))
			}

			// Run a loop, CA should share the remaining allowance between the groups.
			synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)
			assert.Equal(t, 3, len(fakes.K8s.Nodes().Items), "new nodes must fit the cluster allowance")
		})
	})

	t.Run("existing upcoming node counts once", func(t *testing.T) {
		testConfig := integration.NewTestConfig().
			WithOverrides(
				integration.WithEnforceNodeGroupMinSize(),
				integration.WithMaxNodesTotal(5),
				integration.WithMaxNodeStartupTime(time.Minute),
			)

		options := testConfig.ResolveOptions()
		infra := integration.SetupInfrastructure(t)
		fakes := infra.Fakes

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := test.GetTestContextWithCancel(t)
			defer synctestutils.TearDown(cancel)

			autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
			assert.NoError(t, err)

			// Configure ng1 with a registered starting Node and a Ready Node.
			startingNode := test.BuildTestNode("ng1-node-1", 1000, 1000, test.IsReady(false))
			startingNode.CreationTimestamp.Time = time.Now()
			n1 := test.BuildTestNode("ng1-node-0", 1000, 1000, test.IsReady(true))
			fakes.CloudProvider.AddNodeGroup("ng1", fakecloudprovider.WithNode(startingNode), fakecloudprovider.WithNode(n1), fakecloudprovider.WithNGSize(3, 4))
			fakes.K8s.AddPod(test.BuildScheduledTestPod("p1", 600, 100, n1.Name))

			// Configure ng2 with one Ready Node, leaving room for two more Nodes.
			n2 := test.BuildTestNode("ng2-node-0", 1000, 1000, test.IsReady(true))
			fakes.CloudProvider.AddNodeGroup("ng2", fakecloudprovider.WithNode(n2), fakecloudprovider.WithNGSize(3, 4))
			fakes.K8s.AddPod(test.BuildScheduledTestPod("p2", 600, 100, n2.Name))

			// Run a loop, CA should count the starting Node once and add two Nodes.
			synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)
			assert.Equal(t, 5, len(fakes.K8s.Nodes().Items), "the starting node must count once against the cluster allowance")
		})
	})
}

// TestFixNodeGroupSize_ZeroOrMaxNodeScaling verifies that fixNodeGroupSize skips
// attempting DecreaseTargetSize on ZeroOrMaxNodeScaling node groups when registered
// nodes mismatch the target size after MaxNodeProvisionTime.
func TestFixNodeGroupSize_ZeroOrMaxNodeScaling(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithCloudProviderName("gce"),
		)

	options := testConfig.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := test.GetTestContextWithCancel(t)
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		nodeGroupOpts := &config.NodeGroupAutoscalingOptions{
			ZeroOrMaxNodeScaling: true,
			MaxNodeProvisionTime: 10 * time.Second,
		}

		templateNode := test.BuildTestNode("atomic-ng-node-0", 2000, 8*1024*1024*1024, test.IsReady(true))
		ng := fakes.CloudProvider.AddNodeGroup(
			"atomic-ng",
			fakecloudprovider.WithNodes(templateNode, 2),
			fakecloudprovider.WithNGSize(0, 10),
			fakecloudprovider.WithOptions(nodeGroupOpts),
		)

		// Add non-empty pods running on each node to prevent scale-down from deleting them.
		fakes.K8s.AddPod(test.BuildScheduledTestPod("pod-0", 1000, 1000, "atomic-ng-node-0"))
		fakes.K8s.AddPod(test.BuildScheduledTestPod("pod-1", 1000, 1000, "atomic-ng-node-1"))

		// First cycle: Autoscaler initializes cluster state with the 2 registered nodes.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Simulate discrepancy: TargetSize is 4 in cloud provider, but only 2 nodes exist.
		ng.SetTargetSize(4)

		// Second cycle: Autoscaler observes discrepancy (2 registered nodes vs target 4).
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Third cycle: Advance time past MaxNodeProvisionTime (10s).
		// fixNodeGroupSize triggers: for ZeroOrMaxNodeScaling, it skips decreasing target size.
		// RunOnce succeeds without error.
		err = synctestutils.RunOnceAfter(ctx, t, autoscaler, 15*time.Second)
		assert.NoError(t, err)

		// Verify target size was not decreased and remained 4.
		targetSize, err := ng.TargetSize(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 4, targetSize)
	})
}
