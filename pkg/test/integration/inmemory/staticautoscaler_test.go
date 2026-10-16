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
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

func TestScaleUp_WithNvidiaMigAndTimeSlicedNodes(t *testing.T) {
	testCases := []struct {
		name         string
		resourceName apiv1.ResourceName
	}{
		{name: "MIG", resourceName: "nvidia.com/mig-2g.24gb"},
		{name: "time-sliced GPU", resourceName: "nvidia.com/gpu.shared"},
		{name: "time-sliced MIG", resourceName: "nvidia.com/mig-2g.24gb.shared"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Four false GPU-unready nodes exceed both thresholds and block unrelated scale-ups.
			options := integration.NewTestConfig().WithOverrides(
				integration.WithCloudProviderName("gce"),
				func(options *config.AutoscalingOptions) {
					options.OkTotalUnreadyCount = 3
					options.MaxTotalUnreadyPercentage = 45
				},
			).ResolveOptions()
			infra := integration.SetupInfrastructure(t)
			fakes := infra.Fakes

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := test.GetTestContextWithCancel(t)
				defer synctestutils.TearDown(cancel)

				autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
				assert.NoError(t, err)

				gpuNode := test.BuildTestNode("gpu-node", 1000, 1000,
					test.IsReady(true),
					test.WithNodeLabels(map[string]string{"gpu-label": "nvidia-h100"}),
				)
				gpuNode.Status.Capacity[tc.resourceName] = *resource.NewQuantity(8, resource.DecimalSI)
				gpuNode.Status.Allocatable[tc.resourceName] = *resource.NewQuantity(8, resource.DecimalSI)
				fakes.CloudProvider.AddNodeGroup("gpu", fakecloudprovider.WithNodes(gpuNode, 4), fakecloudprovider.WithNGSize(4, 4))
				for i := range 4 {
					fakes.K8s.AddPod(test.BuildScheduledTestPod(fmt.Sprintf("gpu-workload-%d", i), 600, 100, fmt.Sprintf("gpu-node-%d", i)))
				}

				cpuNode := test.BuildTestNode("cpu-node", 1000, 1000, test.IsReady(true))
				fakes.CloudProvider.AddNodeGroup("cpu", fakecloudprovider.WithNode(cpuNode), fakecloudprovider.WithNGSize(1, 2))
				fakes.K8s.AddPod(test.BuildScheduledTestPod("scheduled", 600, 100, cpuNode.Name))
				fakes.K8s.AddPod(test.BuildTestPod("pending", 600, 100, test.MarkUnschedulable()))

				synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

				targetSize, err := fakes.CloudProvider.GetNodeGroup("cpu").TargetSize(ctx)
				assert.NoError(t, err)
				assert.Equal(t, 2, targetSize, "MIG and time-sliced nodes must not make the cluster unhealthy")
			})
		})
	}
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
