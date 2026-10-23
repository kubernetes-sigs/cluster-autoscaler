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
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	fakecloudprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestProvReqFullLifecycle(t *testing.T) {
	config := integration.NewTestConfig().
		WithOverrides(
			integration.WithScaleDownUnneededTime(10*time.Minute),
			integration.WithProvisioningRequestEnabled(),
		)

	options := config.ResolveOptions()
	infra := integration.SetupSimpleInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := test.GetTestContextWithCancel(t)
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		n := test.BuildTestNode("ng1-node-0", 1000, 1000, test.IsReady(true))
		fakes.CloudProvider.AddNodeGroup("ng1", fakecloudprovider.WithNode(n))
		fakes.K8s.AddPod(test.BuildScheduledTestPod("p1", 600, 100, n.Name))

		// Create a ProvisioningRequest
		wrapper := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
			Name:     "test-pr",
			CPU:      "600m",
			Memory:   "100",
			PodCount: 1,
			Class:    v1.ProvisioningClassBestEffortAtomicScaleUp,
		})

		fakes.K8s.AddPodTemplate(wrapper.PodTemplates[0])
		_, err = fakes.PRClient.AutoscalingV1().ProvisioningRequests(wrapper.ProvisioningRequest.Namespace).Create(ctx, wrapper.ProvisioningRequest, metav1.CreateOptions{})
		assert.NoError(t, err)

		tg1, _ := fakes.CloudProvider.GetNodeGroup("ng1").TargetSize(ctx)
		assert.Equal(t, 1, tg1)

		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

		// The NodeGroup should have scaled up by 1 due to the PR
		tg1After, _ := fakes.CloudProvider.GetNodeGroup("ng1").TargetSize(ctx)
		assert.Equal(t, 2, tg1After)

		// Scale-Down
		n2 := test.BuildTestNode("ng1-node-1", 1000, 1000, test.IsReady(true))
		fakes.K8s.AddNode(n2)

		err = fakes.PRClient.AutoscalingV1().ProvisioningRequests(wrapper.ProvisioningRequest.Namespace).Delete(ctx, wrapper.ProvisioningRequest.Name, metav1.DeleteOptions{})
		assert.NoError(t, err)

		// Run CA once to trigger the unneeded evaluation
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

		// Step time forward by the ScaleDownUnneededTime (10 mins) + buffer (5 mins)
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 15*time.Minute)

		tg1Final, _ := fakes.CloudProvider.GetNodeGroup("ng1").TargetSize(ctx)
		assert.Equal(t, 1, tg1Final)
	})
}

func TestProvReqBatchFullLoop(t *testing.T) {
	for _, batchSize := range []int{1000, 100} {
		t.Run(fmt.Sprintf("batch-size-%d", batchSize), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := test.GetTestContextWithCancel(t)
				defer synctestutils.TearDown(cancel)

				env := newProvReqBatchTest(ctx, t, batchSize)
				env.addNodeGroup("cpu", false)
				requests := env.addRequests("requests", 1000, false)

				var wantResizes []provReqBatchResize
				for loop := 1; loop <= 1000/batchSize; loop++ {
					synctestutils.MustRunOnceAfter(ctx, t, env.autoscaler, 10*time.Second)
					wantResizes = append(wantResizes, provReqBatchResize{"cpu", batchSize})
					assert.Equal(t, wantResizes, env.resizes, "loop %d", loop)
					env.assertTargetSize("cpu", 1+loop*batchSize)
					env.assertProvisioned(requests, loop*batchSize)
				}

				synctestutils.MustRunOnceAfter(ctx, t, env.autoscaler, 10*time.Second)
				assert.Equal(t, wantResizes, env.resizes, "admitted requests must not trigger another resize")
				env.assertProvisioned(requests, 1000)
			})
		})
	}
}

func TestProvReqBatchFailingGPUDoesNotBlockCPU(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := test.GetTestContextWithCancel(t)
		defer synctestutils.TearDown(cancel)

		env := newProvReqBatchTest(ctx, t, 100)
		env.addNodeGroup("gpu", true)
		env.addNodeGroup("cpu", false)
		env.capacity["gpu"] = 0
		gpuRequests := env.addRequests("gpu", 2, true)

		for round := 0; round < 2; round++ {
			// GPU requests are older, including when they become eligible for another retry.
			time.Sleep(time.Second)
			cpuRequests := env.addRequests(fmt.Sprintf("cpu-%d", round), 4, false)
			before := len(env.resizes)

			synctestutils.MustRunOnceAfter(ctx, t, env.autoscaler, 10*time.Second)
			require.Greater(t, len(env.resizes), before, "the GPU resize must actually be attempted")
			assert.Equal(t, "gpu", env.resizes[before].group)
			env.assertProvisioned(gpuRequests, 0)
			env.assertProvisioned(cpuRequests, 0)

			for loop := 0; loop < 2; loop++ {
				synctestutils.MustRunOnceAfter(ctx, t, env.autoscaler, 10*time.Second)
			}
			env.assertProvisioned(gpuRequests, 0)
			env.assertProvisioned(cpuRequests, len(cpuRequests))
			env.assertTargetSize("gpu", 1)
			env.assertTargetSize("cpu", 1+(round+1)*len(cpuRequests))
			assert.Equal(t, provReqBatchResize{"cpu", len(cpuRequests)}, env.resizes[len(env.resizes)-1])

			if round == 0 {
				env.addReadyNodes("cpu", len(cpuRequests))
				time.Sleep(6 * time.Minute)
			}
		}
	})
}

func TestProvReqBatchShrinksRejectedResize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := test.GetTestContextWithCancel(t)
		defer synctestutils.TearDown(cancel)

		env := newProvReqBatchTest(ctx, t, 100)
		env.addNodeGroup("cpu", false)
		env.capacity["cpu"] = 25
		requests := env.addRequests("requests", 100, false)

		synctestutils.MustRunOnceAfter(ctx, t, env.autoscaler, 10*time.Second)

		assert.Equal(t, []provReqBatchResize{{"cpu", 100}, {"cpu", 50}, {"cpu", 25}}, env.resizes)
		env.assertTargetSize("cpu", 26)
		env.assertProvisioned(requests, 25)
		for _, request := range requests[25:] {
			updated, err := env.fakes.PRClient.AutoscalingV1().ProvisioningRequests(request.Namespace).Get(ctx, request.Name, metav1.GetOptions{})
			require.NoError(t, err)
			assert.Nil(t, apimeta.FindStatusCondition(updated.Status.Conditions, v1.Provisioned),
				"deferred request %s must not be marked as failed", request.Name)
		}

		env.capacity["cpu"] = 25
		synctestutils.MustRunOnceAfter(ctx, t, env.autoscaler, 10*time.Second)
		assert.Equal(t, []provReqBatchResize{{"cpu", 100}, {"cpu", 50}, {"cpu", 25}, {"cpu", 25}}, env.resizes,
			"the next loop must start at the successful size without retrying rejected sizes")
		env.assertTargetSize("cpu", 51)
		env.assertProvisioned(requests, 50)
	})
}

// This characterizes the best-effort lifecycle: a late scale-up failure doesn't revoke admission.
func TestProvReqBatchLateFailureKeepsAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := test.GetTestContextWithCancel(t)
		defer synctestutils.TearDown(cancel)

		env := newProvReqBatchTest(ctx, t, 4)
		env.addNodeGroup("cpu", false)
		requests := env.addRequests("requests", 4, false)

		synctestutils.MustRunOnceAfter(ctx, t, env.autoscaler, 10*time.Second)
		env.assertTargetSize("cpu", 5)
		env.assertProvisioned(requests, len(requests))
		for _, request := range requests {
			pod := test.BuildTestPod("consumer-"+request.Name, 1000, 100, test.MarkUnschedulable())
			pod.Annotations = map[string]string{v1.ProvisioningRequestPodAnnotationKey: request.Name}
			env.fakes.K8s.AddPod(pod)
		}

		// No requested nodes arrive. Let both the reservation and the scale-up expire.
		synctestutils.MustRunOnceAfter(ctx, t, env.autoscaler, 16*time.Minute)
		env.assertTargetSize("cpu", 1)
		env.assertProvisioned(requests, len(requests))
		for _, request := range requests {
			updated, err := env.fakes.PRClient.AutoscalingV1().ProvisioningRequests(request.Namespace).Get(ctx, request.Name, metav1.GetOptions{})
			require.NoError(t, err)
			assert.True(t, apimeta.IsStatusConditionTrue(updated.Status.Conditions, v1.BookingExpired))
			assert.False(t, apimeta.IsStatusConditionTrue(updated.Status.Conditions, v1.CapacityRevoked))
		}

		synctestutils.MustRunOnceAfter(ctx, t, env.autoscaler, 6*time.Minute)
		assert.Equal(t, []provReqBatchResize{{"cpu", 4}, {"cpu", -4}}, env.resizes,
			"consumer pods remain excluded from scale-up even after node-group backoff expires")

		env.fakes.K8s.AddPod(test.BuildTestPod("ordinary-pending", 1000, 100, test.MarkUnschedulable()))
		synctestutils.MustRunOnceAfter(ctx, t, env.autoscaler, 10*time.Second)
		assert.Equal(t, []provReqBatchResize{{"cpu", 4}, {"cpu", -4}, {"cpu", 1}}, env.resizes,
			"ordinary pending pods can still scale up without the ProvisioningRequest consumers")
		env.assertTargetSize("cpu", 2)
	})
}

type provReqBatchResize struct {
	group string
	delta int
}

type provReqBatchTest struct {
	t          *testing.T
	ctx        context.Context
	fakes      *integration.FakeSet
	provider   *fakecloudprovider.TestCloudProvider
	autoscaler core.Autoscaler
	resizes    []provReqBatchResize
	capacity   map[string]int
}

func newProvReqBatchTest(ctx context.Context, t *testing.T, batchSize int) *provReqBatchTest {
	t.Helper()
	options := integration.NewTestConfig().
		WithOverrides(integration.WithProvisioningRequestEnabled()).
		ResolveOptions()
	options.ScaleDownEnabled = false
	options.BestEffortAtomicBatchProcessing = true
	options.BestEffortAtomicProvisioningRequestMaxBatchSize = batchSize
	options.MaxNodesPerScaleUp = 1000
	options.PodInjectionLimit = 5000
	options.ProvisioningRequestInitialBackoffTime = time.Minute
	options.ProvisioningRequestMaxBackoffTime = 10 * time.Minute
	options.ProvisioningRequestMaxBackoffCacheSize = 1000
	options.InitialNodeGroupBackoffDuration = 5 * time.Minute
	options.MaxNodeGroupBackoffDuration = 30 * time.Minute
	options.NodeGroupBackoffResetTimeout = 3 * time.Hour
	options.NodeGroupDefaults.MaxNodeProvisionTime = 15 * time.Minute
	options.NodeGroupDefaults.MaxNodeStartupTime = 15 * time.Minute
	options.KubeClientOpts.KubeClientBurst = 10

	infra := integration.SetupSimpleInfrastructure(t)
	env := &provReqBatchTest{t: t, ctx: ctx, fakes: infra.Fakes, capacity: make(map[string]int)}
	env.provider = fakecloudprovider.NewTestCloudProviderBuilder().WithOnScaleUp(func(groupID string, delta int) error {
		env.resizes = append(env.resizes, provReqBatchResize{groupID, delta})
		if available, limited := env.capacity[groupID]; limited && delta > 0 {
			if delta > available {
				// TestCloudProvider increments its target before invoking the callback.
				group := env.provider.GetNodeGroup(groupID).(*fakecloudprovider.TestNodeGroup)
				size, err := group.TargetSize(ctx)
				if err != nil {
					return err
				}
				group.SetTargetSize(size - delta)
				return fmt.Errorf("%w: insufficient capacity for %d nodes", cloudprovider.ErrAtomicIncreaseRejected, delta)
			}
			env.capacity[groupID] -= delta
		}
		return nil
	}).Build()
	var err error
	env.autoscaler, _, err = integration.DefaultAutoscalingBuilder(options, infra).
		WithCloudProvider(env.provider).
		Build(ctx)
	require.NoError(t, err)
	return env
}

func (env *provReqBatchTest) addNodeGroup(name string, gpu bool) {
	env.t.Helper()
	node := test.BuildTestNode(name+"-node-0", 1000, 1000, test.IsReady(true))
	node.Labels["pool"] = name
	if gpu {
		node.Status.Capacity["nvidia.com/gpu"] = resource.MustParse("1")
		node.Status.Allocatable["nvidia.com/gpu"] = resource.MustParse("1")
	}
	env.provider.AddNodeGroup(name, 1, 1001, 1)
	env.provider.AddNode(name, node)
	env.fakes.K8s.AddNode(node)
	env.fakes.K8s.AddPod(test.BuildScheduledTestPod(name+"-occupied", 1000, 100, node.Name))
}

func (env *provReqBatchTest) addReadyNodes(group string, count int) {
	env.t.Helper()
	for i := 1; i <= count; i++ {
		node := test.BuildTestNode(fmt.Sprintf("%s-node-%d", group, i), 1000, 1000, test.IsReady(true))
		node.Labels["pool"] = group
		env.provider.AddNode(group, node)
		env.fakes.K8s.AddNode(node)
	}
}

func (env *provReqBatchTest) addRequests(prefix string, count int, gpu bool) []*v1.ProvisioningRequest {
	env.t.Helper()
	var requests []*v1.ProvisioningRequest
	for i := 0; i < count; i++ {
		options := provreqwrapper.TestProvReqOptions{
			Name: fmt.Sprintf("%s-%04d", prefix, i), CPU: "1", Memory: "100", PodCount: 1,
			Class: v1.ProvisioningClassBestEffortAtomicScaleUp,
		}
		pool := "cpu"
		if gpu {
			options.GPU = "1"
			pool = "gpu"
		}
		request := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(options)
		request.UID = types.UID(request.Name)
		request.Generation = 1
		request.PodTemplates[0].Template.Spec.NodeSelector = map[string]string{"pool": pool}
		env.fakes.K8s.AddPodTemplate(request.PodTemplates[0])
		created, err := env.fakes.PRClient.AutoscalingV1().ProvisioningRequests(request.Namespace).Create(env.ctx, request.ProvisioningRequest, metav1.CreateOptions{})
		require.NoError(env.t, err)
		requests = append(requests, created)
	}
	return requests
}

func (env *provReqBatchTest) assertProvisioned(requests []*v1.ProvisioningRequest, count int) {
	env.t.Helper()
	for i, request := range requests {
		updated, err := env.fakes.PRClient.AutoscalingV1().ProvisioningRequests(request.Namespace).Get(env.ctx, request.Name, metav1.GetOptions{})
		require.NoError(env.t, err)
		require.Equal(env.t, i < count, apimeta.IsStatusConditionTrue(updated.Status.Conditions, v1.Provisioned), request.Name)
		require.False(env.t, apimeta.IsStatusConditionTrue(updated.Status.Conditions, v1.Failed), request.Name)
	}
}

func (env *provReqBatchTest) assertTargetSize(group string, want int) {
	env.t.Helper()
	size, err := env.provider.GetNodeGroup(group).TargetSize(env.ctx)
	require.NoError(env.t, err)
	assert.Equal(env.t, want, size)
}
