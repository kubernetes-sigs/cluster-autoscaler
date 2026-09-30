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
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachineryruntime "k8s.io/apimachinery/pkg/runtime"
	k8s_testing "k8s.io/client-go/testing"
	fakecloudprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestScaleDown_PartialFailure(t *testing.T) {
	for _, dynamicDelayEnabled := range []bool{true, false} {
		testName := fmt.Sprintf("DynamicDelay_%v", dynamicDelayEnabled)
		t.Run(testName, func(t *testing.T) {
			configBuilder := integration.NewTestConfig().
				WithOverrides(
					integration.WithScaleDownUnneededTime(time.Minute),
					func(o *config.AutoscalingOptions) {
						o.DynamicNodeDeleteDelayAfterTaintEnabled = dynamicDelayEnabled
					},
				)

			options := configBuilder.ResolveOptions()
			infra := integration.SetupInfrastructure(t)
			fakes := infra.Fakes

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := test.GetTestContextWithCancel(t)
				defer synctestutils.TearDown(cancel)

				autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
				assert.NoError(t, err)

				// Create atomic node group
				atomicOpts := options.NodeGroupDefaults
				atomicOpts.ZeroOrMaxNodeScaling = true
				nAtomicTemplate := test.BuildTestNode("ng-atomic-node-template", 1000, 1000, test.IsReady(true))
				fakes.CloudProvider.AddNodeGroup("ng-atomic",
					fakecloudprovider.WithNodes(nAtomicTemplate, 2),
					fakecloudprovider.WithOptions(&atomicOpts),
				)
				// Create non-atomic node group
				nNonAtomicTemplate := test.BuildTestNode("ng-nonatomic-node-template", 1000, 1000, test.IsReady(true))
				fakes.CloudProvider.AddNodeGroup("ng-nonatomic",
					fakecloudprovider.WithNodes(nNonAtomicTemplate, 2),
				)

				// Create happy-path node group with no errors
				nHappyPathTemplate := test.BuildTestNode("ng-happy-path-node-template", 1000, 1000, test.IsReady(true))
				fakes.CloudProvider.AddNodeGroup("ng-happy-path",
					fakecloudprovider.WithNodes(nHappyPathTemplate, 2),
				)

				nodeToFailAtomic := "ng-atomic-node-0"
				nodeToFailNonAtomic := "ng-nonatomic-node-0"

				// Simulate API failure when applying ToBeDeletedTaint to specific nodes
				fakes.KubeClient.Fake.PrependReactor("update", "nodes", func(action k8s_testing.Action) (handled bool, ret apimachineryruntime.Object, err error) {
					updateAction, ok := action.(k8s_testing.UpdateAction)
					if !ok {
						return false, nil, nil
					}
					node, ok := updateAction.GetObject().(*corev1.Node)
					if !ok {
						return false, nil, nil
					}

					if node.Name == nodeToFailAtomic || node.Name == nodeToFailNonAtomic {
						if taints.HasToBeDeletedTaint(node) {
							return true, nil, fmt.Errorf("simulated network error updating taint on node %s", node.Name)
						}
					}
					return false, nil, nil
				})

				// 1st loop: Marks all candidate nodes as unneeded.
				synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

				// 2nd loop: timer exceeds unneeded time, deletion triggered
				err = synctestutils.RunOnceAfter(ctx, t, autoscaler, time.Minute+time.Second)
				assert.NoError(t, err, "partial scale-down should not return an error when some deletions started")

				// Wait for Actuator.deleteNodesAsync to wake up from its nodeDeleteDelayAfterTaint sleep
				time.Sleep(2 * time.Second)
				synctest.Wait()

				// Verify expected target sizes after partial deletion
				atomicGroup := fakes.CloudProvider.GetNodeGroup("ng-atomic")
				assert.NotNil(t, atomicGroup, "expected ng-atomic group to exist")
				atomicSize, err := atomicGroup.TargetSize(ctx)
				assert.NoError(t, err)
				assert.Equal(t, 2, atomicSize, "atomic target size mismatch")

				nonAtomicGroup := fakes.CloudProvider.GetNodeGroup("ng-nonatomic")
				assert.NotNil(t, nonAtomicGroup, "expected ng-nonatomic group to exist")
				nonAtomicSize, err := nonAtomicGroup.TargetSize(ctx)
				assert.NoError(t, err)
				assert.Equal(t, 1, nonAtomicSize, "nonatomic target size mismatch")

				happyPathGroup := fakes.CloudProvider.GetNodeGroup("ng-happy-path")
				assert.NotNil(t, happyPathGroup, "expected ng-happy-path group to exist")
				happyPathSize, err := happyPathGroup.TargetSize(ctx)
				assert.NoError(t, err)
				assert.Equal(t, 0, happyPathSize, "expected happy path nodegroup to be fully scaled down")

				// Validate end state: the exact correct nodes are deleted, and all remaining nodes have no taints
				var remainingNodeNames []string
				for _, n := range fakes.K8s.Nodes().Items {
					remainingNodeNames = append(remainingNodeNames, n.Name)
					assert.False(t, taints.HasToBeDeletedTaint(&n), "node %s should have clean taints", n.Name)
				}
				assert.ElementsMatch(t, []string{"ng-atomic-node-0", "ng-atomic-node-1", "ng-nonatomic-node-0"}, remainingNodeNames, "expected successfully tainted non-atomic sibling to be deleted while failed nodes remain")
			})
		})
	}
}

func TestScaleDown_HappyPath(t *testing.T) {
	testCases := []struct {
		name                 string
		zeroOrMaxNodeScaling bool
	}{
		{
			name:                 "Atomic nodegroup",
			zeroOrMaxNodeScaling: true,
		},
		{
			name:                 "Non-atomic nodegroup",
			zeroOrMaxNodeScaling: false,
		},
	}

	for _, tc := range testCases {
		for _, dynamicDelayEnabled := range []bool{true, false} {
			testName := fmt.Sprintf("%s/DynamicDelay_%v", tc.name, dynamicDelayEnabled)
			t.Run(testName, func(t *testing.T) {
				configBuilder := integration.NewTestConfig().
					WithOverrides(
						integration.WithScaleDownUnneededTime(time.Minute),
						func(o *config.AutoscalingOptions) {
							o.DynamicNodeDeleteDelayAfterTaintEnabled = dynamicDelayEnabled
						},
					)

				options := configBuilder.ResolveOptions()
				infra := integration.SetupInfrastructure(t)
				fakes := infra.Fakes

				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := test.GetTestContextWithCancel(t)
					defer synctestutils.TearDown(cancel)

					autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
					assert.NoError(t, err)

					ngOpts := options.NodeGroupDefaults
					ngOpts.ZeroOrMaxNodeScaling = tc.zeroOrMaxNodeScaling
					nTemplate := test.BuildTestNode("ng-node-template", 1000, 1000, test.IsReady(true))
					fakes.CloudProvider.AddNodeGroup("ng",
						fakecloudprovider.WithNodes(nTemplate, 2),
						fakecloudprovider.WithOptions(&ngOpts),
					)

					// 1st loop: Marks all candidate nodes as unneeded.
					synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

					// 2nd loop: timer exceeds unneeded time, deletion triggered
					synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Minute+time.Second)

					// Wait for Actuator.deleteNodesAsync to wake up from its nodeDeleteDelayAfterTaint sleep
					time.Sleep(2 * time.Second)
					synctest.Wait()

					// Verify expected target sizes after deletion
					group := fakes.CloudProvider.GetNodeGroup("ng")
					assert.NotNil(t, group, "expected ng group to exist")
					size, err := group.TargetSize(ctx)
					assert.NoError(t, err)
					assert.Equal(t, 0, size, "expected target size to be 0 for happy path scaledown")

					// Validate end state: all nodes are deleted
					var remainingNodeNames []string
					for _, n := range fakes.K8s.Nodes().Items {
						remainingNodeNames = append(remainingNodeNames, n.Name)
						assert.False(t, taints.HasToBeDeletedTaint(&n), "node %s should have clean taints", n.Name)
					}
					assert.Empty(t, remainingNodeNames, "all nodes should be successfully deleted")
				})
			})
		}
	}
}

func TestScaleDown_DrainAndMixedAtomicPartialFailure(t *testing.T) {
	for _, dynamicDelayEnabled := range []bool{true, false} {
		testName := fmt.Sprintf("DynamicDelay_%v", dynamicDelayEnabled)
		t.Run(testName, func(t *testing.T) {
			configBuilder := integration.NewTestConfig().
				WithOverrides(
					integration.WithScaleDownUnneededTime(time.Minute),
					func(o *config.AutoscalingOptions) {
						o.DynamicNodeDeleteDelayAfterTaintEnabled = dynamicDelayEnabled
						// The default of 1 would crop every drain bucket but one.
						o.MaxDrainParallelism = 10
					},
				)

			options := configBuilder.ResolveOptions()
			infra := integration.SetupInfrastructure(t)
			fakes := infra.Fakes

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := test.GetTestContextWithCancel(t)
				defer synctestutils.TearDown(cancel)

				autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
				assert.NoError(t, err)

				// Anchor node group (>50% utilized) so drained pods have a destination node to reschedule onto
				nAnchorTemplate := test.BuildTestNode("ng-anchor-template", 1000, 1000, test.IsReady(true))
				fakes.CloudProvider.AddNodeGroup("ng-anchor", fakecloudprovider.WithNodes(nAnchorTemplate, 1))
				anchorPod := test.BuildScheduledTestPod("anchor-pod", 600, 100, "ng-anchor-node-0")
				fakes.K8s.AddPod(anchorPod)

				// Mixed atomic node group: ng-atomic-node-0 is empty (succeeds taint), ng-atomic-node-1 has a pod (fails taint)
				atomicOpts := options.NodeGroupDefaults
				atomicOpts.ZeroOrMaxNodeScaling = true
				nAtomicTemplate := test.BuildTestNode("ng-atomic-template", 1000, 1000, test.IsReady(true))
				fakes.CloudProvider.AddNodeGroup("ng-atomic",
					fakecloudprovider.WithNodes(nAtomicTemplate, 2),
					fakecloudprovider.WithOptions(&atomicOpts),
				)
				atomicDrainPod := test.BuildScheduledTestPod("atomic-drain-pod", 100, 50, "ng-atomic-node-1")
				atomicDrainPod.Annotations = map[string]string{"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"}
				fakes.K8s.AddPod(atomicDrainPod)

				// Non-atomic node group: ng-nonatomic-node-0 fails taint, ng-nonatomic-node-1 succeeds taint and scales down
				nNonAtomicTemplate := test.BuildTestNode("ng-nonatomic-template", 1000, 1000, test.IsReady(true))
				fakes.CloudProvider.AddNodeGroup("ng-nonatomic", fakecloudprovider.WithNodes(nNonAtomicTemplate, 2))

				// Non-atomic drain node group: both nodes have a pod. ng-drain-node-0 fails taint, ng-drain-node-1 is drained and scaled down.
				nDrainTemplate := test.BuildTestNode("ng-drain-template", 1000, 1000, test.IsReady(true))
				fakes.CloudProvider.AddNodeGroup("ng-drain", fakecloudprovider.WithNodes(nDrainTemplate, 2))
				for i := 0; i < 2; i++ {
					drainPod := test.BuildScheduledTestPod(fmt.Sprintf("drain-pod-%d", i), 50, 50, fmt.Sprintf("ng-drain-node-%d", i))
					drainPod.Annotations = map[string]string{"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"}
					fakes.K8s.AddPod(drainPod)
				}

				// The fake client doesn't remove evicted pods, so drain would wait until it times out.
				fakes.KubeClient.Fake.PrependReactor("create", "pods", func(action k8s_testing.Action) (handled bool, ret apimachineryruntime.Object, err error) {
					if action.GetSubresource() != "eviction" {
						return false, nil, nil
					}
					eviction, ok := action.(k8s_testing.CreateAction).GetObject().(metav1.Object)
					if !ok {
						return false, nil, nil
					}
					// Use the tracker directly: calling the client from a reactor would deadlock.
					if err := fakes.KubeClient.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), action.GetNamespace(), eviction.GetName()); err != nil {
						return true, nil, err
					}
					return true, nil, nil
				})

				var failTaints atomic.Bool
				failTaints.Store(true)
				fakes.KubeClient.Fake.PrependReactor("update", "nodes", func(action k8s_testing.Action) (handled bool, ret apimachineryruntime.Object, err error) {
					if !failTaints.Load() {
						return false, nil, nil
					}
					updateAction, ok := action.(k8s_testing.UpdateAction)
					if !ok {
						return false, nil, nil
					}
					node, ok := updateAction.GetObject().(*corev1.Node)
					if !ok {
						return false, nil, nil
					}
					if (node.Name == "ng-atomic-node-1" || node.Name == "ng-nonatomic-node-0" || node.Name == "ng-drain-node-0") && taints.HasToBeDeletedTaint(node) {
						return true, nil, fmt.Errorf("simulated taint error on %s", node.Name)
					}
					return false, nil, nil
				})

				// 1st loop: mark candidates as unneeded
				synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

				// 2nd loop: trigger scale-down actuation
				synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Minute+time.Second)

				time.Sleep(2 * time.Second)
				synctest.Wait()

				// Verify atomic group was completely aborted (even though ng-atomic-node-0 was empty and succeeded tainting!)
				atomicSize, err := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
				assert.NoError(t, err)
				assert.Equal(t, 2, atomicSize, "mixed atomic group should abort completely when drain node fails taint")

				// Verify non-atomic group partially scaled down (ng-nonatomic-node-1 deleted, ng-nonatomic-node-0 retained)
				nonAtomicSize, err := fakes.CloudProvider.GetNodeGroup("ng-nonatomic").TargetSize(ctx)
				assert.NoError(t, err)
				assert.Equal(t, 1, nonAtomicSize, "non-atomic group should scale down the successfully tainted node")

				// Verify the drain group partially scaled down: the surviving tainted node was drained and deleted.
				drainSize, err := fakes.CloudProvider.GetNodeGroup("ng-drain").TargetSize(ctx)
				assert.NoError(t, err)
				assert.Equal(t, 1, drainSize, "drain group should drain and scale down the successfully tainted node")

				var remainingNodeNames []string
				for _, n := range fakes.K8s.Nodes().Items {
					remainingNodeNames = append(remainingNodeNames, n.Name)
					assert.False(t, taints.HasToBeDeletedTaint(&n), "node %s should have clean taints", n.Name)
				}
				assert.ElementsMatch(t, []string{"ng-anchor-node-0", "ng-atomic-node-0", "ng-atomic-node-1", "ng-nonatomic-node-0", "ng-drain-node-0"}, remainingNodeNames)

				// 3rd loop: once the transient taint error clears, the mixed atomic group is not stuck
				// and scales down completely along with the remaining non-atomic nodes.
				failTaints.Store(false)
				synctestutils.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)
				time.Sleep(2 * time.Second)
				synctest.Wait()

				atomicSize, err = fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
				assert.NoError(t, err)
				assert.Equal(t, 0, atomicSize, "mixed atomic group should scale down to 0 on subsequent loop after taint error clears")
			})
		})
	}
}
