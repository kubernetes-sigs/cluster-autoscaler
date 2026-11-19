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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	actuationTestUnneededTime = 1 * time.Minute
	actuationStepDuration     = 10 * time.Second
)

// TestActuation_AtomicNodeGroupBatchedDrainWithPdbRecovery verifies that atomic node groups
// are drained in PDB-constrained batches so that each batch gets a fresh MaxPodEvictionTime
// window, while still draining nodes in parallel within the allowed PDB budget.
func TestActuation_AtomicNodeGroupBatchedDrainWithPdbRecovery(t *testing.T) {
	const (
		pdbRecoveryDuration = 80 * time.Second
		maxPodEvictionTime  = 2 * time.Minute
	)

	testCases := []struct {
		name               string
		disruptionsAllowed int32
		// expectedDrainLoops is the number of 10s steps after StartDeletion needed for all
		// batches to finish draining and deleting the group.
		// - With budget=1 across 3 nodes and 80s recovery: Batch 1 evicts at 0s, Batch 2 at 80s,
		//   Batch 3 at 160s -> 17 loops (170s), which exceeds the 2m MaxPodEvictionTime window.
		// - With budget=2 across 3 nodes and 80s recovery: Batch 1 evicts 2 pods in parallel at 0s,
		//   Batch 2 evicts the 3rd pod at 80s -> 9 loops (90s), proving parallel drain within budget.
		expectedDrainLoops int
	}{
		{
			name:               "budget of 1 across 3 nodes drains in 3 sequential batches",
			disruptionsAllowed: 1,
			expectedDrainLoops: 17,
		},
		{
			name:               "budget of 2 across 3 nodes drains 2 nodes in parallel then 1",
			disruptionsAllowed: 2,
			expectedDrainLoops: 9,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithOverrides(
					integration.WithScaleDownUnneededTime(actuationTestUnneededTime),
					func(o *config.AutoscalingOptions) {
						o.MaxPodEvictionTime = maxPodEvictionTime
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

				var mu sync.Mutex
				var recoveryTimes []time.Time
				fakes.KubeClient.Fake.PrependReactor("create", "pods", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					createAction, podName, ok := extractPodEviction(action)
					if !ok {
						return false, nil, nil
					}
					mu.Lock()
					defer mu.Unlock()
					now := time.Now()
					kept := recoveryTimes[:0]
					for _, rt := range recoveryTimes {
						if now.Before(rt) {
							kept = append(kept, rt)
						}
					}
					recoveryTimes = kept
					if int32(len(recoveryTimes)) >= tc.disruptionsAllowed {
						return true, nil, apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 1)
					}
					recoveryTimes = append(recoveryTimes, now.Add(pdbRecoveryDuration))
					_ = fakes.KubeClient.Tracker().Delete(
						schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
						action.GetNamespace(),
						podName,
					)
					return true, createAction.GetObject(), nil
				})

				destNode := test.BuildTestNode("dest-node", 10000, 10000, test.IsReady(true))
				fakes.K8s.AddNode(destNode)

				aTemplate := test.BuildTestNode("node-a", 1000, 1000, test.IsReady(true))
				atomicOpts := options.NodeGroupDefaults
				atomicOpts.ZeroOrMaxNodeScaling = true
				fakes.CloudProvider.AddNodeGroup("ng-atomic",
					fakecloudprovider.WithNodes(aTemplate, 3),
					fakecloudprovider.WithNGOptions(&atomicOpts),
				)

				maxUnavailable := intstr.FromInt32(tc.disruptionsAllowed)
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
						DisruptionsAllowed: tc.disruptionsAllowed,
					},
				}
				_, err = fakes.KubeClient.PolicyV1().PodDisruptionBudgets("default").Create(ctx, pdb, metav1.CreateOptions{})
				assert.NoError(t, err)

				for i, nodeName := range []string{"ng-atomic-node-0", "ng-atomic-node-1", "ng-atomic-node-2"} {
					pod := test.SetRSPodSpec(test.BuildScheduledTestPod("pod-a-"+string(rune('0'+i)), 100, 100, nodeName), "rs-a")
					pod.Labels = map[string]string{"app": "test"}
					fakes.K8s.AddPod(pod)
				}

				// Mark nodes unneeded, then advance past unneededTime to trigger StartDeletion.
				synctestutils.MustRunOnceAfter(ctx, t, autoscaler, actuationStepDuration)
				synctestutils.MustRunOnceAfter(ctx, t, autoscaler, actuationTestUnneededTime+time.Second)

				for range tc.expectedDrainLoops {
					synctestutils.MustRunOnceAfter(ctx, t, autoscaler, actuationStepDuration)
				}

				finalSizeA, err := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
				assert.NoError(t, err)
				assert.Equal(t, 0, finalSizeA, "Atomic node group should scale down completely across PDB-constrained drain batches")
			})
		})
	}
}

// TestActuation_AtomicNodeGroupBatchedDrainStopsOnFailure verifies that when an earlier PDB-constrained
// drain batch fails for an atomic node group, subsequent batches do not start evicting pods, ToBeDeleted
// taints are cleaned up on all nodes, and the group can scale down cleanly in a subsequent attempt.
func TestActuation_AtomicNodeGroupBatchedDrainStopsOnFailure(t *testing.T) {
	const maxPodEvictionTime = 2 * time.Minute

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithScaleDownUnneededTime(actuationTestUnneededTime),
			func(o *config.AutoscalingOptions) {
				o.MaxPodEvictionTime = maxPodEvictionTime
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

		var mu sync.Mutex
		failBatch1 := true
		firstPod := ""
		evictionAttempts := map[string]int{}
		fakes.KubeClient.Fake.PrependReactor("create", "pods", func(action clientgotesting.Action) (bool, runtime.Object, error) {
			createAction, podName, ok := extractPodEviction(action)
			if !ok {
				return false, nil, nil
			}
			mu.Lock()
			if firstPod == "" {
				firstPod = podName
			}
			evictionAttempts[podName]++
			shouldFail := failBatch1 && podName == firstPod
			mu.Unlock()
			if shouldFail {
				return true, nil, apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 1)
			}
			_ = fakes.KubeClient.Tracker().Delete(
				schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
				action.GetNamespace(),
				podName,
			)
			return true, createAction.GetObject(), nil
		})

		destNode := test.BuildTestNode("dest-node", 10000, 10000, test.IsReady(true))
		fakes.K8s.AddNode(destNode)

		aTemplate := test.BuildTestNode("node-a", 1000, 1000, test.IsReady(true))
		atomicOpts := options.NodeGroupDefaults
		atomicOpts.ZeroOrMaxNodeScaling = true
		fakes.CloudProvider.AddNodeGroup("ng-atomic",
			fakecloudprovider.WithNodes(aTemplate, 3),
			fakecloudprovider.WithNGOptions(&atomicOpts),
		)

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
				DisruptionsAllowed: 1,
			},
		}
		_, err = fakes.KubeClient.PolicyV1().PodDisruptionBudgets("default").Create(ctx, pdb, metav1.CreateOptions{})
		assert.NoError(t, err)

		for i, nodeName := range []string{"ng-atomic-node-0", "ng-atomic-node-1", "ng-atomic-node-2"} {
			pod := test.SetRSPodSpec(test.BuildScheduledTestPod("pod-a-"+string(rune('0'+i)), 100, 100, nodeName), "rs-a")
			pod.Labels = map[string]string{"app": "test"}
			fakes.K8s.AddPod(pod)
		}

		// Mark nodes unneeded, then advance past unneededTime to trigger StartDeletion.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, actuationStepDuration)
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, actuationTestUnneededTime+time.Second)

		// Advance just past MaxPodEvictionTime (130s) so Batch 1 times out and aborts the group,
		// without waiting another full unneededTime yet.
		synctest.Wait()
		time.Sleep(maxPodEvictionTime + actuationStepDuration)
		synctest.Wait()

		sizeAfterFailure, err := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 3, sizeAfterFailure, "Atomic node group must not scale down when Batch 1 drain fails")

		mu.Lock()
		assert.NotEmpty(t, firstPod, "Batch 1 should have attempted pod eviction")
		assert.Greater(t, evictionAttempts[firstPod], 0, "Batch 1 pod should have had eviction attempted")
		assert.Len(t, evictionAttempts, 1, "Subsequent batches must not evict pods after Batch 1 failed")
		failBatch1 = false
		mu.Unlock()

		for _, nodeName := range []string{"ng-atomic-node-0", "ng-atomic-node-1", "ng-atomic-node-2"} {
			node, err := fakes.KubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
			if assert.NoError(t, err) {
				assert.False(t, taints.HasToBeDeletedTaint(node), "ToBeDeleted taint should be cleaned up on %s", nodeName)
			}
		}

		// Verify all deletion tracking was cleaned up by allowing eviction to succeed and confirming
		// the atomic group scales down in the next cycle.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, actuationStepDuration)
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, actuationTestUnneededTime+time.Second)
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, actuationStepDuration)

		finalSizeA, err := fakes.CloudProvider.GetNodeGroup("ng-atomic").TargetSize(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 0, finalSizeA, "Atomic node group should scale down once eviction succeeds")
	})
}

func extractPodEviction(action clientgotesting.Action) (clientgotesting.CreateAction, string, bool) {
	if action.GetSubresource() != "eviction" {
		return nil, "", false
	}
	createAction, ok := action.(clientgotesting.CreateAction)
	if !ok {
		return nil, "", false
	}
	switch eviction := createAction.GetObject().(type) {
	case *policyv1beta1.Eviction:
		return createAction, eviction.Name, eviction.Name != ""
	case *policyv1.Eviction:
		return createAction, eviction.Name, eviction.Name != ""
	default:
		return nil, "", false
	}
}
