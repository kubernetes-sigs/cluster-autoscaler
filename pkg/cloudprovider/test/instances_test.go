/*
Copyright 2026 The Kubernetes Authors.

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

package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/experimental"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	fakek8s "sigs.k8s.io/cluster-autoscaler/pkg/utils/fake"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func newFakeK8s() *fakek8s.Kubernetes {
	client := fake.NewSimpleClientset()
	return fakek8s.NewKubernetes(client, informers.NewSharedInformerFactory(client, 0))
}

func instanceIds(instances []cloudprovider.Instance) []string {
	var ids []string
	for _, instance := range instances {
		ids = append(ids, instance.Id)
	}
	return ids
}

func hasNotReadyTaint(node *apiv1.Node) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == apiv1.TaintNodeNotReady {
			return true
		}
	}
	return false
}

func TestFakeNodeGroupIncreaseSizeWithInstances(t *testing.T) {
	templateNode := testutils.BuildTestNode("template", 1000, 1000)

	for _, tc := range []struct {
		testName       string
		withK8s        bool
		ngOpts         []NodeGroupOption
		delta          int
		wantErr        bool
		wantTargetSize int
	}{
		{
			testName:       "creates instances, without K8s fake",
			ngOpts:         []NodeGroupOption{WithTemplate(framework.NewTestNodeInfo(templateNode))},
			delta:          3,
			wantTargetSize: 3,
		},
		{
			testName:       "creates instances, with K8s fake",
			withK8s:        true,
			ngOpts:         []NodeGroupOption{WithTemplate(framework.NewTestNodeInfo(templateNode))},
			delta:          3,
			wantTargetSize: 3,
		},
		{
			testName:       "creates instances on top of existing nodes",
			withK8s:        true,
			ngOpts:         []NodeGroupOption{WithNodes(templateNode, 2)},
			delta:          2,
			wantTargetSize: 4,
		},
		{
			testName:       "zero delta",
			withK8s:        true,
			ngOpts:         []NodeGroupOption{WithNGSize(0, 10), WithNode(templateNode)},
			delta:          0,
			wantErr:        true,
			wantTargetSize: 1,
		},
		{
			testName:       "negative delta",
			withK8s:        true,
			ngOpts:         []NodeGroupOption{WithNGSize(0, 10), WithNode(templateNode)},
			delta:          -2,
			wantErr:        true,
			wantTargetSize: 1,
		},
		{
			testName:       "delta exceeding max size",
			withK8s:        true,
			ngOpts:         []NodeGroupOption{WithNGSize(0, 2), WithNode(templateNode)},
			delta:          2,
			wantErr:        true,
			wantTargetSize: 1,
		},
		{
			testName:       "no template",
			withK8s:        true,
			delta:          1,
			wantErr:        true,
			wantTargetSize: 0,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			ctx := context.Background()
			var k8s *fakek8s.Kubernetes
			if tc.withK8s {
				k8s = newFakeK8s()
			}
			provider := NewCloudProvider(k8s)
			ng := provider.AddNodeGroup("ng1", tc.ngOpts...)
			nodesBefore, err := ng.Nodes(ctx)
			require.NoError(t, err)

			var creator experimental.InstanceCreator = ng
			instances, err := creator.IncreaseSizeWithInstances(ctx, tc.delta)

			targetSize, tsErr := ng.TargetSize(ctx)
			require.NoError(t, tsErr)
			assert.Equal(t, tc.wantTargetSize, targetSize)
			nodesAfter, err2 := ng.Nodes(ctx)
			require.NoError(t, err2)

			if tc.wantErr {
				assert.Error(t, err)
				assert.Empty(t, instances)
				assert.ElementsMatch(t, instanceIds(nodesBefore), instanceIds(nodesAfter))
				return
			}
			require.NoError(t, err)
			require.Len(t, instances, tc.delta)

			// The returned instances should be exactly the newly added ones reported by Nodes().
			var newIds []string
			before := map[string]bool{}
			for _, id := range instanceIds(nodesBefore) {
				before[id] = true
			}
			for _, instance := range nodesAfter {
				if !before[instance.Id] {
					newIds = append(newIds, instance.Id)
				}
			}
			assert.ElementsMatch(t, newIds, instanceIds(instances))
			for _, instance := range instances {
				require.NotNil(t, instance.Status)
				assert.Equal(t, cloudprovider.InstanceRunning, instance.Status.State)
			}

			if !tc.withK8s {
				return
			}
			// The returned instance ids should match Spec.ProviderID of the created Nodes, which should map back to the node group.
			k8sNodes := map[string]*apiv1.Node{}
			for i := range k8s.Nodes().Items {
				node := &k8s.Nodes().Items[i]
				k8sNodes[node.Spec.ProviderID] = node
			}
			for _, instance := range instances {
				node, found := k8sNodes[instance.Id]
				if assert.True(t, found, "no Node with ProviderID %q", instance.Id) {
					gotNg, err := provider.NodeGroupForNode(ctx, node)
					require.NoError(t, err)
					require.NotNil(t, gotNg)
					assert.Equal(t, "ng1", gotNg.Id())
				}
			}
		})
	}
}

func TestFakeNodeGroupIncreaseSizeMatchesIncreaseSizeWithInstances(t *testing.T) {
	ctx := context.Background()
	provider := NewCloudProvider(newFakeK8s())
	ng := provider.AddNodeGroup("ng1", WithTemplate(framework.NewTestNodeInfo(testutils.BuildTestNode("template", 1000, 1000))))

	require.NoError(t, ng.IncreaseSize(ctx, 2))
	targetSize, err := ng.TargetSize(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, targetSize)
	nodes, err := ng.Nodes(ctx)
	require.NoError(t, err)
	assert.Len(t, nodes, 2)
}

func TestFakeNodeGroupIncreaseSizeWithInstancesDelays(t *testing.T) {
	for _, tc := range []struct {
		testName          string
		registrationDelay time.Duration
		readinessDelay    time.Duration
		wantInitialReady  bool
	}{
		{
			testName:         "no delays",
			wantInitialReady: true,
		},
		{
			testName:          "registration delay only",
			registrationDelay: 10 * time.Millisecond,
			wantInitialReady:  true,
		},
		{
			testName:         "readiness delay only",
			readinessDelay:   10 * time.Millisecond,
			wantInitialReady: false,
		},
		{
			testName:          "registration and readiness delays",
			registrationDelay: 10 * time.Millisecond,
			readinessDelay:    10 * time.Millisecond,
			wantInitialReady:  false,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			k8s := newFakeK8s()
			// Watching the fake client records every state each Node goes through, so intermediate states can be asserted
			// deterministically, without relying on polling timing.
			watcher, err := k8s.Client.CoreV1().Nodes().Watch(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			defer watcher.Stop()

			provider := NewCloudProvider(k8s)
			ng := provider.AddNodeGroup("ng1",
				WithTemplate(framework.NewTestNodeInfo(testutils.BuildTestNode("template", 1000, 1000))),
				WithNodeRegistrationDelay(tc.registrationDelay),
				WithNodeReadinessDelay(tc.readinessDelay),
			)

			instances, err := ng.IncreaseSizeWithInstances(ctx, 2)
			require.NoError(t, err)
			require.Len(t, instances, 2)

			// Each created Node should first be added in the expected initial state, and if it's initially unready, it should then
			// be updated to Ready without the not-ready taint.
			wantEvents := len(instances)
			if !tc.wantInitialReady {
				wantEvents *= 2
			}
			added := map[string]*apiv1.Node{}
			modified := map[string]*apiv1.Node{}
			for range wantEvents {
				select {
				case event, ok := <-watcher.ResultChan():
					require.True(t, ok, "watch channel closed unexpectedly")
					node, ok := event.Object.(*apiv1.Node)
					require.True(t, ok, "unexpected watch object %T", event.Object)
					switch event.Type {
					case watch.Added:
						assert.NotContains(t, added, node.Spec.ProviderID, "Node %s added twice", node.Name)
						added[node.Spec.ProviderID] = node
					case watch.Modified:
						assert.Contains(t, added, node.Spec.ProviderID, "Node %s modified before being added", node.Name)
						modified[node.Spec.ProviderID] = node
					default:
						t.Fatalf("unexpected watch event type %v for Node %s", event.Type, node.Name)
					}
				case <-ctx.Done():
					t.Fatalf("timed out waiting for Node watch events, got added: %d, modified: %d", len(added), len(modified))
				}
			}

			for _, instance := range instances {
				addedNode, found := added[instance.Id]
				require.True(t, found, "no Node added for instance %q", instance.Id)
				readiness, err := kube_util.GetNodeReadiness(addedNode)
				require.NoError(t, err)
				assert.Equal(t, tc.wantInitialReady, readiness.Ready, "initial readiness of Node %s", addedNode.Name)
				assert.Equal(t, !tc.wantInitialReady, hasNotReadyTaint(addedNode), "initial not-ready taint on Node %s", addedNode.Name)

				if tc.wantInitialReady {
					assert.NotContains(t, modified, instance.Id)
					continue
				}
				modifiedNode, found := modified[instance.Id]
				require.True(t, found, "Node for instance %q never updated to Ready", instance.Id)
				readiness, err = kube_util.GetNodeReadiness(modifiedNode)
				require.NoError(t, err)
				assert.True(t, readiness.Ready, "Node %s should be Ready after the readiness delay", modifiedNode.Name)
				assert.False(t, hasNotReadyTaint(modifiedNode), "Node %s should have the not-ready taint removed after the readiness delay", modifiedNode.Name)
			}

			// The final state in the API should match the last observed state.
			finalNodes := k8s.Nodes().Items
			require.Len(t, finalNodes, len(instances))
			for i := range finalNodes {
				node := &finalNodes[i]
				readiness, err := kube_util.GetNodeReadiness(node)
				require.NoError(t, err)
				assert.True(t, readiness.Ready, "Node %s should be Ready in the final state", node.Name)
				assert.False(t, hasNotReadyTaint(node), "Node %s has the not-ready taint in the final state", node.Name)
			}
		})
	}
}

func TestFakeNodeGroupIncreaseSizeWithInstancesConcurrent(t *testing.T) {
	ctx := context.Background()
	k8s := newFakeK8s()
	provider := NewCloudProvider(k8s)
	ng := provider.AddNodeGroup("ng1",
		WithNGSize(0, 6),
		WithTemplate(framework.NewTestNodeInfo(testutils.BuildTestNode("template", 1000, 1000))),
	)

	const workers = 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	var allInstances []cloudprovider.Instance
	for range workers {
		wg.Add(3)
		go func() {
			defer wg.Done()
			insts, err := ng.IncreaseSizeWithInstances(ctx, 2)
			if err == nil {
				mu.Lock()
				allInstances = append(allInstances, insts...)
				mu.Unlock()
			}
		}()
		go func() {
			defer wg.Done()
			_, err := ng.Nodes(ctx)
			assert.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			_, err := ng.TargetSize(ctx)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	targetSize, err := ng.TargetSize(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, targetSize)
	require.Len(t, allInstances, 6)

	nodes, err := ng.Nodes(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, instanceIds(nodes), instanceIds(allInstances))
}

func TestTestNodeGroupIncreaseSizeWithInstances(t *testing.T) {
	ctx := context.Background()

	t.Run("not implemented without hook", func(t *testing.T) {
		provider := NewTestCloudProviderBuilder().Build()
		provider.AddNodeGroup("ng1", 0, 10, 3)
		ng := provider.GetNodeGroup("ng1")
		creator, ok := cloudprovider.As[experimental.InstanceCreator](ng)
		require.True(t, ok, "TestNodeGroup should implement experimental.InstanceCreator")

		instances, err := creator.IncreaseSizeWithInstances(ctx, 2)
		assert.ErrorIs(t, err, cloudprovider.ErrNotImplemented)
		assert.Empty(t, instances)
		targetSize, err := ng.TargetSize(ctx)
		require.NoError(t, err)
		assert.Equal(t, 3, targetSize)
	})

	t.Run("not implemented without cloud provider", func(t *testing.T) {
		ng := &TestNodeGroup{id: "ng1", maxSize: 10, targetSize: 3}
		instances, err := ng.IncreaseSizeWithInstances(ctx, 2)
		assert.ErrorIs(t, err, cloudprovider.ErrNotImplemented)
		assert.Empty(t, instances)
		targetSize, err := ng.TargetSize(ctx)
		require.NoError(t, err)
		assert.Equal(t, 3, targetSize)
	})

	t.Run("hook returning ErrNotImplemented leaves targetSize unchanged", func(t *testing.T) {
		provider := NewTestCloudProviderBuilder().
			WithOnScaleUpWithInstances(func(string, int) ([]cloudprovider.Instance, error) {
				return nil, cloudprovider.ErrNotImplemented
			}).
			Build()
		provider.AddNodeGroup("ng1", 0, 10, 3)
		ng := provider.GetNodeGroup("ng1")
		creator, ok := cloudprovider.As[experimental.InstanceCreator](ng)
		require.True(t, ok)

		instances, err := creator.IncreaseSizeWithInstances(ctx, 2)
		assert.ErrorIs(t, err, cloudprovider.ErrNotImplemented)
		assert.Empty(t, instances)
		targetSize, err := ng.TargetSize(ctx)
		require.NoError(t, err)
		assert.Equal(t, 3, targetSize)
	})

	t.Run("hook returning wrapped ErrNotImplemented leaves targetSize unchanged", func(t *testing.T) {
		provider := NewTestCloudProviderBuilder().
			WithOnScaleUpWithInstances(func(string, int) ([]cloudprovider.Instance, error) {
				return []cloudprovider.Instance{{Id: "ignored"}}, fmt.Errorf("not supported: %w", cloudprovider.ErrNotImplemented)
			}).
			Build()
		provider.AddNodeGroup("ng1", 0, 10, 3)
		ng := provider.GetNodeGroup("ng1")
		creator, ok := cloudprovider.As[experimental.InstanceCreator](ng)
		require.True(t, ok)

		instances, err := creator.IncreaseSizeWithInstances(ctx, 2)
		assert.ErrorIs(t, err, cloudprovider.ErrNotImplemented)
		assert.Empty(t, instances)
		targetSize, err := ng.TargetSize(ctx)
		require.NoError(t, err)
		assert.Equal(t, 3, targetSize)
	})

	t.Run("hook can call locked node group and provider methods without deadlocking", func(t *testing.T) {
		var provider *TestCloudProvider
		var targetSizeInHook int
		provider = NewTestCloudProviderBuilder().
			WithOnScaleUpWithInstances(func(ngId string, delta int) ([]cloudprovider.Instance, error) {
				ng := provider.GetNodeGroup(ngId)
				var err error
				targetSizeInHook, err = ng.TargetSize(ctx)
				if err != nil {
					return nil, err
				}
				node := testutils.BuildTestNode("new-node-1", 1000, 1000)
				provider.AddNode(ngId, node)
				return []cloudprovider.Instance{{Id: node.Name}}, nil
			}).
			Build()
		provider.AddNodeGroup("ng1", 0, 10, 3)
		ng := provider.GetNodeGroup("ng1")
		creator, ok := cloudprovider.As[experimental.InstanceCreator](ng)
		require.True(t, ok)

		instances, err := creator.IncreaseSizeWithInstances(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, 3, targetSizeInHook)
		assert.Equal(t, []cloudprovider.Instance{{Id: "new-node-1"}}, instances)
		targetSize, err := ng.TargetSize(ctx)
		require.NoError(t, err)
		assert.Equal(t, 4, targetSize)
	})

	for _, tc := range []struct {
		testName       string
		hookInstances  []cloudprovider.Instance
		hookErr        error
		wantTargetSize int
	}{
		{
			testName: "hook returns instances",
			hookInstances: []cloudprovider.Instance{
				{Id: "instance-1", Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating}},
				{Id: "instance-2"},
			},
			wantTargetSize: 5,
		},
		{
			testName: "hook returns fewer instances than delta",
			hookInstances: []cloudprovider.Instance{
				{Id: "instance-1"},
			},
			wantTargetSize: 5,
		},
		{
			testName:       "hook returns partial instances and error",
			hookInstances:  []cloudprovider.Instance{{Id: "instance-1"}},
			hookErr:        errors.New("partial failure"),
			wantTargetSize: 4,
		},
		{
			testName:       "hook returns full error with no instances",
			hookErr:        errors.New("quota exceeded"),
			wantTargetSize: 3,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			type call struct {
				ngId  string
				delta int
			}
			var calls []call
			onScaleUpCalled := false
			provider := NewTestCloudProviderBuilder().
				WithOnScaleUp(func(string, int) error {
					onScaleUpCalled = true
					return nil
				}).
				WithOnScaleUpWithInstances(func(ngId string, delta int) ([]cloudprovider.Instance, error) {
					calls = append(calls, call{ngId: ngId, delta: delta})
					return tc.hookInstances, tc.hookErr
				}).
				Build()
			provider.AddNodeGroup("ng1", 0, 10, 3)
			provider.AddNodeGroup("ng2", 0, 10, 1)
			ng := provider.GetNodeGroup("ng1")
			creator, ok := cloudprovider.As[experimental.InstanceCreator](ng)
			require.True(t, ok, "TestNodeGroup should implement experimental.InstanceCreator")

			instances, err := creator.IncreaseSizeWithInstances(ctx, 2)
			if tc.hookErr != nil {
				assert.ErrorIs(t, err, tc.hookErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.hookInstances, instances)
			assert.Equal(t, []call{{ngId: "ng1", delta: 2}}, calls)
			assert.False(t, onScaleUpCalled, "IncreaseSizeWithInstances shouldn't call the OnScaleUp hook")

			targetSize, err := ng.TargetSize(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.wantTargetSize, targetSize)
			otherTargetSize, err := provider.GetNodeGroup("ng2").TargetSize(ctx)
			require.NoError(t, err)
			assert.Equal(t, 1, otherTargetSize)
		})
	}
}
