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

package atomic

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"

	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

func TestIsAtomicNodeGroup(t *testing.T) {
	ctx := context.Background()
	n1 := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec:       apiv1.NodeSpec{ProviderID: "n1"},
	}

	provider := testprovider.NewTestCloudProviderBuilder().Build()
	atomicOpts := &config.NodeGroupAutoscalingOptions{
		ZeroOrMaxNodeScaling: true,
	}
	provider.AddNodeGroupWithCustomOptions("ng-atomic", 0, 10, 2, atomicOpts)
	provider.AddNode("ng-atomic", n1)

	n2 := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n2"},
		Spec:       apiv1.NodeSpec{ProviderID: "n2"},
	}
	provider.AddNodeGroup("ng-standard", 0, 10, 2)
	provider.AddNode("ng-standard", n2)

	autoscalingCtx := ca_context.AutoscalingContext{
		CloudProvider: provider,
	}

	ng, isAtomic := IsAtomicNodeGroup(ctx, &autoscalingCtx, n1)
	assert.True(t, isAtomic)
	assert.Equal(t, "ng-atomic", ng.Id())

	ng2, isAtomic2 := IsAtomicNodeGroup(ctx, &autoscalingCtx, n2)
	assert.False(t, isAtomic2)
	assert.Nil(t, ng2)

	// n3 is not added to any node group.
	n3 := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n3"},
		Spec:       apiv1.NodeSpec{ProviderID: "n3"},
	}
	ng3, isAtomic3 := IsAtomicNodeGroup(ctx, &autoscalingCtx, n3)
	assert.False(t, isAtomic3)
	assert.Nil(t, ng3)
}

func TestAllowsNonAtomicScaleUpToMax(t *testing.T) {
	testCases := []struct {
		name string
		opts *config.NodeGroupAutoscalingOptions
		want bool
	}{
		{
			name: "atomic with AllowNonAtomicScaleUpToMax",
			opts: &config.NodeGroupAutoscalingOptions{ZeroOrMaxNodeScaling: true, AllowNonAtomicScaleUpToMax: true},
			want: true,
		},
		{
			name: "atomic without AllowNonAtomicScaleUpToMax",
			opts: &config.NodeGroupAutoscalingOptions{ZeroOrMaxNodeScaling: true},
			want: false,
		},
		{
			name: "no custom options",
			opts: nil,
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			if tc.opts != nil {
				provider.AddNodeGroupWithCustomOptions("ng", 0, 10, 2, tc.opts)
			} else {
				provider.AddNodeGroup("ng", 0, 10, 2)
			}
			autoscalingCtx := &ca_context.AutoscalingContext{CloudProvider: provider}
			got := AllowsNonAtomicScaleUpToMax(context.Background(), autoscalingCtx, provider.GetNodeGroup("ng"))
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCountRegisteredNodesPerGroup(t *testing.T) {
	buildNode := func(name string) *apiv1.Node {
		return &apiv1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       apiv1.NodeSpec{ProviderID: name},
		}
	}
	ng1Node1, ng1Node2, ng1Node3 := buildNode("ng1-node-1"), buildNode("ng1-node-2"), buildNode("ng1-node-3")
	ng2Node1 := buildNode("ng2-node-1")
	ng3Node1 := buildNode("ng3-node-1")
	allNodes := []*apiv1.Node{ng1Node1, ng1Node2, ng1Node3, ng2Node1, ng3Node1}
	errNodes := errors.New("nodes failed")
	errList := errors.New("list failed")

	testCases := []struct {
		name string
		// groups are IDs of the node groups to count registered nodes for.
		groups []string
		// failingGroups are IDs of the node groups for which listing instances fails.
		failingGroups map[string]bool
		listedNodes   []*apiv1.Node
		listErr       error
		want          map[string]int
		wantErr       error
	}{
		{
			name:        "counts registered nodes of each node group",
			groups:      []string{"ng1", "ng2"},
			listedNodes: allNodes,
			want:        map[string]int{"ng1": 3, "ng2": 1},
		},
		{
			name:        "instances not registered in the cluster are not counted",
			groups:      []string{"ng1", "ng2"},
			listedNodes: []*apiv1.Node{ng1Node1, ng1Node3},
			want:        map[string]int{"ng1": 2, "ng2": 0},
		},
		{
			name:        "nodes from other node groups are not counted",
			groups:      []string{"ng1"},
			listedNodes: allNodes,
			want:        map[string]int{"ng1": 3},
		},
		{
			name:          "node group whose instances can't be listed is omitted",
			groups:        []string{"ng1", "ng2"},
			failingGroups: map[string]bool{"ng2": true},
			listedNodes:   allNodes,
			want:          map[string]int{"ng1": 3},
		},
		{
			name:    "node lister error is returned",
			groups:  []string{"ng1"},
			listErr: errList,
			wantErr: errList,
		},
		{
			name:    "no node groups",
			listErr: errList,
			want:    map[string]int{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			provider.AddNodeGroup("ng1", 0, 10, 3)
			provider.AddNodeGroup("ng2", 0, 10, 1)
			provider.AddNodeGroup("ng3", 0, 10, 1)
			for _, n := range []*apiv1.Node{ng1Node1, ng1Node2, ng1Node3} {
				provider.AddNode("ng1", n)
			}
			provider.AddNode("ng2", ng2Node1)
			provider.AddNode("ng3", ng3Node1)

			var groups []cloudprovider.NodeGroup
			for _, id := range tc.groups {
				var ng cloudprovider.NodeGroup = provider.GetNodeGroup(id)
				if tc.failingGroups[id] {
					ng = &nodesErrorNodeGroup{NodeGroup: ng, err: errNodes}
				}
				groups = append(groups, ng)
			}
			var nodeLister kube_util.NodeLister = kube_util.NewTestNodeLister(tc.listedNodes)
			if tc.listErr != nil {
				nodeLister = &errorNodeLister{err: tc.listErr}
			}

			got, err := CountRegisteredNodesPerGroup(context.Background(), nodeLister, groups)
			assert.ErrorIs(t, err, tc.wantErr)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("CountRegisteredNodesPerGroup() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// nodesErrorNodeGroup wraps a NodeGroup and fails listing its instances.
type nodesErrorNodeGroup struct {
	cloudprovider.NodeGroup
	err error
}

func (g *nodesErrorNodeGroup) Nodes(context.Context) ([]cloudprovider.Instance, error) {
	return nil, g.err
}

// errorNodeLister is a NodeLister that always fails.
type errorNodeLister struct {
	err error
}

func (l *errorNodeLister) List() ([]*apiv1.Node, error) {
	return nil, l.err
}

func (l *errorNodeLister) Get(string) (*apiv1.Node, error) {
	return nil, l.err
}

func TestGroupAtomicNodes(t *testing.T) {
	ctx := context.Background()

	nodeAtomic1 := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-atomic-1"},
		Spec:       apiv1.NodeSpec{ProviderID: "node-atomic-1"},
	}
	nodeAtomic2 := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-atomic-2"},
		Spec:       apiv1.NodeSpec{ProviderID: "node-atomic-2"},
	}
	nodeAtomic3 := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-atomic-3"},
		Spec:       apiv1.NodeSpec{ProviderID: "node-atomic-3"},
	}
	nodeStandard := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-standard"},
		Spec:       apiv1.NodeSpec{ProviderID: "node-standard"},
	}

	provider := testprovider.NewTestCloudProviderBuilder().Build()
	atomicOpts := &config.NodeGroupAutoscalingOptions{
		ZeroOrMaxNodeScaling: true,
	}
	provider.AddNodeGroupWithCustomOptions("ng-atomic-1", 0, 10, 2, atomicOpts)
	provider.AddNode("ng-atomic-1", nodeAtomic1)
	provider.AddNode("ng-atomic-1", nodeAtomic2)

	provider.AddNodeGroupWithCustomOptions("ng-atomic-2", 0, 10, 1, atomicOpts)
	provider.AddNode("ng-atomic-2", nodeAtomic3)

	provider.AddNodeGroup("ng-standard", 0, 10, 1)
	provider.AddNode("ng-standard", nodeStandard)

	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	clustersnapshot.InitializeClusterSnapshotOrDie(t, snapshot, []*apiv1.Node{nodeAtomic1, nodeAtomic2, nodeAtomic3, nodeStandard}, nil)

	autoscalingCtx := &ca_context.AutoscalingContext{
		CloudProvider:   provider,
		ClusterSnapshot: snapshot,
	}

	testCases := []struct {
		name      string
		nodeNames []string
		want      map[string][]string
	}{
		{
			name:      "groups atomic nodes by nodegroup and ignores standard and unknown nodes",
			nodeNames: []string{"node-atomic-1", "node-standard", "node-atomic-2", "node-atomic-3", "non-existent-node"},
			want: map[string][]string{
				"ng-atomic-1": {"node-atomic-1", "node-atomic-2"},
				"ng-atomic-2": {"node-atomic-3"},
			},
		},
		{
			name:      "empty node names returns empty map",
			nodeNames: []string{},
			want:      map[string][]string{},
		},
		{
			name:      "only standard nodes returns empty map",
			nodeNames: []string{"node-standard"},
			want:      map[string][]string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := GroupAtomicNodes(ctx, autoscalingCtx, tc.nodeNames)
			assert.Equal(t, tc.want, got)
		})
	}
}
