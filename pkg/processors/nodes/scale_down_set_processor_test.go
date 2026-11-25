/*
Copyright 2024 The Kubernetes Authors.

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

package nodes

import (
	gocontext "context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	. "sigs.k8s.io/cluster-autoscaler/pkg/core/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestAtomicResizeFilterUnremovableNodes(t *testing.T) {
	testCases := []struct {
		name       string
		nodeGroups []struct {
			nodeGroupName        string
			nodeGroupTargetSize  int
			zeroOrMaxNodeScaling bool
		}
		removableCandidates []struct {
			candidate simulator.NodeToBeRemoved
			nodeGroup string
		}
		allowNonAtomicScaleUpToMax bool
		scaleDownContext           *ScaleDownContext
		expectedToBeRemoved        []simulator.NodeToBeRemoved
		expectedUnremovable        []simulator.UnremovableNode
		cloudProviderNodes         []struct {
			node      *v1.Node
			nodeGroup string
		}
	}{
		{
			name: "Atomic removal",
			nodeGroups: []struct {
				nodeGroupName        string
				nodeGroupTargetSize  int
				zeroOrMaxNodeScaling bool
			}{
				{
					nodeGroupName:        "ng1",
					nodeGroupTargetSize:  3,
					zeroOrMaxNodeScaling: true,
				},
				{
					nodeGroupName:        "ng2",
					nodeGroupTargetSize:  4,
					zeroOrMaxNodeScaling: true,
				},
			},
			removableCandidates: []struct {
				candidate simulator.NodeToBeRemoved
				nodeGroup string
			}{
				{
					candidate: buildRemovableNode("ng1-node-1"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng1-node-2"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng1-node-3"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng2-node-1"),
					nodeGroup: "ng2",
				},
				{
					candidate: buildRemovableNode("ng2-node-2"),
					nodeGroup: "ng2",
				},
			},
			scaleDownContext: NewDefaultScaleDownContext(),
			expectedToBeRemoved: []simulator.NodeToBeRemoved{
				buildRemovableNode("ng1-node-1"),
				buildRemovableNode("ng1-node-2"),
				buildRemovableNode("ng1-node-3"),
			},
			expectedUnremovable: []simulator.UnremovableNode{
				buildUnremovableNode("ng2-node-1", simulator.AtomicScaleDownFailed),
				buildUnremovableNode("ng2-node-2", simulator.AtomicScaleDownFailed),
			},
			cloudProviderNodes: []struct {
				node      *v1.Node
				nodeGroup string
			}{
				{
					node:      BuildTestNode("ng2-node-3", 1000, 10),
					nodeGroup: "ng2",
				},
			},
		},
		{
			name: "Mixed Groups",
			nodeGroups: []struct {
				nodeGroupName        string
				nodeGroupTargetSize  int
				zeroOrMaxNodeScaling bool
			}{
				{
					nodeGroupName:        "ng1",
					nodeGroupTargetSize:  4,
					zeroOrMaxNodeScaling: false,
				},
				{
					nodeGroupName:        "ng2",
					nodeGroupTargetSize:  4,
					zeroOrMaxNodeScaling: true,
				},
			},
			removableCandidates: []struct {
				candidate simulator.NodeToBeRemoved
				nodeGroup string
			}{
				{
					candidate: buildRemovableNode("ng1-node-1"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng1-node-2"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng1-node-3"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng2-node-1"),
					nodeGroup: "ng2",
				},
				{
					candidate: buildRemovableNode("ng2-node-2"),
					nodeGroup: "ng2",
				},
			},
			scaleDownContext: NewDefaultScaleDownContext(),
			expectedToBeRemoved: []simulator.NodeToBeRemoved{
				buildRemovableNode("ng1-node-1"),
				buildRemovableNode("ng1-node-2"),
				buildRemovableNode("ng1-node-3"),
			},
			expectedUnremovable: []simulator.UnremovableNode{
				buildUnremovableNode("ng2-node-1", simulator.AtomicScaleDownFailed),
				buildUnremovableNode("ng2-node-2", simulator.AtomicScaleDownFailed),
			},
			cloudProviderNodes: []struct {
				node      *v1.Node
				nodeGroup string
			}{
				{
					node:      BuildTestNode("ng2-node-3", 1000, 10),
					nodeGroup: "ng2",
				},
			},
		},
		{
			name: "No atomic groups",
			nodeGroups: []struct {
				nodeGroupName        string
				nodeGroupTargetSize  int
				zeroOrMaxNodeScaling bool
			}{
				{
					nodeGroupName:        "ng1",
					nodeGroupTargetSize:  3,
					zeroOrMaxNodeScaling: false,
				},
				{
					nodeGroupName:        "ng2",
					nodeGroupTargetSize:  4,
					zeroOrMaxNodeScaling: false,
				},
			},
			removableCandidates: []struct {
				candidate simulator.NodeToBeRemoved
				nodeGroup string
			}{
				{
					candidate: buildRemovableNode("ng1-node-1"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng1-node-2"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng1-node-3"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng2-node-1"),
					nodeGroup: "ng2",
				},
				{
					candidate: buildRemovableNode("ng2-node-2"),
					nodeGroup: "ng2",
				},
			},
			scaleDownContext: NewDefaultScaleDownContext(),
			expectedToBeRemoved: []simulator.NodeToBeRemoved{
				buildRemovableNode("ng1-node-1"),
				buildRemovableNode("ng1-node-2"),
				buildRemovableNode("ng1-node-3"),
				buildRemovableNode("ng2-node-1"),
				buildRemovableNode("ng2-node-2"),
			},
		},
		{
			name: "Atomic removal with failed instances - all registered nodes are removable",
			nodeGroups: []struct {
				nodeGroupName        string
				nodeGroupTargetSize  int
				zeroOrMaxNodeScaling bool
			}{{
				nodeGroupName:        "ng1",
				nodeGroupTargetSize:  10,
				zeroOrMaxNodeScaling: true,
			}},
			allowNonAtomicScaleUpToMax: true,
			removableCandidates: []struct {
				candidate simulator.NodeToBeRemoved
				nodeGroup string
			}{
				{
					candidate: buildRemovableNode("ng1-node-1"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng1-node-2"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng1-node-3"),
					nodeGroup: "ng1",
				},
			},
			scaleDownContext: NewDefaultScaleDownContext(),
			expectedToBeRemoved: []simulator.NodeToBeRemoved{
				buildRemovableNode("ng1-node-1"),
				buildRemovableNode("ng1-node-2"),
				buildRemovableNode("ng1-node-3"),
			},
			cloudProviderNodes: []struct {
				node      *v1.Node
				nodeGroup string
			}{
				{
					node:      BuildTestNode("ng2-node-1", 1000, 10),
					nodeGroup: "ng2",
				},
			},
		},
		{
			name: "Atomic removal with failed instances - not all registered nodes are unremovable",
			nodeGroups: []struct {
				nodeGroupName        string
				nodeGroupTargetSize  int
				zeroOrMaxNodeScaling bool
			}{{
				nodeGroupName:        "ng1",
				nodeGroupTargetSize:  10,
				zeroOrMaxNodeScaling: true,
			}},
			allowNonAtomicScaleUpToMax: true,
			removableCandidates: []struct {
				candidate simulator.NodeToBeRemoved
				nodeGroup string
			}{
				{
					candidate: buildRemovableNode("ng1-node-1"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng1-node-2"),
					nodeGroup: "ng1",
				},
				{
					candidate: buildRemovableNode("ng1-node-3"),
					nodeGroup: "ng1",
				},
			},
			scaleDownContext: NewDefaultScaleDownContext(),
			expectedUnremovable: []simulator.UnremovableNode{
				buildUnremovableNode("ng1-node-1", simulator.AtomicScaleDownFailed),
				buildUnremovableNode("ng1-node-2", simulator.AtomicScaleDownFailed),
				buildUnremovableNode("ng1-node-3", simulator.AtomicScaleDownFailed),
			},
			cloudProviderNodes: []struct {
				node      *v1.Node
				nodeGroup string
			}{
				{
					node:      BuildTestNode("ng1-node-4", 1000, 10),
					nodeGroup: "ng1",
				},
			},
		},
		{
			name: "Node without node group",
			removableCandidates: []struct {
				candidate simulator.NodeToBeRemoved
				nodeGroup string
			}{
				{
					candidate: buildRemovableNode("no-ng-node"),
					nodeGroup: "",
				},
			},
			scaleDownContext:    NewDefaultScaleDownContext(),
			expectedToBeRemoved: []simulator.NodeToBeRemoved{},
			expectedUnremovable: []simulator.UnremovableNode{
				buildUnremovableNode("no-ng-node", simulator.NotAutoscaled),
			},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var nodes []*v1.Node
			processor := NewAtomicResizeFilteringProcessor()
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			for _, ng := range tc.nodeGroups {
				provider.AddNodeGroupWithCustomOptions(ng.nodeGroupName, 0, 100, ng.nodeGroupTargetSize, &config.NodeGroupAutoscalingOptions{
					ZeroOrMaxNodeScaling: ng.zeroOrMaxNodeScaling, AllowNonAtomicScaleUpToMax: tc.allowNonAtomicScaleUpToMax,
				})
			}
			candidates := []simulator.NodeToBeRemoved{}
			for _, node := range tc.removableCandidates {
				provider.AddNode(node.nodeGroup, node.candidate.Node)
				candidates = append(candidates, node.candidate)
				nodes = append(nodes, node.candidate.Node)
			}
			for _, node := range tc.cloudProviderNodes {
				provider.AddNode(node.nodeGroup, node.node)
				nodes = append(nodes, node.node)
			}
			context, _ := NewScaleTestAutoscalingContext(config.AutoscalingOptions{
				NodeGroupDefaults: config.NodeGroupAutoscalingOptions{},
			}, &fake.Clientset{}, nil, provider, nil, nil, nil)
			clustersnapshot.InitializeClusterSnapshotOrDie(t, context.ClusterSnapshot, nodes, nil)

			toBeRemoved, unRemovable := processor.FilterUnremovableNodes(gocontext.Background(), &context, tc.scaleDownContext, candidates)

			assert.ElementsMatch(t, tc.expectedToBeRemoved, toBeRemoved)
			assert.ElementsMatch(t, tc.expectedUnremovable, unRemovable)
		})
	}
}

func TestAtomicResizeFilterUnremovableNodesPreservesOrder(t *testing.T) {
	t.Parallel()

	const atomicGroupCount = 20
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup("std", 0, 100, 2)
	for i := range atomicGroupCount {
		provider.AddNodeGroupWithCustomOptions(fmt.Sprintf("atomic-%02d", i), 0, 100, 2, &config.NodeGroupAutoscalingOptions{
			ZeroOrMaxNodeScaling: true,
		})
	}
	provider.AddNodeGroupWithCustomOptions("partial", 0, 100, 2, &config.NodeGroupAutoscalingOptions{
		ZeroOrMaxNodeScaling: true,
	})

	var (
		allNodes   []*v1.Node
		candidates []simulator.NodeToBeRemoved
	)
	addCandidate := func(ng, name string) {
		n := buildRemovableNode(name)
		provider.AddNode(ng, n.Node)
		allNodes = append(allNodes, n.Node)
		candidates = append(candidates, n)
	}

	addCandidate("std", "std-0")
	// Add the first node of every atomic group in order, then the second node in
	// reverse group order, so only first-seen candidate order matches atomic-00..19.
	for i := range atomicGroupCount {
		addCandidate(fmt.Sprintf("atomic-%02d", i), fmt.Sprintf("atomic-%02d-a", i))
	}
	addCandidate("partial", "partial-a")
	partialExtra := BuildTestNode("partial-b", 1000, 10)
	provider.AddNode("partial", partialExtra)
	allNodes = append(allNodes, partialExtra)
	for i := atomicGroupCount - 1; i >= 0; i-- {
		addCandidate(fmt.Sprintf("atomic-%02d", i), fmt.Sprintf("atomic-%02d-b", i))
	}
	addCandidate("std", "std-1")

	autoscalingCtx, err := NewScaleTestAutoscalingContext(config.AutoscalingOptions{}, &fake.Clientset{}, nil, provider, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewScaleTestAutoscalingContext() failed: %v", err)
	}
	clustersnapshot.InitializeClusterSnapshotOrDie(t, autoscalingCtx.ClusterSnapshot, allNodes, nil)

	processor := NewAtomicResizeFilteringProcessor()
	gotRemovable, gotUnremovable := processor.FilterUnremovableNodes(t.Context(), &autoscalingCtx, NewDefaultScaleDownContext(), candidates)

	wantRemovable := make([]string, 0, 2+atomicGroupCount*2)
	wantRemovable = append(wantRemovable, "std-0", "std-1")
	for i := range atomicGroupCount {
		wantRemovable = append(wantRemovable, fmt.Sprintf("atomic-%02d-a", i), fmt.Sprintf("atomic-%02d-b", i))
	}
	gotRemovableNames := make([]string, len(gotRemovable))
	for i, n := range gotRemovable {
		gotRemovableNames[i] = n.Node.Name
	}
	if diff := cmp.Diff(wantRemovable, gotRemovableNames); diff != "" {
		t.Errorf("AtomicResizeFilteringProcessor.FilterUnremovableNodes() removable order diff (-want +got):\n%s", diff)
	}

	wantUnremovable := []string{"partial-a"}
	gotUnremovableNames := make([]string, len(gotUnremovable))
	for i, n := range gotUnremovable {
		gotUnremovableNames[i] = n.Node.Name
	}
	if diff := cmp.Diff(wantUnremovable, gotUnremovableNames); diff != "" {
		t.Errorf("AtomicResizeFilteringProcessor.FilterUnremovableNodes() unremovable diff (-want +got):\n%s", diff)
	}
}

func buildRemovableNode(name string) simulator.NodeToBeRemoved {
	return simulator.NodeToBeRemoved{
		Node: BuildTestNode(name, 1000, 10),
	}
}

func buildUnremovableNode(name string, reason simulator.UnremovableReason) simulator.UnremovableNode {
	return simulator.UnremovableNode{
		Node:   BuildTestNode(name, 1000, 10),
		Reason: reason,
	}
}
