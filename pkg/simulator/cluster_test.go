/*
Copyright 2016 The Kubernetes Authors.

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

package simulator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/options"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type simulateNodeRemovalTestConfig struct {
	name        string
	pods        []*apiv1.Pod
	allNodes    []*apiv1.Node
	nodeName    string
	toRemove    *NodeToBeRemoved
	unremovable *UnremovableNode
}

func TestSimulateNodeRemoval(t *testing.T) {
	emptyNode := BuildTestNode("n1", 1000, 2000000)

	// two small pods backed by ReplicaSet
	drainableNode := BuildTestNode("n2", 1000, 2000000)

	// one small pod, not backed by anything
	nonDrainableNode := BuildTestNode("n3", 1000, 2000000)

	// one very large pod
	fullNode := BuildTestNode("n4", 1000, 2000000)

	// noExistNode it doesn't have any node info in the cluster snapshot.
	noExistNode := &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n5"}}

	SetNodeReadyState(emptyNode, true, time.Time{})
	SetNodeReadyState(drainableNode, true, time.Time{})
	SetNodeReadyState(nonDrainableNode, true, time.Time{})
	SetNodeReadyState(fullNode, true, time.Time{})

	replicas := int32(5)
	replicaSets := []*appsv1.ReplicaSet{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rs",
				Namespace: "default",
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicas,
			},
		},
	}
	rsLister, err := kube_util.NewTestReplicaSetLister(replicaSets)
	assert.NoError(t, err)
	registry := kube_util.NewListerRegistry(nil, nil, nil, nil, nil, nil, nil, rsLister, nil)

	ownerRefs := GenerateOwnerReferences("rs", "ReplicaSet", "extensions/v1beta1", "")

	// Pods for the basic drain/capacity test cases.
	drainableReplicaPodA := BuildTestPod("drainable-rs-pod-a", 100, 100000)
	drainableReplicaPodA.OwnerReferences = ownerRefs
	drainableReplicaPodA.Spec.NodeName = drainableNode.Name

	drainableReplicaPodB := BuildTestPod("drainable-rs-pod-b", 100, 100000)
	drainableReplicaPodB.OwnerReferences = ownerRefs
	drainableReplicaPodB.Spec.NodeName = drainableNode.Name

	standalonePodOnNonDrainable := BuildTestPod("standalone-on-non-drainable", 100, 100000)
	standalonePodOnNonDrainable.Spec.NodeName = nonDrainableNode.Name

	largePodOnFull := BuildTestPod("large-pod-on-full", 1000, 100000)
	largePodOnFull.Spec.NodeName = fullNode.Name

	clusterSnapshot := testsnapshot.NewTestSnapshotOrDie(t)

	topoNode1 := BuildTestNode("topo-n1", 1000, 2000000)
	topoNode2 := BuildTestNode("topo-n2", 1000, 2000000)
	topoNode3 := BuildTestNode("topo-n3", 1000, 2000000)
	topoNode1.Labels = map[string]string{"kubernetes.io/hostname": "topo-n1"}
	topoNode2.Labels = map[string]string{"kubernetes.io/hostname": "topo-n2"}
	topoNode3.Labels = map[string]string{"kubernetes.io/hostname": "topo-n3"}

	SetNodeReadyState(topoNode1, true, time.Time{})
	SetNodeReadyState(topoNode2, true, time.Time{})
	SetNodeReadyState(topoNode3, true, time.Time{})

	// buildTopoPod creates a topology-spread-constrained pod on a given node.
	buildTopoPod := func(name, nodeName string, tsc apiv1.TopologySpreadConstraint) *apiv1.Pod {
		pod := BuildTestPod(name, 100, 100000)
		pod.Labels = map[string]string{"app": "topo-app"}
		pod.OwnerReferences = ownerRefs
		pod.Spec.NodeName = nodeName
		pod.Spec.TopologySpreadConstraints = []apiv1.TopologySpreadConstraint{tsc}
		return pod
	}

	minDomains := int32(2)
	maxSkew := int32(1)
	defaultTSC := apiv1.TopologySpreadConstraint{
		MaxSkew:           maxSkew,
		TopologyKey:       "kubernetes.io/hostname",
		WhenUnsatisfiable: apiv1.DoNotSchedule,
		MinDomains:        &minDomains,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "topo-app"},
		},
	}

	// NodeTaintsPolicy: Honor — ghost node's taint excludes it from domain
	// counting, so removal succeeds even with maxSkew=1.
	honorPolicy := apiv1.NodeInclusionPolicyHonor
	honorTSC := defaultTSC
	honorTSC.NodeTaintsPolicy = &honorPolicy

	// Pods for topology-spread test cases (default policy).
	defaultPodN1 := buildTopoPod("default-tsc-pod-n1", topoNode1.Name, defaultTSC)
	defaultPodN2 := buildTopoPod("default-tsc-pod-n2", topoNode2.Name, defaultTSC)
	defaultPodN3 := buildTopoPod("default-tsc-pod-n3", topoNode3.Name, defaultTSC)
	blockerN2 := BuildTestPod("blocker-n2", 100, 100000)
	blockerN2.Spec.NodeName = topoNode2.Name
	blockerN3 := BuildTestPod("blocker-n3", 100, 100000)
	blockerN3.Spec.NodeName = topoNode3.Name

	// Pods for topology-spread test cases (Honor policy).
	honorPodN1 := buildTopoPod("honor-tsc-pod-n1", topoNode1.Name, honorTSC)
	honorPodN2 := buildTopoPod("honor-tsc-pod-n2", topoNode2.Name, honorTSC)
	honorPodN3 := buildTopoPod("honor-tsc-pod-n3", topoNode3.Name, honorTSC)

	tests := []simulateNodeRemovalTestConfig{
		{
			name:        "just an empty node, should be removed",
			nodeName:    emptyNode.Name,
			allNodes:    []*apiv1.Node{emptyNode},
			toRemove:    &NodeToBeRemoved{Node: emptyNode},
			unremovable: nil,
		},
		{
			name:     "just a drainable node, but nowhere for pods to go to",
			pods:     []*apiv1.Pod{drainableReplicaPodA, drainableReplicaPodB},
			nodeName: drainableNode.Name,
			allNodes: []*apiv1.Node{drainableNode},
			toRemove: nil,
			unremovable: &UnremovableNode{
				Node:   drainableNode,
				Reason: NoPlaceToMovePods,
			},
		},
		{
			name:        "drainable node, and a mostly empty node that can take its pods",
			pods:        []*apiv1.Pod{drainableReplicaPodA, drainableReplicaPodB, standalonePodOnNonDrainable},
			nodeName:    drainableNode.Name,
			allNodes:    []*apiv1.Node{drainableNode, nonDrainableNode},
			toRemove:    &NodeToBeRemoved{Node: drainableNode, PodsToReschedule: []*apiv1.Pod{drainableReplicaPodA, drainableReplicaPodB}},
			unremovable: nil,
		},
		{
			name:        "drainable node, and a full node that cannot fit anymore pods",
			pods:        []*apiv1.Pod{drainableReplicaPodA, drainableReplicaPodB, largePodOnFull},
			nodeName:    drainableNode.Name,
			allNodes:    []*apiv1.Node{drainableNode, fullNode},
			toRemove:    nil,
			unremovable: &UnremovableNode{Node: drainableNode, Reason: NoPlaceToMovePods},
		},
		{
			name:        "4 nodes, 1 empty, 1 drainable",
			pods:        []*apiv1.Pod{drainableReplicaPodA, drainableReplicaPodB, standalonePodOnNonDrainable, largePodOnFull},
			nodeName:    emptyNode.Name,
			allNodes:    []*apiv1.Node{emptyNode, drainableNode, fullNode, nonDrainableNode},
			toRemove:    &NodeToBeRemoved{Node: emptyNode},
			unremovable: nil,
		},
		{
			name:     "topology spread constraint test - node unremovable due to phantom zone",
			pods:     []*apiv1.Pod{defaultPodN1, defaultPodN2, defaultPodN3, blockerN2, blockerN3},
			allNodes: []*apiv1.Node{topoNode1, topoNode2, topoNode3},
			nodeName: topoNode1.Name,
			toRemove: nil,
			unremovable: &UnremovableNode{
				Node:   topoNode1,
				Reason: NoPlaceToMovePods,
			},
		},
		{
			name:        "topology spread constraint test - node removable with nodeTaintsPolicy Honor",
			pods:        []*apiv1.Pod{honorPodN1, honorPodN2, honorPodN3},
			allNodes:    []*apiv1.Node{topoNode1, topoNode2, topoNode3},
			nodeName:    topoNode1.Name,
			toRemove:    &NodeToBeRemoved{Node: topoNode1, PodsToReschedule: []*apiv1.Pod{honorPodN1}},
			unremovable: nil,
		},
		{
			name:        "candidate not in clusterSnapshot should be marked unremovable",
			nodeName:    noExistNode.Name,
			allNodes:    []*apiv1.Node{},
			pods:        []*apiv1.Pod{},
			toRemove:    nil,
			unremovable: &UnremovableNode{Node: noExistNode, Reason: NoNodeInfo},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			destinations := make(map[string]bool)
			for _, node := range test.allNodes {
				destinations[node.Name] = true
			}
			clustersnapshot.InitializeClusterSnapshotOrDie(t, clusterSnapshot, test.allNodes, test.pods)
			r := NewRemovalSimulator(registry, clusterSnapshot, testDeleteOptions(), nil, false)
			toRemove, unremovable := r.SimulateNodeRemoval(context.Background(), test.nodeName, destinations, time.Now(), nil)
			assert.Equal(t, test.toRemove, toRemove)
			assert.Equal(t, test.unremovable, unremovable)
		})
	}
}

func TestSimulateNodesGroupRemoval(t *testing.T) {
	node1 := BuildTestNode("n1", 1000, 2000000)
	node2 := BuildTestNode("n2", 1000, 2000000)
	destNode := BuildTestNode("dest", 2000, 4000000)

	SetNodeReadyState(node1, true, time.Time{})
	SetNodeReadyState(node2, true, time.Time{})
	SetNodeReadyState(destNode, true, time.Time{})

	replicas := int32(5)
	replicaSets := []*appsv1.ReplicaSet{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rs",
				Namespace: "default",
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicas,
			},
		},
	}
	rsLister, err := kube_util.NewTestReplicaSetLister(replicaSets)
	assert.NoError(t, err)
	registry := kube_util.NewListerRegistry(nil, nil, nil, nil, nil, nil, nil, rsLister, nil)
	ownerRefs := GenerateOwnerReferences("rs", "ReplicaSet", "extensions/v1beta1", "")

	pod1 := BuildTestPod("pod1", 100, 100000)
	pod1.OwnerReferences = ownerRefs
	pod1.Spec.NodeName = node1.Name

	pod2 := BuildTestPod("pod2", 100, 100000)
	pod2.OwnerReferences = ownerRefs
	pod2.Spec.NodeName = node2.Name

	podUnmovable := BuildTestPod("pod-unmovable", 100, 100000)
	podUnmovable.Spec.NodeName = node2.Name

	testCases := []struct {
		name                 string
		groupNodes           []string
		pods                 []*apiv1.Pod
		initialDestinations  map[string]bool
		expectUnremovable    bool
		expectedFailedIndex  int
		expectedRemovableLen int
		expectedDestinations map[string]bool
	}{
		{
			name:                 "all nodes in group removable",
			groupNodes:           []string{"n1", "n2"},
			pods:                 []*apiv1.Pod{pod1, pod2},
			initialDestinations:  map[string]bool{"dest": true, "n1": true, "n2": true},
			expectUnremovable:    false,
			expectedFailedIndex:  -1,
			expectedRemovableLen: 2,
			expectedDestinations: map[string]bool{"dest": true, "n1": false, "n2": false},
		},
		{
			name:                 "node in group unremovable aborts early and rolls back destinations",
			groupNodes:           []string{"n1", "n2"},
			pods:                 []*apiv1.Pod{pod1, podUnmovable},
			initialDestinations:  map[string]bool{"dest": true, "n1": true, "n2": true},
			expectUnremovable:    true,
			expectedFailedIndex:  1,
			expectedRemovableLen: 0,
			// destinations should be rolled back to their original state
			expectedDestinations: map[string]bool{"dest": true, "n1": true, "n2": true},
		},
		{
			name:                 "empty group succeeds with empty result",
			groupNodes:           []string{},
			pods:                 []*apiv1.Pod{pod1, pod2},
			initialDestinations:  map[string]bool{"dest": true, "n1": true, "n2": true},
			expectUnremovable:    false,
			expectedFailedIndex:  -1,
			expectedRemovableLen: 0,
			expectedDestinations: map[string]bool{"dest": true, "n1": true, "n2": true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			clustersnapshot.InitializeClusterSnapshotOrDie(t, snapshot, []*apiv1.Node{node1, node2, destNode}, tc.pods)

			destinations := make(map[string]bool, len(tc.initialDestinations))
			for k, v := range tc.initialDestinations {
				destinations[k] = v
			}

			r := NewRemovalSimulator(registry, snapshot, testDeleteOptions(), nil, false)
			removable, unremovable, failedIdx := r.SimulateNodesGroupRemoval(context.Background(), tc.groupNodes, destinations, time.Now(), nil)

			if tc.expectUnremovable {
				assert.NotNil(t, unremovable)
				assert.Nil(t, removable)
			} else {
				assert.Nil(t, unremovable)
				assert.Len(t, removable, tc.expectedRemovableLen)
			}
			assert.Equal(t, tc.expectedFailedIndex, failedIdx)
			for k, expectedVal := range tc.expectedDestinations {
				assert.Equalf(t, expectedVal, destinations[k], "destinationMap mismatch for node %s", k)
			}
		})
	}
}

func testDeleteOptions() options.NodeDeleteOptions {
	return options.NodeDeleteOptions{
		SkipNodesWithSystemPods:           true,
		SkipNodesWithLocalStorage:         true,
		SkipNodesWithCustomControllerPods: true,
	}
}
