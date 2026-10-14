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

package estimator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

func makePodEquivalenceGroup(pod *apiv1.Pod, podCount int) PodEquivalenceGroup {
	pods := []*apiv1.Pod{}
	for i := 0; i < podCount; i++ {
		pods = append(pods, pod)
	}
	return PodEquivalenceGroup{
		Pods: pods,
	}
}

func makeNode(cpu, mem, podCount int64, name string, zone string) *apiv1.Node {
	node := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"kubernetes.io/hostname":      name,
				"topology.kubernetes.io/zone": zone,
			},
		},
		Status: apiv1.NodeStatus{
			Capacity: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(cpu, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(mem*units.MiB, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(podCount, resource.DecimalSI),
			},
		},
	}
	node.Status.Allocatable = node.Status.Capacity
	SetNodeReadyState(node, true, time.Time{})
	return node
}

func TestBinpackingEstimate(t *testing.T) {
	highResourcePodGroup := makePodEquivalenceGroup(
		BuildTestPod(
			"estimatee",
			500,
			1000,
			WithNamespace("universe"),
			WithLabels(map[string]string{
				"app": "estimatee",
			}),
		),
		10,
	)
	testCases := []struct {
		name                 string
		millicores           int64
		memory               int64
		maxNodes             int
		podsEquivalenceGroup []PodEquivalenceGroup
		topologySpreadingKey string
		expectNodeCount      int
		expectPodCount       int
		expectProcessedPods  []*apiv1.Pod
	}{
		{
			name:       "simple resource-based binpacking",
			millicores: 350*3 - 50,
			memory:     2 * 1000,
			podsEquivalenceGroup: []PodEquivalenceGroup{makePodEquivalenceGroup(
				BuildTestPod(
					"estimatee",
					350,
					1000,
					WithNamespace("universe"),
					WithLabels(map[string]string{
						"app": "estimatee",
					})), 10)},
			expectNodeCount: 5,
			expectPodCount:  10,
		},
		{
			name:       "pods-per-node bound binpacking",
			millicores: 10000,
			memory:     20000,
			podsEquivalenceGroup: []PodEquivalenceGroup{makePodEquivalenceGroup(
				BuildTestPod(
					"estimatee",
					10,
					100,
					WithNamespace("universe"),
					WithLabels(map[string]string{
						"app": "estimatee",
					})), 20)},
			expectNodeCount: 2,
			expectPodCount:  20,
		},
		{
			name:       "hostport conflict forces pod-per-node",
			millicores: 1000,
			memory:     5000,
			podsEquivalenceGroup: []PodEquivalenceGroup{makePodEquivalenceGroup(
				BuildTestPod(
					"estimatee",
					200,
					1000,
					WithNamespace("universe"),
					WithLabels(map[string]string{
						"app": "estimatee",
					}),
					WithHostPort(5555)), 8)},
			expectNodeCount: 8,
			expectPodCount:  8,
		},
		{
			name:       "limiter cuts binpacking",
			millicores: 1000,
			memory:     5000,
			podsEquivalenceGroup: []PodEquivalenceGroup{makePodEquivalenceGroup(
				BuildTestPod(
					"estimatee",
					500,
					1000,
					WithNamespace("universe"),
					WithLabels(map[string]string{
						"app": "estimatee",
					})), 20)},
			maxNodes:        5,
			expectNodeCount: 5,
			expectPodCount:  10,
		},
		{
			name:       "decreasing ordered pods are processed first",
			millicores: 1000,
			memory:     5000,
			podsEquivalenceGroup: append([]PodEquivalenceGroup{makePodEquivalenceGroup(
				BuildTestPod(
					"estimatee",
					50,
					1000,
					WithNamespace("universe"),
					WithLabels(map[string]string{
						"app": "estimatee",
					})), 10)}, highResourcePodGroup),
			maxNodes:            5,
			expectNodeCount:     5,
			expectPodCount:      10,
			expectProcessedPods: highResourcePodGroup.Pods,
		},
		{
			name:       "hostname topology spreading with maxSkew=2 forces 2 pods/node",
			millicores: 1000,
			memory:     5000,
			podsEquivalenceGroup: []PodEquivalenceGroup{makePodEquivalenceGroup(
				BuildTestPod(
					"estimatee",
					200,
					200,
					WithNamespace("universe"),
					WithLabels(map[string]string{
						"app": "estimatee",
					}),
					WithMaxSkew(2, "kubernetes.io/hostname", 1)), 8)},
			expectNodeCount: 4,
			expectPodCount:  8,
		},
		{
			name:       "zonal topology spreading with maxSkew=2 only allows 2 pods to schedule",
			millicores: 1000,
			memory:     5000,
			podsEquivalenceGroup: []PodEquivalenceGroup{makePodEquivalenceGroup(
				BuildTestPod(
					"estimatee",
					20,
					100,
					WithNamespace("universe"),
					WithLabels(map[string]string{
						"app": "estimatee",
					}),
					WithMaxSkew(2, "topology.kubernetes.io/zone", 1)), 8)},
			expectNodeCount: 1,
			expectPodCount:  2,
		},
		{
			name:       "hostname topology spreading with maxSkew=1 with a large scaleup handles scheduling pods retroactively",
			millicores: 1000,
			memory:     5000,
			podsEquivalenceGroup: []PodEquivalenceGroup{makePodEquivalenceGroup(
				BuildTestPod(
					"estimatee",
					20,
					100,
					WithNamespace("universe"),
					WithLabels(map[string]string{
						"app": "estimatee",
					}),
					WithMaxSkew(1, "kubernetes.io/hostname", 3)), 12)},
			expectNodeCount: 3,
			expectPodCount:  12,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clusterSnapshot := testsnapshot.NewTestSnapshotOrDie(t)
			// Add one node in different zone to trigger topology spread constraints
			err := clusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(makeNode(100, 100, 10, "oldnode", "zone-jupiter")))
			assert.NoError(t, err)

			limiter := NewThresholdBasedEstimationLimiter([]Threshold{NewStaticThreshold(tc.maxNodes, time.Duration(0))})
			processor := NewDecreasingPodOrderer()
			estimator := NewBinpackingNodeEstimator(clusterSnapshot, limiter, processor, nil /* EstimationContext */, nil /* EstimationAnalyserFunc */, false)
			node := makeNode(tc.millicores, tc.memory, 10, "template", "zone-mars")
			nodeInfo := framework.NewTestNodeInfo(node)

			estimatedNodes, estimatedPods := estimator.Estimate(context.Background(), tc.podsEquivalenceGroup, nodeInfo, nil)
			assert.Equal(t, tc.expectNodeCount, estimatedNodes)
			assert.Equal(t, tc.expectPodCount, len(estimatedPods))
			if tc.expectProcessedPods != nil {
				assert.Equal(t, tc.expectProcessedPods, estimatedPods)
			}
			// For single pod group estimations, the result should be consistent with fastpath and non-fastpath
			if len(tc.podsEquivalenceGroup) == 1 {
				fastpathEstimator := NewBinpackingNodeEstimator(clusterSnapshot, limiter, processor, nil /* EstimationContext */, nil /* EstimationAnalyserFunc */, true)
				fastpathEstimatedNodes, fastpathEstimatedPods := fastpathEstimator.Estimate(context.Background(), tc.podsEquivalenceGroup, nodeInfo, nil)
				assert.Equal(t, fastpathEstimatedNodes, estimatedNodes)
				assert.Equal(t, fastpathEstimatedPods, estimatedPods)
			}
		})
	}
}

// TestBinpackingEstimatePlacementOnAddedNodes checks on which of the nodes added during the
// estimation pods from later equivalence groups end up.
func TestBinpackingEstimatePlacementOnAddedNodes(t *testing.T) {
	const zoneKey = "topology.kubernetes.io/zone"
	pods := func(name string, millicores int64, count int, options ...func(*apiv1.Pod)) PodEquivalenceGroup {
		options = append([]func(*apiv1.Pod){WithNamespace("universe"), WithLabels(map[string]string{"app": name})}, options...)
		return makePodEquivalenceGroup(BuildTestPod(name, millicores, 100, options...), count)
	}
	// Pods of this group can't share a zone, so every pod after the first one fails to schedule
	// and leaves an empty node added for it behind.
	zonalAntiAffinityPods := func(name string, millicores int64, count int) PodEquivalenceGroup {
		return pods(name, millicores, count, WithPodAntiAffinity(map[string]string{"app": name}, zoneKey))
	}

	// Nodes added by the estimator are named after the template, with the index of the node in the
	// order they were added (see addNewNodeToSnapshot), so the highest index is the last node.
	const templateName = "template"
	addedNode := func(index int) string { return fmt.Sprintf("%s-e-%d", templateName, index) }

	testCases := []struct {
		name string
		// All groups have different pod sizes, so the orderer keeps them in this order.
		podsEquivalenceGroups []PodEquivalenceGroup
		fastpath              bool
		maxNodes              int
		expectNodeCount       int
		expectPodCount        int
		// Number of pods on each node added during the estimation, by node name.
		expectPodsPerNode map[string]int
	}{
		{
			name: "pods from later groups go to the last node while it has room",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				pods("big", 600, 2),
				pods("a", 150, 1),
				pods("b", 120, 1),
				pods("c", 100, 1),
			},
			expectNodeCount:   2,
			expectPodCount:    5,
			expectPodsPerNode: map[string]int{addedNode(0): 1, addedNode(1): 4},
		},
		{
			name: "pods go to an earlier node when the last node is full",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				pods("big", 600, 2),
				pods("medium", 400, 1),
				pods("small", 300, 1),
			},
			expectNodeCount:   2,
			expectPodCount:    4,
			expectPodsPerNode: map[string]int{addedNode(0): 2, addedNode(1): 2},
		},
		{
			name: "pods go to an earlier node when the last node has a host port conflict",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				pods("big", 600, 2),
				pods("hostport", 200, 2, WithHostPort(5555)),
			},
			expectNodeCount:   2,
			expectPodCount:    4,
			expectPodsPerNode: map[string]int{addedNode(0): 2, addedNode(1): 2},
		},
		{
			name: "pods go to an earlier node when the last node fails hostname anti-affinity",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				pods("big", 600, 2),
				pods("spread", 200, 2, WithPodHostnameAntiAffinity(map[string]string{"app": "spread"})),
			},
			expectNodeCount:   2,
			expectPodCount:    4,
			expectPodsPerNode: map[string]int{addedNode(0): 2, addedNode(1): 2},
		},
		{
			name: "empty last node doesn't take pods that fit on a node with pods",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				zonalAntiAffinityPods("zonal", 600, 2),
				pods("small", 300, 1),
			},
			expectNodeCount:   1,
			expectPodCount:    2,
			expectPodsPerNode: map[string]int{addedNode(0): 2, addedNode(1): 0},
		},
		{
			name: "empty last node is used when pods don't fit on any node with pods",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				zonalAntiAffinityPods("zonal", 600, 2),
				pods("medium", 500, 1),
			},
			expectNodeCount:   2,
			expectPodCount:    2,
			expectPodsPerNode: map[string]int{addedNode(0): 1, addedNode(1): 1},
		},
		{
			// The last group is binpacked with fastpath: 10 of its 29 remaining pods fill the empty
			// last node, and the other 19 are counted on 2 more nodes that aren't added to the snapshot.
			name: "fastpath reuses an empty last node",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				zonalAntiAffinityPods("zonal", 600, 2),
				pods("small", 300, 1),
				pods("many", 100, 30),
			},
			fastpath:          true,
			expectNodeCount:   4,
			expectPodCount:    32,
			expectPodsPerNode: map[string]int{addedNode(0): 3, addedNode(1): 10},
		},
		{
			// Adding another node instead of reusing the empty one would use up the limit one node
			// early and leave the last 9 pods out.
			name: "fastpath reuses an empty last node without using up the node limit",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				zonalAntiAffinityPods("zonal", 600, 2),
				pods("small", 300, 1),
				pods("many", 100, 30),
			},
			fastpath:          true,
			maxNodes:          4,
			expectNodeCount:   4,
			expectPodCount:    32,
			expectPodsPerNode: map[string]int{addedNode(0): 3, addedNode(1): 10},
		},
	}
	stores := map[string]func() clustersnapshot.ClusterSnapshotStore{
		"basic": func() clustersnapshot.ClusterSnapshotStore { return store.NewBasicSnapshotStore() },
		"delta": func() clustersnapshot.ClusterSnapshotStore { return store.NewDeltaSnapshotStore() },
	}
	for _, tc := range testCases {
		for storeName, newStore := range stores {
			t.Run(fmt.Sprintf("%s/%s", tc.name, storeName), func(t *testing.T) {
				clusterSnapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, newStore())
				err := clusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(makeNode(100, 100, 10, "oldnode", "zone-jupiter")))
				assert.NoError(t, err)

				podsPerNode := map[string]int{}
				recordPodsPerNode := func(snapshot clustersnapshot.ClusterSnapshot, _ cloudprovider.NodeGroup, _ map[string]bool) {
					nodeInfos, err := snapshot.ListNodeInfos()
					assert.NoError(t, err)
					for _, nodeInfo := range nodeInfos {
						if name := nodeInfo.Node().Name; name != "oldnode" {
							podsPerNode[name] = len(nodeInfo.Pods())
						}
					}
				}
				limiter := NewThresholdBasedEstimationLimiter([]Threshold{NewStaticThreshold(tc.maxNodes, time.Duration(0))})
				estimator := NewBinpackingNodeEstimator(clusterSnapshot, limiter, NewDecreasingPodOrderer(), nil, recordPodsPerNode, tc.fastpath)
				nodeInfo := framework.NewTestNodeInfo(makeNode(1000, 1000, 10, "template", "zone-mars"))

				estimatedNodes, estimatedPods := estimator.Estimate(t.Context(), tc.podsEquivalenceGroups, nodeInfo, nil)
				assert.Equal(t, tc.expectNodeCount, estimatedNodes)
				assert.Equal(t, tc.expectPodCount, len(estimatedPods))
				assert.Equal(t, tc.expectPodsPerNode, podsPerNode)
			})
		}
	}
}

func BenchmarkBinpackingEstimate(b *testing.B) {
	millicores := int64(1000)
	memory := int64(5000)
	podsPerNode := int64(100)
	maxNodes := 3000
	expectNodeCount := 2595
	expectPodCount := 51000
	podsEquivalenceGroup := []PodEquivalenceGroup{
		makePodEquivalenceGroup(
			BuildTestPod(
				"estimatee",
				50,
				100,
				WithNamespace("universe"),
				WithLabels(map[string]string{
					"app": "estimatee",
				})),
			50000,
		),
		makePodEquivalenceGroup(
			BuildTestPod(
				"estimatee",
				95,
				190,
				WithNamespace("universe"),
				WithLabels(map[string]string{
					"app": "estimatee",
				})),
			1000,
		),
	}

	for i := 0; i < b.N; i++ {
		clusterSnapshot := testsnapshot.NewTestSnapshotOrDie(b)
		err := clusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(makeNode(100, 100, 10, "oldnode", "zone-jupiter")))
		assert.NoError(b, err)

		limiter := NewThresholdBasedEstimationLimiter([]Threshold{NewStaticThreshold(maxNodes, time.Duration(0))})
		processor := NewDecreasingPodOrderer()
		estimator := NewBinpackingNodeEstimator(clusterSnapshot, limiter, processor, nil /* EstimationContext */, nil /* EstimationAnalyserFunc */, false)
		node := makeNode(millicores, memory, podsPerNode, "template", "zone-mars")
		nodeInfo := framework.NewTestNodeInfo(node)

		estimatedNodes, estimatedPods := estimator.Estimate(context.Background(), podsEquivalenceGroup, nodeInfo, nil)
		assert.Equal(b, expectNodeCount, estimatedNodes)
		assert.Equal(b, expectPodCount, len(estimatedPods))
	}
}

func TestDetermineBestPodEquivalenceGroupToFastpath(t *testing.T) {
	testCases := []struct {
		name                  string
		podsEquivalenceGroups []PodEquivalenceGroup
		expectResult          int
	}{
		{
			name: "both groups 2 nodes, take group with more pods",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				makePodEquivalenceGroup(BuildTestPod("estimatee1", 30, 5), 5), // 2 nodes, 5 pods, p - p/n = 3
				makePodEquivalenceGroup(BuildTestPod("estimatee2", 50, 6), 3), // 2 nodes, 3 pods, p - p/n = 2
			},
			expectResult: 0,
		},
		{
			name: "take group with largest score, even if it has less pods",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				makePodEquivalenceGroup(BuildTestPod("estimatee1", 30, 5), 6),  // 2 nodes, 6 pods, p - p/n = 3
				makePodEquivalenceGroup(BuildTestPod("estimatee2", 90, 90), 5), // 5 nodes, 5 pods, p - p/n = 4
			},
			expectResult: 1,
		},
		{
			name: "take group with largest score, even if it has less nodes",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				makePodEquivalenceGroup(BuildTestPod("estimatee1", 15, 5), 10), // 2 nodes, 10 pods, p - p/n = 5
				makePodEquivalenceGroup(BuildTestPod("estimatee2", 90, 90), 5), // 5 nodes, 5  pods, p - p/n = 4
			},
			expectResult: 0,
		},
		{
			name: "equal score default to last group",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				makePodEquivalenceGroup(BuildTestPod("estimatee2", 80, 80), 3), // 3 nodes, 3 pods, p - p/n = 2
				makePodEquivalenceGroup(BuildTestPod("estimatee2", 70, 70), 3), // 3 nodes, 3 pods, p - p/n = 2
				makePodEquivalenceGroup(BuildTestPod("estimatee1", 50, 5), 4),  // 2 nodes, 4 pods, p - p/n = 2
			},
			expectResult: 2,
		},
		{
			name: "no fastpath supporting pod groups",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				makePodEquivalenceGroup(BuildTestPod("estimatee1", 5, 5, WithMaxSkew(2, "kubernetes.io/hostname", 1)), 10),
				makePodEquivalenceGroup(BuildTestPod("estimatee2", 8, 8, WithMaxSkew(2, "kubernetes.io/hostname", 1)), 3),
			},
			expectResult: -1,
		},
		{
			name: "no resource requests defaults to last group, still valid fastpath group",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				makePodEquivalenceGroup(&apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "estimatee1"}, Spec: apiv1.PodSpec{Containers: []apiv1.Container{}}}, 2),
				makePodEquivalenceGroup(&apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "estimatee2"}, Spec: apiv1.PodSpec{Containers: []apiv1.Container{}}}, 4),
			},
			expectResult: 1,
		},
		{
			name: "groups with only 1 node have the lowest score",
			podsEquivalenceGroups: []PodEquivalenceGroup{
				makePodEquivalenceGroup(BuildTestPod("estimatee2", 60, 60), 2), // 2 nodes, 2  pods, p - p/n = 1
				makePodEquivalenceGroup(BuildTestPod("estimatee1", 1, 5), 100), // 1 node, 100 pods, p - p/n = 0
			},
			expectResult: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodeInfo := framework.NewTestNodeInfo(makeNode(100, 100, 10, "oldnode", "zone-jupiter"))
			result := determineBestPEGToFastpath(tc.podsEquivalenceGroups, nodeInfo)
			assert.Equal(t, tc.expectResult, result)
		})
	}
}
