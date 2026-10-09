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

package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func BenchmarkBuildNodeInfoList(b *testing.B) {
	testCases := []struct {
		nodeCount int
	}{
		{
			nodeCount: 1000,
		},
		{
			nodeCount: 5000,
		},
		{
			nodeCount: 15000,
		},
		{
			nodeCount: 100000,
		},
	}

	for _, tc := range testCases {
		b.Run(fmt.Sprintf("fork add 1000 to %d", tc.nodeCount), func(b *testing.B) {
			nodes := clustersnapshot.CreateTestNodes(tc.nodeCount + 1000)
			deltaStore := NewDeltaSnapshotStore()
			for _, node := range nodes[:tc.nodeCount] {
				nodeInfo := framework.NewNodeInfo(node, nil)
				if err := deltaStore.StoreNodeInfo(nodeInfo); err != nil {
					assert.NoError(b, err)
				}
			}
			_ = deltaStore.data.getNodeInfoList()
			deltaStore.Fork()
			for _, node := range nodes[tc.nodeCount:] {
				nodeInfo := framework.NewNodeInfo(node, nil)
				if err := deltaStore.StoreNodeInfo(nodeInfo); err != nil {
					assert.NoError(b, err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				list := deltaStore.data.buildNodeInfoList()
				assert.Equal(b, tc.nodeCount+1000, len(list))
			}
		})
	}
	for _, tc := range testCases {
		b.Run(fmt.Sprintf("fork modify 1000 in %d", tc.nodeCount), func(b *testing.B) {
			nodes := clustersnapshot.CreateTestNodes(tc.nodeCount)
			deltaStore := NewDeltaSnapshotStore()
			for _, node := range nodes {
				nodeInfo := framework.NewNodeInfo(node, nil)
				if err := deltaStore.StoreNodeInfo(nodeInfo); err != nil {
					assert.NoError(b, err)
				}
			}
			_ = deltaStore.data.getNodeInfoList()
			deltaStore.Fork()
			for j := 0; j < 1000; j++ {
				pod := test.BuildTestPod(fmt.Sprintf("pod-%d", j), 100, 1024)
				if err := deltaStore.StorePodInfo(framework.NewPodInfo(pod, nil), nodes[j].Name); err != nil {
					assert.NoError(b, err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				list := deltaStore.data.buildNodeInfoList()
				assert.Equal(b, tc.nodeCount, len(list))
			}
		})
	}
	for _, tc := range testCases {
		b.Run(fmt.Sprintf("base %d", tc.nodeCount), func(b *testing.B) {
			nodes := clustersnapshot.CreateTestNodes(tc.nodeCount)
			deltaStore := NewDeltaSnapshotStore()
			for _, node := range nodes {
				nodeInfo := framework.NewNodeInfo(node, nil)
				if err := deltaStore.StoreNodeInfo(nodeInfo); err != nil {
					assert.NoError(b, err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				list := deltaStore.data.buildNodeInfoList()
				assert.Equal(b, tc.nodeCount, len(list))
			}
		})
	}
}

// BenchmarkStorePodInfoAndListNodeInfos benchmarks the Cluster Autoscaler scheduling
// simulation pattern where each pod placement (StorePodInfo) is followed by a
// NodeInfos().List() call (as invoked by kube-scheduler's RunPreFilterPlugins).
func BenchmarkStorePodInfoAndListNodeInfos(b *testing.B) {
	const podCount = 1000
	testCases := []struct {
		nodeCount int
	}{
		{nodeCount: 1000},
		{nodeCount: 5000},
		{nodeCount: 15000},
	}

	podInfos := make([]*framework.PodInfo, podCount)
	for j := 0; j < podCount; j++ {
		pod := test.BuildTestPod(fmt.Sprintf("scheduled-pod-%06d", j), 100, 1024*1024)
		podInfos[j] = framework.NewPodInfo(pod, nil)
	}

	for _, tc := range testCases {
		nodes := make([]*apiv1.Node, tc.nodeCount)
		for idx := 0; idx < tc.nodeCount; idx++ {
			nodes[idx] = test.BuildTestNode(fmt.Sprintf("k8s-worker-node-pool-default-zone-a-%06d", idx), 4000, 8*1024*1024*1024)
		}

		b.Run(fmt.Sprintf("depth0_unforked_%d_nodes_1000_pods", tc.nodeCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				deltaStore := NewDeltaSnapshotStore()
				for _, node := range nodes {
					if err := deltaStore.StoreNodeInfo(framework.NewNodeInfo(node, nil)); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := deltaStore.NodeInfos().List(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				for j := 0; j < podCount; j++ {
					if err := deltaStore.StorePodInfo(podInfos[j], nodes[j].Name); err != nil {
						b.Fatal(err)
					}
					list, err := deltaStore.NodeInfos().List()
					if err != nil || len(list) != tc.nodeCount {
						b.Fatal(err)
					}
				}
			}
		})

		b.Run(fmt.Sprintf("depth1_forked_%d_nodes_1000_pods", tc.nodeCount), func(b *testing.B) {
			deltaStore := NewDeltaSnapshotStore()
			for _, node := range nodes {
				if err := deltaStore.StoreNodeInfo(framework.NewNodeInfo(node, nil)); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := deltaStore.NodeInfos().List(); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				deltaStore.Fork()
				for j := 0; j < podCount; j++ {
					if err := deltaStore.StorePodInfo(podInfos[j], nodes[j].Name); err != nil {
						b.Fatal(err)
					}
					list, err := deltaStore.NodeInfos().List()
					if err != nil || len(list) != tc.nodeCount {
						b.Fatal(err)
					}
				}
				deltaStore.Revert()
			}
		})

		// Nodes added in a fork and modified in a nested fork: positions of the added nodes must
		// be resolved without building an O(n) index in the parent fork.
		b.Run(fmt.Sprintf("depth2_modify_nodes_added_in_depth1_%d_nodes_1000_pods", tc.nodeCount), func(b *testing.B) {
			const templateCount = 100
			templates := make([]*apiv1.Node, templateCount)
			for idx := range templates {
				templates[idx] = test.BuildTestNode(fmt.Sprintf("template-node-%03d", idx), 4000, 8*1024*1024*1024)
			}
			deltaStore := NewDeltaSnapshotStore()
			for _, node := range nodes {
				if err := deltaStore.StoreNodeInfo(framework.NewNodeInfo(node, nil)); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := deltaStore.NodeInfos().List(); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				deltaStore.Fork()
				for _, template := range templates {
					if err := deltaStore.StoreNodeInfo(framework.NewNodeInfo(template, nil)); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := deltaStore.NodeInfos().List(); err != nil {
					b.Fatal(err)
				}
				deltaStore.Fork()
				for j := 0; j < podCount; j++ {
					if err := deltaStore.StorePodInfo(podInfos[j], templates[j%templateCount].Name); err != nil {
						b.Fatal(err)
					}
					list, err := deltaStore.NodeInfos().List()
					if err != nil || len(list) != tc.nodeCount+templateCount {
						b.Fatal(err)
					}
				}
				deltaStore.Revert()
				deltaStore.Revert()
			}
		})
	}
}

// BenchmarkScaleDownCandidate benchmarks the RemovalSimulator pattern (pkg/simulator/cluster.go) for a
// single scale-down candidate: in a fork, replace the node with a pod-less ghost copy, reschedule its
// pods onto other nodes (listing nodes after each placement), remove the ghost, and commit. The commit
// goes into an outer fork that is reverted afterwards, so that every iteration starts from the same
// state.
func BenchmarkScaleDownCandidate(b *testing.B) {
	const podCount = 10
	for _, nodeCount := range []int{1000, 5000, 15000} {
		nodes := make([]*apiv1.Node, nodeCount)
		for idx := range nodes {
			nodes[idx] = test.BuildTestNode(fmt.Sprintf("k8s-worker-node-pool-default-zone-a-%06d", idx), 4000, 8*1024*1024*1024)
		}
		podInfos := make([]*framework.PodInfo, podCount)
		for j := range podInfos {
			podInfos[j] = framework.NewPodInfo(test.BuildTestPod(fmt.Sprintf("rescheduled-pod-%03d", j), 100, 1024*1024), nil)
		}

		b.Run(fmt.Sprintf("%d_nodes_%d_pods", nodeCount, podCount), func(b *testing.B) {
			ctx := context.Background()
			deltaStore := NewDeltaSnapshotStore()
			for _, node := range nodes {
				if err := deltaStore.StoreNodeInfo(framework.NewNodeInfo(node, nil)); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := deltaStore.NodeInfos().List(); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				candidate := i % nodeCount
				deltaStore.Fork()
				deltaStore.Fork()
				if err := deltaStore.RemoveNodeInfo(ctx, nodes[candidate].Name); err != nil {
					b.Fatal(err)
				}
				if err := deltaStore.StoreNodeInfo(framework.NewNodeInfo(nodes[candidate], nil)); err != nil {
					b.Fatal(err)
				}
				for j := 0; j < podCount; j++ {
					target := nodes[(candidate+1+j)%nodeCount].Name
					if err := deltaStore.StorePodInfo(podInfos[j], target); err != nil {
						b.Fatal(err)
					}
					if list, err := deltaStore.NodeInfos().List(); err != nil || len(list) != nodeCount {
						b.Fatal(err)
					}
				}
				if err := deltaStore.RemoveNodeInfo(ctx, nodes[candidate].Name); err != nil {
					b.Fatal(err)
				}
				if list, err := deltaStore.NodeInfos().List(); err != nil || len(list) != nodeCount-1 {
					b.Fatal(err)
				}
				if err := deltaStore.Commit(); err != nil {
					b.Fatal(err)
				}
				if list, err := deltaStore.NodeInfos().List(); err != nil || len(list) != nodeCount-1 {
					b.Fatal(err)
				}
				deltaStore.Revert()
			}
		})
	}
}
