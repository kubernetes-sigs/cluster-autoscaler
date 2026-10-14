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

package predicate

import (
	"context"
	"fmt"
	"slices"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	scheduler_config_latest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"

	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// BenchmarkRunFiltersUntilPassingNode_LargeScale measures RunFiltersUntilPassingNode in large clusters, depending on:
//   - where the only passing Node is in the order in which the Nodes are checked,
//   - why the other Nodes don't pass: either a Filter plugin rejects them, or opts.IsNodeAcceptable rejects them
//     before any Filter plugin runs (e.g. binpacking only accepts the Nodes added during the simulation),
//   - how many workers check the Nodes in parallel.
//
// All Nodes are full, except for one Node that has room for one more Pod.
func BenchmarkRunFiltersUntilPassingNode_LargeScale(b *testing.B) {
	const (
		podsPerNode = 10
		podCPU      = 100
		podMem      = 1000
	)
	// Fits only on the Node that has room for one more Pod.
	pod := BuildTestPod("pod", podCPU, podMem)
	// Doesn't fit on any Node.
	bigPod := BuildTestPod("big-pod", 2*podCPU, 2*podMem)

	b.ReportAllocs()
	for _, nodeCount := range []int{1000, 5000, 15000} {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			pluginRunner, snapshot := newTestPluginRunnerAndDeltaSnapshot(b)
			freeNodeName := fmt.Sprintf("n-%d", nodeCount/2)
			for i := range nodeCount {
				nodeName := fmt.Sprintf("n-%d", i)
				podCount := podsPerNode
				if nodeName == freeNodeName {
					podCount--
				}
				pods := make([]*apiv1.Pod, podCount)
				for j := range pods {
					pods[j] = BuildTestPod(fmt.Sprintf("%s-p-%d", nodeName, j), podCPU, podMem, WithNodeName(nodeName))
				}
				node := BuildTestNode(nodeName, podsPerNode*podCPU, podsPerNode*podMem)
				if err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)); err != nil {
					b.Fatalf("AddNodeInfo(): unexpected error: %v", err)
				}
			}

			// DeltaSnapshotStore (the default store in CA) lists the Nodes in the same order until the snapshot is
			// modified, so NodeOrdering can put the free Node at a chosen position in the order in which Nodes are checked.
			nodeInfos, err := snapshot.ListNodeInfos()
			if err != nil {
				b.Fatalf("ListNodeInfos(): unexpected error: %v", err)
			}
			freeNodeIndex := slices.IndexFunc(nodeInfos, func(nodeInfo *framework.NodeInfo) bool {
				return nodeInfo.Node().Name == freeNodeName
			})

			for _, scenario := range []struct {
				name             string
				pod              *apiv1.Pod
				isNodeAcceptable func(*framework.NodeInfo) bool
				// Position of the free Node in the order in which Nodes are checked.
				freeNodePosition int
				// Empty if no Node should pass.
				wantNodeName string
			}{
				{name: "passing-node-first", pod: pod, freeNodePosition: 0, wantNodeName: freeNodeName},
				{name: "passing-node-middle", pod: pod, freeNodePosition: nodeCount / 2, wantNodeName: freeNodeName},
				{name: "no-passing-node", pod: bigPod},
				{name: "no-acceptable-node", pod: pod, isNodeAcceptable: func(*framework.NodeInfo) bool { return false }},
			} {
				// A new lastIndexOrderMapping checks the Nodes starting from nodeInfos[firstNodeIndex].
				firstNodeIndex := (freeNodeIndex - scenario.freeNodePosition + nodeCount) % nodeCount

				// Make sure that the Nodes are checked in the expected order, so that the scenario measures what it claims to.
				pluginRunner.parallelism = 1
				checkedNodes := 0
				node, _, schedErr := pluginRunner.RunFiltersUntilPassingNode(scenario.pod, clustersnapshot.SchedulingOptions{
					NodeOrdering: clustersnapshot.NewLastIndexOrderMapping(firstNodeIndex),
					IsNodeAcceptable: func(nodeInfo *framework.NodeInfo) bool {
						checkedNodes++
						return scenario.isNodeAcceptable == nil || scenario.isNodeAcceptable(nodeInfo)
					},
				})
				wantCheckedNodes := nodeCount
				if scenario.wantNodeName != "" {
					wantCheckedNodes = scenario.freeNodePosition + 1
				}
				if checkedNodes != wantCheckedNodes {
					b.Fatalf("%s: checked %d Nodes before returning (Node: %v, error: %v), want %d", scenario.name, checkedNodes, node, schedErr, wantCheckedNodes)
				}

				for _, parallelism := range []int{1, 4, 16} {
					b.Run(fmt.Sprintf("%s/parallelism=%d", scenario.name, parallelism), func(b *testing.B) {
						pluginRunner.parallelism = parallelism
						for i := 0; i < b.N; i++ {
							node, _, err := pluginRunner.RunFiltersUntilPassingNode(scenario.pod, clustersnapshot.SchedulingOptions{
								NodeOrdering:     clustersnapshot.NewLastIndexOrderMapping(firstNodeIndex),
								IsNodeAcceptable: scenario.isNodeAcceptable,
							})
							if scenario.wantNodeName == "" && err == nil {
								b.Fatalf("RunFiltersUntilPassingNode(): got Node %q, want no Node", node.Name)
							}
							if scenario.wantNodeName != "" && (err != nil || node.Name != scenario.wantNodeName) {
								b.Fatalf("RunFiltersUntilPassingNode(): got Node %v and error %v, want Node %q", node, err, scenario.wantNodeName)
							}
						}
					})
				}
			}
		})
	}
}

// newTestPluginRunnerAndDeltaSnapshot is like newTestPluginRunnerAndSnapshot with the default scheduler config,
// but it uses DeltaSnapshotStore, which is the default store in CA.
func newTestPluginRunnerAndDeltaSnapshot(b *testing.B) (*SchedulerPluginRunner, clustersnapshot.ClusterSnapshot) {
	schedConfig, err := scheduler_config_latest.Default()
	if err != nil {
		b.Fatalf("scheduler_config_latest.Default(): unexpected error: %v", err)
	}
	fwHandle, err := framework.NewHandle(context.Background(), informers.NewSharedInformerFactory(clientsetfake.NewSimpleClientset(), 0), schedConfig, true, false)
	if err != nil {
		b.Fatalf("framework.NewHandle(): unexpected error: %v", err)
	}
	snapshot := NewPredicateSnapshot(store.NewDeltaSnapshotStore(), fwHandle, true, 1, false, 0)
	return NewSchedulerPluginRunner(fwHandle, snapshot, 1, 0), snapshot
}
