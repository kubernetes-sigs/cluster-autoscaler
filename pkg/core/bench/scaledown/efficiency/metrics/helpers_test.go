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

package metrics

import (
	"time"

	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// nodeInfoWithRequests returns a NodeInfo with given allocatable CPU (millicores) and memory (bytes).
// If reqCPU or reqMem is non-zero, a single pod with these requests is scheduled on the node.
func nodeInfoWithRequests(name string, allocCPU, allocMem, reqCPU, reqMem int64) *framework.NodeInfo {
	node := test.BuildTestNode(name, allocCPU, allocMem)
	if reqCPU == 0 && reqMem == 0 {
		return framework.NewTestNodeInfo(node)
	}
	return framework.NewTestNodeInfo(node, test.BuildScheduledTestPod(name+"-pod", reqCPU, reqMem, name))
}

// nodeInfoWithLabels returns a NodeInfo with given allocatable CPU (millicores), memory (bytes) and node labels.
func nodeInfoWithLabels(name string, allocCPU, allocMem int64, labels map[string]string) *framework.NodeInfo {
	return framework.NewTestNodeInfo(test.BuildTestNode(name, allocCPU, allocMem, test.WithNodeLabels(labels)))
}

// instanceTypeLabel returns node labels with the stable instance type label set to instanceType.
func instanceTypeLabel(instanceType string) map[string]string {
	return map[string]string{apiv1.LabelInstanceTypeStable: instanceType}
}

// clusterStateWithNodes returns a ClusterState with given nodeInfos, no scaled down nodes and current time.
func clusterStateWithNodes(nodeInfos ...*framework.NodeInfo) ClusterState {
	return NewClusterState(nil, nodeInfos, time.Now())
}
