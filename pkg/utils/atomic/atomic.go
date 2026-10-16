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

	apiv1 "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

// IsAtomicNodeGroup returns the node group and true if the given node belongs to an atomic node group (ZeroOrMaxNodeScaling).
func IsAtomicNodeGroup(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, node *apiv1.Node) (cloudprovider.NodeGroup, bool) {
	logger := klog.FromContext(ctx)
	nodeGroup, err := autoscalingCtx.CloudProvider.NodeGroupForNode(ctx, node)
	if err != nil || nodeGroup == nil {
		return nil, false
	}
	autoscalingOptions, err := nodeGroup.GetOptions(ctx, autoscalingCtx.NodeGroupDefaults)
	if err != nil && err != cloudprovider.ErrNotImplemented {
		logger.Error(err, "Failed to get autoscaling options for node group", "nodeGroupId", nodeGroup.Id())
		return nil, false
	}
	if autoscalingOptions != nil && autoscalingOptions.ZeroOrMaxNodeScaling {
		return nodeGroup, true
	}
	return nil, false
}

// AllowsNonAtomicScaleUpToMax returns true if the given node group has the AllowNonAtomicScaleUpToMax
// option enabled. For such atomic node groups, partially failed scale-ups are not cleaned up, so the
// number of registered nodes may permanently differ from the target size.
func AllowsNonAtomicScaleUpToMax(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup) bool {
	logger := klog.FromContext(ctx)
	autoscalingOptions, err := nodeGroup.GetOptions(ctx, autoscalingCtx.NodeGroupDefaults)
	if err != nil && err != cloudprovider.ErrNotImplemented {
		logger.Error(err, "Failed to get autoscaling options for node group", "nodeGroupId", nodeGroup.Id())
		return false
	}
	return autoscalingOptions != nil && autoscalingOptions.AllowNonAtomicScaleUpToMax
}

// CountRegisteredNodesForGroup returns the number of nodes registered in the cluster that belong to nodeGroup.
// Nodes are matched with the node group instances by provider ID, since node group membership
// is cloud provider specific.
func CountRegisteredNodesForGroup(ctx context.Context, nodeLister kube_util.NodeLister, g cloudprovider.NodeGroup) (int, error) {
	instances, err := g.Nodes(ctx)
	if err != nil {
		return 0, err
	}
	providerIDs := make(map[string]bool, len(instances))
	for _, instance := range instances {
		providerIDs[instance.Id] = true
	}
	nodes, err := nodeLister.List()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, node := range nodes {
		if providerIDs[node.Spec.ProviderID] {
			count++
		}
	}
	return count, nil
}

// GroupAtomicNodes groups unneeded atomic node names by their NodeGroup ID.
func GroupAtomicNodes(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, nodeNames []string) map[string][]string {
	atomicGroups := make(map[string][]string)
	for _, nodeName := range nodeNames {
		nodeInfo, err := autoscalingCtx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil || nodeInfo == nil || nodeInfo.Node() == nil {
			continue
		}
		node := nodeInfo.Node()
		nodeGroup, isAtomic := IsAtomicNodeGroup(ctx, autoscalingCtx, node)
		if isAtomic && nodeGroup != nil {
			ngID := nodeGroup.Id()
			atomicGroups[ngID] = append(atomicGroups[ngID], nodeName)
		}
	}
	return atomicGroups
}
