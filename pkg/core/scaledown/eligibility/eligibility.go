/*
Copyright 2022 The Kubernetes Authors.

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

package eligibility

import (
	"context"
	"time"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/actuation"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/unremovable"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/utilization"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/atomic"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/klogx"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

const (
	// ScaleDownDisabledKey is the name of annotation marking node as not eligible for scale down.
	ScaleDownDisabledKey = "cluster-autoscaler.kubernetes.io/scale-down-disabled"
)

// Checker is responsible for deciding which nodes pass the criteria for scale down.
type Checker struct {
	configGetter nodeGroupConfigGetter
}

type nodeGroupConfigGetter interface {
	// GetScaleDownUtilizationThreshold returns ScaleDownUtilizationThreshold value that should be used for a given NodeGroup.
	GetScaleDownUtilizationThreshold(ctx context.Context, nodeGroup cloudprovider.NodeGroup) (float64, error)
	// GetScaleDownGpuUtilizationThreshold returns ScaleDownGpuUtilizationThreshold value that should be used for a given NodeGroup.
	GetScaleDownGpuUtilizationThreshold(ctx context.Context, nodeGroup cloudprovider.NodeGroup) (float64, error)
	// GetIgnoreDaemonSetsUtilization returns IgnoreDaemonSetsUtilization value that should be used for a given NodeGroup.
	GetIgnoreDaemonSetsUtilization(ctx context.Context, nodeGroup cloudprovider.NodeGroup) (bool, error)
}

// NewChecker creates a new Checker object.
func NewChecker(configGetter nodeGroupConfigGetter) *Checker {
	return &Checker{
		configGetter: configGetter,
	}
}

// FilterOutUnremovable accepts a list of nodes that are candidates for
// scale down and filters out nodes that cannot be removed, along with node
// utilization info.
// TODO(x13n): Node utilization could actually be calculated independently for
// all nodes and just used here. Next refactor...
func (c *Checker) FilterOutUnremovable(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleDownCandidates []*apiv1.Node, timestamp time.Time, unremovableNodes *unremovable.Nodes) ([]string, map[string]utilization.Info, []*simulator.UnremovableNode) {
	logger := klog.FromContext(ctx)
	ineligible := []*simulator.UnremovableNode{}
	skipped := 0
	utilizationMap := make(map[string]utilization.Info)
	currentlyUnneededNodeNames := make([]string, 0, len(scaleDownCandidates))
	utilLogsQuota := klogx.NewLoggingQuota(20)

	for _, node := range scaleDownCandidates {
		nodeInfo, err := autoscalingCtx.ClusterSnapshot.GetNodeInfo(node.Name)
		if err != nil {
			logger.Error(err, "Can't retrieve scale-down candidate from snapshot", "node", klog.KObj(node))
			ineligible = append(ineligible, &simulator.UnremovableNode{Node: node, Reason: simulator.UnexpectedError})
			continue
		}

		// Skip nodes that were recently checked.
		if unremovableNodes.IsRecent(node.Name) {
			ineligible = append(ineligible, &simulator.UnremovableNode{Node: node, Reason: simulator.RecentlyUnremovable})
			skipped++
			continue
		}

		reason, utilInfo := c.unremovableReasonAndNodeUtilization(ctx, autoscalingCtx, timestamp, nodeInfo, utilLogsQuota)
		if utilInfo != nil {
			utilizationMap[node.Name] = *utilInfo
		}
		if reason != simulator.NoReason {
			ineligible = append(ineligible, &simulator.UnremovableNode{Node: node, Reason: reason})
			continue
		}

		currentlyUnneededNodeNames = append(currentlyUnneededNodeNames, node.Name)
	}

	klogx.V(4).Over(utilLogsQuota).Infof("Skipped logging utilization for %d other nodes", -utilLogsQuota.Left())
	if skipped > 0 {
		logger.V(1).Info("Scale-down calculation: ignoring unremovable nodes", "nodesCount", skipped, "timeout", autoscalingCtx.AutoscalingOptions.UnremovableNodeRecheckTimeout)
	}

	currentlyUnneededNodeNames, atomicIneligible := c.filterIncompleteAtomicNodeGroups(ctx, autoscalingCtx, currentlyUnneededNodeNames)
	ineligible = append(ineligible, atomicIneligible...)

	return currentlyUnneededNodeNames, utilizationMap, ineligible
}

func allNodes(s clustersnapshot.ClusterSnapshot) ([]*apiv1.Node, error) {
	nodeInfos, err := s.ListNodeInfos()
	if err != nil {
		return nil, err
	}
	nodes := make([]*apiv1.Node, len(nodeInfos))
	for i, ni := range nodeInfos {
		nodes[i] = ni.Node()
	}
	return nodes, nil
}

// filterIncompleteAtomicNodeGroups identifies atomic node groups in currentlyUnneededNodeNames.
// If an atomic node group has fewer unneeded candidates than its target size, it cannot be
// atomically scaled down. Such incomplete atomic node groups are filtered out of simulation,
// and returned as ineligible with Reason: simulator.AtomicScaleDownFailed.
func (c *Checker) filterIncompleteAtomicNodeGroups(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, nodeNames []string) ([]string, []*simulator.UnremovableNode) {
	logger := klog.FromContext(ctx)
	atomicGroupNodes := make(map[string][]string)
	atomicGroupObj := make(map[string]cloudprovider.NodeGroup)
	nodeNameToNode := make(map[string]*apiv1.Node)

	for _, nodeName := range nodeNames {
		nodeInfo, err := autoscalingCtx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil || nodeInfo == nil || nodeInfo.Node() == nil {
			continue
		}
		node := nodeInfo.Node()
		nodeNameToNode[nodeName] = node
		nodeGroup, isAtomic := atomic.IsAtomicNodeGroup(ctx, autoscalingCtx, node)
		if isAtomic && nodeGroup != nil {
			ngID := nodeGroup.Id()
			atomicGroupNodes[ngID] = append(atomicGroupNodes[ngID], nodeName)
			atomicGroupObj[ngID] = nodeGroup
		}
	}

	if len(atomicGroupNodes) == 0 {
		return nodeNames, nil
	}

	allNodesList, err := allNodes(autoscalingCtx.ClusterSnapshot)
	if err != nil {
		return nodeNames, nil
	}

	incompleteNodes := make(map[string]bool)
	var ineligible []*simulator.UnremovableNode

	for ngID, unneededNodes := range atomicGroupNodes {
		ng := atomicGroupObj[ngID]
		targetSize, err := ng.TargetSize(ctx)
		if err != nil {
			logger.Error(err, "Failed to get target size for node group", "nodeGroupId", ng.Id())
			continue
		}

		if len(unneededNodes) < targetSize {
			registeredCount, countErr := atomic.CountRegisteredNodesForGroup(ctx, ng, allNodesList)
			if countErr == nil && len(unneededNodes) == registeredCount {
				// All registered nodes in the snapshot are unneeded
				continue
			}
			logger.V(2).Info("Atomic node group needs all nodes to be unneeded for scale down. Filtering out from scale down simulation.", "nodeGroupId", ng.Id(), "ngUnneededNodesCount", len(unneededNodes), "ngTargetSize", targetSize)
			for _, nodeName := range unneededNodes {
				incompleteNodes[nodeName] = true
				if node, ok := nodeNameToNode[nodeName]; ok {
					ineligible = append(ineligible, &simulator.UnremovableNode{
						Node:   node,
						Reason: simulator.AtomicScaleDownFailed,
					})
				}
			}
		}
	}

	if len(incompleteNodes) == 0 {
		return nodeNames, nil
	}

	filteredNodeNames := make([]string, 0, len(nodeNames)-len(incompleteNodes))
	for _, nodeName := range nodeNames {
		if !incompleteNodes[nodeName] {
			filteredNodeNames = append(filteredNodeNames, nodeName)
		}
	}

	return filteredNodeNames, ineligible
}

func (c *Checker) unremovableReasonAndNodeUtilization(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, timestamp time.Time, nodeInfo *framework.NodeInfo, utilLogsQuota *klogx.Quota) (simulator.UnremovableReason, *utilization.Info) {
	logger := klog.FromContext(ctx)
	node := nodeInfo.Node()

	if actuation.IsNodeBeingDeleted(node, timestamp) {
		logger.V(1).Info("Skipping node from delete consideration - it is currently being deleted", "node", klog.KObj(node))
		return simulator.CurrentlyBeingDeleted, nil
	}

	// Skip nodes marked with no scale down annotation
	if HasNoScaleDownAnnotation(node) {
		logger.V(1).Info("Skipping node from delete consideration - it is marked as no scale down", "node", klog.KObj(node))
		return simulator.ScaleDownDisabledAnnotation, nil
	}

	nodeGroup, err := autoscalingCtx.CloudProvider.NodeGroupForNode(ctx, node)
	if err != nil {
		logger.Info("Node group not found for node", "node", klog.KObj(node), "err", err)
		return simulator.UnexpectedError, nil
	}
	if nodeGroup == nil {
		// We should never get here as non-autoscaled nodes should not be included in scaleDownCandidates list
		// (and the default PreFilteringScaleDownNodeProcessor would indeed filter them out).
		logger.Info("Skipped node from delete consideration - it is not autoscaled", "node", klog.KObj(node))
		return simulator.NotAutoscaled, nil
	}

	ignoreDaemonSetsUtilization, err := c.configGetter.GetIgnoreDaemonSetsUtilization(ctx, nodeGroup)
	if err != nil {
		logger.Info("Couldn't retrieve `IgnoreDaemonSetsUtilization` option for node", "node", klog.KObj(node), "err", err)
		return simulator.UnexpectedError, nil
	}

	gpuConfig := autoscalingCtx.CloudProvider.GetNodeGpuConfig(ctx, node)
	utilInfo, err := utilization.Calculate(ctx, nodeInfo, ignoreDaemonSetsUtilization, autoscalingCtx.IgnoreMirrorPodsUtilization, autoscalingCtx.DynamicResourceAllocationEnabled, gpuConfig, timestamp)
	if err != nil {
		logger.Info("Failed to calculate utilization for node", "node", klog.KObj(node), "err", err)
		return simulator.UnexpectedError, nil
	}

	// If scale down of unready nodes is disabled, skip the node if it is unready
	if !autoscalingCtx.ScaleDownUnreadyEnabled {
		ready, _, _ := kube_util.GetReadinessState(node)
		if !ready {
			logger.V(4).Info("Skipping unready node from delete consideration - scale-down of unready nodes is disabled", "node", klog.KObj(node))
			return simulator.ScaleDownUnreadyDisabled, nil
		}
	}

	underutilized, err := c.isNodeAtOrBelowUtilizationThreshold(ctx, autoscalingCtx, node, nodeGroup, utilInfo)
	if err != nil {
		logger.Info("Failed to check utilization thresholds for node", "node", klog.KObj(node), "err", err)
		return simulator.UnexpectedError, nil
	}
	if !underutilized {
		logger.V(4).Info("Node unremovable: resource utilization is above the scale-down utilization threshold", "node", klog.KObj(node), "resource", utilInfo.ResourceName, "utilization", utilInfo.Utilization*100)
		return simulator.NotUnderutilized, &utilInfo
	}

	klogx.V(4).UpTo(utilLogsQuota).Infof("Node %s - %s requested is %.6g%% of allocatable", node.Name, utilInfo.ResourceName, utilInfo.Utilization*100)

	return simulator.NoReason, &utilInfo
}

// isNodeAtOrBelowUtilizationThreshold determines if a given node utilization is at or below threshold.
func (c *Checker) isNodeAtOrBelowUtilizationThreshold(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, node *apiv1.Node, nodeGroup cloudprovider.NodeGroup, utilInfo utilization.Info) (bool, error) {
	var threshold float64
	var err error
	gpuConfig := autoscalingCtx.CloudProvider.GetNodeGpuConfig(ctx, node)
	if gpuConfig != nil {
		threshold, err = c.configGetter.GetScaleDownGpuUtilizationThreshold(ctx, nodeGroup)
		if err != nil {
			return false, err
		}
	} else {
		threshold, err = c.configGetter.GetScaleDownUtilizationThreshold(ctx, nodeGroup)
		if err != nil {
			return false, err
		}
	}
	if utilInfo.Utilization > threshold {
		return false, nil
	}
	return true, nil
}

// HasNoScaleDownAnnotation checks whether the node has an annotation blocking it from being scaled down.
func HasNoScaleDownAnnotation(node *apiv1.Node) bool {
	return node.Annotations[ScaleDownDisabledKey] == "true"
}
