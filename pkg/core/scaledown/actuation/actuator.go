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

package actuation

import (
	"context"
	"strings"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/budgets"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/deletiontracker"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/pdb"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/utils"
	"sigs.k8s.io/cluster-autoscaler/pkg/metrics"
	"sigs.k8s.io/cluster-autoscaler/pkg/observers/nodegroupchange"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	csisnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/csi/snapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/drainability/rules"
	drasnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/snapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/options"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/utilization"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/expiring"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const (
	pastLatencyExpireDuration  = time.Hour
	maxConcurrentNodesTainting = 5
)

// Actuator is responsible for draining and deleting nodes.
type Actuator struct {
	autoscalingCtx        *ca_context.AutoscalingContext
	nodeDeletionTracker   *deletiontracker.NodeDeletionTracker
	nodeDeletionScheduler *GroupDeletionScheduler
	deleteOptions         options.NodeDeleteOptions
	drainabilityRules     rules.Rules
	// TODO: Move budget processor to scaledown planner, potentially merge into PostFilteringScaleDownNodeProcessor
	// This is a larger change to the code structure which impacts some existing actuator unit tests
	// as well as Cluster Autoscaler implementations that may override ScaleDownSetProcessor
	budgetProcessor           *budgets.ScaleDownBudgetProcessor
	configGetter              actuatorNodeGroupConfigGetter
	nodeDeleteDelayAfterTaint time.Duration
	pastLatencies             *expiring.List
}

// actuatorNodeGroupConfigGetter is an interface to limit the functions that can be used
// from NodeGroupConfigProcessor interface
type actuatorNodeGroupConfigGetter interface {
	// GetIgnoreDaemonSetsUtilization returns IgnoreDaemonSetsUtilization value that should be used for a given NodeGroup.
	GetIgnoreDaemonSetsUtilization(ctx context.Context, nodeGroup cloudprovider.NodeGroup) (bool, error)
}

// NewActuator returns a new instance of Actuator.
func NewActuator(autoscalingCtx *ca_context.AutoscalingContext, scaleStateNotifier nodegroupchange.NodeGroupChangeObserver, ndt *deletiontracker.NodeDeletionTracker, deleteOptions options.NodeDeleteOptions, drainabilityRules rules.Rules, configGetter actuatorNodeGroupConfigGetter) *Actuator {
	ndb := NewNodeDeletionBatcher(autoscalingCtx, scaleStateNotifier, ndt, autoscalingCtx.NodeDeletionBatcherInterval)
	legacyFlagDrainConfig := SingleRuleDrainConfig(autoscalingCtx.MaxGracefulTerminationSec)
	var evictor Evictor
	if len(autoscalingCtx.DrainPriorityConfig) > 0 {
		evictor = NewEvictor(ndt, autoscalingCtx.DrainPriorityConfig, true)
	} else {
		evictor = NewEvictor(ndt, legacyFlagDrainConfig, false)
	}
	return &Actuator{
		autoscalingCtx:            autoscalingCtx,
		nodeDeletionTracker:       ndt,
		nodeDeletionScheduler:     NewGroupDeletionScheduler(autoscalingCtx, ndt, ndb, evictor),
		budgetProcessor:           budgets.NewScaleDownBudgetProcessor(autoscalingCtx),
		deleteOptions:             deleteOptions,
		drainabilityRules:         drainabilityRules,
		configGetter:              configGetter,
		nodeDeleteDelayAfterTaint: autoscalingCtx.NodeDeleteDelayAfterTaint,
		pastLatencies:             expiring.NewList(),
	}
}

// CheckStatus should returns an immutable snapshot of ongoing deletions.
func (a *Actuator) CheckStatus() scaledown.ActuationStatus {
	return a.nodeDeletionTracker.Snapshot()
}

// ClearResultsNotNewerThan removes information about deletions finished before or exactly at the provided timestamp.
func (a *Actuator) ClearResultsNotNewerThan(t time.Time) {
	a.nodeDeletionTracker.ClearResultsNotNewerThan(t)
}

// DeletionResults returns deletion results since the last ClearResultsNotNewerThan call
// in a map form, along with the timestamp of last result.
func (a *Actuator) DeletionResults() (map[string]status.NodeDeleteResult, time.Time) {
	return a.nodeDeletionTracker.DeletionResults()
}

// StartDeletion triggers a new deletion process.
func (a *Actuator) StartDeletion(ctx context.Context, empty, drain []*apiv1.Node) (status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError) {
	return a.startDeletion(ctx, empty, drain, false)
}

// StartForceDeletion triggers a new forced deletion process. It will bypass PDBs and forcefully delete the pods and the nodes.
func (a *Actuator) StartForceDeletion(ctx context.Context, empty, drain []*apiv1.Node) (status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError) {
	return a.startDeletion(ctx, empty, drain, true)
}

// startDeletion contains the shared logic for deleting nodes. It handles both
// normal deletions (respecting PDBs) and forced deletions (bypassing PDBs),
// determined by the 'force' parameter.
func (a *Actuator) startDeletion(ctx context.Context, empty, drain []*apiv1.Node, force bool) (status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError) {
	a.nodeDeletionScheduler.ResetAndReportMetrics()
	deletionStartTime := time.Now()
	defer func() { metrics.UpdateDuration(ctx, metrics.ScaleDownNodeDeletion, time.Since(deletionStartTime)) }()

	var nodesToClean []*apiv1.Node
	defer func() {
		// nodesToClean is populated only if PartialTaintActuationEnabled is true.
		if len(nodesToClean) > 0 {
			workqueue.ParallelizeUntil(context.Background(), maxConcurrentNodesTainting, len(nodesToClean), func(piece int) {
				_, _ = taints.CleanToBeDeleted(ctx, nodesToClean[piece], a.autoscalingCtx.ClientSet, a.autoscalingCtx.CordonNodeBeforeTerminate)
			})
		}
	}()

	scaledDownNodes := make([]*status.ScaleDownNode, 0)
	emptyToDelete, drainToDelete := a.budgetProcessor.CropNodes(ctx, a.nodeDeletionTracker, empty, drain)
	if len(emptyToDelete) == 0 && len(drainToDelete) == 0 {
		return status.ScaleDownNoNodeDeleted, nil, nil
	}

	if len(emptyToDelete) > 0 {
		// Taint all empty nodes synchronously
		taintRes, err := a.taintNodesSync(ctx, emptyToDelete)
		if len(taintRes.nodesToClean) > 0 {
			nodesToClean = append(nodesToClean, taintRes.nodesToClean...)
		}
		if err != nil {
			return status.ScaleDownError, scaledDownNodes, err
		}

		emptyScaledDown := a.deleteAsyncEmpty(ctx, taintRes.successfulNodes, taintRes.delayAfterTaint, force)
		scaledDownNodes = append(scaledDownNodes, emptyScaledDown...)
	}

	if len(drainToDelete) > 0 {
		// Taint all nodes that need drain synchronously, but don't start any drain/deletion yet. Otherwise, pods evicted from one to-be-deleted node
		// could get recreated on another.
		taintRes, err := a.taintNodesSync(ctx, drainToDelete)
		if len(taintRes.nodesToClean) > 0 {
			nodesToClean = append(nodesToClean, taintRes.nodesToClean...)
		}
		if err != nil {
			return status.ScaleDownError, scaledDownNodes, err
		}

		// All nodes involved in the scale-down should be tainted now - start draining and deleting nodes asynchronously.
		drainScaledDown := a.deleteAsyncDrain(ctx, taintRes.successfulNodes, taintRes.delayAfterTaint, force)
		scaledDownNodes = append(scaledDownNodes, drainScaledDown...)
	}

	return status.ScaleDownNodeDeleteStarted, scaledDownNodes, nil
}

// deleteAsyncEmpty immediately starts deletions asynchronously.
// scaledDownNodes return value contains all nodes for which deletion successfully started.
func (a *Actuator) deleteAsyncEmpty(ctx context.Context, NodeGroupViews []*budgets.NodeGroupView, nodeDeleteDelayAfterTaint time.Duration, force bool) (reportedSDNodes []*status.ScaleDownNode) {
	logger := klog.FromContext(ctx)
	for _, bucket := range NodeGroupViews {
		for _, node := range bucket.Nodes {
			logger.V(0).Info("Scale-down: removing empty node", "node", klog.KObj(node))
			a.autoscalingCtx.LogRecorder.Eventf(apiv1.EventTypeNormal, "ScaleDownEmpty", "Scale-down: removing empty node %q", node.Name)

			if sdNode, err := a.scaleDownNodeToReport(ctx, node, false); err == nil {
				reportedSDNodes = append(reportedSDNodes, sdNode)
			} else {
				logger.Error(err, "Scale-down: couldn't report scaled down node")
			}

			a.nodeDeletionTracker.StartDeletion(bucket.Group.Id(), node.Name)
		}
	}

	for _, bucket := range NodeGroupViews {
		go a.deleteNodesAsync(ctx, bucket.Nodes, bucket.Group, false, force, bucket.BatchSize, nodeDeleteDelayAfterTaint)
	}

	return reportedSDNodes
}

type taintNodesResult struct {
	delayAfterTaint time.Duration
	successfulNodes []*budgets.NodeGroupView
	nodesToClean    []*apiv1.Node
}

// taintNodesSync synchronously taints all provided nodes with NoSchedule.
// When PartialTaintActuationEnabled is false, if tainting fails for any of the nodes, already applied taints are cleaned up.
// When PartialTaintActuationEnabled is true:
//   - if tainting fails in any of the nodes in a non-atomic nodegroup, successfully tainted nodes remain tainted and can proceed to scaledown.
//   - if tainting fails in any of the nodes in an atomic nodegroup, already taints that were applied to nodes in this group are added to nodesToClean in the result.
//
// Returns:
//   - delayAfterTaint how much time actuator needs to wait before draining nodes from pods
//   - successfulNodes nodes that can be scaled down.
//   - nodesToClean nodes that need to be cleaned up from taints. This is only populated if PartialTaintActuationEnabled is true.
func (a *Actuator) taintNodesSync(ctx context.Context, nodeGroupViews []*budgets.NodeGroupView) (taintNodesResult, errors.AutoscalerError) {
	type taintResult struct {
		node *apiv1.Node
		err  error
	}
	nodesToTaint := make([]*apiv1.Node, 0)
	var updateLatencyTracker *UpdateLatencyTracker
	nodeDeleteDelayAfterTaint := a.nodeDeleteDelayAfterTaint

	for _, bucket := range nodeGroupViews {
		for _, node := range bucket.Nodes {
			nodesToTaint = append(nodesToTaint, node)
		}
	}

	// start tracking the node taint latency
	if a.autoscalingCtx.AutoscalingOptions.DynamicNodeDeleteDelayAfterTaintEnabled {
		updateLatencyTracker = NewUpdateLatencyTracker(a.autoscalingCtx.AutoscalingKubeClients.ListerRegistry.AllNodeLister(), len(nodesToTaint))
		for _, node := range nodesToTaint {
			updateLatencyTracker.StartTimeChan <- nodeTaintStartTime{node.Name, time.Now()}
		}
		go updateLatencyTracker.Start(ctx)
	}

	// Taint nodes concurrently and record failures
	failedNodesChan := make(chan taintResult, len(nodesToTaint))
	failedNodes := make(map[types.UID]struct{})
	workqueue.ParallelizeUntil(context.Background(), maxConcurrentNodesTainting, len(nodesToTaint), func(piece int) {
		node := nodesToTaint[piece]
		err := a.taintNode(ctx, node)
		if err != nil {
			failedNodesChan <- taintResult{node: node, err: err}
		}
	})
	close(failedNodesChan)
	for result := range failedNodesChan {
		failedNodes[result.node.UID] = struct{}{}
		a.autoscalingCtx.Recorder.Eventf(result.node, apiv1.EventTypeWarning, "ScaleDownFailed", "failed to mark the node as toBeDeleted/unschedulable: %v", result.err)
	}

	// Parial taint actualtion disabled: failure mode
	// Clean up all applied taints.
	if len(failedNodes) > 0 && !a.autoscalingCtx.AutoscalingOptions.PartialTaintActuationEnabled {
		// Clean up already applied taints in case of issues.
		for _, taintedNode := range nodesToTaint {
			if _, found := failedNodes[taintedNode.UID]; found {
				continue
			}
			_, _ = taints.CleanToBeDeleted(ctx, taintedNode, a.autoscalingCtx.ClientSet, a.autoscalingCtx.CordonNodeBeforeTerminate)
		}
		// No need to record taint propagation latency, all taints are cleaned up.
		if a.autoscalingCtx.AutoscalingOptions.DynamicNodeDeleteDelayAfterTaintEnabled {
			close(updateLatencyTracker.ExpectedNodeCountChan)
		}
		return taintNodesResult{delayAfterTaint: nodeDeleteDelayAfterTaint, successfulNodes: nil, nodesToClean: nil}, errors.NewAutoscalerErrorf(errors.ApiCallError, "couldn't taint %d nodes with ToBeDeleted", len(failedNodes))
	}

	// Compute taint propagation latency for successfully tainted nodes
	if a.autoscalingCtx.AutoscalingOptions.DynamicNodeDeleteDelayAfterTaintEnabled {
		updateLatencyTracker.ExpectedNodeCountChan <- len(nodesToTaint) - len(failedNodes)
		latency, ok := <-updateLatencyTracker.ResultChan
		if ok {
			a.pastLatencies.RegisterElement(latency)
			a.pastLatencies.DropNotNewerThan(time.Now().Add(-1 * pastLatencyExpireDuration))
			nodeDeleteDelayAfterTaint = 2 * maxLatency(a.pastLatencies.ToSlice())

		}
	}

	// Partial taint actuation enabled: failure mode
	// Compute which taints need to be cleaned up and which nodes can be scaled down
	if len(failedNodes) > 0 && a.autoscalingCtx.AutoscalingOptions.PartialTaintActuationEnabled {
		var retErr errors.AutoscalerError
		var successfulNodeGroupViews []*budgets.NodeGroupView
		var nodesToClean []*apiv1.Node
		klog.Infof("couldn't taint %d nodes with ToBeDeleted, proceeding with partial scale down or bucket cleanup", len(failedNodes))

		for _, bucket := range nodeGroupViews {
			bucketWithSuccessfulNodes, bucketNodesToClean := a.resolveBucketFailures(ctx, bucket, failedNodes)
			if bucketWithSuccessfulNodes != nil {
				successfulNodeGroupViews = append(successfulNodeGroupViews, bucketWithSuccessfulNodes)
			}
			nodesToClean = append(nodesToClean, bucketNodesToClean...)
		}
		if len(successfulNodeGroupViews) == 0 {
			retErr = errors.NewAutoscalerErrorf(errors.ApiCallError, "couldn't taint %d nodes with ToBeDeleted and no nodes can be scaled down", len(failedNodes))
		}
		return taintNodesResult{delayAfterTaint: nodeDeleteDelayAfterTaint, successfulNodes: successfulNodeGroupViews, nodesToClean: nodesToClean}, retErr

	}

	// No failed nodes
	return taintNodesResult{delayAfterTaint: nodeDeleteDelayAfterTaint, successfulNodes: nodeGroupViews, nodesToClean: nil}, nil

}

// resolveTaintFailures evaluates how to handle taint failures in a nodegroup.
//
// When actuator fails to apply a taint to a node:
//   - If the node is from an atomic nodegroup, actuator must clean up all successfully applied taints in this nodegroup. The group won't be scaled down.
//   - If the node is from a regular, non-atomic nodegorup, other successfully tainted nodes from this nodegroup can be deleted.
//
// Returns a nodegroup view that contains nodes that can be deleted, and a list of nodes from which the taint has to be cleaned up.
func (a *Actuator) resolveBucketFailures(ctx context.Context, bucket *budgets.NodeGroupView, failedNodes map[types.UID]struct{}) (*budgets.NodeGroupView, []*apiv1.Node) {
	opts, err := bucket.Group.GetOptions(ctx, a.autoscalingCtx.NodeGroupDefaults)
	isAtomic := false
	if err != nil {
		klog.Warningf("Failed to get options for node group %v: %v, assuming atomic node group to be safe", bucket.Group.Id(), err)
		isAtomic = true
	} else if opts != nil && opts.ZeroOrMaxNodeScaling {
		isAtomic = true
	}

	// If any nodes in an atomic nodegroup failed to taint, we cannot scale it down and need to clean up all applied taints.
	if isAtomic {
		failedBucket := false
		for _, node := range bucket.Nodes {
			if _, found := failedNodes[node.UID]; found {
				failedBucket = true
				break
			}
		}

		if !failedBucket {
			return bucket, nil
		}

		var toClean []*apiv1.Node
		for _, node := range bucket.Nodes {
			if _, found := failedNodes[node.UID]; !found {
				toClean = append(toClean, node)
			}
		}

		return nil, toClean
	}

	// In a non-atomic nodegroup, any successfully tainted nodes can be scaled down.
	successfulNodes := make([]*apiv1.Node, 0, len(bucket.Nodes))

	for _, node := range bucket.Nodes {
		if _, found := failedNodes[node.UID]; !found {
			successfulNodes = append(successfulNodes, node)
		} else {
		}
	}
	if len(successfulNodes) == 0 {
		return nil, nil
	}
	if len(successfulNodes) == len(bucket.Nodes) {
		return bucket, nil
	}
	newBucket := *bucket
	newBucket.Nodes = successfulNodes
	return &newBucket, nil

}

// deleteAsyncDrain asynchronously starts deletions with drain for all provided nodes. scaledDownNodes return value contains all nodes for which
// deletion successfully started.
func (a *Actuator) deleteAsyncDrain(ctx context.Context, NodeGroupViews []*budgets.NodeGroupView, nodeDeleteDelayAfterTaint time.Duration, force bool) (reportedSDNodes []*status.ScaleDownNode) {
	logger := klog.FromContext(ctx)
	for _, bucket := range NodeGroupViews {
		for _, drainNode := range bucket.Nodes {
			if sdNode, err := a.scaleDownNodeToReport(ctx, drainNode, true); err == nil {
				logger.V(0).Info("Scale-down: removing node", "node", klog.KObj(drainNode), "utilization", sdNode.UtilInfo, "podsToReschedule", joinPodNames(sdNode.EvictedPods))
				a.autoscalingCtx.LogRecorder.Eventf(apiv1.EventTypeNormal, "ScaleDown", "Scale-down: removing node %s, utilization: %v, pods to reschedule: %s", drainNode.Name, sdNode.UtilInfo, joinPodNames(sdNode.EvictedPods))
				reportedSDNodes = append(reportedSDNodes, sdNode)
			} else {
				logger.Error(err, "Scale-down: couldn't report scaled down node")
			}

			a.nodeDeletionTracker.StartDeletionWithDrain(bucket.Group.Id(), drainNode.Name)
		}
	}

	for _, bucket := range NodeGroupViews {
		go a.deleteNodesAsync(ctx, bucket.Nodes, bucket.Group, true, force, bucket.BatchSize, nodeDeleteDelayAfterTaint)
	}

	return reportedSDNodes
}

func (a *Actuator) deleteNodesAsync(ctx context.Context, nodes []*apiv1.Node, nodeGroup cloudprovider.NodeGroup, drain bool, force bool, batchSize int, nodeDeleteDelayAfterTaint time.Duration) {
	logger := klog.FromContext(ctx)
	var remainingPdbTracker pdb.RemainingPdbTracker
	var registry kube_util.ListerRegistry

	if len(nodes) == 0 {
		return
	}

	if nodeDeleteDelayAfterTaint > time.Duration(0) {
		logger.V(0).Info("Scale-down: waiting before trying to delete nodes", "delay", nodeDeleteDelayAfterTaint)
		time.Sleep(nodeDeleteDelayAfterTaint)
	}

	clusterSnapshot, err := a.createSnapshot(ctx, nodes)
	if err != nil {
		nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerErrorf(errors.InternalError, "createSnapshot returned error %v", err)}
		for _, node := range nodes {
			a.nodeDeletionScheduler.AbortNodeDeletionDueToError(ctx, node, nodeGroup.Id(), drain, "failed to create delete snapshot", nodeDeleteResult)
		}
		return
	}

	if drain {
		pdbs, err := a.autoscalingCtx.PodDisruptionBudgetLister().List()
		if err != nil {
			nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerErrorf(errors.InternalError, "podDisruptionBudgetLister.List returned error %v", err)}
			for _, node := range nodes {
				a.nodeDeletionScheduler.AbortNodeDeletionDueToError(ctx, node, nodeGroup.Id(), drain, "failed to fetch pod disruption budgets", nodeDeleteResult)
			}
			return
		}
		remainingPdbTracker = pdb.NewBasicRemainingPdbTracker()
		remainingPdbTracker.SetPdbs(pdbs)
		registry = a.autoscalingCtx.ListerRegistry
	}

	if batchSize == 0 {
		batchSize = len(nodes)
	}

	for _, node := range nodes {
		nodeInfo, err := clusterSnapshot.GetNodeInfo(node.Name)
		if err != nil {
			nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerErrorf(errors.InternalError, "nodeInfos.Get for %q returned error: %v", node.Name, err)}
			a.nodeDeletionScheduler.AbortNodeDeletionDueToError(ctx, node, nodeGroup.Id(), drain, "failed to get node info", nodeDeleteResult)
			continue
		}

		podMoveInfo, err := simulator.GetPodsToMove(ctx, nodeInfo, a.deleteOptions, a.drainabilityRules, registry, remainingPdbTracker, time.Now())
		if err != nil {
			nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerErrorf(errors.InternalError, "GetPodsToMove for %q returned error: %v", node.Name, err)}
			a.nodeDeletionScheduler.AbortNodeDeletion(ctx, node, nodeGroup.Id(), drain, "failed to get pods to move on node", nodeDeleteResult, true)
			continue
		}

		if !drain {
			if len(podMoveInfo.Pods) != 0 {
				nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerErrorf(errors.InternalError, "failed to delete empty node %q, new pods scheduled", node.Name)}
				a.nodeDeletionScheduler.AbortNodeDeletion(ctx, node, nodeGroup.Id(), drain, "node is not empty", nodeDeleteResult, true)
				continue
			}
			if len(podMoveInfo.OnCompletionPods) != 0 {
				nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerErrorf(errors.InternalError, "failed to delete empty node %q, active on-completion pods present", node.Name)}
				a.nodeDeletionScheduler.AbortNodeDeletion(ctx, node, nodeGroup.Id(), drain, "active on-completion pods present", nodeDeleteResult, true)
				continue
			}
		}

		if force {
			go a.nodeDeletionScheduler.scheduleForceDeletion(ctx, nodeInfo, nodeGroup, batchSize, drain)
			continue
		}

		go a.nodeDeletionScheduler.ScheduleDeletion(ctx, nodeInfo, nodeGroup, batchSize, drain)
	}
}

func (a *Actuator) scaleDownNodeToReport(ctx context.Context, node *apiv1.Node, drain bool) (*status.ScaleDownNode, error) {
	nodeGroup, err := a.autoscalingCtx.CloudProvider.NodeGroupForNode(ctx, node)
	if err != nil {
		return nil, errors.NewAutoscalerErrorf(errors.InternalError, "failed to get node group for node %s: %v", node.Name, err)
	}
	if nodeGroup == nil {
		return nil, errors.NewAutoscalerErrorf(errors.NodeGroupDoesNotExistError, "no node group for node %s", node.Name)
	}
	nodeInfo, err := a.autoscalingCtx.ClusterSnapshot.GetNodeInfo(node.Name)
	if err != nil {
		return nil, errors.NewAutoscalerErrorf(errors.InternalError, "failed to get node info for %s: %v", node.Name, err)
	}

	ignoreDaemonSetsUtilization, err := a.configGetter.GetIgnoreDaemonSetsUtilization(ctx, nodeGroup)
	if err != nil {
		return nil, errors.NewAutoscalerErrorf(errors.InternalError, "failed to get ignoreDaemonSetsUtilization for node group %s: %v", nodeGroup.Id(), err)
	}

	gpuConfig := a.autoscalingCtx.CloudProvider.GetNodeGpuConfig(ctx, node)
	utilInfo, err := utilization.Calculate(ctx, nodeInfo, ignoreDaemonSetsUtilization, a.autoscalingCtx.IgnoreMirrorPodsUtilization, a.autoscalingCtx.DynamicResourceAllocationEnabled, gpuConfig, time.Now())
	if err != nil {
		return nil, errors.NewAutoscalerErrorf(errors.InternalError, "failed to calculate utilization for %s: %v", node.Name, err)
	}
	var evictedPods []*apiv1.Pod
	if drain {
		_, nonDsPodsToEvict := podsToEvict(nodeInfo, a.autoscalingCtx.DaemonSetEvictionForOccupiedNodes)
		evictedPods = nonDsPodsToEvict
	}
	return &status.ScaleDownNode{
		Node:        node,
		NodeGroup:   nodeGroup,
		EvictedPods: evictedPods,
		UtilInfo:    utilInfo,
	}, nil
}

// taintNode taints the node with NoSchedule to prevent new pods scheduling on it.
func (a *Actuator) taintNode(ctx context.Context, node *apiv1.Node) error {
	if _, err := taints.MarkToBeDeleted(ctx, node, a.autoscalingCtx.ClientSet, a.autoscalingCtx.CordonNodeBeforeTerminate); err != nil {
		a.autoscalingCtx.Recorder.Eventf(node, apiv1.EventTypeWarning, "ScaleDownFailed", "failed to mark the node as toBeDeleted/unschedulable: %v", err)
		return errors.ToAutoscalerError(errors.ApiCallError, err)
	}
	a.autoscalingCtx.Recorder.Eventf(node, apiv1.EventTypeNormal, "ScaleDown", "marked the node as toBeDeleted/unschedulable")
	return nil
}

func (a *Actuator) createSnapshot(ctx context.Context, nodes []*apiv1.Node) (clustersnapshot.ClusterSnapshot, error) {
	snapshot := predicate.NewPredicateSnapshot(store.NewBasicSnapshotStore(), a.autoscalingCtx.FrameworkHandle, a.autoscalingCtx.DynamicResourceAllocationEnabled, a.autoscalingCtx.PredicateParallelism, a.autoscalingCtx.CSINodeAwareSchedulingEnabled, a.autoscalingCtx.SchedulerVerbosityOffset)
	pods, err := a.autoscalingCtx.AllPodLister().List()
	if err != nil {
		return nil, err
	}

	scheduledPods := kube_util.ScheduledPods(pods)
	nonExpendableScheduledPods := utils.FilterOutExpendablePods(scheduledPods, a.autoscalingCtx.ExpendablePodsPriorityCutoff)

	var draSnapshot *drasnapshot.Snapshot
	if a.autoscalingCtx.DynamicResourceAllocationEnabled && a.autoscalingCtx.DraProvider != nil {
		draSnapshot, err = a.autoscalingCtx.DraProvider.Snapshot()
		if err != nil {
			return nil, err
		}
	}

	var csiSnapshot *csisnapshot.Snapshot
	if a.autoscalingCtx.CSINodeAwareSchedulingEnabled {
		csiSnapshot, err = a.autoscalingCtx.CsiProvider.Snapshot()
		if err != nil {
			return nil, err
		}
	}

	err = snapshot.SetClusterState(ctx, nodes, nonExpendableScheduledPods, draSnapshot, csiSnapshot)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func joinPodNames(pods []*apiv1.Pod) string {
	var names []string
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	return strings.Join(names, ",")
}
