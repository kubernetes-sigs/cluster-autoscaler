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
	goerrors "errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
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
	budgetProcessor          *budgets.ScaleDownBudgetProcessor
	configGetter             actuatorNodeGroupConfigGetter
	scaledownDelayCalculator *nodeDelayAfterTaintCalculator
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
		autoscalingCtx:           autoscalingCtx,
		nodeDeletionTracker:      ndt,
		nodeDeletionScheduler:    NewGroupDeletionScheduler(autoscalingCtx, ndt, ndb, evictor),
		budgetProcessor:          budgets.NewScaleDownBudgetProcessor(autoscalingCtx),
		deleteOptions:            deleteOptions,
		drainabilityRules:        drainabilityRules,
		configGetter:             configGetter,
		scaledownDelayCalculator: newNodeDelayAfterTaintCalculator(autoscalingCtx.NodeDeleteDelayAfterTaint, autoscalingCtx.AutoscalingOptions.DynamicNodeDeleteDelayAfterTaintEnabled),
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
//
// Returns:
//   - (ScaleDownNoNodeDeleted, nil, nil) if no candidate nodes remain after budget cropping.
//   - (ScaleDownNodeDeleteStarted, scaledDownNodes, nil) if deletion was started for at least one
//     node (even if other nodes or atomic node groups failed to taint).
//   - (ScaleDownError, nil, err) if candidate nodes remained after budget cropping, but none of
//     them could be tainted.
func (a *Actuator) startDeletion(ctx context.Context, empty, drain []*apiv1.Node, force bool) (status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError) {
	a.nodeDeletionScheduler.ResetAndReportMetrics()
	deletionStartTime := time.Now()
	defer func() { metrics.UpdateDuration(ctx, metrics.ScaleDownNodeDeletion, time.Since(deletionStartTime)) }()

	bucketsToDelete := a.budgetProcessor.CropNodes(ctx, a.nodeDeletionTracker, empty, drain)
	if len(bucketsToDelete) == 0 {
		return status.ScaleDownNoNodeDeleted, nil, nil
	}

	croppedNodesCount := 0
	for _, bucket := range bucketsToDelete {
		croppedNodesCount += len(bucket.Nodes)
	}
	latencyTracker := a.scaledownDelayCalculator.startLatencyTracker(ctx, a.autoscalingCtx.AutoscalingKubeClients.ListerRegistry.AllNodeLister(), croppedNodesCount)
	tainted, err := a.taintNodesSync(ctx, bucketsToDelete, latencyTracker)

	latencyTracker.stopWaitingForNodes(nodeNames(tainted.nodesToClean))
	// Nodes of aborted atomic node groups are untainted after the other deletions have started.
	defer a.cleanTaintsSync(ctx, tainted.nodesToClean)

	if err != nil {
		return status.ScaleDownError, nil, err
	}

	scaledDownNodes := a.registerDeletions(ctx, tainted.buckets)

	go func() {
		delay := a.scaledownDelayCalculator.computeDelay(ctx, latencyTracker)
		klog.FromContext(ctx).V(0).Info("Scale-down: waiting before trying to delete nodes", "delay", delay)
		time.Sleep(delay)
		for _, bucket := range tainted.buckets {
			go a.deleteNodesAsync(ctx, bucket, force)
		}
	}()

	return status.ScaleDownNodeDeleteStarted, scaledDownNodes, nil
}

// registerDeletions logs, records events, and marks nodes as being deleted in nodeDeletionTracker.
func (a *Actuator) registerDeletions(ctx context.Context, views []*budgets.NodeGroupView) []*status.ScaleDownNode {
	logger := klog.FromContext(ctx)
	var reportedSDNodes []*status.ScaleDownNode
	for _, bucket := range views {
		for _, node := range bucket.Nodes {
			sdNode := a.scaleDownNodeToReport(ctx, node, bucket.Group, bucket.Drain)
			reportedSDNodes = append(reportedSDNodes, sdNode)
			if bucket.Drain {
				logger.V(0).Info("Scale-down: removing node", "node", klog.KObj(node), "utilization", sdNode.UtilInfo, "podsToReschedule", joinPodNames(sdNode.EvictedPods))
				a.autoscalingCtx.LogRecorder.Eventf(apiv1.EventTypeNormal, "ScaleDown", "Scale-down: removing node %s, utilization: %v, pods to reschedule: %s", node.Name, sdNode.UtilInfo, joinPodNames(sdNode.EvictedPods))
				a.nodeDeletionTracker.StartDeletionWithDrain(bucket.Group.Id(), node.Name)
			} else {
				logger.V(0).Info("Scale-down: removing empty node", "node", klog.KObj(node))
				a.autoscalingCtx.LogRecorder.Eventf(apiv1.EventTypeNormal, "ScaleDownEmpty", "Scale-down: removing empty node %q", node.Name)
				a.nodeDeletionTracker.StartDeletion(bucket.Group.Id(), node.Name)
			}
		}
	}
	return reportedSDNodes
}

// summarizeTaintErrors counts taint failures by error code, e.g. "TooManyRequests(429)": 2.
func summarizeTaintErrors(failedNodes map[string]error) map[string]int {
	counts := make(map[string]int)
	for _, err := range failedNodes {
		counts[taintErrorCode(err)]++
	}
	return counts
}

// taintErrorCode returns a label like "TooManyRequests(429)" for API errors, or "OTHER" for other errors.
// API errors without a reason (e.g. a 502 from a proxy) get the HTTP status text instead: "Bad Gateway(502)".
func taintErrorCode(err error) string {
	var apiStatus apierrors.APIStatus
	if !goerrors.As(err, &apiStatus) {
		return "OTHER"
	}
	status := apiStatus.Status()
	name := string(status.Reason)
	if status.Reason == metav1.StatusReasonUnknown {
		name = http.StatusText(int(status.Code))
	}
	if name == "" {
		name = "UNKNOWN"
	}
	return fmt.Sprintf("%s(%d)", name, status.Code)
}

func nodeNames(nodes []*apiv1.Node) []string {
	names := make([]string, len(nodes))
	for i, node := range nodes {
		names[i] = node.Name
	}
	return names
}

// taintResult is what taintNodesSync returns.
type taintResult struct {
	// buckets are the node group views with the tainted nodes that go on to deletion.
	buckets []*budgets.NodeGroupView
	// nodesToClean are the tainted nodes of atomic node groups whose scale-down was aborted.
	nodesToClean []*apiv1.Node
}

// taintNodesSync taints all nodes from buckets and returns the node group views with the nodes that
// should be deleted. Nodes that failed to taint are left out. Atomic node groups are scaled down
// all or nothing: after the first taint failure in an atomic node group, its remaining nodes are
// skipped and its already tainted nodes are returned in nodesToClean.
// Returns an error only if zero nodes can be scaled down.
func (a *Actuator) taintNodesSync(
	ctx context.Context,
	buckets []*budgets.NodeGroupView,
	latencyTracker *updateLatencyTracker,
) (taintResult, errors.AutoscalerError) {
	logger := klog.FromContext(ctx)
	type taintTarget struct {
		node   *apiv1.Node
		bucket *budgets.NodeGroupView
	}
	var targets []taintTarget
	for _, bucket := range buckets {
		for _, node := range bucket.Nodes {
			targets = append(targets, taintTarget{node: node, bucket: bucket})
		}
	}

	var mu sync.Mutex
	failed := make(map[string]error)
	failedAtomicGroups := make(map[string]bool)

	taintedAtomic := make(map[string][]*apiv1.Node)

	// context.Background() makes sure every node is either tainted, failed, or skipped.
	workqueue.ParallelizeUntil(context.Background(), a.autoscalingCtx.MaxConcurrentNodesTainting, len(targets), func(i int) {
		t := targets[i]
		// CropNodes sets BatchSize only for atomic node groups.
		isAtomic := t.bucket.BatchSize > 0

		// If another node from the same atomic ng has failed to taint, there is no point in tainting its other nodes.
		// This group's scaledown will be aborted.
		if isAtomic {
			mu.Lock()
			skip := failedAtomicGroups[t.bucket.Group.Id()]
			mu.Unlock()
			if skip {
				return
			}
		}

		// Capture the start time here rather than when the node is queued, so time spent waiting for a free worker doesn't inflate the latency.
		startTime := time.Now()
		if err := a.taintNode(ctx, t.node); err != nil {
			mu.Lock()
			failed[t.node.Name] = err
			if isAtomic {
				failedAtomicGroups[t.bucket.Group.Id()] = true
			}
			mu.Unlock()
			return
		}
		latencyTracker.recordStartTime(t.node.Name, startTime)
		if isAtomic {
			mu.Lock()
			taintedAtomic[t.bucket.Group.Id()] = append(taintedAtomic[t.bucket.Group.Id()], t.node)
			mu.Unlock()
		}
	})

	if len(failed) > 0 {
		logger.Error(nil, "Failed to taint nodes", "errorCount", len(failed), "errorsByCode", summarizeTaintErrors(failed))
	}
	if len(failedAtomicGroups) > 0 {
		logger.Error(nil, "Aborting scaledown for atomic node groups with taint failures", "failedAtomicNodeGroups", slices.Sorted(maps.Keys(failedAtomicGroups)))
	}

	var result taintResult
	for id := range failedAtomicGroups {
		result.nodesToClean = append(result.nodesToClean, taintedAtomic[id]...)
	}

	batchErr := errors.NewAutoscalerErrorf(errors.ApiCallError, "no nodes scaled down: couldn't taint %d of %d nodes with ToBeDeleted", len(failed), len(targets))
	for _, bucket := range buckets {
		var nodes []*apiv1.Node
		for _, node := range bucket.Nodes {
			if failed[node.Name] != nil || failedAtomicGroups[bucket.Group.Id()] {
				nodeErr := failed[node.Name]
				if nodeErr == nil {
					nodeErr = batchErr
				}
				a.nodeDeletionTracker.AddDeletionResult(node.Name, status.NodeDeleteResult{
					ResultType: status.NodeDeleteErrorFailedToMarkToBeDeleted,
					Err:        nodeErr,
				})
				continue
			}
			nodes = append(nodes, node)
		}
		if len(nodes) > 0 {
			filteredBucket := *bucket
			filteredBucket.Nodes = nodes
			result.buckets = append(result.buckets, &filteredBucket)
		}
	}
	if len(result.buckets) == 0 {
		return result, batchErr
	}
	return result, nil
}

// cleanTaintsSync removes ToBeDeletedTaint concurrently from nodes whose atomic nodegroup scale-down was aborted.
func (a *Actuator) cleanTaintsSync(ctx context.Context, nodes []*apiv1.Node) {
	workqueue.ParallelizeUntil(context.Background(), a.autoscalingCtx.MaxConcurrentNodesTainting, len(nodes), func(piece int) {
		node := nodes[piece]
		if _, err := taints.CleanToBeDeleted(ctx, node, a.autoscalingCtx.ClientSet, a.autoscalingCtx.CordonNodeBeforeTerminate); err != nil {
			klog.FromContext(ctx).Error(err, "Failed to remove ToBeDeleted taint after a failed scale-down", "node", klog.KObj(node))
		}
	})
}

func (a *Actuator) deleteNodesAsync(ctx context.Context, bucket *budgets.NodeGroupView, force bool) {
	nodes, nodeGroup, drain, batchSize := bucket.Nodes, bucket.Group, bucket.Drain, bucket.BatchSize
	var remainingPdbTracker pdb.RemainingPdbTracker
	var registry kube_util.ListerRegistry

	if len(nodes) == 0 {
		return
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

// scaleDownNodeToReport builds the status entry for a node whose deletion is being started. Utilization and the
// list of evicted pods are informational: the lookups below already succeeded for this node during planning, but if
// one of them fails here, the error is logged and the fields are left empty rather than dropping the node from the report.
func (a *Actuator) scaleDownNodeToReport(ctx context.Context, node *apiv1.Node, nodeGroup cloudprovider.NodeGroup, drain bool) *status.ScaleDownNode {
	sdNode := &status.ScaleDownNode{
		Node:      node,
		NodeGroup: nodeGroup,
	}
	logger := klog.FromContext(ctx)
	nodeInfo, err := a.autoscalingCtx.ClusterSnapshot.GetNodeInfo(node.Name)
	if err != nil {
		logger.Error(err, "Scale-down: couldn't get node info for scaled down node", "node", klog.KObj(node))
		return sdNode
	}
	if drain {
		_, nonDsPodsToEvict := podsToEvict(nodeInfo, a.autoscalingCtx.DaemonSetEvictionForOccupiedNodes)
		sdNode.EvictedPods = nonDsPodsToEvict
	}

	ignoreDaemonSetsUtilization, err := a.configGetter.GetIgnoreDaemonSetsUtilization(ctx, nodeGroup)
	if err != nil {
		logger.Error(err, "Scale-down: couldn't get ignoreDaemonSetsUtilization for scaled down node", "node", klog.KObj(node), "nodeGroupId", nodeGroup.Id())
		return sdNode
	}
	gpuConfig := a.autoscalingCtx.CloudProvider.GetNodeGpuConfig(ctx, node)
	utilInfo, err := utilization.Calculate(ctx, nodeInfo, ignoreDaemonSetsUtilization, a.autoscalingCtx.IgnoreMirrorPodsUtilization, a.autoscalingCtx.DynamicResourceAllocationEnabled, gpuConfig, time.Now())
	if err != nil {
		logger.Error(err, "Scale-down: couldn't calculate utilization of scaled down node", "node", klog.KObj(node))
		return sdNode
	}
	sdNode.UtilInfo = utilInfo
	return sdNode
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
