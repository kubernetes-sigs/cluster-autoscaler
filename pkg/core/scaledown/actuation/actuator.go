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
	pastLatenciesMu           sync.Mutex
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

	emptyToDelete, drainToDelete := a.budgetProcessor.CropNodes(ctx, a.nodeDeletionTracker, empty, drain)
	if len(emptyToDelete) == 0 && len(drainToDelete) == 0 {
		return status.ScaleDownNoNodeDeleted, nil, nil
	}

	var latencyTracker *UpdateLatencyTracker
	if a.autoscalingCtx.AutoscalingOptions.DynamicNodeDeleteDelayAfterTaintEnabled {
		latencyTracker = NewUpdateLatencyTracker(a.autoscalingCtx.AutoscalingKubeClients.ListerRegistry.AllNodeLister(), nodeNames(collectNodes(emptyToDelete, drainToDelete)))
		go latencyTracker.Start(ctx)
	}

	tainted := a.taintNodesSync(ctx, emptyToDelete, drainToDelete, latencyTracker)
	if len(tainted.empty) == 0 && len(tainted.drain) == 0 {
		a.cleanTaintsSync(ctx, tainted.nodesToClean)
		return status.ScaleDownError, nil, errors.NewAutoscalerErrorf(errors.ApiCallError, "no nodes scaled down: couldn't taint %d of %d nodes with ToBeDeleted", tainted.failedCount, tainted.nodesCount)
	}

	// Deletion goroutines wait for the delay after taint themselves, so the main loop doesn't block on it.
	delayAfterTaint := a.nodeDeleteDelayAfterTaintFunc(ctx, latencyTracker)
	scaledDownNodes := a.deleteAsyncEmpty(ctx, tainted.empty, delayAfterTaint, force)
	scaledDownNodes = append(scaledDownNodes, a.deleteAsyncDrain(ctx, tainted.drain, delayAfterTaint, force)...)

	// Nodes of aborted atomic node groups are untainted after the other deletions have started.
	a.cleanTaintsSync(ctx, tainted.nodesToClean)

	return status.ScaleDownNodeDeleteStarted, scaledDownNodes, nil
}

// deleteAsyncEmpty immediately starts deletions asynchronously.
// scaledDownNodes return value contains all nodes for which deletion successfully started.
func (a *Actuator) deleteAsyncEmpty(ctx context.Context, NodeGroupViews []*budgets.NodeGroupView, delayAfterTaint func() time.Duration, force bool) (reportedSDNodes []*status.ScaleDownNode) {
	logger := klog.FromContext(ctx)
	for _, bucket := range NodeGroupViews {
		for _, node := range bucket.Nodes {
			logger.V(0).Info("Scale-down: removing empty node", "node", klog.KObj(node))
			a.autoscalingCtx.LogRecorder.Eventf(apiv1.EventTypeNormal, "ScaleDownEmpty", "Scale-down: removing empty node %q", node.Name)

			reportedSDNodes = append(reportedSDNodes, a.scaleDownNodeToReport(ctx, node, bucket.Group, false))
			a.nodeDeletionTracker.StartDeletion(bucket.Group.Id(), node.Name)
		}
	}

	for _, bucket := range NodeGroupViews {
		go a.deleteNodesAsync(ctx, bucket.Nodes, bucket.Group, false, force, bucket.BatchSize, delayAfterTaint)
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

func collectNodes(viewLists ...[]*budgets.NodeGroupView) []*apiv1.Node {
	var nodes []*apiv1.Node
	for _, views := range viewLists {
		for _, bucket := range views {
			nodes = append(nodes, bucket.Nodes...)
		}
	}
	return nodes
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
	// empty and drain are the node group views with the tainted nodes that go on to deletion.
	empty, drain []*budgets.NodeGroupView
	// nodesToClean are the tainted nodes of atomic node groups whose scale-down was aborted.
	nodesToClean []*apiv1.Node
	// nodesCount is the number of nodes in the batch, failedCount the number of nodes that failed to taint.
	nodesCount, failedCount int
}

// taintNodesSync taints all nodes from emptyToDelete and drainToDelete and returns the node group views
// with the nodes that should be deleted. Nodes that failed to taint are left out. Atomic node groups
// are scaled down all or nothing: after the first taint failure in an atomic node group, its remaining
// nodes are skipped and its already tainted nodes are returned in nodesToClean.
// taintNodesSync also does all the latency tracker bookkeeping: every node that won't be deleted is dropped.
func (a *Actuator) taintNodesSync(
	ctx context.Context,
	emptyToDelete, drainToDelete []*budgets.NodeGroupView,
	latencyTracker *UpdateLatencyTracker,
) taintResult {
	logger := klog.FromContext(ctx)
	type taintTarget struct {
		node   *apiv1.Node
		bucket *budgets.NodeGroupView
	}
	var targets []taintTarget
	for _, bucket := range slices.Concat(emptyToDelete, drainToDelete) {
		for _, node := range bucket.Nodes {
			targets = append(targets, taintTarget{node: node, bucket: bucket})
		}
	}

	var mu sync.Mutex
	failed := make(map[string]error)
	// failedAtomicGroups counts taint failures per atomic node group.
	failedAtomicGroups := make(map[string]int)
	var tainted []taintTarget
	// context.Background() makes sure every node is either tainted, failed, or skipped.
	workqueue.ParallelizeUntil(context.Background(), maxConcurrentNodesTainting, len(targets), func(piece int) {
		t := targets[piece]
		// CropNodes sets BatchSize only for atomic node groups.
		atomic := t.bucket.BatchSize > 0
		mu.Lock()
		skip := atomic && failedAtomicGroups[t.bucket.Group.Id()] > 0
		mu.Unlock()
		if skip {
			if latencyTracker != nil {
				latencyTracker.DropNodes(t.node.Name)
			}
			return
		}

		if latencyTracker != nil {
			latencyTracker.RecordStartTime(t.node.Name)
		}
		err := a.taintNode(ctx, t.node)
		if err != nil && latencyTracker != nil {
			latencyTracker.DropNodes(t.node.Name)
		}

		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			failed[t.node.Name] = err
			if atomic {
				failedAtomicGroups[t.bucket.Group.Id()]++
			}
			return
		}
		tainted = append(tainted, t)
	})
	if len(failed) > 0 {
		logger.Error(nil, "Failed to taint nodes", "nodesCount", len(targets), "errorCount", len(failed), "errorsByCode", summarizeTaintErrors(failed), "failedAtomicNodeGroupsCount", len(failedAtomicGroups))
	}

	result := taintResult{nodesCount: len(targets), failedCount: len(failed)}
	inFailedAtomicGroup := func(bucket *budgets.NodeGroupView) bool {
		return bucket.BatchSize > 0 && failedAtomicGroups[bucket.Group.Id()] > 0
	}
	// Per aborted atomic node group: how many of its nodes are in the batch, and how many were tainted before the failure.
	abortedNodesCount := make(map[string]int, len(failedAtomicGroups))
	untaintedCount := make(map[string]int, len(failedAtomicGroups))
	for _, t := range targets {
		if inFailedAtomicGroup(t.bucket) {
			abortedNodesCount[t.bucket.Group.Id()]++
		}
	}
	for _, t := range tainted {
		if inFailedAtomicGroup(t.bucket) {
			result.nodesToClean = append(result.nodesToClean, t.node)
			untaintedCount[t.bucket.Group.Id()]++
		}
	}
	for groupId, failures := range failedAtomicGroups {
		logger.Error(nil, "Not scaling down atomic node group: some of its nodes couldn't be tainted", "nodeGroupId", groupId, "nodesCount", abortedNodesCount[groupId], "taintFailures", failures, "skipped", abortedNodesCount[groupId]-failures-untaintedCount[groupId], "untainted", untaintedCount[groupId])
	}
	if len(result.nodesToClean) > 0 && latencyTracker != nil {
		latencyTracker.DropNodes(nodeNames(result.nodesToClean)...)
	}

	filterTainted := func(views []*budgets.NodeGroupView) []*budgets.NodeGroupView {
		var filtered []*budgets.NodeGroupView
		for _, bucket := range views {
			if inFailedAtomicGroup(bucket) {
				continue
			}
			var nodes []*apiv1.Node
			for _, node := range bucket.Nodes {
				if _, isFailed := failed[node.Name]; !isFailed {
					nodes = append(nodes, node)
				}
			}
			if len(nodes) > 0 {
				filteredBucket := *bucket
				filteredBucket.Nodes = nodes
				filtered = append(filtered, &filteredBucket)
			}
		}
		return filtered
	}
	result.empty = filterTainted(emptyToDelete)
	result.drain = filterTainted(drainToDelete)
	return result
}

// cleanTaintsSync removes ToBeDeletedTaint concurrently from nodes whose atomic nodegroup scale-down was aborted.
func (a *Actuator) cleanTaintsSync(ctx context.Context, nodes []*apiv1.Node) {
	workqueue.ParallelizeUntil(context.Background(), maxConcurrentNodesTainting, len(nodes), func(piece int) {
		node := nodes[piece]
		if _, err := taints.CleanToBeDeleted(ctx, node, a.autoscalingCtx.ClientSet, a.autoscalingCtx.CordonNodeBeforeTerminate); err != nil {
			klog.FromContext(ctx).Error(err, "Failed to remove ToBeDeleted taint after a failed scale-down", "node", klog.KObj(node))
		}
	})
}

// deleteAsyncDrain asynchronously starts deletions with drain for all provided nodes. scaledDownNodes return value contains all nodes for which
// deletion successfully started.
func (a *Actuator) deleteAsyncDrain(ctx context.Context, NodeGroupViews []*budgets.NodeGroupView, delayAfterTaint func() time.Duration, force bool) (reportedSDNodes []*status.ScaleDownNode) {
	logger := klog.FromContext(ctx)
	for _, bucket := range NodeGroupViews {
		for _, drainNode := range bucket.Nodes {
			sdNode := a.scaleDownNodeToReport(ctx, drainNode, bucket.Group, true)
			logger.V(0).Info("Scale-down: removing node", "node", klog.KObj(drainNode), "utilization", sdNode.UtilInfo, "podsToReschedule", joinPodNames(sdNode.EvictedPods))
			a.autoscalingCtx.LogRecorder.Eventf(apiv1.EventTypeNormal, "ScaleDown", "Scale-down: removing node %s, utilization: %v, pods to reschedule: %s", drainNode.Name, sdNode.UtilInfo, joinPodNames(sdNode.EvictedPods))
			reportedSDNodes = append(reportedSDNodes, sdNode)
			a.nodeDeletionTracker.StartDeletionWithDrain(bucket.Group.Id(), drainNode.Name)
		}
	}

	for _, bucket := range NodeGroupViews {
		go a.deleteNodesAsync(ctx, bucket.Nodes, bucket.Group, true, force, bucket.BatchSize, delayAfterTaint)
	}

	return reportedSDNodes
}

// nodeDeleteDelayAfterTaintFunc returns a function that tells the deletion goroutines how long to wait after
// tainting before draining and deleting nodes. Without a latency tracker that's the static nodeDeleteDelayAfterTaint.
// With one, the delay is computed once per batch in the background and the returned function blocks until the
// taints have shown up in CA's node watch cache. Either way the main loop doesn't wait for it.
func (a *Actuator) nodeDeleteDelayAfterTaintFunc(ctx context.Context, latencyTracker *UpdateLatencyTracker) func() time.Duration {
	if latencyTracker == nil {
		return func() time.Duration { return a.nodeDeleteDelayAfterTaint }
	}
	ready := make(chan struct{})
	var delay time.Duration
	go func() {
		defer close(ready)
		delay = a.dynamicNodeDeleteDelayAfterTaint(ctx, latencyTracker)
	}()
	return func() time.Duration {
		<-ready
		return delay
	}
}

// dynamicNodeDeleteDelayAfterTaint waits for the taint latency of the batch, records it, and returns twice the
// maximum latency observed over the last hour. If the latency couldn't be measured, it returns the static delay.
func (a *Actuator) dynamicNodeDeleteDelayAfterTaint(ctx context.Context, latencyTracker *UpdateLatencyTracker) time.Duration {
	latency, err := latencyTracker.WaitForLatency()
	if err != nil {
		klog.FromContext(ctx).Error(err, "Failed to measure taint latency, using the static node delete delay after taint")
		return a.nodeDeleteDelayAfterTaint
	}
	a.pastLatenciesMu.Lock()
	defer a.pastLatenciesMu.Unlock()
	a.pastLatencies.RegisterElement(latency)
	a.pastLatencies.DropNotNewerThan(time.Now().Add(-1 * pastLatencyExpireDuration))
	// CA is expected to wait 3 times the round-trip time between CA and the api-server.
	// At this point, we have already tainted all the nodes.
	// Therefore, the nodeDeleteDelayAfterTaint is set 2 times the maximum latency observed during the last hour.
	return 2 * maxLatency(a.pastLatencies.ToSlice())
}

func (a *Actuator) deleteNodesAsync(ctx context.Context, nodes []*apiv1.Node, nodeGroup cloudprovider.NodeGroup, drain bool, force bool, batchSize int, delayAfterTaint func() time.Duration) {
	logger := klog.FromContext(ctx)
	var remainingPdbTracker pdb.RemainingPdbTracker
	var registry kube_util.ListerRegistry

	if len(nodes) == 0 {
		return
	}

	if nodeDeleteDelayAfterTaint := delayAfterTaint(); nodeDeleteDelayAfterTaint > time.Duration(0) {
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

// scaleDownNodeToReport builds the status entry for a node whose deletion is being started. Utilization and the
// list of evicted pods are informational: if they can't be computed, the error is logged and they are left empty.
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
