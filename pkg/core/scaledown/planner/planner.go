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

package planner

import (
	"context"
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/eligibility"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/nodeevaltracker"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/pdb"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/unneeded"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/unremovable"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/drainability/rules"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/options"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/utilization"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/atomic"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	pod_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/pod"
)

type eligibilityChecker interface {
	FilterOutUnremovable(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleDownCandidates []*apiv1.Node, timestamp time.Time, unremovableNodes *unremovable.Nodes) ([]string, map[string]utilization.Info, []*simulator.UnremovableNode)
}

type removalSimulator interface {
	DropOldHints()
	SimulateNodeRemoval(ctx context.Context, node string, podDestinations map[string]bool, timestamp time.Time, remainingPdbTracker pdb.RemainingPdbTracker) (*simulator.NodeToBeRemoved, *simulator.UnremovableNode)
}

// controllerReplicasCalculator calculates a number of target and expected replicas for a given controller.
type controllerReplicasCalculator interface {
	getReplicas(metav1.OwnerReference, string) (*replicasInfo, error)
}

type replicasInfo struct {
	targetReplicas, currentReplicas int32
}

// Planner is responsible for deciding which nodes should be deleted during scale down.
type Planner struct {
	autoscalingCtx        *ca_context.AutoscalingContext
	unremovableNodes      *unremovable.Nodes
	unneededNodes         *unneeded.Nodes
	rs                    removalSimulator
	actuationInjector     *scheduling.HintingSimulator
	latestUpdate          time.Time
	minUpdateInterval     time.Duration
	eligibilityChecker    eligibilityChecker
	nodeUtilizationMap    map[string]utilization.Info
	cc                    controllerReplicasCalculator
	quotasTrackerFactory  *resourcequotas.TrackerFactory
	scaleDownSetProcessor nodes.ScaleDownSetProcessor
	scaleDownContext      *nodes.ScaleDownContext
	maxNodeSkipEvalTime   *nodeevaltracker.MaxNodeSkipEvalTime
}

// New creates a new Planner object.
func New(autoscalingCtx *ca_context.AutoscalingContext, processors *processors.AutoscalingProcessors, deleteOptions options.NodeDeleteOptions, drainabilityRules rules.Rules, quotasTrackerFactory *resourcequotas.TrackerFactory) *Planner {
	minUpdateInterval := autoscalingCtx.AutoscalingOptions.NodeGroupDefaults.ScaleDownUnneededTime
	if minUpdateInterval == 0*time.Nanosecond {
		minUpdateInterval = 1 * time.Nanosecond
	}

	unneededNodes := unneeded.NewNodes(processors.NodeGroupConfigProcessor)
	if autoscalingCtx.AutoscalingOptions.NodeDeletionCandidateTTL != 0 {
		unneededNodes.LoadFromExistingTaints(autoscalingCtx, time.Now())
	}

	var maxNodeSkipEvalTime *nodeevaltracker.MaxNodeSkipEvalTime
	if autoscalingCtx.AutoscalingOptions.MaxNodeSkipEvalTimeTrackerEnabled {
		maxNodeSkipEvalTime = nodeevaltracker.NewMaxNodeSkipEvalTime(time.Now())
	}

	return &Planner{
		autoscalingCtx:        autoscalingCtx,
		unremovableNodes:      unremovable.NewNodes(),
		unneededNodes:         unneededNodes,
		rs:                    simulator.NewRemovalSimulator(autoscalingCtx.ListerRegistry, autoscalingCtx.ClusterSnapshot, deleteOptions, drainabilityRules, true),
		actuationInjector:     scheduling.NewHintingSimulator(),
		eligibilityChecker:    eligibility.NewChecker(processors.NodeGroupConfigProcessor),
		nodeUtilizationMap:    make(map[string]utilization.Info),
		cc:                    newControllerReplicasCalculator(autoscalingCtx.ListerRegistry),
		quotasTrackerFactory:  quotasTrackerFactory,
		scaleDownSetProcessor: processors.ScaleDownSetProcessor,
		scaleDownContext:      nodes.NewDefaultScaleDownContext(),
		minUpdateInterval:     minUpdateInterval,
		maxNodeSkipEvalTime:   maxNodeSkipEvalTime,
	}
}

// UpdateClusterState needs to be periodically invoked to provide Planner with
// up-to-date information about the cluster.
// Planner will evaluate scaleDownCandidates in the order provided here.
func (p *Planner) UpdateClusterState(ctx context.Context, podDestinations, scaleDownCandidates []*apiv1.Node, as scaledown.ActuationStatus, currentTime time.Time) errors.AutoscalerError {
	logger := klog.FromContext(ctx)
	updateInterval := currentTime.Sub(p.latestUpdate)
	if updateInterval < p.minUpdateInterval {
		p.minUpdateInterval = updateInterval
	}
	p.latestUpdate = currentTime
	p.scaleDownContext.ActuationStatus = as
	// Avoid persisting changes done by the simulation.
	p.autoscalingCtx.ClusterSnapshot.Fork()
	defer p.autoscalingCtx.ClusterSnapshot.Revert()
	err := p.injectRecentlyEvictedPods()
	if err != nil {
		logger.Info("Not all recently evicted pods could be injected", "err", err)

	}
	deletions := asMap(merged(as.DeletionsInProgress()))
	podDestinations = filterOutOngoingDeletions(podDestinations, deletions)
	scaleDownCandidates = filterOutOngoingDeletions(scaleDownCandidates, deletions)
	p.categorizeNodes(ctx, asMap(nodeNames(podDestinations)), scaleDownCandidates)
	p.rs.DropOldHints()
	p.actuationInjector.DropOldHints()
	return nil
}

// CleanUpUnneededNodes forces Planner to forget about all nodes considered
// unneeded so far.
func (p *Planner) CleanUpUnneededNodes(ctx context.Context) {
	p.unneededNodes.Clear(ctx)
}

// NodesToDelete returns all Nodes that could be removed right now, according
// to the Planner.
func (p *Planner) NodesToDelete(ctx context.Context, _ time.Time) (empty, needDrain []*apiv1.Node) {
	logger := klog.FromContext(ctx)
	empty, needDrain = []*apiv1.Node{}, []*apiv1.Node{}

	nodes, err := allNodes(p.autoscalingCtx.ClusterSnapshot)
	if err != nil {
		logger.Error(err, "Failed to list nodes for final limit check")
		return nil, nil
	}

	tracker, err := p.quotasTrackerFactory.NewMinQuotasTracker(ctx, p.autoscalingCtx, nodes)
	if err != nil {
		logger.Error(err, "Failed to create tracker for final limit check")
		return nil, nil
	}
	p.scaleDownContext.Tracker = tracker

	emptyRemovableNodes, needDrainRemovableNodes, unremovableNodes := p.unneededNodes.RemovableAt(ctx, p.autoscalingCtx, *p.scaleDownContext, p.latestUpdate)
	p.addUnremovableNodes(unremovableNodes)

	needDrainRemovableNodes = sortByRisk(needDrainRemovableNodes)
	candidatesToBeRemoved := append(emptyRemovableNodes, needDrainRemovableNodes...)

	nodesToRemove, unremovableNodes := p.scaleDownSetProcessor.FilterUnremovableNodes(ctx, p.autoscalingCtx, p.scaleDownContext, candidatesToBeRemoved)
	p.addUnremovableNodes(unremovableNodes)

	for _, nodeToRemove := range nodesToRemove {
		if len(nodeToRemove.OnCompletionPods) > 0 {
			logger.V(2).Info("Node has active on-completion pods, delaying scale down", "node", klog.KObj(nodeToRemove.Node))
			p.addUnremovableNodes([]simulator.UnremovableNode{{
				Node:   nodeToRemove.Node,
				Reason: simulator.BlockedByOnCompletionPod,
			}})
			continue
		}

		if len(nodeToRemove.PodsToReschedule) > 0 {
			needDrain = append(needDrain, nodeToRemove.Node)
		} else {
			empty = append(empty, nodeToRemove.Node)
		}
	}

	return empty, needDrain
}

func (p *Planner) addUnremovableNodes(unremovableNodes []simulator.UnremovableNode) {
	for _, u := range unremovableNodes {
		p.unremovableNodes.Add(&u)
	}
}

func allNodes(s clustersnapshot.ClusterSnapshot) ([]*apiv1.Node, error) {
	nodeInfos, err := s.ListNodeInfos()
	if err != nil {
		// This should never happen, List() returns err only because scheduler interface requires it.
		return nil, err
	}
	nodes := make([]*apiv1.Node, len(nodeInfos))
	for i, ni := range nodeInfos {
		nodes[i] = ni.Node()
	}
	return nodes, nil
}

// UnneededNodes returns a list of nodes currently considered as unneeded.
func (p *Planner) UnneededNodes() []*scaledown.UnneededNode {
	return p.unneededNodes.AsList()
}

// UnremovableNodes returns a list of nodes currently considered as unremovable.
func (p *Planner) UnremovableNodes() []*simulator.UnremovableNode {
	return p.unremovableNodes.AsList()
}

// NodeUtilizationMap returns a map with utilization of nodes.
func (p *Planner) NodeUtilizationMap() map[string]utilization.Info {
	return p.nodeUtilizationMap
}

// injectRecentlyEvictedPods injects pods into ClusterSnapshot, to allow
// subsequent simulation to anticipate which pods will end up getting replaced
// due to being evicted by previous scale down(s). This function injects pods
// which were recently evicted (it is up to ActuationStatus to decide what
// "recently" means in this case). The existing pods from currently drained
// nodes are already added before scale-up to optimize scale-up latency.
//
// For pods that are controlled by controller known by CA, it will check whether
// they have been recreated and will inject only not yet recreated pods.
func (p *Planner) injectRecentlyEvictedPods() error {
	recentlyEvictedRecreatablePods := pod_util.FilterRecreatablePods(p.scaleDownContext.ActuationStatus.RecentEvictions())
	return p.injectPods(filterOutRecreatedPods(recentlyEvictedRecreatablePods, p.cc))
}

func filterOutRecreatedPods(pods []*apiv1.Pod, cc controllerReplicasCalculator) []*apiv1.Pod {
	var podsToInject []*apiv1.Pod
	addedReplicas := make(map[string]int32)
	for _, pod := range pods {
		ownerRef := getKnownOwnerRef(pod.GetOwnerReferences())
		// in case of unknown ownerRef (i.e. not recognized by CA) we still inject
		// the pod, to be on the safe side in case there is some custom controller
		// that will recreate the pod.
		if ownerRef == nil {
			podsToInject = append(podsToInject, pod)
			continue
		}
		rep, err := cc.getReplicas(*ownerRef, pod.Namespace)
		if err != nil {
			podsToInject = append(podsToInject, pod)
			continue
		}
		ownerUID := string(ownerRef.UID)
		if rep.targetReplicas > rep.currentReplicas && addedReplicas[ownerUID] < rep.targetReplicas-rep.currentReplicas {
			podsToInject = append(podsToInject, pod)
			addedReplicas[ownerUID] += 1
		}
	}
	return podsToInject
}

func (p *Planner) injectPods(pods []*apiv1.Pod) error {
	pods = pod_util.ClearPodNodeNames(pods)
	// Note: We're using ScheduleAnywhere, but the pods won't schedule back
	// on the drained nodes due to taints.
	schedulingResult, err := p.actuationInjector.TrySchedulePods(context.Background(), p.autoscalingCtx.ClusterSnapshot, pods, true, clustersnapshot.SchedulingOptions{IsNodeAcceptable: scheduling.ScheduleAnywhere})

	if err != nil {
		return fmt.Errorf("cannot scale down, an unexpected error occurred: %v", err)
	}
	if len(schedulingResult.Statuses) != len(pods) {
		return fmt.Errorf("can reschedule only %d out of %d pods from ongoing deletions", len(schedulingResult.Statuses), len(pods))
	}
	return nil
}

// categorizeNodes determines, for each node, whether it can be eventually
// removed or if there are reasons preventing that.
func (p *Planner) categorizeNodes(ctx context.Context, podDestinations map[string]bool, scaleDownCandidates []*apiv1.Node) {
	logger := klog.FromContext(ctx)
	unremovableTimeout := p.latestUpdate.Add(p.autoscalingCtx.AutoscalingOptions.UnremovableNodeRecheckTimeout)
	unremovableCount := 0
	var removableList []simulator.NodeToBeRemoved
	atomicScaleDownNodesCount := 0
	nonAtomicRemovableCount := 0
	p.unremovableNodes.Update(ctx, p.autoscalingCtx.ClusterSnapshot, p.latestUpdate)
	currentlyUnneededNodeNames, utilizationMap, ineligible := p.eligibilityChecker.FilterOutUnremovable(ctx, p.autoscalingCtx, scaleDownCandidates, p.latestUpdate, p.unremovableNodes)
	for _, n := range ineligible {
		p.unremovableNodes.Add(n)
	}
	p.nodeUtilizationMap = utilizationMap

	// Prefilter incomplete atomic node groups so simulation isn't wasted on atomic groups that cannot scale down.
	var prefilteredUnremovableCount int
	currentlyUnneededNodeNames, prefilteredUnremovableCount = p.prefilterIncompleteAtomicNodeGroups(ctx, currentlyUnneededNodeNames, unremovableTimeout)
	unremovableCount += prefilteredUnremovableCount

	timer := time.NewTimer(p.autoscalingCtx.ScaleDownSimulationTimeout)
	var skippedNodes []string
	atomicGroups := p.groupAtomicNodes(ctx, currentlyUnneededNodeNames)
	processedAtomicGroups := make(map[string]bool)

	for i, nodeName := range currentlyUnneededNodeNames {
		if timedOut(timer) {
			skippedNodes = p.appendUnprocessed(ctx, skippedNodes, currentlyUnneededNodeNames[i:], processedAtomicGroups)
			logger.Info("Some nodes skipped in scale down simulation due to timeout.", "skippedNodesCount", len(skippedNodes), "nodesCount", len(currentlyUnneededNodeNames))
			break
		}

		nodeInfo, err := p.autoscalingCtx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil || nodeInfo == nil || nodeInfo.Node() == nil {
			logger.Error(err, "Failed to get node info", "nodeName", nodeName)
			continue
		}
		node := nodeInfo.Node()
		nodeGroup, isAtomic := atomic.IsAtomicNodeGroup(ctx, p.autoscalingCtx, node)

		if isAtomic && nodeGroup != nil {
			ngID := nodeGroup.Id()
			if processedAtomicGroups[ngID] {
				continue
			}
			groupRemovable, groupUnremovableCount, groupSkipped, timedOut := p.simulateAtomicNodeGroup(ctx, ngID, atomicGroups[ngID], podDestinations, unremovableTimeout, timer)
			if timedOut {
				skippedNodes = p.appendUnprocessed(ctx, skippedNodes, currentlyUnneededNodeNames[i:], processedAtomicGroups)
				klog.Warningf("%d out of %d nodes skipped in scale down simulation due to timeout.", len(skippedNodes), len(currentlyUnneededNodeNames))
				break
			}
			processedAtomicGroups[ngID] = true
			if len(groupRemovable) > 0 {
				removableList = append(removableList, groupRemovable...)
				atomicScaleDownNodesCount += len(groupRemovable)
			}
			unremovableCount += groupUnremovableCount
			skippedNodes = append(skippedNodes, groupSkipped...)
			continue
		}

		if nonAtomicRemovableCount >= p.unneededNodesLimit() {
			skippedNodes = append(skippedNodes, nodeName)
			logger.V(4).Info("Skipping non-atomic node in scale down simulation: there are already some non-atomic unneeded nodes.", "node", klog.KObj(node), "nonAtomicUnneededNodesCount", nonAtomicRemovableCount)
			continue
		}

		removable, unremovable := p.rs.SimulateNodeRemoval(ctx, nodeName, podDestinations, p.latestUpdate, p.autoscalingCtx.RemainingPdbTracker)
		if removable != nil {
			_, inParallel, _ := p.autoscalingCtx.RemainingPdbTracker.CanRemovePods(removable.PodsToReschedule)
			if !inParallel {
				removable.IsRisky = true
			}
			delete(podDestinations, removable.Node.Name)
			p.autoscalingCtx.RemainingPdbTracker.RemovePods(removable.PodsToReschedule)
			removableList = append(removableList, *removable)
			nonAtomicRemovableCount++
		}
		if unremovable != nil {
			unremovableCount += 1
			p.unremovableNodes.AddTimeout(unremovable, unremovableTimeout)
		}
	}

	p.handleUnprocessedNodes(skippedNodes)
	p.unneededNodes.Update(ctx, p.autoscalingCtx, removableList, p.latestUpdate)
	if unremovableCount > 0 {
		logger.V(1).Info("Some nodes found to be unremovable in simulation, will re-check them within timeout.", "unremovableNodesCount", unremovableCount, "timeout", unremovableTimeout)
	}
}

// prefilterIncompleteAtomicNodeGroups identifies atomic node groups in currentlyUnneededNodeNames.
// If an atomic node group has fewer unneeded candidates than its target size, it cannot be
// atomically scaled down. Such incomplete atomic node groups are filtered out of simulation,
// and their nodes are marked as unremovable.
func (p *Planner) prefilterIncompleteAtomicNodeGroups(ctx context.Context, nodeNames []string, unremovableTimeout time.Time) ([]string, int) {
	logger := klog.FromContext(ctx)
	atomicGroupNodes := make(map[string][]string)
	atomicGroupObj := make(map[string]cloudprovider.NodeGroup)
	nodeNameToNode := make(map[string]*apiv1.Node)

	for _, nodeName := range nodeNames {
		nodeInfo, err := p.autoscalingCtx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil || nodeInfo == nil || nodeInfo.Node() == nil {
			continue
		}
		node := nodeInfo.Node()
		nodeNameToNode[nodeName] = node
		nodeGroup, isAtomic := atomic.IsAtomicNodeGroup(ctx, p.autoscalingCtx, node)
		if isAtomic && nodeGroup != nil {
			ngID := nodeGroup.Id()
			atomicGroupNodes[ngID] = append(atomicGroupNodes[ngID], nodeName)
			atomicGroupObj[ngID] = nodeGroup
		}
	}

	if len(atomicGroupNodes) == 0 {
		return nodeNames, 0
	}

	incompleteNodes := make(map[string]bool)
	unremovableCount := 0

	allNodes, err := allNodes(p.autoscalingCtx.ClusterSnapshot)
	if err != nil {
		return nil, 0
	}

	for ngID, unneededNodes := range atomicGroupNodes {
		ng := atomicGroupObj[ngID]
		targetSize, err := ng.TargetSize(ctx)
		if err != nil {
			logger.Error(err, "Failed to get target size for node group", "nodeGroupId", ng.Id())
			continue
		}

		if len(unneededNodes) < targetSize {
			registeredCount, countErr := atomic.CountRegisteredNodesForGroup(ctx, ng, allNodes)
			if countErr == nil && len(unneededNodes) >= registeredCount {
				// All registered nodes in the snapshot are unneeded
				continue
			}
			logger.V(2).Info("Atomic node group has some unneeded nodes. Filtering out from scale down simulation.", "nodeGroupId", ng.Id(), "ngUnneededNodesCount", len(unneededNodes), "ngTargetSize", targetSize)
			for _, nodeName := range unneededNodes {
				incompleteNodes[nodeName] = true
				if node, ok := nodeNameToNode[nodeName]; ok {
					p.unremovableNodes.AddTimeout(&simulator.UnremovableNode{
						Node:   node,
						Reason: simulator.AtomicScaleDownFailed,
					}, unremovableTimeout)
					unremovableCount++
				}
			}
		}
	}

	if len(incompleteNodes) == 0 {
		return nodeNames, 0
	}

	filteredNodeNames := make([]string, 0, len(nodeNames)-len(incompleteNodes))
	for _, nodeName := range nodeNames {
		if !incompleteNodes[nodeName] {
			filteredNodeNames = append(filteredNodeNames, nodeName)
		}
	}

	return filteredNodeNames, unremovableCount
}

// groupAtomicNodes groups unneeded atomic node names by their NodeGroup ID.
func (p *Planner) groupAtomicNodes(ctx context.Context, nodeNames []string) map[string][]string {
	atomicGroups := make(map[string][]string)
	for _, nodeName := range nodeNames {
		nodeInfo, err := p.autoscalingCtx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil || nodeInfo == nil || nodeInfo.Node() == nil {
			continue
		}
		node := nodeInfo.Node()
		nodeGroup, isAtomic := atomic.IsAtomicNodeGroup(ctx, p.autoscalingCtx, node)
		if isAtomic && nodeGroup != nil {
			ngID := nodeGroup.Id()
			atomicGroups[ngID] = append(atomicGroups[ngID], nodeName)
		}
	}
	return atomicGroups
}

// appendUnprocessed appends remaining node names to skippedNodes, excluding nodes belonging to
// atomic node groups that were already simulated.
func (p *Planner) appendUnprocessed(ctx context.Context, skippedNodes []string, nodeNames []string, processedAtomicGroups map[string]bool) []string {
	for _, nodeName := range nodeNames {
		nodeInfo, err := p.autoscalingCtx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err == nil && nodeInfo != nil && nodeInfo.Node() != nil {
			nodeGroup, isAtomic := atomic.IsAtomicNodeGroup(ctx, p.autoscalingCtx, nodeInfo.Node())
			if isAtomic && nodeGroup != nil && processedAtomicGroups[nodeGroup.Id()] {
				continue
			}
		}
		skippedNodes = append(skippedNodes, nodeName)
	}
	return skippedNodes
}

// simulateAtomicNodeGroup simulates removal of all nodes in an atomic node group as an all-or-nothing unit.
// If any node in the group is unremovable, simulation of remaining nodes is aborted early, and any temporary
// podDestinations and RemainingPdbTracker mutations are rolled back.
// Returns removableList for the group, unremovableCount, skippedNodes, and whether simulation timed out.
func (p *Planner) simulateAtomicNodeGroup(
	ctx context.Context,
	ngID string,
	ngNodes []string,
	podDestinations map[string]bool,
	unremovableTimeout time.Time,
	timer *time.Timer,
) ([]simulator.NodeToBeRemoved, int, []string, bool) {
	logger := klog.FromContext(ctx)

	if timedOut(timer) {
		return nil, 0, ngNodes, true
	}

	podDestinationsCopy := make(map[string]bool, len(podDestinations))
	for k, v := range podDestinations {
		podDestinationsCopy[k] = v
	}
	pdbsSnapshot := p.autoscalingCtx.RemainingPdbTracker.GetPdbs()

	var groupRemovable []simulator.NodeToBeRemoved

	for j, nodeName := range ngNodes {
		if timedOut(timer) {
			for k := range podDestinations {
				delete(podDestinations, k)
			}
			for k, v := range podDestinationsCopy {
				podDestinations[k] = v
			}
			if err := p.autoscalingCtx.RemainingPdbTracker.SetPdbs(pdbsSnapshot); err != nil {
				logger.Error(err, "failed to restore PDB snapshot on timeout for atomic group", "nodeGroupId", ngID)
			}
			return nil, 0, ngNodes, true
		}

		removable, unremovable := p.rs.SimulateNodeRemoval(ctx, nodeName, podDestinations, p.latestUpdate, p.autoscalingCtx.RemainingPdbTracker)
		if removable != nil {
			_, inParallel, _ := p.autoscalingCtx.RemainingPdbTracker.CanRemovePods(removable.PodsToReschedule)
			if !inParallel {
				removable.IsRisky = true
			}
			delete(podDestinations, removable.Node.Name)
			p.autoscalingCtx.RemainingPdbTracker.RemovePods(removable.PodsToReschedule)
			groupRemovable = append(groupRemovable, *removable)
			continue
		}

		// Early-Abort: nodeName is unremovable. Because this is an atomic node group,
		// if any node fails simulation, the entire group cannot scale down.
		logger.V(2).Info("Atomic node group cannot scale down because node is unremovable. Early aborting simulation for nodes.", "nodeGroupId", ngID, "nodeName", nodeName, "ngNodesCount", len(ngNodes))

		for k := range podDestinations {
			delete(podDestinations, k)
		}
		for k, v := range podDestinationsCopy {
			podDestinations[k] = v
		}
		if err := p.autoscalingCtx.RemainingPdbTracker.SetPdbs(pdbsSnapshot); err != nil {
			logger.Error(err, "failed to restore PDB snapshot after early abort for atomic group", "nodeGroupId", ngID)
		}

		for idx, name := range ngNodes {
			if idx == j && unremovable != nil {
				p.unremovableNodes.AddTimeout(unremovable, unremovableTimeout)
			} else {
				nodeInfo, err := p.autoscalingCtx.ClusterSnapshot.GetNodeInfo(name)
				if err == nil && nodeInfo != nil && nodeInfo.Node() != nil {
					p.unremovableNodes.AddTimeout(&simulator.UnremovableNode{
						Node:   nodeInfo.Node(),
						Reason: simulator.AtomicScaleDownFailed,
					}, unremovableTimeout)
				}
			}
		}
		return nil, len(ngNodes), nil, false
	}

	logger.V(2).Info("Atomic node group with some nodes successfully simulated for removal.", "nodeGroupId", ngID, "removableNodesCountForAtomicNodeGroup", len(groupRemovable))
	return groupRemovable, 0, nil, false
}

// isNodeAtomicScaleDown checks if the node would be considered for atomic scale down.
func (p *Planner) isNodeAtomicScaleDown(ctx context.Context, node *apiv1.Node) bool {
	_, isAtomic := atomic.IsAtomicNodeGroup(ctx, p.autoscalingCtx, node)
	return isAtomic
}

// unneededNodesLimit returns the number of nodes after which calculating more
// unneeded nodes is a waste of time. The reasoning behind it is essentially as
// follows.
// If the nodes are being removed instantly, then during each iteration we're
// going to delete up to MaxScaleDownParallelism nodes. Therefore, it doesn't
// really make sense to add more unneeded nodes than that.
// Let N = MaxScaleDownParallelism. When there are no unneeded nodes, we only
// need to find N of them in the first iteration. Once the unneeded time
// accumulates for them, only up to N will get deleted in a single iteration.
// When there are >0 unneeded nodes, we only need to add N more: once the first
// N will be deleted, we'll need another iteration for the next N nodes to get
// deleted.
// Of course, a node may stop being unneeded at any given time. To prevent
// slowdown stemming from having too little unneeded nodes, we're adding an
// extra buffer of N nodes. Note that we don't have to be super precise about
// the buffer size - if it is too small, we'll simply remove less than N nodes
// in one iteration.
// Finally, we know that in practice nodes are not removed instantly,
// especially when they require draining, so incrementing the limit by N every
// loop may in practice lead the limit to increase too much after a number of
// loops. To help with that, we can put another, not incremental upper bound on
// the limit: with max unneeded time U and loop interval I, we're going to have
// up to U/I loops before a node is removed. This means that the total number
// of unneeded nodes shouldn't really exceed N*U/I - scale down will not be
// able to keep up with removing them anyway.
func (p *Planner) unneededNodesLimit() int {
	n := p.autoscalingCtx.AutoscalingOptions.MaxScaleDownParallelism
	extraBuffer := n
	limit := len(p.unneededNodes.AsList()) + n + extraBuffer
	// TODO(x13n): Use moving average instead of min.
	loopInterval := int64(p.minUpdateInterval)
	u := int64(p.autoscalingCtx.AutoscalingOptions.NodeGroupDefaults.ScaleDownUnneededTime)
	if u < loopInterval {
		u = loopInterval
	}
	upperBound := n*int(u/loopInterval) + extraBuffer
	if upperBound < limit {
		return upperBound
	}
	return limit
}

// handleUnprocessedNodes is used to track the longest time that a node is being skipped during ScaleDown
func (p *Planner) handleUnprocessedNodes(unprocessedNodeNames []string) {
	// if p.maxNodeSkipEvalTime is nil (flag is disabled) do not do anything
	if p.maxNodeSkipEvalTime == nil {
		return
	}
	p.maxNodeSkipEvalTime.Update(unprocessedNodeNames, time.Now())
}

// getKnownOwnerRef returns ownerRef that is known by CA and CA knows the logic of how this controller recreates pods.
func getKnownOwnerRef(ownerRefs []metav1.OwnerReference) *metav1.OwnerReference {
	for _, ownerRef := range ownerRefs {
		switch ownerRef.Kind {
		case "StatefulSet", "Job", "ReplicaSet", "ReplicationController":
			return &ownerRef
		}
	}
	return nil
}

func merged(a, b []string) []string {
	return append(append(make([]string, 0, len(a)+len(b)), a...), b...)
}

func asMap(strs []string) map[string]bool {
	m := make(map[string]bool, len(strs))
	for _, s := range strs {
		m[s] = true
	}
	return m
}

func nodeNames(nodes []*apiv1.Node) []string {
	names := make([]string, len(nodes))
	for i, node := range nodes {
		names[i] = node.Name
	}
	return names
}

func filterOutOngoingDeletions(ns []*apiv1.Node, deleted map[string]bool) []*apiv1.Node {
	rv := make([]*apiv1.Node, 0, len(ns))
	for _, n := range ns {
		if deleted[n.Name] {
			continue
		}
		rv = append(rv, n)
	}
	return rv
}

func sortByRisk(nodes []simulator.NodeToBeRemoved) []simulator.NodeToBeRemoved {
	riskyNodes := []simulator.NodeToBeRemoved{}
	okNodes := []simulator.NodeToBeRemoved{}
	for _, nodeToRemove := range nodes {
		if nodeToRemove.IsRisky {
			riskyNodes = append(riskyNodes, nodeToRemove)
		} else {
			okNodes = append(okNodes, nodeToRemove)
		}
	}
	return append(okNodes, riskyNodes...)
}

func timedOut(timer *time.Timer) bool {
	select {
	case <-timer.C:
		return true
	default:
		return false
	}
}
