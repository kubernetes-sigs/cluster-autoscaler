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

package predicate

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	schedulerinterface "k8s.io/kube-scheduler/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"

	apiv1 "k8s.io/api/core/v1"
	schedulerimpl "k8s.io/kubernetes/pkg/scheduler/framework"
)

// SchedulerPluginRunner can be used to run various phases of scheduler plugins through the scheduler framework.
type SchedulerPluginRunner struct {
	fwHandle            *framework.Handle
	snapshot            clustersnapshot.ClusterSnapshot
	defaultNodeOrdering clustersnapshot.NodeOrderMapping
	parallelism         int
	verbosityOffset     int
	extenders           []schedulerinterface.Extender
}

// NewSchedulerPluginRunner builds a SchedulerPluginRunner using the extenders configured on fwHandle.
func NewSchedulerPluginRunner(fwHandle *framework.Handle, snapshot clustersnapshot.ClusterSnapshot, parallelism int, verbosityOffset int) *SchedulerPluginRunner {
	return &SchedulerPluginRunner{
		fwHandle:            fwHandle,
		snapshot:            snapshot,
		defaultNodeOrdering: clustersnapshot.NewLastIndexOrderMapping(1),
		parallelism:         parallelism,
		verbosityOffset:     verbosityOffset,
		extenders:           fwHandle.Extenders,
	}
}

// nodeFilterResult holds the result of running framework Filter plugins on a node.
type nodeFilterResult struct {
	orderIndex int
	nodeIndex  int
	nodeInfo   *framework.NodeInfo
}

// RunFiltersUntilPassingNode runs the scheduler framework PreFilter phase once, and then keeps running the Filter phase for all nodes in the cluster that match the
// opts.IsNodeAcceptable function - until a Node where the Filters pass is found. Filters are only run for matching Nodes. If no matching Node with passing Filters is found, an error is returned.
func (p *SchedulerPluginRunner) RunFiltersUntilPassingNode(pod *apiv1.Pod, opts clustersnapshot.SchedulingOptions) (*apiv1.Node, *schedulerimpl.CycleState, clustersnapshot.SchedulingError) {
	// Currently, the scheduler checks that verbosity threshold for logs is at least 4 before adding contextual data to the logger.
	// We don't want scheduler to add contextual data to the logger, as it causes performance issues in CA.
	// This line sets the verbosity threshold of scheduler's logs to match that specified by --scheduler-verbosity flag.
	loggingCtx := klog.NewContext(context.TODO(), klog.Background().V(p.verbosityOffset))

	nodeInfosList, err := p.snapshot.ListNodeInfos()
	if err != nil {
		return nil, nil, clustersnapshot.NewSchedulingInternalError(pod, fmt.Sprintf("error listing NodeInfos: %v", err))
	}

	p.fwHandle.DelegatingLister.UpdateDelegate(p.snapshot)
	defer p.fwHandle.DelegatingLister.ResetDelegate()

	state := schedulerimpl.NewCycleState()
	// Run the PreFilter phase of the framework for the Pod. This allows plugins to precompute some things (for all Nodes in the cluster at once) and
	// save them in the CycleState. During the Filter phase, plugins can retrieve the precomputes from the CycleState and use them for answering the Filter
	// for a given Node.
	preFilterResult, preFilterStatus, _ := p.fwHandle.Framework.RunPreFilterPlugins(loggingCtx, state, pod)
	if !preFilterStatus.IsSuccess() {
		// If any of the plugin PreFilter methods isn't successful, the corresponding Filter method can't be run, so the whole scheduling cycle is aborted.
		// Match that behavior here.
		return nil, nil, clustersnapshot.NewFailingPredicateError(pod, preFilterStatus.Plugin(), preFilterStatus.Reasons(), "PreFilter failed", "")
	}

	nodeOrdering := opts.NodeOrdering
	if nodeOrdering == nil {
		nodeOrdering = p.defaultNodeOrdering
	}

	nodeOrdering.Reset(nodeInfosList)

	shouldRunExtenders := p.hasInterestedFilterExtender(pod)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu           sync.Mutex
		passingNodes []nodeFilterResult
	)

	checkNode := func(i int) {
		nodeIndex := nodeOrdering.At(i)
		if nodeIndex < 0 {
			cancel()
			return
		}

		nodeInfo := nodeInfosList[nodeIndex]

		// Plugins can filter some Nodes out during the PreFilter phase, if they're sure the Nodes won't work for the Pod at that stage.
		// Filters are only run for Nodes that haven't been filtered out during the PreFilter phase. Match that behavior here - skip such Nodes.
		if !preFilterResult.AllNodes() && !preFilterResult.NodeNames.Has(nodeInfo.Node().Name) {
			return
		}

		// Nodes with the Unschedulable bit set will be rejected by one of the plugins during the Filter phase below. We can check that quickly here
		// and short-circuit to avoid running the expensive Filter phase at all in this case.
		if nodeInfo.Node().Spec.Unschedulable {
			return
		}

		// Check if the NodeInfo matches the provided filtering condition. This should be less expensive than running the Filter phase below, so
		// check this first.
		if opts.IsNodeAcceptable != nil && !opts.IsNodeAcceptable(nodeInfo) {
			return
		}

		// Run the Filter phase of the framework. Plugins retrieve the state they saved during PreFilter from CycleState, and answer whether the
		// given Pod can be scheduled on the given Node.
		filterStatus := p.fwHandle.Framework.RunFilterPlugins(loggingCtx, state, pod, nodeInfo)
		if filterStatus.IsSuccess() {
			// Filter passed for all plugins, so this pod can be scheduled on this Node.
			mu.Lock()
			passingNodes = append(passingNodes, nodeFilterResult{orderIndex: i, nodeIndex: nodeIndex, nodeInfo: nodeInfo})
			if !shouldRunExtenders {
				cancel()
			}
			mu.Unlock()
		}
		// Filter didn't pass for some plugin, so this Node won't work - move on to the next one.
	}

	workqueue.ParallelizeUntil(ctx, p.parallelism, len(nodeInfosList), checkNode, workqueue.WithChunkSize(chunkSizeFor(len(nodeInfosList), p.parallelism)))

	if len(passingNodes) == 0 {
		return nil, nil, clustersnapshot.NewNoNodesPassingPredicatesFoundError(pod)
	}

	if shouldRunExtenders {
		extenderFiltered, err := p.runExtenderFilters(pod, passingNodes)
		if err != nil {
			return nil, nil, err
		}
		passingNodes = extenderFiltered
		if len(passingNodes) == 0 {
			return nil, nil, clustersnapshot.NewNoNodesPassingPredicatesFoundError(pod)
		}
	}

	earliest := passingNodes[0]
	for _, result := range passingNodes[1:] {
		if result.orderIndex < earliest.orderIndex {
			earliest = result
		}
	}

	nodeOrdering.MarkMatch(earliest.nodeIndex)
	return earliest.nodeInfo.Node(), state, nil
}

func (p *SchedulerPluginRunner) hasInterestedFilterExtender(pod *apiv1.Pod) bool {
	for _, extender := range p.extenders {
		if extender.IsFilter() && extender.IsInterested(pod) {
			return true
		}
	}
	return false
}

// RunFiltersOnNode runs the scheduler framework PreFilter and Filter phases to check if the given pod can be scheduled on the given node.
func (p *SchedulerPluginRunner) RunFiltersOnNode(pod *apiv1.Pod, nodeName string) (*apiv1.Node, *schedulerimpl.CycleState, clustersnapshot.SchedulingError) {
	// Currently, the scheduler checks that verbosity threshold for logs is at least 4 before adding contextual data to the logger.
	// We don't want scheduler to add contextual data to the logger, as it causes performance issues in CA.
	// The .V() method creates a new logger with a verbosity of the original logger + the provided offset value.
	// klog.Background() has the verbosity of the globally initialized logger which comes from the --v flag.
	// Verbosity offset is computed as the difference between --v and  --scheduler-verbosity flag values.
	loggingCtx := klog.NewContext(context.TODO(), klog.Background().V(p.verbosityOffset))

	nodeInfo, err := p.snapshot.GetNodeInfo(nodeName)
	if err != nil {
		return nil, nil, clustersnapshot.NewSchedulingInternalError(pod, fmt.Sprintf("error obtaining NodeInfo for name %q: %v", nodeName, err))
	}

	p.fwHandle.DelegatingLister.UpdateDelegate(p.snapshot)
	defer p.fwHandle.DelegatingLister.ResetDelegate()

	state := schedulerimpl.NewCycleState()
	// Run the PreFilter phase of the framework for the Pod and check the results. See the corresponding comments in RunFiltersUntilPassingNode() for more info.
	preFilterResult, preFilterStatus, nodeFilteringPlugins := p.fwHandle.Framework.RunPreFilterPlugins(loggingCtx, state, pod)
	if !preFilterStatus.IsSuccess() {
		// nil check on preFilterStatus not required, as IsSuccess returns true for nil
		return nil, nil, clustersnapshot.NewFailingPredicateError(pod, preFilterStatus.Plugin(), preFilterStatus.Reasons(), "PreFilter failed", "")
	}
	if !preFilterResult.AllNodes() && !preFilterResult.NodeNames.Has(nodeInfo.Node().Name) {
		// in this scope, preFilterStatus is most likely nil
		return nil, nil, clustersnapshot.NewFailingPredicateError(pod, strings.Join(nodeFilteringPlugins.UnsortedList(), ", "), nil, "PreFilter filtered the Node out", "")
	}

	// Run the Filter phase of the framework for the Pod and the Node and check the results. See the corresponding comments in RunFiltersUntilPassingNode() for more info.
	filterStatus := p.fwHandle.Framework.RunFilterPlugins(loggingCtx, state, pod, nodeInfo)
	if !filterStatus.IsSuccess() {
		filterName := filterStatus.Plugin()
		filterReasons := filterStatus.Reasons()
		unexpectedErrMsg := ""
		if !filterStatus.IsRejected() {
			unexpectedErrMsg = fmt.Sprintf("unexpected filter status %q", filterStatus.Code().String())
		}
		return nil, nil, clustersnapshot.NewFailingPredicateError(pod, filterName, filterReasons, unexpectedErrMsg, p.failingFilterDebugInfo(filterName, nodeInfo))
	}

	if len(p.extenders) > 0 {
		passingNodes, schedErr := p.runExtenderFilters(pod, []nodeFilterResult{{nodeInfo: nodeInfo}})
		if schedErr != nil {
			return nil, nil, schedErr
		}
		if len(passingNodes) == 0 {
			return nil, nil, clustersnapshot.NewFailingPredicateError(pod, "ExtenderFilter", nil, "extender(s) filtered out the node", "")
		}
	}

	// PreFilter and Filter phases checked, this Pod can be scheduled on this Node.
	return nodeInfo.Node(), state, nil
}

// RunReserveOnNode runs the scheduler framework Reserve phase to update the scheduler plugins state to reflect the Pod being scheduled on the Node.
func (p *SchedulerPluginRunner) RunReserveOnNode(pod *apiv1.Pod, nodeName string, postFilterState *schedulerimpl.CycleState) error {
	p.fwHandle.DelegatingLister.UpdateDelegate(p.snapshot)
	defer p.fwHandle.DelegatingLister.ResetDelegate()

	status := p.fwHandle.Framework.RunReservePluginsReserve(context.Background(), postFilterState, pod, nodeName)
	if !status.IsSuccess() {
		return fmt.Errorf("couldn't reserve node %s for pod %s/%s: %v", nodeName, pod.Namespace, pod.Name, status.Message())
	}
	return nil
}

// runExtenderFilters calls each configured extender's Filter endpoint for the given pod and candidate nodes.
// It returns the subset of nodes that pass all extender filters.
func (p *SchedulerPluginRunner) runExtenderFilters(pod *apiv1.Pod, candidates []nodeFilterResult) ([]nodeFilterResult, clustersnapshot.SchedulingError) {
	candidateNodes := make([]schedulerinterface.NodeInfo, len(candidates))
	for i, candidate := range candidates {
		candidateNodes[i] = candidate.nodeInfo
	}

	for _, extender := range p.extenders {
		if !extender.IsFilter() || !extender.IsInterested(pod) {
			continue
		}

		filteredNodes, _, _, err := extender.Filter(pod, candidateNodes)
		if err != nil {
			if extender.IsIgnorable() {
				continue
			}
			return nil, clustersnapshot.NewFailingPredicateError(pod, "ExtenderFilter", nil,
				fmt.Sprintf("extender %q filter failed: %v", extender.Name(), err), "")
		}

		filteredNodeNames := make(map[string]bool, len(filteredNodes))
		for _, node := range filteredNodes {
			filteredNodeNames[node.Node().Name] = true
		}

		newCandidates := make([]nodeFilterResult, 0, len(filteredNodes))
		for _, candidate := range candidates {
			if filteredNodeNames[candidate.nodeInfo.Node().Name] {
				newCandidates = append(newCandidates, candidate)
			}
		}
		candidates = newCandidates
		candidateNodes = filteredNodes
		if len(candidates) == 0 {
			break
		}
	}

	return candidates, nil
}

func (p *SchedulerPluginRunner) failingFilterDebugInfo(filterName string, nodeInfo *framework.NodeInfo) string {
	infoParts := []string{fmt.Sprintf("nodeName: %q", nodeInfo.Node().Name)}

	switch filterName {
	case "TaintToleration":
		infoParts = append(infoParts, fmt.Sprintf("nodeTaints: %#v", nodeInfo.Node().Spec.Taints))
	}

	return strings.Join(infoParts, ", ")
}

// chunkSizeFor returns a chunk size for the given number of items to use for
// parallel work. The size aims to produce good CPU utilization.
// returns max(1, min(sqrt(n), n/Parallelism))
// mimics k8s.io/kubernetes/pkg/scheduler/framework/parallelize/parallelism.go
func chunkSizeFor(n, parallelism int) int {
	s := int(math.Sqrt(float64(n)))

	if r := n/parallelism + 1; s > r {
		s = r
	} else if s < 1 {
		s = 1
	}
	return s
}
