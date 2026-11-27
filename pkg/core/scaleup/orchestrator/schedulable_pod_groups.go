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

package orchestrator

import (
	"context"
	"slices"

	"k8s.io/klog/v2"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/equivalence"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// schedulablePodGroupsProvider provides pod equivalence groups which can be scheduled on a node group.
type schedulablePodGroupsProvider interface {
	// Get returns the pod equivalence groups which can be scheduled on the node group, and whether
	// the node group is known to the provider. Node groups unknown to the provider must not be
	// considered for scale-up, e.g. as similar node groups.
	Get(ctx context.Context, nodeGroupId string) ([]estimator.PodEquivalenceGroup, bool)
}

// schedulablePodGroupsStore is a schedulablePodGroupsProvider which can also be updated, e.g. with
// node groups created during scale-up.
type schedulablePodGroupsStore interface {
	schedulablePodGroupsProvider
	// Set stores the pod equivalence groups which can be scheduled on the node group.
	Set(nodeGroup cloudprovider.NodeGroup, podGroups []estimator.PodEquivalenceGroup)
	// Delete forgets the node group.
	Delete(nodeGroupId string)
}

// staticSchedulablePodGroups is a schedulablePodGroupsStore backed by precomputed results. It
// adapts the map-based exported API of ScaleUpOrchestrator.
type staticSchedulablePodGroups map[string][]estimator.PodEquivalenceGroup

// Get implements schedulablePodGroupsProvider.
func (s staticSchedulablePodGroups) Get(_ context.Context, nodeGroupId string) ([]estimator.PodEquivalenceGroup, bool) {
	podGroups, found := s[nodeGroupId]
	return podGroups, found
}

// Set implements schedulablePodGroupsStore.
func (s staticSchedulablePodGroups) Set(nodeGroup cloudprovider.NodeGroup, podGroups []estimator.PodEquivalenceGroup) {
	s[nodeGroup.Id()] = podGroups
}

// Delete implements schedulablePodGroupsStore.
func (s staticSchedulablePodGroups) Delete(nodeGroupId string) {
	delete(s, nodeGroupId)
}

// schedulablePodGroupsCache computes schedulable pod groups for a node group the first time they are
// requested and caches the result, so that only node groups actually reached by binpacking (and
// their similar node groups) are simulated.
//
// To keep the snapshot fork caches effective, each node group is evaluated at most once, against all
// pod equivalence groups, within a single snapshot fork. Results are never partial, so similar node
// groups can be compared reliably.
//
// If the context deadline is exceeded before all pod equivalence groups were evaluated for a node
// group, it is as if nothing happened: no result is returned or cached, pod equivalence groups are
// not updated, and the node group stays unevaluated and is remembered as dropped. Dropped node groups
// are not computed again by the cache, e.g. when looked up as similar node groups, so a slow node
// group is simulated at most once during binpacking.
//
// Only eligible node groups (node groups valid for scale-up and node groups created during the
// scale-up) are computed. Other node groups, e.g. skipped ones returned by FindSimilarNodeGroups,
// are unknown to the cache.
//
// schedulablePodGroupsCache is not thread safe.
type schedulablePodGroupsCache struct {
	orchestrator         *ScaleUpOrchestrator
	podEquivalenceGroups []*equivalence.PodGroup
	// nodeInfos is shared with the scale-up logic, which adds node infos of created node groups.
	nodeInfos map[string]*framework.NodeInfo
	// nodeGroups are the eligible node groups, in binpacking order.
	nodeGroups []cloudprovider.NodeGroup
	eligible   map[string]cloudprovider.NodeGroup
	computed   map[string][]estimator.PodEquivalenceGroup
	// evaluations is the number of node groups simulated by the cache.
	evaluations int
	// dropped holds node groups whose computation was dropped because the context was done. They
	// stay unevaluated and were not considered for binpacking.
	dropped map[string]bool
}

func newSchedulablePodGroupsCache(
	orchestrator *ScaleUpOrchestrator,
	podEquivalenceGroups []*equivalence.PodGroup,
	nodeInfos map[string]*framework.NodeInfo,
	nodeGroups []cloudprovider.NodeGroup,
) *schedulablePodGroupsCache {
	eligible := make(map[string]cloudprovider.NodeGroup, len(nodeGroups))
	for _, nodeGroup := range nodeGroups {
		eligible[nodeGroup.Id()] = nodeGroup
	}
	return &schedulablePodGroupsCache{
		orchestrator:         orchestrator,
		podEquivalenceGroups: podEquivalenceGroups,
		nodeInfos:            nodeInfos,
		nodeGroups:           slices.Clone(nodeGroups),
		eligible:             eligible,
		computed:             make(map[string][]estimator.PodEquivalenceGroup, len(nodeGroups)),
		dropped:              make(map[string]bool),
	}
}

// Get implements schedulablePodGroupsProvider.
func (c *schedulablePodGroupsCache) Get(ctx context.Context, nodeGroupId string) ([]estimator.PodEquivalenceGroup, bool) {
	if podGroups, found := c.computed[nodeGroupId]; found {
		return podGroups, true
	}
	nodeGroup, found := c.eligible[nodeGroupId]
	if !found || c.dropped[nodeGroupId] {
		return nil, false
	}
	return c.compute(ctx, nodeGroup, true)
}

// computeAll computes schedulable pod groups for all eligible node groups which weren't computed
// yet, without any time limit. It is used when lazy computation is disabled.
func (c *schedulablePodGroupsCache) computeAll(ctx context.Context) {
	for _, nodeGroup := range c.nodeGroups {
		if _, found := c.computed[nodeGroup.Id()]; !found {
			c.compute(ctx, nodeGroup, false)
		}
	}
}

// compute computes and caches schedulable pod groups for the node group. If stopOnContextDone is
// set and the computation is stopped before all pod equivalence groups were evaluated, the node
// group is remembered as dropped and no result is returned.
func (c *schedulablePodGroupsCache) compute(ctx context.Context, nodeGroup cloudprovider.NodeGroup, stopOnContextDone bool) ([]estimator.PodEquivalenceGroup, bool) {
	nodeGroupId := nodeGroup.Id()
	logger := klog.FromContext(ctx)
	var podGroups []estimator.PodEquivalenceGroup
	// Node groups valid for scale-up always have node infos, so this is not expected to happen.
	if nodeInfo, found := c.nodeInfos[nodeGroupId]; !found {
		logger.Error(nil, "No node info for node group, assuming no pods can be scheduled on it", "nodeGroupId", nodeGroupId)
	} else {
		var evaluated int
		podGroups, evaluated = c.orchestrator.schedulablePodGroups(ctx, c.podEquivalenceGroups, nodeGroup, nodeInfo, stopOnContextDone)
		if evaluated < len(c.podEquivalenceGroups) {
			// The computation was stopped: drop the partial result. Pod equivalence groups were not updated.
			logger.V(1).Info("Couldn't compute schedulable pod groups in time", "nodeGroupId", nodeGroupId,
				"maxDuration", c.orchestrator.autoscalingCtx.MaxPerNodeGroupSchedulablePodGroupsDuration, "evaluatedPodGroups", evaluated, "podGroups", len(c.podEquivalenceGroups))
			c.dropped[nodeGroupId] = true
			return nil, false
		}
		c.evaluations++
	}
	c.computed[nodeGroupId] = podGroups
	return podGroups, true
}

// Set implements schedulablePodGroupsStore.
func (c *schedulablePodGroupsCache) Set(nodeGroup cloudprovider.NodeGroup, podGroups []estimator.PodEquivalenceGroup) {
	nodeGroupId := nodeGroup.Id()
	if _, found := c.eligible[nodeGroupId]; !found {
		c.nodeGroups = append(c.nodeGroups, nodeGroup)
	}
	c.eligible[nodeGroupId] = nodeGroup
	c.computed[nodeGroupId] = podGroups
	delete(c.dropped, nodeGroupId)
}

// Delete implements schedulablePodGroupsStore.
func (c *schedulablePodGroupsCache) Delete(nodeGroupId string) {
	delete(c.eligible, nodeGroupId)
	delete(c.computed, nodeGroupId)
	delete(c.dropped, nodeGroupId)
	c.nodeGroups = slices.DeleteFunc(c.nodeGroups, func(ng cloudprovider.NodeGroup) bool { return ng.Id() == nodeGroupId })
}

// schedulableOnDropped returns whether the pod equivalence group is schedulable on any node group
// which was dropped from binpacking (so scaling it up wasn't considered for the pod equivalence
// group), and whether it is schedulable only on such node groups. Node groups with a scheduling
// error, e.g. because scaling them up failed, can't help the pod equivalence group anymore, so they
// are ignored.
func (c *schedulablePodGroupsCache) schedulableOnDropped(eg *equivalence.PodGroup) (onAnyDropped, onlyOnDropped bool) {
	if !eg.Schedulable || len(eg.SchedulableGroups) == 0 {
		return false, false
	}
	onlyOnDropped = true
	for _, nodeGroupId := range eg.SchedulableGroups {
		if _, failed := eg.SchedulingErrors[nodeGroupId]; failed {
			continue
		}
		if c.dropped[nodeGroupId] {
			onAnyDropped = true
		} else {
			onlyOnDropped = false
		}
	}
	return onAnyDropped, onAnyDropped && onlyOnDropped
}

// unevaluated returns the eligible node groups for which schedulable pod groups were not computed:
// node groups not reached by binpacking first, in binpacking order, then node groups dropped from
// binpacking. Dropped node groups already exceeded their maximum duration once, so they are the
// least likely to be evaluated within the reporting budget.
func (c *schedulablePodGroupsCache) unevaluated() []cloudprovider.NodeGroup {
	var result, dropped []cloudprovider.NodeGroup
	for _, nodeGroup := range c.nodeGroups {
		if _, found := c.computed[nodeGroup.Id()]; found {
			continue
		}
		if c.dropped[nodeGroup.Id()] {
			dropped = append(dropped, nodeGroup)
		} else {
			result = append(result, nodeGroup)
		}
	}
	return append(result, dropped...)
}
