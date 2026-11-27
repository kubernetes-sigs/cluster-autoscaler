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
	}
}

// Get implements schedulablePodGroupsProvider.
func (c *schedulablePodGroupsCache) Get(ctx context.Context, nodeGroupId string) ([]estimator.PodEquivalenceGroup, bool) {
	if podGroups, found := c.computed[nodeGroupId]; found {
		return podGroups, true
	}
	nodeGroup, found := c.eligible[nodeGroupId]
	if !found {
		return nil, false
	}
	return c.compute(ctx, nodeGroup)
}

// computeAll computes schedulable pod groups for all eligible node groups which weren't computed
// yet. It is used when lazy computation is disabled.
func (c *schedulablePodGroupsCache) computeAll(ctx context.Context) {
	for _, nodeGroup := range c.nodeGroups {
		if _, found := c.computed[nodeGroup.Id()]; !found {
			c.compute(ctx, nodeGroup)
		}
	}
}

// compute computes and caches schedulable pod groups for the node group.
func (c *schedulablePodGroupsCache) compute(ctx context.Context, nodeGroup cloudprovider.NodeGroup) ([]estimator.PodEquivalenceGroup, bool) {
	nodeGroupId := nodeGroup.Id()
	var podGroups []estimator.PodEquivalenceGroup
	// Node groups valid for scale-up always have node infos, so this is not expected to happen.
	if nodeInfo, found := c.nodeInfos[nodeGroupId]; !found {
		klog.FromContext(ctx).Error(nil, "No node info for node group, assuming no pods can be scheduled on it", "nodeGroupId", nodeGroupId)
	} else {
		podGroups = c.orchestrator.SchedulablePodGroups(ctx, c.podEquivalenceGroups, nodeGroup, nodeInfo)
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
}

// Delete implements schedulablePodGroupsStore.
func (c *schedulablePodGroupsCache) Delete(nodeGroupId string) {
	delete(c.eligible, nodeGroupId)
	delete(c.computed, nodeGroupId)
	c.nodeGroups = slices.DeleteFunc(c.nodeGroups, func(ng cloudprovider.NodeGroup) bool { return ng.Id() == nodeGroupId })
}

// unevaluated returns the eligible node groups for which schedulable pod groups were not computed,
// in binpacking order.
func (c *schedulablePodGroupsCache) unevaluated() []cloudprovider.NodeGroup {
	var result []cloudprovider.NodeGroup
	for _, nodeGroup := range c.nodeGroups {
		if _, found := c.computed[nodeGroup.Id()]; !found {
			result = append(result, nodeGroup)
		}
	}
	return result
}
