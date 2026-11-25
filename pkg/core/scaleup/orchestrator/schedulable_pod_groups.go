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

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
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
