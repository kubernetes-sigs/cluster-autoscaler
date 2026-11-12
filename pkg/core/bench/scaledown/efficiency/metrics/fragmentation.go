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

package metrics

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

func (m *resourceFragmentationMetric) computeResourceFragmentation(clusterState ClusterState) (float64, error) {
	var unallocatedTotal, hhi float64
	unallocatedPerNode := make([]float64, 0, len(clusterState.nodeInfos))
	for _, nodeInfo := range clusterState.nodeInfos {
		res, err := resourceAllocatableRequested(m.resourceName, nodeInfo, clusterState.currentTime)
		if err != nil {
			return 0, err
		}
		unallocatedOnNode := res.allocatable - res.utilization*res.allocatable
		// may signal overcommit, where packing may be dangerous and add to the node throttling
		if unallocatedOnNode < 0 {
			unallocatedOnNode = 0
		}
		unallocatedPerNode = append(unallocatedPerNode, unallocatedOnNode)
		unallocatedTotal += unallocatedOnNode
	}
	if unallocatedTotal <= 0 {
		klog.V(5).Infof("Metric %s, Total unallocated: 0.00, HHI: N/A, Fragmentation: 0.0 (cluster fully allocated)", m.Name())
		return 0.0, nil
	}
	for _, unallocatedOnN := range unallocatedPerNode {
		nodeShare := unallocatedOnN / unallocatedTotal
		hhi += nodeShare * nodeShare
	}
	frag := 1.0 - hhi
	klog.V(5).Infof("Metric: %s, Total unallocated: %.2f, HHI: %.2f, Fragmentation: %.2f", m.Name(), unallocatedTotal, hhi, frag)
	return frag, nil
}

// resourceFragmentationMetric measures how evenly the unallocated capacity of
// one resource is spread across nodes. It is defined as 1 - HHI, where HHI is
// the Herfindahl-Hirschman Index of each node's share of the cluster's total
// unallocated capacity:
//
//	share_i       = unallocated_i / sum(unallocated)
//	fragmentation = 1 - sum(share_i^2)
//
// Overcommitted nodes count as having no unallocated capacity.
//
// The value lies in [0, 1 - 1/k], where k is the number of nodes with
// unallocated capacity. The maximum is reached when the unallocated capacity
// is spread evenly over k nodes; because it depends on k, removing nodes lowers
// the maximum even if the remaining capacity stays evenly spread.
//
// A value of 0 is ambiguous and must be read together with the
// <resource>_utilization_% metric:
//
//	0, utilization >= 100%  no unallocated capacity at all
//	0, utilization <  100%  all unallocated capacity is on a single node
//	> 0                     unallocated capacity is on two or more nodes
type resourceFragmentationMetric struct {
	resourceName apiv1.ResourceName
}

// NewResourceFragmentationMetric creates a new instance of resourceFragmentationMetric.
func NewResourceFragmentationMetric(rn apiv1.ResourceName) *resourceFragmentationMetric {
	return &resourceFragmentationMetric{resourceName: rn}
}

func (m *resourceFragmentationMetric) Name() string {
	return fmt.Sprintf("%s_fragmentation", m.resourceName.String())
}

func (m *resourceFragmentationMetric) Compute(clusterState ClusterState) (float64, error) {
	frag, err := m.computeResourceFragmentation(clusterState)
	if err != nil {
		return 0, err
	}
	return frag, nil
}

// Summarize reports the fragmentation of initial and final cluster state and their difference.
// diff < 0 means unallocated capacity became more consolidated (or became fully allocated).
// diff > 0 means unallocated capacity became more fragmented.
// diff == 0 means fragmentation is unchanged.
func (m *resourceFragmentationMetric) Summarize(metricValues []float64) map[string]float64 {
	first := metricValues[0]
	last := metricValues[len(metricValues)-1]
	diff := last - first
	r := map[string]float64{
		fmt.Sprintf("%s_init", m.Name()):  first,
		fmt.Sprintf("%s_final", m.Name()): last,
		fmt.Sprintf("%s_diff", m.Name()):  diff,
	}
	return r
}
