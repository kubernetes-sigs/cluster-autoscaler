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
	var totalFree, hhi float64
	nodesFree := make(map[string]float64)
	for _, nodeInfo := range clusterState.nodeInfos {
		if nodeInfo == nil || nodeInfo.Node() == nil {
			continue
		}
		nodeName := nodeInfo.Node().Name
		res, err := resourceAllocatableRequested(m.resourceName, nodeInfo, clusterState.currentTime)
		if err != nil {
			return 0, err
		}
		freeOnNode := res.allocatable - res.utilization*res.allocatable
		// may signal overcommit, where packing may be dangerous and add to the node throttling
		if freeOnNode < 0 {
			freeOnNode = 0
		}
		nodesFree[nodeName] = freeOnNode
		totalFree += freeOnNode
	}
	frag := -1.0
	if totalFree <= 0 {
		klog.V(5).Infof("Metric %s, Total free: 0.00, HHI: N/A, Fragmentation: Undefined (cluster fully allocated)", m.Name())
		return frag, nil
	}
	for _, freeOnNode := range nodesFree {
		nodeShare := freeOnNode / totalFree
		hhi += nodeShare * nodeShare
	}
	frag = 1.0 - hhi
	klog.V(5).Infof("Metric: %s, Total free: %.2f, HHI: %.2f, Fragmentation: %.2f", m.Name(), totalFree, hhi, frag)
	return frag, nil
}

// resourceFragmentationMetric computes fragmentation for given resource of generic ResourceName using Herfindahl-Hirschman Index.
// HHI originally measures market concentration and competitiveness, here repurposed to measure consolidation of free resource in the cluster.
// If HHI is lower, it indicates more competition, if higher it indicates consolidation.
// In this context, higher values mean free resource are consolidated to fewer nodes, meaning lower fragmentation.
// Fragmentation is therefore computed as 1-HHI.
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

// Summarize for resourceFragmentationMetric has to handle edge cases (fully allocated cluster).
// For initial and final states -1 denotes fully allocated cluster - in such case, delta is reset to 0.
// If -1 < finalDelta < 0 = fragmentation decreased. If finalDelta > 0 = fragmentation increased.
func (m *resourceFragmentationMetric) Summarize(metricValues []float64) map[string]float64 {
	first := metricValues[0]
	last := metricValues[len(metricValues)-1]
	initialDelta := first
	finalDelta := last
	if initialDelta == -1.0 {
		initialDelta = 0.0
	}
	if finalDelta == -1.0 {
		finalDelta = 0.0
	}
	fragDelta := finalDelta - initialDelta
	r := map[string]float64{
		fmt.Sprintf("%s_init", m.Name()):  first,
		fmt.Sprintf("%s_final", m.Name()): last,
		fmt.Sprintf("%s_diff", m.Name()):  fragDelta,
	}
	return r
}
