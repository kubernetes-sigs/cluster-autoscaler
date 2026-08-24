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

	"k8s.io/klog/v2"
)

// clusterCostMetric computes cluster cost for every loop.
type clusterCostMetric struct{}

// NewClusterCostMetric creates a new instance of nodeCostMetric.
func NewClusterCostMetric() *clusterCostMetric {
	return &clusterCostMetric{}
}

func (m *clusterCostMetric) Name() string {
	return "cluster_cost"
}

func (m *clusterCostMetric) Compute(clusterState ClusterState) (float64, error) {
	var totalClusterCost float64
	for _, nodeInfo := range clusterState.nodeInfos {
		if nodeInfo == nil || nodeInfo.Node() == nil {
			continue
		}
		cost, err := calculateNodeCost(nodeInfo.Node())
		if err == nil {
			totalClusterCost += cost
		}
	}
	klog.V(5).Infof("Metric: %s, Total hourly: $%.5f", m.Name(), totalClusterCost)
	return totalClusterCost, nil
}

// Summarize reports diff < 0 if total cost decreased. If total cost increased, diff > 0.
func (m *clusterCostMetric) Summarize(metricValues []float64) map[string]float64 {
	first := metricValues[0]
	last := metricValues[len(metricValues)-1]
	costDiff := last - first
	costDiffPerc := 0.0
	if first > 0 {
		costDiffPerc = (costDiff / first) * 100
	}
	r := map[string]float64{
		fmt.Sprintf("%s_init", m.Name()):    first,
		fmt.Sprintf("%s_final", m.Name()):   last,
		fmt.Sprintf("%s_diff", m.Name()):    costDiff,
		fmt.Sprintf("%s_%%_diff", m.Name()): costDiffPerc,
	}
	return r
}
