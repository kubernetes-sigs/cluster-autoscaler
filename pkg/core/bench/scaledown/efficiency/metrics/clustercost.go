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

// NewClusterCostMetric creates a new instance of clusterCostMetric.
func NewClusterCostMetric() *clusterCostMetric {
	return &clusterCostMetric{}
}

func (m *clusterCostMetric) Name() string {
	return "cluster_cost"
}

func (m *clusterCostMetric) Compute(clusterState ClusterState) (float64, error) {
	var totalClusterCost float64
	for _, nodeInfo := range clusterState.nodeInfos {
		cost, err := calculateNodeCost(nodeInfo.Node())
		if err != nil {
			klog.Infof("Metric: %s, cost of node %s excluded from total cluster cost, err: %s", m.Name(), nodeInfo.GetName(), err.Error())
			continue
		}
		totalClusterCost += cost
	}
	klog.V(5).Infof("Metric: %s, Total hourly: $%.5f", m.Name(), totalClusterCost)
	return totalClusterCost, nil
}

// Summarize reports the initial and final total cluster cost and their difference.
// diff < 0 means total cost decreased, diff > 0 means it increased.
// The percentage difference is omitted when the init cost = 0, because a relative change from 0 is undefined.
func (m *clusterCostMetric) Summarize(metricValues []float64) map[string]float64 {
	first := metricValues[0]
	last := metricValues[len(metricValues)-1]
	diff := last - first
	r := map[string]float64{
		fmt.Sprintf("%s_init", m.Name()):  first,
		fmt.Sprintf("%s_final", m.Name()): last,
		fmt.Sprintf("%s_diff", m.Name()):  diff,
	}
	if first > 0 {
		r[fmt.Sprintf("%s_%%_diff", m.Name())] = diff / first * 100
	}
	return r
}
