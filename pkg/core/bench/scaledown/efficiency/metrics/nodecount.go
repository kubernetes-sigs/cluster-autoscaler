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

import "fmt"

// nodeCountMetric counts number of nodes in the cluster snapshot.
type nodeCountMetric struct{}

// NewNodeCountMetric creates a new instance of nodeCountMetric.
func NewNodeCountMetric() *nodeCountMetric {
	return &nodeCountMetric{}
}

func (m *nodeCountMetric) Name() string {
	return "node_count"
}

func (m *nodeCountMetric) Compute(clusterState ClusterState) (float64, error) {
	return float64(len(clusterState.nodeInfos)), nil
}

// Summarize reports diff < 0 if total number of nodes decreased. If total number of nodes increased, diff > 0.
func (m *nodeCountMetric) Summarize(metricValues []float64) map[string]float64 {
	first := metricValues[0]
	last := metricValues[len(metricValues)-1]
	nodesDiff := last - first
	r := map[string]float64{
		fmt.Sprintf("%s_init", m.Name()):  first,
		fmt.Sprintf("%s_final", m.Name()): last,
		fmt.Sprintf("%s_diff", m.Name()):  nodesDiff,
	}
	return r
}
