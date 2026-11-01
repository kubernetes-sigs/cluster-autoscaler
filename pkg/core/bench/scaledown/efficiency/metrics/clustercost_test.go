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
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

func TestClusterCostCompute(t *testing.T) {
	testCases := []struct {
		name  string
		state ClusterState
		want  float64
	}{
		{
			name:  "empty cluster costs nothing",
			state: clusterStateWithNodes(),
			want:  0,
		},
		{
			name:  "single node",
			state: clusterStateWithNodes(nodeInfoWithLabels("n1", 2000, 4*units.GiB, instanceTypeLabel("s-class"))),
			want:  2*1 + 4*0.13,
		},
		{
			name:  "fractional cores are priced proportionally",
			state: clusterStateWithNodes(nodeInfoWithLabels("n1", 500, 0, instanceTypeLabel("s-class"))),
			want:  0.5 * 1,
		},
		{
			name: "cost of all nodes is summed",
			state: clusterStateWithNodes(
				nodeInfoWithLabels("n1", 2000, 4*units.GiB, instanceTypeLabel("s-class")),
				nodeInfoWithLabels("n2", 1000, units.GiB, instanceTypeLabel("xl-class")),
			),
			want: (2*1 + 4*0.13) + (1*8 + 1*1.06),
		},
		{
			name: "nodes with invalid instance type are skipped",
			state: clusterStateWithNodes(
				nodeInfoWithLabels("n1", 2000, 4*units.GiB, instanceTypeLabel("s-class")),
				nodeInfoWithLabels("n2", 1000, units.GiB, map[string]string{}),
				nodeInfoWithLabels("n3", 1000, units.GiB, instanceTypeLabel("zz-class")),
			),
			want: 2*1 + 4*0.13,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewClusterCostMetric().Compute(tc.state)
			assert.NoError(t, err)
			assert.InDelta(t, tc.want, got, 1e-9)
		})
	}
}

func TestClusterCostSummarize(t *testing.T) {
	testCases := []struct {
		name   string
		values []float64
		want   map[string]float64
	}{
		{
			name:   "cost decreased",
			values: []float64{10, 9, 7.5},
			want:   map[string]float64{"cluster_cost_init": 10, "cluster_cost_final": 7.5, "cluster_cost_diff": -2.5, "cluster_cost_%_diff": -25},
		},
		{
			name:   "cost increased",
			values: []float64{10, 12},
			want:   map[string]float64{"cluster_cost_init": 10, "cluster_cost_final": 12, "cluster_cost_diff": 2, "cluster_cost_%_diff": 20},
		},
		{
			name:   "cost unchanged",
			values: []float64{10},
			want:   map[string]float64{"cluster_cost_init": 10, "cluster_cost_final": 10, "cluster_cost_diff": 0, "cluster_cost_%_diff": 0},
		},
		{
			name:   "zero initial cost omits percentage diff",
			values: []float64{0, 5},
			want:   map[string]float64{"cluster_cost_init": 0, "cluster_cost_final": 5, "cluster_cost_diff": 5},
		},
		{
			name:   "non-zero initial cost and zero final cost yield 100% diff",
			values: []float64{40, 0},
			want:   map[string]float64{"cluster_cost_init": 40, "cluster_cost_final": 0, "cluster_cost_diff": -40, "cluster_cost_%_diff": -100},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := NewClusterCostMetric().Summarize(tc.values)
			// Reported keys must match exactly, so that omitted keys are detected.
			assert.Len(t, got, len(tc.want))
			for key := range got {
				assert.Contains(t, tc.want, key)
			}
			for key, want := range tc.want {
				assert.InDelta(t, want, got[key], 1e-9, key)
			}
		})
	}
}
