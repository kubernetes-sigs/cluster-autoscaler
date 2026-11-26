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

func TestNodeCountCompute(t *testing.T) {
	testCases := []struct {
		name  string
		state ClusterState
		want  float64
	}{
		{
			name:  "empty cluster",
			state: clusterStateWithNodes(),
			want:  0,
		},
		{
			name:  "single node",
			state: clusterStateWithNodes(nodeInfoWithRequests("n1", 1000, units.GiB, 0, 0)),
			want:  1,
		},
		{
			name: "multiple nodes",
			state: clusterStateWithNodes(
				nodeInfoWithRequests("n1", 1000, units.GiB, 0, 0),
				nodeInfoWithRequests("n2", 1000, units.GiB, 0, 0),
				nodeInfoWithRequests("n3", 1000, units.GiB, 0, 0),
			),
			want: 3,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewNodeCountMetric().Compute(tc.state)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestNodeCountSummarize(t *testing.T) {
	testCases := []struct {
		name   string
		values []float64
		want   map[string]float64
	}{
		{
			name:   "nodes removed",
			values: []float64{5, 4, 3},
			want:   map[string]float64{"node_count_init": 5, "node_count_final": 3, "node_count_diff": -2},
		},
		{
			name:   "nodes added",
			values: []float64{3, 4},
			want:   map[string]float64{"node_count_init": 3, "node_count_final": 4, "node_count_diff": 1},
		},
		{
			name:   "node count unchanged",
			values: []float64{3},
			want:   map[string]float64{"node_count_init": 3, "node_count_final": 3, "node_count_diff": 0},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, NewNodeCountMetric().Summarize(tc.values))
		})
	}
}
