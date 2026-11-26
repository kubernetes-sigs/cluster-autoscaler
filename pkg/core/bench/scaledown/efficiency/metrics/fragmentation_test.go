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
	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

func TestResourceFragmentationCompute(t *testing.T) {
	testCases := []struct {
		name     string
		resource apiv1.ResourceName
		state    ClusterState
		want     float64
		wantErr  bool
	}{
		{
			name:     "empty cluster is not fragmented",
			resource: apiv1.ResourceCPU,
			state:    clusterStateWithNodes(),
			want:     0,
		},
		{
			name:     "fully allocated cluster is not fragmented",
			resource: apiv1.ResourceCPU,
			state: clusterStateWithNodes(
				nodeInfoWithRequests("n1", 1000, units.GiB, 1000, 0),
				nodeInfoWithRequests("n2", 1000, units.GiB, 1000, 0),
			),
			want: 0,
		},
		{
			name:     "single node cluster with unallocated capacity is not fragmented",
			resource: apiv1.ResourceCPU,
			state:    clusterStateWithNodes(nodeInfoWithRequests("n1", 1000, units.GiB, 500, 0)),
			want:     0,
		},
		{
			name:     "all unallocated capacity on a single node is not fragmented",
			resource: apiv1.ResourceCPU,
			state: clusterStateWithNodes(
				nodeInfoWithRequests("n1", 1000, units.GiB, 1000, 0),
				nodeInfoWithRequests("n2", 1000, units.GiB, 500, 0),
			),
			want: 0,
		},
		{
			name:     "overcommitted node counts as having no unallocated capacity",
			resource: apiv1.ResourceCPU,
			state: clusterStateWithNodes(
				nodeInfoWithRequests("n1", 1000, units.GiB, 1500, 0),
				nodeInfoWithRequests("n2", 1000, units.GiB, 0, 0),
			),
			want: 0,
		},
		{
			name:     "unallocated capacity spread evenly over 2 nodes reaches maximum 1-1/2",
			resource: apiv1.ResourceCPU,
			state: clusterStateWithNodes(
				nodeInfoWithRequests("n1", 1000, units.GiB, 500, 0),
				nodeInfoWithRequests("n2", 1000, units.GiB, 500, 0),
			),
			want: 0.5,
		},
		{
			name:     "unallocated capacity spread evenly over 4 nodes reaches maximum 1-1/4",
			resource: apiv1.ResourceCPU,
			state: clusterStateWithNodes(
				nodeInfoWithRequests("n1", 1000, units.GiB, 750, 0),
				nodeInfoWithRequests("n2", 1000, units.GiB, 750, 0),
				nodeInfoWithRequests("n3", 1000, units.GiB, 750, 0),
				nodeInfoWithRequests("n4", 1000, units.GiB, 750, 0),
			),
			want: 0.75,
		},
		{
			name:     "unevenly spread unallocated capacity is less fragmented than evenly spread",
			resource: apiv1.ResourceCPU,
			// Unallocated total = 1000 (900 + 100), shares 0.9 and 0.1, frag 1 - (0.81 + 0.01).
			state: clusterStateWithNodes(
				nodeInfoWithRequests("n1", 1000, units.GiB, 100, 0),
				nodeInfoWithRequests("n2", 1000, units.GiB, 900, 0),
			),
			want: 0.18,
		},
		{
			name:     "memory unallocated capacity spread evenly over 2 nodes",
			resource: apiv1.ResourceMemory,
			state: clusterStateWithNodes(
				nodeInfoWithRequests("n1", 1000, 4*units.GiB, 0, 2*units.GiB),
				nodeInfoWithRequests("n2", 1000, 4*units.GiB, 0, 2*units.GiB),
			),
			want: 0.5,
		},
		{
			name:     "node with zero allocatable resource returns error",
			resource: apiv1.ResourceCPU,
			state:    clusterStateWithNodes(nodeInfoWithRequests("n1", 0, units.GiB, 0, 0)),
			wantErr:  true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewResourceFragmentationMetric(tc.resource).Compute(tc.state)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.InDelta(t, tc.want, got, 1e-9)
		})
	}
}

func TestResourceFragmentationDependsOnNodeCount(t *testing.T) {
	m := NewResourceFragmentationMetric(apiv1.ResourceCPU)

	fourNodes, err := m.Compute(clusterStateWithNodes(
		nodeInfoWithRequests("n1", 1000, units.GiB, 750, 0),
		nodeInfoWithRequests("n2", 1000, units.GiB, 750, 0),
		nodeInfoWithRequests("n3", 1000, units.GiB, 750, 0),
		nodeInfoWithRequests("n4", 1000, units.GiB, 750, 0),
	))
	assert.NoError(t, err)

	twoNodes, err := m.Compute(clusterStateWithNodes(
		nodeInfoWithRequests("n1", 1000, units.GiB, 750, 0),
		nodeInfoWithRequests("n2", 1000, units.GiB, 750, 0),
	))
	assert.NoError(t, err)

	assert.InDelta(t, 0.75, fourNodes, 1e-9)
	assert.InDelta(t, 0.5, twoNodes, 1e-9)
}

func TestResourceFragmentationSummarize(t *testing.T) {
	testCases := []struct {
		name   string
		values []float64
		want   map[string]float64
	}{
		{
			name:   "fragmentation decreased",
			values: []float64{0.75, 0.5, 0},
			want:   map[string]float64{"cpu_fragmentation_init": 0.75, "cpu_fragmentation_final": 0, "cpu_fragmentation_diff": -0.75},
		},
		{
			name:   "fragmentation increased",
			values: []float64{0, 0.5},
			want:   map[string]float64{"cpu_fragmentation_init": 0, "cpu_fragmentation_final": 0.5, "cpu_fragmentation_diff": 0.5},
		},
		{
			name:   "fragmentation unchanged",
			values: []float64{0.5, 0.25, 0.5},
			want:   map[string]float64{"cpu_fragmentation_init": 0.5, "cpu_fragmentation_final": 0.5, "cpu_fragmentation_diff": 0},
		},
		{
			name:   "single value",
			values: []float64{0.3},
			want:   map[string]float64{"cpu_fragmentation_init": 0.3, "cpu_fragmentation_final": 0.3, "cpu_fragmentation_diff": 0},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := NewResourceFragmentationMetric(apiv1.ResourceCPU).Summarize(tc.values)
			assert.Len(t, got, len(tc.want))
			for key, want := range tc.want {
				assert.InDelta(t, want, got[key], 1e-9, key)
			}
		})
	}
}
