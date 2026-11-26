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
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

func TestResourceAllocatableRequested(t *testing.T) {
	nodeInfo := nodeInfoWithRequests("n1", 2000, 4*units.GiB, 500, units.GiB)

	t.Run("cpu allocatable in millicores", func(t *testing.T) {
		got, err := resourceAllocatableRequested(apiv1.ResourceCPU, nodeInfo, time.Now())
		assert.NoError(t, err)
		assert.InDelta(t, 2000, got.allocatable, 1e-9)
		assert.InDelta(t, 0.25, got.utilization, 1e-9)
	})

	t.Run("memory allocatable in bytes", func(t *testing.T) {
		got, err := resourceAllocatableRequested(apiv1.ResourceMemory, nodeInfo, time.Now())
		assert.NoError(t, err)
		assert.InDelta(t, float64(4*units.GiB), got.allocatable, 1e-9)
		assert.InDelta(t, 0.25, got.utilization, 1e-9)
	})

	t.Run("zero allocatable returns error", func(t *testing.T) {
		_, err := resourceAllocatableRequested(apiv1.ResourceCPU, nodeInfoWithRequests("n2", 0, units.GiB, 0, 0), time.Now())
		assert.Error(t, err)
	})
}

func TestResourceUtilizationCompute(t *testing.T) {
	testCases := []struct {
		name     string
		resource apiv1.ResourceName
		state    ClusterState
		want     float64
		wantErr  bool
	}{
		{
			name:     "empty cluster",
			resource: apiv1.ResourceCPU,
			state:    clusterStateWithNodes(),
			want:     0,
		},
		{
			name:     "half utilized node",
			resource: apiv1.ResourceCPU,
			state:    clusterStateWithNodes(nodeInfoWithRequests("n1", 1000, units.GiB, 500, 0)),
			want:     0.5,
		},
		{
			name:     "cluster utilization is weighted by allocatable, not an average of node utilizations",
			resource: apiv1.ResourceCPU,
			// Requested 1000 of 4000 total allocatable; average of node utilizations would be 0.5.
			state: clusterStateWithNodes(
				nodeInfoWithRequests("n1", 1000, units.GiB, 1000, 0),
				nodeInfoWithRequests("n2", 3000, units.GiB, 0, 0),
			),
			want: 0.25,
		},
		{
			name:     "memory utilization",
			resource: apiv1.ResourceMemory,
			state:    clusterStateWithNodes(nodeInfoWithRequests("n1", 1000, 4*units.GiB, 0, units.GiB)),
			want:     0.25,
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
			got, err := NewResourceUtilizationMetric(tc.resource).Compute(tc.state)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.InDelta(t, tc.want, got, 1e-9)
		})
	}
}

func TestResourceUtilizationSummarize(t *testing.T) {
	got := NewResourceUtilizationMetric(apiv1.ResourceCPU).Summarize([]float64{0.5, 0.6, 0.75})
	want := map[string]float64{
		"cpu_utilization_%_init":  50,
		"cpu_utilization_%_final": 75,
		"cpu_utilization_%_diff":  25,
	}
	assert.Len(t, got, len(want))
	for key, w := range want {
		assert.InDelta(t, w, got[key], 1e-9, key)
	}
}
