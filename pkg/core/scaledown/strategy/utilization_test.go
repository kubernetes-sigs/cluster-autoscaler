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

package strategy

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestUtilizationDimensionScore(t *testing.T) {
	testCases := []struct {
		name    string
		nodeCPU int64
		podsCPU []int64
		want    float64
		wantErr bool
	}{
		{
			name:    "empty node",
			nodeCPU: 1000,
			want:    100,
		},
		{
			name:    "quarter utilized node",
			nodeCPU: 1000,
			podsCPU: []int64{250},
			want:    75,
		},
		{
			name:    "half utilized node with multiple pods",
			nodeCPU: 1000,
			podsCPU: []int64{200, 300},
			want:    50,
		},
		{
			name:    "fully utilized node",
			nodeCPU: 1000,
			podsCPU: []int64{1000},
			want:    0,
		},
		{
			name:    "over utilized node is clamped to min score",
			nodeCPU: 1000,
			podsCPU: []int64{1500},
			want:    0,
		},
		{
			name:    "zero allocatable CPU returns error",
			nodeCPU: 0,
			wantErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := test.BuildTestNode("n1", tc.nodeCPU, 1000)
			var pods []*apiv1.Pod
			for i, cpu := range tc.podsCPU {
				pods = append(pods, test.BuildScheduledTestPod(fmt.Sprintf("p%d", i), cpu, 0, node.Name))
			}
			got, err := NewUtilizationDimension().Score(framework.NewTestNodeInfo(node, pods...))
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.InDelta(t, tc.want, got, 1e-9)
		})
	}

}
