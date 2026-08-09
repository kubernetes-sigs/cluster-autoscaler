/*
Copyright 2025 The Kubernetes Authors.

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

package context

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
)

func TestNewResourceLimiterFromAutoscalingOptions(t *testing.T) {
	testCases := []struct {
		name    string
		options config.AutoscalingOptions
		wantMin map[string]int64
		wantMax map[string]int64
	}{
		{
			name: "cores and memory only",
			options: config.AutoscalingOptions{
				MinCoresTotal:  2,
				MaxCoresTotal:  100,
				MinMemoryTotal: 1073741824,   // 1 GiB
				MaxMemoryTotal: 107374182400, // 100 GiB
			},
			wantMin: map[string]int64{
				cloudprovider.ResourceNameCores:  2,
				cloudprovider.ResourceNameMemory: 1073741824,
			},
			wantMax: map[string]int64{
				cloudprovider.ResourceNameCores:  100,
				cloudprovider.ResourceNameMemory: 107374182400,
			},
		},
		{
			name: "with GPU limits",
			options: config.AutoscalingOptions{
				MinCoresTotal:  2,
				MaxCoresTotal:  100,
				MinMemoryTotal: 1073741824,
				MaxMemoryTotal: 107374182400,
				GpuTotal: []config.GpuLimits{
					{GpuType: "nvidia-tesla-k80", Min: 1, Max: 10},
				},
			},
			wantMin: map[string]int64{
				cloudprovider.ResourceNameCores:  2,
				cloudprovider.ResourceNameMemory: 1073741824,
				"nvidia-tesla-k80":               1,
			},
			wantMax: map[string]int64{
				cloudprovider.ResourceNameCores:  100,
				cloudprovider.ResourceNameMemory: 107374182400,
				"nvidia-tesla-k80":               10,
			},
		},
		{
			name: "with DRA limits",
			options: config.AutoscalingOptions{
				MinCoresTotal:  2,
				MaxCoresTotal:  100,
				MinMemoryTotal: 1073741824,
				MaxMemoryTotal: 107374182400,
				DraTotal: []config.DraLimits{
					{Driver: "gpu.nvidia.com", DeviceAttributeName: "productName", DeviceAttributeValue: "A100", Min: 0, Max: 64},
				},
			},
			wantMin: map[string]int64{
				cloudprovider.ResourceNameCores:       2,
				cloudprovider.ResourceNameMemory:      1073741824,
				"dra:gpu.nvidia.com/productName=A100": 0, // min=0 is filtered out by NewResourceLimiter, GetMin returns 0 default
			},
			wantMax: map[string]int64{
				cloudprovider.ResourceNameCores:       100,
				cloudprovider.ResourceNameMemory:      107374182400,
				"dra:gpu.nvidia.com/productName=A100": 64,
			},
		},
		{
			name: "with DRA limits non-zero min",
			options: config.AutoscalingOptions{
				MinCoresTotal:  2,
				MaxCoresTotal:  100,
				MinMemoryTotal: 1073741824,
				MaxMemoryTotal: 107374182400,
				DraTotal: []config.DraLimits{
					{Driver: "gpu.nvidia.com", DeviceAttributeName: "productName", DeviceAttributeValue: "A100", Min: 8, Max: 64},
				},
			},
			wantMin: map[string]int64{
				cloudprovider.ResourceNameCores:       2,
				cloudprovider.ResourceNameMemory:      1073741824,
				"dra:gpu.nvidia.com/productName=A100": 8,
			},
			wantMax: map[string]int64{
				cloudprovider.ResourceNameCores:       100,
				cloudprovider.ResourceNameMemory:      107374182400,
				"dra:gpu.nvidia.com/productName=A100": 64,
			},
		},
		{
			name: "multiple DRA limits",
			options: config.AutoscalingOptions{
				MinCoresTotal:  2,
				MaxCoresTotal:  100,
				MinMemoryTotal: 1073741824,
				MaxMemoryTotal: 107374182400,
				DraTotal: []config.DraLimits{
					{Driver: "gpu.nvidia.com", DeviceAttributeName: "productName", DeviceAttributeValue: "A100", Min: 8, Max: 64},
					{Driver: "gpu.nvidia.com", DeviceAttributeName: "productName", DeviceAttributeValue: "H100", Min: 4, Max: 32},
				},
			},
			wantMin: map[string]int64{
				cloudprovider.ResourceNameCores:       2,
				cloudprovider.ResourceNameMemory:      1073741824,
				"dra:gpu.nvidia.com/productName=A100": 8,
				"dra:gpu.nvidia.com/productName=H100": 4,
			},
			wantMax: map[string]int64{
				cloudprovider.ResourceNameCores:       100,
				cloudprovider.ResourceNameMemory:      107374182400,
				"dra:gpu.nvidia.com/productName=A100": 64,
				"dra:gpu.nvidia.com/productName=H100": 32,
			},
		},
		{
			name: "GPU and DRA together",
			options: config.AutoscalingOptions{
				MinCoresTotal:  2,
				MaxCoresTotal:  100,
				MinMemoryTotal: 1073741824,
				MaxMemoryTotal: 107374182400,
				GpuTotal: []config.GpuLimits{
					{GpuType: "nvidia-tesla-k80", Min: 1, Max: 10},
				},
				DraTotal: []config.DraLimits{
					{Driver: "gpu.nvidia.com", DeviceAttributeName: "productName", DeviceAttributeValue: "A100", Min: 8, Max: 64},
				},
			},
			wantMin: map[string]int64{
				cloudprovider.ResourceNameCores:       2,
				cloudprovider.ResourceNameMemory:      1073741824,
				"nvidia-tesla-k80":                    1,
				"dra:gpu.nvidia.com/productName=A100": 8,
			},
			wantMax: map[string]int64{
				cloudprovider.ResourceNameCores:       100,
				cloudprovider.ResourceNameMemory:      107374182400,
				"nvidia-tesla-k80":                    10,
				"dra:gpu.nvidia.com/productName=A100": 64,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rl := NewResourceLimiterFromAutoscalingOptions(tc.options)

			for resource, expectedMin := range tc.wantMin {
				assert.Equal(t, expectedMin, rl.GetMin(resource), "GetMin(%s)", resource)
			}
			for resource, expectedMax := range tc.wantMax {
				assert.Equal(t, expectedMax, rl.GetMax(resource), "GetMax(%s)", resource)
			}
		})
	}
}
