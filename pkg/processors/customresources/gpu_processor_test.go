/*
Copyright 2021 The Kubernetes Authors.

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

package customresources

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
)

const (
	GPULabel = "TestGPULabel/accelerator"
)

func TestFilterOutNodesWithUnreadyResources(t *testing.T) {
	start := time.Now()
	later := start.Add(10 * time.Minute)
	expectedReadiness := make(map[string]bool)
	gpuLabels := map[string]string{
		GPULabel: "nvidia-tesla-k80",
	}
	readyCondition := apiv1.NodeCondition{
		Type:               apiv1.NodeReady,
		Status:             apiv1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(later),
	}
	unreadyCondition := apiv1.NodeCondition{
		Type:               apiv1.NodeReady,
		Status:             apiv1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(later),
	}

	nodeGpuReady := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "nodeGpuReady",
			Labels:            gpuLabels,
			CreationTimestamp: metav1.NewTime(start),
		},
		Status: apiv1.NodeStatus{
			Capacity:    apiv1.ResourceList{},
			Allocatable: apiv1.ResourceList{},
			Conditions:  []apiv1.NodeCondition{readyCondition},
		},
	}
	nodeGpuReady.Status.Allocatable[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(1, resource.DecimalSI)
	nodeGpuReady.Status.Capacity[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(1, resource.DecimalSI)
	expectedReadiness[nodeGpuReady.Name] = true

	nodeGpuUnready := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "nodeGpuUnready",
			Labels:            gpuLabels,
			CreationTimestamp: metav1.NewTime(start),
		},
		Status: apiv1.NodeStatus{
			Capacity:    apiv1.ResourceList{},
			Allocatable: apiv1.ResourceList{},
			Conditions:  []apiv1.NodeCondition{readyCondition},
		},
	}
	nodeGpuUnready.Status.Allocatable[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(0, resource.DecimalSI)
	nodeGpuUnready.Status.Capacity[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(0, resource.DecimalSI)
	expectedReadiness[nodeGpuUnready.Name] = false

	nodeDirectXReady := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "nodeDirectXReady",
			Labels:            gpuLabels,
			CreationTimestamp: metav1.NewTime(start),
		},
		Status: apiv1.NodeStatus{
			Capacity:    apiv1.ResourceList{},
			Allocatable: apiv1.ResourceList{},
			Conditions:  []apiv1.NodeCondition{readyCondition},
		},
	}
	nodeDirectXReady.Status.Allocatable[gpu.ResourceDirectX] = *resource.NewQuantity(1, resource.DecimalSI)
	nodeDirectXReady.Status.Capacity[gpu.ResourceDirectX] = *resource.NewQuantity(1, resource.DecimalSI)
	expectedReadiness[nodeDirectXReady.Name] = true

	nodeDirectXUnready := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "nodeDirectXUnready",
			Labels:            gpuLabels,
			CreationTimestamp: metav1.NewTime(start),
		},
		Status: apiv1.NodeStatus{
			Capacity:    apiv1.ResourceList{},
			Allocatable: apiv1.ResourceList{},
			Conditions:  []apiv1.NodeCondition{readyCondition},
		},
	}
	nodeDirectXUnready.Status.Allocatable[gpu.ResourceDirectX] = *resource.NewQuantity(0, resource.DecimalSI)
	nodeDirectXUnready.Status.Capacity[gpu.ResourceDirectX] = *resource.NewQuantity(0, resource.DecimalSI)
	expectedReadiness[nodeDirectXUnready.Name] = false

	nodeGpuUnready2 := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "nodeGpuUnready2",
			Labels:            gpuLabels,
			CreationTimestamp: metav1.NewTime(start),
		},
		Status: apiv1.NodeStatus{
			Conditions: []apiv1.NodeCondition{readyCondition},
		},
	}
	expectedReadiness[nodeGpuUnready2.Name] = false

	nodeNoGpuReady := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "nodeNoGpuReady",
			Labels:            make(map[string]string),
			CreationTimestamp: metav1.NewTime(start),
		},
		Status: apiv1.NodeStatus{
			Conditions: []apiv1.NodeCondition{readyCondition},
		},
	}
	expectedReadiness[nodeNoGpuReady.Name] = true

	nodeNoGpuUnready := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "nodeNoGpuUnready",
			Labels:            make(map[string]string),
			CreationTimestamp: metav1.NewTime(start),
		},
		Status: apiv1.NodeStatus{
			Conditions: []apiv1.NodeCondition{unreadyCondition},
		},
	}
	expectedReadiness[nodeNoGpuUnready.Name] = false

	nodeGPUReadyDra := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "nodeGPUViaDra",
			Labels: map[string]string{
				"cloud.google.com/gke-gpu-dra-driver": "true",
			},
			CreationTimestamp: metav1.NewTime(start),
		},
		Status: apiv1.NodeStatus{
			Conditions: []apiv1.NodeCondition{readyCondition},
		},
	}
	expectedReadiness[nodeGPUReadyDra.Name] = true

	initialReadyNodes := []*apiv1.Node{
		nodeGpuReady,
		nodeGpuUnready,
		nodeGpuUnready2,
		nodeDirectXReady,
		nodeDirectXUnready,
		nodeNoGpuReady,
		nodeGPUReadyDra,
	}
	initialAllNodes := []*apiv1.Node{
		nodeGpuReady,
		nodeGpuUnready,
		nodeGpuUnready2,
		nodeDirectXReady,
		nodeDirectXUnready,
		nodeNoGpuReady,
		nodeNoGpuUnready,
		nodeGPUReadyDra,
	}

	processor := GpuCustomResourcesProcessor{}
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	autoscalingCtx := &ca_context.AutoscalingContext{CloudProvider: provider}
	newAllNodes, newReadyNodes := processor.FilterOutNodesWithUnreadyResources(context.Background(), autoscalingCtx, initialAllNodes, initialReadyNodes, nil, nil)

	foundInReady := make(map[string]bool)
	for _, node := range newReadyNodes {
		foundInReady[node.Name] = true
		assert.True(t, expectedReadiness[node.Name], fmt.Sprintf("Node %s found in ready nodes list (it shouldn't be there)", node.Name))
	}
	for nodeName, expected := range expectedReadiness {
		if expected {
			assert.True(t, foundInReady[nodeName], fmt.Sprintf("Node %s expected ready, but not found in ready nodes list", nodeName))
		}
	}
	for _, node := range newAllNodes {
		assert.Equal(t, len(node.Status.Conditions), 1)
		if expectedReadiness[node.Name] {
			assert.Equal(t, node.Status.Conditions[0].Status, apiv1.ConditionTrue, fmt.Sprintf("Unexpected ready condition value for node %s", node.Name))
		} else {
			assert.Equal(t, node.Status.Conditions[0].Status, apiv1.ConditionFalse, fmt.Sprintf("Unexpected ready condition value for node %s", node.Name))
		}
	}
}

func TestGpuProcessorGetNodeResourceTargets(t *testing.T) {
	const gpuType = "nvidia-tesla-t4"
	gpuLabels := map[string]string{GPULabel: gpuType}

	buildNode := func(name string, labels map[string]string, gpuResource apiv1.ResourceName, gpuCount int64) *apiv1.Node {
		node := &apiv1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
			Status: apiv1.NodeStatus{
				Capacity:    apiv1.ResourceList{},
				Allocatable: apiv1.ResourceList{},
			},
		}
		if gpuResource != "" {
			node.Status.Capacity[gpuResource] = *resource.NewQuantity(gpuCount, resource.DecimalSI)
			node.Status.Allocatable[gpuResource] = *resource.NewQuantity(gpuCount, resource.DecimalSI)
		}
		return node
	}

	testCases := []struct {
		name         string
		node         *apiv1.Node
		template     *apiv1.Node
		nilNodeGroup bool
		draDriver    string
		wantTargets  []CustomResourceTarget
		wantErr      bool
	}{
		{
			name:        "node without GPU label",
			node:        buildNode("no-gpu", nil, "", 0),
			wantTargets: []CustomResourceTarget{},
		},
		{
			name: "ready nvidia GPU returns both GPU type and extended resource name",
			node: buildNode("ready-nvidia", gpuLabels, gpu.ResourceNvidiaGPU, 2),
			wantTargets: []CustomResourceTarget{
				{ResourceType: gpuType, ResourceCount: 2},
				{ResourceType: gpu.ResourceNvidiaGPU, ResourceCount: 2},
			},
		},
		{
			name: "ready AMD GPU returns the matching extended resource name",
			node: buildNode("ready-amd", gpuLabels, gpu.ResourceAMDGPU, 4),
			wantTargets: []CustomResourceTarget{
				{ResourceType: gpuType, ResourceCount: 4},
				{ResourceType: gpu.ResourceAMDGPU, ResourceCount: 4},
			},
		},
		{
			name:     "unready GPU falls back to node group template",
			node:     buildNode("unready", gpuLabels, "", 0),
			template: buildNode("template", gpuLabels, gpu.ResourceNvidiaGPU, 8),
			wantTargets: []CustomResourceTarget{
				{ResourceType: gpuType, ResourceCount: 8},
				{ResourceType: gpu.ResourceNvidiaGPU, ResourceCount: 8},
			},
		},
		{
			name:        "unready GPU with template without GPUs",
			node:        buildNode("unready", gpuLabels, "", 0),
			template:    buildNode("template", gpuLabels, "", 0),
			wantTargets: []CustomResourceTarget{},
		},
		{
			name:         "unready GPU without node group returns empty targets",
			node:         buildNode("unready", gpuLabels, "", 0),
			nilNodeGroup: true,
			wantTargets:  []CustomResourceTarget{},
		},
		{
			name:        "GPU exposed via DRA is skipped",
			node:        buildNode("dra", gpuLabels, "", 0),
			draDriver:   "gpu.nvidia.com",
			wantTargets: []CustomResourceTarget{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := testprovider.NewTestCloudProviderBuilder()
			if tc.template != nil {
				builder = builder.WithMachineTemplates(map[string]*framework.NodeInfo{
					"ng1": framework.NewTestNodeInfo(tc.template),
				})
			}
			if tc.draDriver != "" {
				builder = builder.WithNodeGpuConfig(func(node *apiv1.Node) *cloudprovider.GpuConfig {
					return &cloudprovider.GpuConfig{Label: GPULabel, Type: gpuType, DraDriverName: tc.draDriver}
				})
			}
			provider := builder.Build()
			provider.AddNodeGroup("ng1", 0, 10, 1)
			var nodeGroup cloudprovider.NodeGroup
			if !tc.nilNodeGroup {
				nodeGroup = provider.GetNodeGroup("ng1")
			}
			autoscalingCtx := &ca_context.AutoscalingContext{CloudProvider: provider}
			processor := &GpuCustomResourcesProcessor{}

			targets, err := processor.GetNodeResourceTargets(t.Context(), autoscalingCtx, tc.node, nodeGroup)
			// GetNodeGpuTarget is kept for backward compatibility and is expected to
			// return the GPU type target, i.e. the first of the targets above.
			gpuTarget, gpuErr := processor.GetNodeGpuTarget(t.Context(), autoscalingCtx, tc.node, nodeGroup)
			if tc.wantErr {
				assert.Error(t, err)
				assert.Error(t, gpuErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantTargets, targets)

			wantGpuTarget := CustomResourceTarget{}
			if len(tc.wantTargets) > 0 {
				wantGpuTarget = tc.wantTargets[0]
			}
			assert.NoError(t, gpuErr)
			assert.Equal(t, wantGpuTarget, gpuTarget)
		})
	}
}
