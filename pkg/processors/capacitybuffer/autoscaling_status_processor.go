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

package capacitybufferpodlister

import (
	"context"
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	cbctrl "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/controller"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
)

// NewCapacityBufferAutoscalingStatusProcessor returns a new CapacityBufferAutoscalingStatusProcessor.
func NewCapacityBufferAutoscalingStatusProcessor(readyReplicasController *cbctrl.ReadyReplicasController, buffersRegistry *fakepods.Registry) *CapacityBufferAutoscalingStatusProcessor {
	return &CapacityBufferAutoscalingStatusProcessor{
		readyReplicasController: readyReplicasController,
		buffersRegistry:         buffersRegistry,
	}
}

// CapacityBufferAutoscalingStatusProcessor counts the buffer replicas that fit
// on the existing nodes and enqueues .status.readyReplicas updates for each
// buffer processed in the current autoscaler loop.
type CapacityBufferAutoscalingStatusProcessor struct {
	readyReplicasController *cbctrl.ReadyReplicasController
	buffersRegistry         *fakepods.Registry
}

// Process processes the status of the cluster after an autoscaling iteration.
func (p *CapacityBufferAutoscalingStatusProcessor) Process(_ context.Context, autoscalingCtx *ca_context.AutoscalingContext, _ *clusterstate.ClusterStateRegistry, _ time.Time) error {
	// Only buffers processed by the pod list processor in the current loop are updated. If the loop
	// exited before pod list processing, the registry is empty and no buffer is updated.
	processedBuffers := p.buffersRegistry.ProcessedBuffers()
	if len(processedBuffers) == 0 {
		return nil
	}
	readyReplicasMap := make(map[types.UID]int32, len(processedBuffers))
	nodeInfos, err := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return fmt.Errorf("unable to list node infos: %w", err)
	}
	for _, nodeInfo := range nodeInfos {
		node := nodeInfo.Node()
		if isUpcomingNode(node) {
			continue
		}
		for _, podInfo := range nodeInfo.Pods() {
			if buffer := p.buffersRegistry.GetCapacityBuffer(podInfo.Pod.UID); buffer != nil {
				readyReplicasMap[buffer.UID]++
			}
		}
	}
	for _, buffer := range processedBuffers {
		p.readyReplicasController.Update(buffer, readyReplicasMap[buffer.UID])
	}
	return nil
}

// CleanUp cleans up the processor's internal structures.
func (p *CapacityBufferAutoscalingStatusProcessor) CleanUp() {}

func isUpcomingNode(node *apiv1.Node) bool {
	if node == nil {
		return false
	}
	_, isUpcoming := node.Annotations[annotations.NodeUpcomingAnnotation]
	return isUpcoming
}
