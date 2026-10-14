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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	v1beta1ac "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/applyconfiguration/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	statusProcessorFieldOwner  = "capacity-buffer-autoscaling-status-processor"
	maxConcurrentStatusUpdates = 10

	// statusUpdatesTimeout bounds the time the status updates can add to the autoscaling loop.
	// Buffers that weren't updated in time are picked up in the next loop.
	statusUpdatesTimeout = 5 * time.Second

	applyErrorLogLimit = 10
)

// NewCapacityBufferAutoscalingStatusProcessor returns a new CapacityBufferAutoscalingStatusProcessor.
func NewCapacityBufferAutoscalingStatusProcessor(kubeClient client.Client, buffersRegistry *fakepods.Registry) *CapacityBufferAutoscalingStatusProcessor {
	return &CapacityBufferAutoscalingStatusProcessor{
		kubeClient:      kubeClient,
		buffersRegistry: buffersRegistry,
	}
}

// CapacityBufferAutoscalingStatusProcessor counts the buffer replicas that fit
// on the existing nodes and applies .status.readyReplicas field to each buffer
// processed in the current autoscaler loop.
type CapacityBufferAutoscalingStatusProcessor struct {
	kubeClient      client.Client
	buffersRegistry *fakepods.Registry
}

// Process processes the status of the cluster after an autoscaling iteration.
func (p *CapacityBufferAutoscalingStatusProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, _ *clusterstate.ClusterStateRegistry, _ time.Time) error {
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
	var buffersToUpdate []*v1beta1.CapacityBuffer
	for _, buffer := range processedBuffers {
		readyReplicas := readyReplicasMap[buffer.UID]
		if currentReplicas := buffer.Status.ReadyReplicas; currentReplicas != nil && *currentReplicas == readyReplicas {
			continue
		}
		buffersToUpdate = append(buffersToUpdate, buffer)
	}
	// TODO: ideally, this part should be extracted from the main loop, for example to its separate controller.
	// Even if applies are called concurrently, and the total duration of this part is constrained
	// to statusUpdatesTimeout, it is not recommended to add synchronous API calls to the autoscaler main loop.
	p.applyReadyReplicas(ctx, buffersToUpdate, readyReplicasMap)
	return nil
}

// applyReadyReplicas concurrently applies ReadyReplicas to the given buffers, within statusUpdatesTimeout.
// Failures are only logged, as the buffers that weren't updated are retried in the next loop.
func (p *CapacityBufferAutoscalingStatusProcessor) applyReadyReplicas(ctx context.Context, buffers []*v1beta1.CapacityBuffer, readyReplicasMap map[types.UID]int32) {
	logger := klog.FromContext(ctx)
	ctx, cancel := context.WithTimeout(ctx, statusUpdatesTimeout)
	defer cancel()
	var updated, logEntries atomic.Int32
	workqueue.ParallelizeUntil(ctx, maxConcurrentStatusUpdates, len(buffers), func(i int) {
		buffer := buffers[i]
		applyCfg := readyReplicasApplyConfiguration(buffer, readyReplicasMap[buffer.UID])
		err := p.kubeClient.Status().Apply(ctx, applyCfg, client.FieldOwner(statusProcessorFieldOwner), client.ForceOwnership)
		if err != nil {
			if logEntries.Load() < applyErrorLogLimit {
				logger.Error(err, "Failed to apply ReadyReplicas to the buffer", "buffer", klog.KObj(buffer))
				logEntries.Add(1)
			}
			return
		}
		updated.Add(1)
	})
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		logger.Info("Timed out applying ReadyReplicas to all capacity buffers, they will be retried in the next loop", "updated", updated.Load(), "total", len(buffers))
	}
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

func readyReplicasApplyConfiguration(buffer *v1beta1.CapacityBuffer, readyReplicas int32) *v1beta1ac.CapacityBufferApplyConfiguration {
	return v1beta1ac.CapacityBuffer(buffer.Name, buffer.Namespace).
		WithStatus(v1beta1ac.CapacityBufferStatus().WithReadyReplicas(readyReplicas))
}
