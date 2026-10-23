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

package nodes

import (
	"context"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/podgroup"
)

// GangScaleDownNodeProcessor filters out scale down candidates hosting pods of
// gang-scheduled PodGroups.
//
// Node removal is simulated pod by pod, which cannot capture that evicting one
// member of a gang might restart the whole group, on every other node it
// spans. Until the simulation is gang-aware, such nodes are left alone.
type GangScaleDownNodeProcessor struct {
	kubeClient client.Reader
}

// NewGangScaleDownNodeProcessor returns a GangScaleDownNodeProcessor.
func NewGangScaleDownNodeProcessor(kubeClient client.Reader) *GangScaleDownNodeProcessor {
	return &GangScaleDownNodeProcessor{kubeClient: kubeClient}
}

// GetPodDestinationCandidates returns all nodes: a node hosting a gang can
// still receive pods evicted from elsewhere.
func (p *GangScaleDownNodeProcessor) GetPodDestinationCandidates(autoscalingCtx *ca_context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return nodes, nil
}

// GetScaleDownCandidates returns the nodes that host no gang pods.
func (p *GangScaleDownNodeProcessor) GetScaleDownCandidates(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	logger := klog.FromContext(ctx)
	resolver := podgroup.NewResolver(p.kubeClient)
	if !resolver.Enabled() {
		return nodes, nil
	}
	result := make([]*apiv1.Node, 0, len(nodes))
	for _, node := range nodes {
		nodeInfo, err := autoscalingCtx.ClusterSnapshot.GetNodeInfo(node.Name)
		if err != nil {
			// Nothing is known about the node's pods; leave the decision to the
			// later stages of scale-down, which handle missing node infos.
			logger.V(4).Info("Can't retrieve scale-down candidate from snapshot, not checking it for gang pods", "node", klog.KObj(node), "err", err)
			result = append(result, node)
			continue
		}
		if resolver.NodeHasGangPods(ctx, nodeInfo) {
			logger.V(4).Info("Skipping node from scale-down consideration - it hosts gang pods", "node", klog.KObj(node))
			continue
		}
		result = append(result, node)
	}
	return result, nil
}

// CleanUp is called at CA termination.
func (p *GangScaleDownNodeProcessor) CleanUp() {
}
