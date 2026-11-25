/*
Copyright 2018 The Kubernetes Authors.

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

package nodegroupset

import (
	"context"
	"fmt"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

// ScaleUpInfo contains information about planned scale-up of a single NodeGroup
type ScaleUpInfo struct {
	// Group is the group to be scaled-up
	Group cloudprovider.NodeGroup
	// CurrentSize is the current size of the Group
	CurrentSize int
	// NewSize is the size the Group will be scaled-up to
	NewSize int
	// MaxSize is the maximum allowed size of the Group
	MaxSize int
}

// String is used for printing ScaleUpInfo for logging, etc
func (s ScaleUpInfo) String() string {
	return fmt.Sprintf("{%v %v->%v (max: %v)}", s.Group.Id(), s.CurrentSize, s.NewSize, s.MaxSize)
}

// ScaleUpInfos is a loggable list of ScaleUpInfo. Loggers don't look for
// fmt.Stringer or logr.Marshaler on elements of a plain slice, so the slice has
// to be wrapped to avoid reflecting into ScaleUpInfo.Group.
type ScaleUpInfos []ScaleUpInfo

// String implements fmt.Stringer, used by loggers preferring text output.
func (s ScaleUpInfos) String() string {
	return fmt.Sprintf("%v", []ScaleUpInfo(s))
}

// MarshalLog implements logr.Marshaler, used by loggers preferring structured output.
func (s ScaleUpInfos) MarshalLog() interface{} {
	infos := make([]scaleUpInfoLog, 0, len(s))
	for _, info := range s {
		infos = append(infos, scaleUpInfoLog{
			NodeGroupID: info.Group.Id(),
			CurrentSize: info.CurrentSize,
			NewSize:     info.NewSize,
			MaxSize:     info.MaxSize,
		})
	}
	return infos
}

// scaleUpInfoLog is the structured logging representation of a single ScaleUpInfo.
type scaleUpInfoLog struct {
	NodeGroupID string `json:"nodeGroupId"`
	CurrentSize int    `json:"currentSize"`
	NewSize     int    `json:"newSize"`
	MaxSize     int    `json:"maxSize"`
}

// NodeGroupSetProcessor finds nodegroups that are similar and allows balancing scale-up between them.
type NodeGroupSetProcessor interface {
	FindSimilarNodeGroups(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup,
		nodeInfosForGroups map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, errors.AutoscalerError)

	BalanceScaleUpBetweenGroups(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]ScaleUpInfo, errors.AutoscalerError)
	CleanUp()
}

// NoOpNodeGroupSetProcessor returns no similar node groups and doesn't do any balancing.
type NoOpNodeGroupSetProcessor struct {
}

// FindSimilarNodeGroups returns a list of NodeGroups similar to the one provided in parameter.
func (n *NoOpNodeGroupSetProcessor) FindSimilarNodeGroups(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup,
	nodeInfosForGroups map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, errors.AutoscalerError) {
	return []cloudprovider.NodeGroup{}, nil
}

// BalanceScaleUpBetweenGroups splits a scale-up between provided NodeGroups.
func (n *NoOpNodeGroupSetProcessor) BalanceScaleUpBetweenGroups(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]ScaleUpInfo, errors.AutoscalerError) {
	return []ScaleUpInfo{}, nil
}

// CleanUp performs final clean up of processor state.
func (n *NoOpNodeGroupSetProcessor) CleanUp() {}

// NewDefaultNodeGroupSetProcessor creates an instance of NodeGroupSetProcessor.
func NewDefaultNodeGroupSetProcessor(ignoredLabels []string, ratioOpts config.NodeGroupDifferenceRatios) NodeGroupSetProcessor {
	return &BalancingNodeGroupSetProcessor{
		Comparator: CreateGenericNodeInfoComparator(ignoredLabels, ratioOpts),
	}
}
