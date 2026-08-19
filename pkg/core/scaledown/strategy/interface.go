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
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// NodeDimensionScore represents a score given to the specific node NodeName by a single Dimension.
// Used for normalization.
type NodeDimensionScore struct {
	NodeName string
	Score    float64
}

// NodeDimensionScoreList holds scores of all nodes for specific Dimension.
// Used for normalization.
type NodeDimensionScoreList []NodeDimensionScore

// DimensionScore represents a score node received for a Dimension DimensionName.
type DimensionScore struct {
	DimensionName string
	Score         float64
}

// NodeRank holds for node Name scores across all Dimensions.
type NodeRank struct {
	Name string
	// RawScores hold Score() output before normalization for each Dimension.
	RawScores []DimensionScore
	// Scores are normalized weighted scores for each Dimension.
	Scores     []DimensionScore
	TotalScore float64
}

// ScoringDimension is the interface representing single scoring Dimension.
type ScoringDimension interface {
	// Name returns the name of the dimension.
	Name() string
	// Score returns the raw score for a specific node in this Dimension.
	Score(nodeInfo *framework.NodeInfo) (float64, error)
}

// DimensionNormalizer is an optional extension interface to normalize raw scores into allowed range.
type DimensionNormalizer interface {
	// Normalize modifies the given NodeDimensionScoreList in place to scale the scores at once.
	Normalize(scores NodeDimensionScoreList) error
}
