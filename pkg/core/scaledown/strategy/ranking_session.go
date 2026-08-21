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
	"sort"

	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

const (
	// MinDimensionScore is the minimum score a Dimension is allowed to return after normalization.
	MinDimensionScore float64 = 0
	// MaxDimensionScore is the maximum score a Dimension is allowed to return after normalization.
	MaxDimensionScore float64 = 100
)

// RankingSession acts as the engine to score, normalize, weight, total, and sort nodes for one CA loop.
// State is tied to a single scaledown evaluation lifecycle.
type RankingSession struct {
	strategy *Strategy
	// Keyed by Node name.
	Rankings map[string]*NodeRank
	// Keyed by Dimension name.
	DimensionScores map[string]NodeDimensionScoreList
}

// NewRankingSession initializes a new empty RankingSession referencing Strategy.
func NewRankingSession(strategy *Strategy) *RankingSession {
	rs := &RankingSession{
		strategy:        strategy,
		Rankings:        make(map[string]*NodeRank),
		DimensionScores: make(map[string]NodeDimensionScoreList),
	}
	return rs
}

// ScoreNode evaluates a single node against all configured Dimensions.
// Both NodeRank and NodeDimensionScoreList are filled with data.
func (rs *RankingSession) ScoreNode(nodeInfo *framework.NodeInfo) error {
	nodeName := nodeInfo.Node().Name

	rank := &NodeRank{
		Name:      nodeName,
		RawScores: []DimensionScore{},
		Scores:    []DimensionScore{},
	}

	// If Dimensions are empty, no scoring happens and no structs are populated.
	for _, dim := range rs.strategy.Dimensions {
		dimName := dim.Name()
		score, err := dim.Score(nodeInfo)
		if err != nil {
			return err
		}

		// Store node raw score inside its NodeRank struct.
		rank.RawScores = append(rank.RawScores, DimensionScore{
			DimensionName: dimName,
			Score:         score,
		})

		// Store node raw score inside Dimension struct for later normalization.
		rs.DimensionScores[dimName] = append(rs.DimensionScores[dimName], NodeDimensionScore{
			NodeName: nodeName,
			Score:    score,
		})
	}
	rs.Rankings[nodeName] = rank
	return nil
}

// Normalize executes score normalization over all configured Dimensions in-place.
// Each Dimension must have its normalized score in range <0,100>
func (rs *RankingSession) Normalize() error {
	for _, dim := range rs.strategy.Dimensions {
		if normalizer, ok := dim.(DimensionNormalizer); ok {
			err := normalizer.Normalize(rs.DimensionScores[dim.Name()])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// WeightTotal applies configured weights and sums the total score for each node.
func (rs *RankingSession) WeightTotal() error {
	for dimName, scoresList := range rs.DimensionScores {
		weight := rs.strategy.Weights[dimName]
		for _, dimensionScore := range scoresList {
			rank := rs.Rankings[dimensionScore.NodeName]
			if rank != nil {
				if dimensionScore.Score > MaxDimensionScore || dimensionScore.Score < MinDimensionScore {
					return fmt.Errorf("normalized dimension %s score not in range, should be in <%1.f,%1f>", dimName, MinDimensionScore, MaxDimensionScore)
				}
				weightedScore := dimensionScore.Score * weight
				rank.Scores = append(rank.Scores, DimensionScore{
					DimensionName: dimName,
					Score:         weightedScore,
				})
				rank.TotalScore += weightedScore
			}
		}
	}
	return nil
}

// Sort returns a sorted copy of unneeded node names according to their TotalScore in descending order.
func (rs *RankingSession) Sort(nodeNames []string) []string {
	sorted := append([]string(nil), nodeNames...)
	sort.Slice(sorted, func(i, j int) bool {
		rankI := rs.Rankings[sorted[i]]
		rankJ := rs.Rankings[sorted[j]]
		var totalI, totalJ float64
		if rankI != nil {
			totalI = rankI.TotalScore
		}
		if rankJ != nil {
			totalJ = rankJ.TotalScore
		}
		if totalI == totalJ {
			return sorted[i] < sorted[j]
		}
		return totalI > totalJ
	})
	return sorted
}
