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
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// fakeDimension is a ScoringDimension returning preconfigured raw scores keyed by node name.
type fakeDimension struct {
	name   string
	scores map[string]float64
	err    error
}

func (d *fakeDimension) Name() string { return d.name }

func (d *fakeDimension) Score(nodeInfo *framework.NodeInfo) (float64, error) {
	if d.err != nil {
		return 0, d.err
	}
	return d.scores[nodeInfo.Node().Name], nil
}

func testNodeInfo(name string) *framework.NodeInfo {
	return framework.NewTestNodeInfo(test.BuildTestNode(name, 1000, 1000))
}

func TestNewRankingSession(t *testing.T) {
	s := &Strategy{}
	rs := NewRankingSession(s)
	assert.Same(t, s, rs.strategy)
	assert.NotNil(t, rs.Rankings)
	assert.Empty(t, rs.Rankings)
	assert.NotNil(t, rs.DimensionScores)
	assert.Empty(t, rs.DimensionScores)
}

func TestRankingSessionScoreNode(t *testing.T) {
	t.Run("scores all nodes in all dimensions", func(t *testing.T) {
		dimA := &fakeDimension{name: "A", scores: map[string]float64{"n1": 10, "n2": 30}}
		dimB := &fakeDimension{name: "B", scores: map[string]float64{"n1": 20, "n2": 40}}
		rs := NewRankingSession(&Strategy{Dimensions: []ScoringDimension{dimA, dimB}})
		assert.NoError(t, rs.ScoreNode(testNodeInfo("n1")))
		assert.NoError(t, rs.ScoreNode(testNodeInfo("n2")))
		assert.Equal(t, map[string]NodeDimensionScoreList{
			"A": {{NodeName: "n1", Score: 10}, {NodeName: "n2", Score: 30}},
			"B": {{NodeName: "n1", Score: 20}, {NodeName: "n2", Score: 40}},
		}, rs.DimensionScores)
		assert.Empty(t, rs.Rankings)
	})

	t.Run("dimension error is returned", func(t *testing.T) {
		failing := &fakeDimension{name: "A", err: fmt.Errorf("scoring failed")}
		rs := NewRankingSession(&Strategy{Dimensions: []ScoringDimension{failing}})
		assert.Error(t, rs.ScoreNode(testNodeInfo("n1")))
		assert.Empty(t, rs.DimensionScores)
		assert.Empty(t, rs.Rankings)
	})
}

func TestRankingSessionWeightTotal(t *testing.T) {
	newSession := func(weights map[string]float64) *RankingSession {
		return NewRankingSession(&Strategy{Weights: weights})
	}

	t.Run("no dimension scores creates no ranks", func(t *testing.T) {
		rs := newSession(map[string]float64{"A": 1})
		assert.NoError(t, rs.WeightTotal())
		assert.Empty(t, rs.Rankings)
	})

	t.Run("weights applied and summed", func(t *testing.T) {
		rs := newSession(map[string]float64{"A": 0.5, "B": 2})
		rs.DimensionScores["A"] = NodeDimensionScoreList{{NodeName: "n1", Score: 100}, {NodeName: "n2", Score: 20}}
		rs.DimensionScores["B"] = NodeDimensionScoreList{{NodeName: "n1", Score: 10}, {NodeName: "n2", Score: 30}}

		assert.NoError(t, rs.WeightTotal())
		assert.Len(t, rs.Rankings, 2)
		assert.Equal(t, 70.0, rs.Rankings["n1"].TotalScore)
		assert.Equal(t, 70.0, rs.Rankings["n2"].TotalScore)
		assert.ElementsMatch(t, []DimensionScore{{DimensionName: "A", Score: 50}, {DimensionName: "B", Score: 20}}, rs.Rankings["n1"].Scores)
		assert.ElementsMatch(t, []DimensionScore{{DimensionName: "A", Score: 10}, {DimensionName: "B", Score: 60}}, rs.Rankings["n2"].Scores)
	})

	t.Run("rank created for every scored node", func(t *testing.T) {
		rs := newSession(map[string]float64{"A": 1, "B": 1})
		rs.DimensionScores["A"] = NodeDimensionScoreList{{NodeName: "n1", Score: 10}, {NodeName: "n2", Score: 20}}
		rs.DimensionScores["B"] = NodeDimensionScoreList{{NodeName: "n1", Score: 30}, {NodeName: "n2", Score: 40}}

		assert.NoError(t, rs.WeightTotal())
		if assert.Len(t, rs.Rankings, 2) {
			assert.Equal(t, "n1", rs.Rankings["n1"].Name)
			assert.ElementsMatch(t, []DimensionScore{{DimensionName: "A", Score: 10}, {DimensionName: "B", Score: 30}}, rs.Rankings["n1"].Scores)
			assert.Equal(t, 40.0, rs.Rankings["n1"].TotalScore)
			assert.Equal(t, "n2", rs.Rankings["n2"].Name)
			assert.ElementsMatch(t, []DimensionScore{{DimensionName: "A", Score: 20}, {DimensionName: "B", Score: 40}}, rs.Rankings["n2"].Scores)
			assert.Equal(t, 60.0, rs.Rankings["n2"].TotalScore)
		}
	})
}

func TestRankingSessionWeightTotalScoreRange(t *testing.T) {
	testCases := []struct {
		name    string
		score   float64
		wantErr bool
	}{
		{name: "min boundary accepted", score: MinDimensionScore},
		{name: "max boundary accepted", score: MaxDimensionScore},
		{name: "above max returns error", score: MaxDimensionScore + 0.1, wantErr: true},
		{name: "below min returns error", score: MinDimensionScore - 0.1, wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rs := NewRankingSession(&Strategy{Weights: map[string]float64{"A": 1}})
			rs.DimensionScores["A"] = NodeDimensionScoreList{{NodeName: "n1", Score: tc.score}}

			err := rs.WeightTotal()
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.score, rs.Rankings["n1"].TotalScore)
		})
	}
}

func TestRankingSessionSort(t *testing.T) {
	rs := NewRankingSession(&Strategy{})
	rs.Rankings["low"] = &NodeRank{Name: "low", TotalScore: 10}
	rs.Rankings["high"] = &NodeRank{Name: "high", TotalScore: 90}
	rs.Rankings["tie-b"] = &NodeRank{Name: "tie-b", TotalScore: 50}
	rs.Rankings["tie-a"] = &NodeRank{Name: "tie-a", TotalScore: 50}

	testCases := []struct {
		name  string
		input []string
		want  []string
	}{
		{
			name:  "descending by total score, ties by name",
			input: []string{"low", "tie-b", "high", "tie-a"},
			want:  []string{"high", "tie-a", "tie-b", "low"},
		},
		{
			name:  "empty input",
			input: []string{},
			want:  []string{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			original := append([]string(nil), tc.input...)
			got := rs.Sort(tc.input)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, original, append([]string(nil), tc.input...), "input slice must not be modified")
		})
	}
}

func TestRankingSessionPipeline(t *testing.T) {
	fakeDim1 := &fakeDimension{name: "FakeDim1", scores: map[string]float64{"n1": 10, "n2": 50, "n3": 90}}
	fakeDim2 := &fakeDimension{name: "FakeDim2", scores: map[string]float64{"n1": 100, "n2": 0, "n3": 50}}

	rs := NewRankingSession(&Strategy{
		Dimensions: []ScoringDimension{fakeDim1, fakeDim2},
		Weights:    map[string]float64{"FakeDim1": 1, "FakeDim2": 0.5},
	})
	nodes := []string{"n1", "n2", "n3"}
	for _, n := range nodes {
		assert.NoError(t, rs.ScoreNode(testNodeInfo(n)))
	}
	assert.NoError(t, rs.Normalize())
	assert.NoError(t, rs.WeightTotal())

	// n1: 10 + 50 = 60, n2: 50 + 0 = 50, n3: 90 + 25 = 115.
	assert.InDelta(t, 60.0, rs.Rankings["n1"].TotalScore, 1e-9)
	assert.InDelta(t, 50.0, rs.Rankings["n2"].TotalScore, 1e-9)
	assert.InDelta(t, 115.0, rs.Rankings["n3"].TotalScore, 1e-9)
	assert.Equal(t, []string{"n3", "n1", "n2"}, rs.Sort(nodes))
}

func TestRankingSessionPipelineEmptyInput(t *testing.T) {
	fakeDim1 := &fakeDimension{name: "FakeDim1", scores: map[string]float64{"n1": 10}}
	fakeDim2 := &fakeDimension{name: "FakeDim2", scores: map[string]float64{"n1": 100}}

	rs := NewRankingSession(&Strategy{
		Dimensions: []ScoringDimension{fakeDim1, fakeDim2},
		Weights:    map[string]float64{"FakeDim1": 1, "FakeDim2": 0.5},
	})
	nodes := []string{}
	for _, n := range nodes {
		assert.NoError(t, rs.ScoreNode(testNodeInfo(n)))
	}
	assert.NoError(t, rs.Normalize())
	assert.NoError(t, rs.WeightTotal())

	assert.Empty(t, rs.DimensionScores)
	assert.Empty(t, rs.Rankings)
	got := rs.Sort(nodes)
	assert.NotNil(t, got)
	assert.Equal(t, []string{}, got)
}
