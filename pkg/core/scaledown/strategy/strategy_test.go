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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStrategy(t *testing.T) {
	// Register a second dimension for multi-dimension case, dimension registered only within this test
	fake := &fakeDimension{name: "Fake"}
	RegisterDimension(fake)
	defer delete(registeredDimensions, "Fake")
	utilizationDim := registeredDimensions["Utilization"]

	testCases := []struct {
		name           string
		strategyStr    string
		wantDimensions []ScoringDimension
		wantWeights    map[string]float64
		wantErr        bool
	}{
		{
			name:           "empty string disables scoring",
			strategyStr:    "",
			wantDimensions: []ScoringDimension{},
			wantWeights:    map[string]float64{},
		},
		{
			name:           "whitespace only disables scoring",
			strategyStr:    "   ",
			wantDimensions: []ScoringDimension{},
			wantWeights:    map[string]float64{},
		},
		{
			name:           "single dimension",
			strategyStr:    "Utilization=1",
			wantDimensions: []ScoringDimension{utilizationDim},
			wantWeights:    map[string]float64{"Utilization": 1},
		},
		{
			name:           "whitespace around name and weight is trimmed",
			strategyStr:    " Utilization = 0.5 ",
			wantDimensions: []ScoringDimension{utilizationDim},
			wantWeights:    map[string]float64{"Utilization": 0.5},
		},
		{
			name:           "multiple dimensions",
			strategyStr:    "Utilization=0.3,Fake=2",
			wantDimensions: []ScoringDimension{utilizationDim, fake},
			wantWeights:    map[string]float64{"Utilization": 0.3, "Fake": 2},
		},
		{
			name:        "missing weight",
			strategyStr: "Utilization",
			wantErr:     true,
		},
		{
			name:        "too many equal signs",
			strategyStr: "Utilization=1=2",
			wantErr:     true,
		},
		{
			name:        "non-numeric weight",
			strategyStr: "Utilization=abc",
			wantErr:     true,
		},
		{
			name:        "negative weight",
			strategyStr: "Utilization=-1",
			wantErr:     true,
		},
		{
			name:        "zero weight",
			strategyStr: "Utilization=0",
			wantErr:     true,
		},
		{
			name:        "unknown dimension",
			strategyStr: "Unknown=1",
			wantErr:     true,
		},
		{
			name:        "trailing comma produces empty part",
			strategyStr: "Utilization=1,",
			wantErr:     true,
		},
		{
			name:        "duplicate dimension",
			strategyStr: "Utilization=1,Utilization=2",
			wantErr:     true,
		},
		{
			name:        "duplicate dimension with other dimension in between",
			strategyStr: "Utilization=1,Fake=1,Utilization=1",
			wantErr:     true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewStrategy(tc.strategyStr)
			if tc.wantErr {
				assert.Error(t, err)
				assert.Nil(t, s)
				return
			}
			assert.NoError(t, err)
			if assert.NotNil(t, s) {
				assert.Equal(t, tc.wantDimensions, s.Dimensions)
				assert.Equal(t, tc.wantWeights, s.Weights)
			}
		})
	}
}
