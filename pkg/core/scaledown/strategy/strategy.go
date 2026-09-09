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
	"strconv"
	"strings"
)

// registeredDimensions is a background Dimension registry.
var registeredDimensions = make(map[string]ScoringDimension)

// RegisterDimension is called by individual Dimensions to register themselves as available.
func RegisterDimension(sd ScoringDimension) {
	registeredDimensions[sd.Name()] = sd
}

// Strategy holds parsed Dimensions and weights that are applied in every CA loop.
type Strategy struct {
	Dimensions []ScoringDimension
	Weights    map[string]float64
}

// parse fails fast if any configuration is incorrect.
func (s *Strategy) parse(strategyStr string) error {
	strategyStr = strings.TrimSpace(strategyStr)
	if strategyStr == "" {
		return nil
	}

	parts := strings.SplitSeq(strategyStr, ",")
	for part := range parts {
		keyValue := strings.Split(part, "=")
		if len(keyValue) != 2 {
			return fmt.Errorf("invalid dimension and weight format '%s', expected 'dimension=score'", part)
		}
		dimName := strings.TrimSpace(keyValue[0])
		weightStr := strings.TrimSpace(keyValue[1])
		weight, err := strconv.ParseFloat(weightStr, 64)
		if err != nil {
			return fmt.Errorf("failed to parse weight '%s' for dimension '%s', err: %s", weightStr, dimName, err.Error())
		}
		if weight < 0 {
			return fmt.Errorf("weight cannot be negative for dimension '%s'", dimName)
		}
		if weight == 0 {
			return fmt.Errorf("setting weight to zero not allowed, to disable %s do not include it in the list", dimName)
		}
		d, ok := registeredDimensions[dimName]
		if !ok {
			return fmt.Errorf("unknown scoring dimension '%s' configured", dimName)
		}
		s.Dimensions = append(s.Dimensions, d)
		s.Weights[dimName] = weight
	}
	return nil
}

// NewStrategy returns new instance of Strategy and error in case of failed parsing.
func NewStrategy(strategyStr string) (*Strategy, error) {
	s := &Strategy{
		Dimensions: []ScoringDimension{},
		Weights:    make(map[string]float64),
	}
	err := s.parse(strategyStr)
	if err != nil {
		return nil, err
	}
	return s, nil
}
