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
	"time"

	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/utilization"
)

func init() {
	RegisterDimension(NewUtilizationDimension())
}

// UtilizationDimension is a Strategy Dimension that scores nodes based on their CPU resource utilization.
type UtilizationDimension struct{}

// NewUtilizationDimension returns a new UtilizationDimension.
func NewUtilizationDimension() *UtilizationDimension {
	return &UtilizationDimension{}
}

// Name returns the name of the utilization dimension.
func (d *UtilizationDimension) Name() string {
	return "Utilization"
}

// Score returns the raw score for a specific Node as its CPU utilization percentage (float in range <0,1>).
func (d *UtilizationDimension) Score(nodeInfo *framework.NodeInfo) (float64, error) {
	util, err := utilization.CalculateUtilizationOfResource(nodeInfo, apiv1.ResourceCPU, true, true, time.Now())
	if err != nil {
		return 0, err
	}
	return util, nil
}

// Normalize normalizes raw scores into the allowed range and inverts the value to correctly interpret higher utilization as lower preference score
// High raw score (85) -> low normalized score (15); low raw score (05)  -> high normalized score (95)
func (d *UtilizationDimension) Normalize(scores NodeDimensionScoreList) error {
	for i := range scores {
		rawUtil := scores[i].Score * 100
		if rawUtil < 0 {
			rawUtil = 0
		}
		if rawUtil > 100 {
			rawUtil = 1
		}
		scores[i].Score = 100 - rawUtil
	}
	return nil
}
