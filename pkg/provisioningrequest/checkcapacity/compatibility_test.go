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

package checkcapacity_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/checkcapacity"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/combinedstatus"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

func TestOriginalCombinedStatusConstructor(t *testing.T) {
	legacy := checkcapacity.NewCombinedStatusSet()
	current := combinedstatus.New()
	failure, _ := status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerError(errors.CloudProviderError, "resize failed"))
	for _, st := range []*status.ScaleUpStatus{failure, {Result: status.ScaleUpSuccessful}} {
		legacy.Add(st)
		current.Add(st)
	}
	assert.Equal(t, current.Result, legacy.Result)
	assert.Len(t, legacy.ScaleupErrors, 1)
	got, err := legacy.Export()
	want, wantErr := current.Export()
	assert.Equal(t, want, got)
	assert.Equal(t, wantErr, err)
}
