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

package nodegroupset

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
)

func testScaleUpInfos() ScaleUpInfos {
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup("ng1", 1, 10, 1)
	provider.AddNodeGroup("ng2", 1, 10, 2)

	return ScaleUpInfos{
		{Group: provider.GetNodeGroup("ng1"), CurrentSize: 1, NewSize: 3, MaxSize: 10},
		{Group: provider.GetNodeGroup("ng2"), CurrentSize: 2, NewSize: 4, MaxSize: 10},
	}
}

func TestScaleUpInfosString(t *testing.T) {
	assert.Equal(t, "[{ng1 1->3 (max: 10)} {ng2 2->4 (max: 10)}]", testScaleUpInfos().String())
}

func TestScaleUpInfosStringEmpty(t *testing.T) {
	assert.Equal(t, "[]", ScaleUpInfos{}.String())
}

func TestScaleUpInfosMarshalLog(t *testing.T) {
	// Structured loggers serialize the result of MarshalLog, so assert on its
	// JSON form to also cover the field names seen in JSON log output.
	marshalled, err := json.Marshal(testScaleUpInfos().MarshalLog())
	assert.NoError(t, err)

	expected := `[{"nodeGroupId":"ng1","currentSize":1,"newSize":3,"maxSize":10},` +
		`{"nodeGroupId":"ng2","currentSize":2,"newSize":4,"maxSize":10}]`
	assert.JSONEq(t, expected, string(marshalled))
}

func TestScaleUpInfosMarshalLogEmpty(t *testing.T) {
	marshalled, err := json.Marshal(ScaleUpInfos{}.MarshalLog())
	assert.NoError(t, err)
	assert.JSONEq(t, `[]`, string(marshalled))
}
