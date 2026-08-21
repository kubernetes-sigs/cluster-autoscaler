/*
Copyright 2017 The Kubernetes Authors.

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

package utils

import (
	"errors"
	"io/ioutil"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	apiv1 "k8s.io/api/core/v1"
	kube_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate/api"

	"github.com/stretchr/testify/assert"
)

type testInfo struct {
	client       *fake.Clientset
	configMap    *apiv1.ConfigMap
	namespace    string
	getError     error
	getCalled    bool
	updateCalled bool
	createCalled bool
	t            *testing.T
}

func setUpTest(t *testing.T) *testInfo {
	namespace := "kube-system"
	result := testInfo{
		client: &fake.Clientset{},
		configMap: &apiv1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "my-cool-configmap",
			},
			Data: map[string]string{},
		},
		namespace:    namespace,
		getCalled:    false,
		updateCalled: false,
		createCalled: false,
		t:            t,
	}
	result.client.Fake.AddReactor("get", "configmaps", func(action core.Action) (bool, runtime.Object, error) {
		get := action.(core.GetAction)
		assert.Equal(result.t, namespace, get.GetNamespace())
		assert.Equal(result.t, "my-cool-configmap", get.GetName())
		result.getCalled = true
		if result.getError != nil {
			return true, nil, result.getError
		}
		return true, result.configMap, nil
	})
	result.client.Fake.AddReactor("update", "configmaps", func(action core.Action) (bool, runtime.Object, error) {
		update := action.(core.UpdateAction)
		assert.Equal(result.t, namespace, update.GetNamespace())
		result.updateCalled = true
		return true, result.configMap, nil
	})
	result.client.Fake.AddReactor("create", "configmaps", func(action core.Action) (bool, runtime.Object, error) {
		create := action.(core.CreateAction)
		assert.Equal(result.t, namespace, create.GetNamespace())
		configMap := create.GetObject().(*apiv1.ConfigMap)
		assert.Equal(result.t, "my-cool-configmap", configMap.ObjectMeta.Name)
		result.createCalled = true
		return true, configMap, nil
	})
	return &result
}

func TestWriteStatusConfigMap(t *testing.T) {
	currentTime := time.Date(2023, 12, 21, 0, 0, 0, 0, time.UTC)
	clusterAutoscalerStatus := api.ClusterAutoscalerStatus{Message: "TEST_MSG"}
	defaultConfigMapForTest := &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "my-cool-configmap",
			Namespace:   "kube-system",
			Annotations: map[string]string{ConfigMapLastUpdatedKey: "2023-12-21 00:00:00 +0000 UTC"},
		},
		Data: map[string]string{"status": "clusterWide:\n  health:\n    lastProbeTime: null\n    lastTransitionTime: null\n    nodeCounts:\n      longUnregistered: 0\n      registered:\n        notStarted: 0\n        ready: 0\n        suspended: 0\n        total: 0\n        unready:\n          resourceUnready: 0\n          total: 0\n      unregistered: 0\n  scaleDown:\n    lastProbeTime: null\n    lastTransitionTime: null\n  scaleUp:\n    lastProbeTime: null\n    lastTransitionTime: null\nmessage: TEST_MSG\ntime: 2023-12-21 00:00:00 +0000 UTC\n"},
	}
	testCases := []struct {
		name             string
		preprocessor     func(*testInfo)
		wantConfigMap    *apiv1.ConfigMap
		wantError        error
		wantGetCalled    bool
		wantUpdateCalled bool
		wantCreateCalled bool
	}{
		{
			name:             "Existing config map",
			preprocessor:     nil,
			wantConfigMap:    defaultConfigMapForTest,
			wantError:        nil,
			wantGetCalled:    true,
			wantUpdateCalled: true,
			wantCreateCalled: false,
		},
		{
			name:             "Existing config map when configmap data is empty",
			preprocessor:     func(ti *testInfo) { ti.configMap.Data = nil },
			wantConfigMap:    defaultConfigMapForTest,
			wantError:        nil,
			wantGetCalled:    true,
			wantUpdateCalled: true,
			wantCreateCalled: false,
		},
		{
			name: "Create config map",
			preprocessor: func(ti *testInfo) {
				ti.getError = kube_errors.NewNotFound(apiv1.Resource("configmap"), "nope, not found")
			},
			wantConfigMap:    defaultConfigMapForTest,
			wantError:        nil,
			wantGetCalled:    true,
			wantUpdateCalled: false,
			wantCreateCalled: true,
		},
		{
			name: "Config map with error",
			preprocessor: func(ti *testInfo) {
				ti.getError = errors.New("stuff bad")
			},
			wantConfigMap:    nil,
			wantError:        errors.New("Failed to retrieve status configmap for update: stuff bad"),
			wantGetCalled:    true,
			wantUpdateCalled: false,
			wantCreateCalled: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ti := setUpTest(t)
			if tc.preprocessor != nil {
				tc.preprocessor(ti)
			}
			result, err := WriteStatusConfigMap(ti.client, ti.namespace, clusterAutoscalerStatus, nil, "my-cool-configmap", currentTime)
			assert.Equal(t, tc.wantError, err)
			assert.Equal(t, tc.wantConfigMap, result)
			assert.Equal(t, tc.wantGetCalled, ti.getCalled)
			assert.Equal(t, tc.wantUpdateCalled, ti.updateCalled)
			assert.Equal(t, tc.wantCreateCalled, ti.createCalled)
		})
	}

}

var status api.ClusterAutoscalerStatus = api.ClusterAutoscalerStatus{
	Message:          "TEST_MSG",
	AutoscalerStatus: "Running",
	ClusterWide: api.ClusterWideStatus{
		Health: api.ClusterHealthCondition{
			Status: "Healthy",
			NodeCounts: api.NodeCount{
				Registered: api.RegisteredNodeCount{
					Total:        11,
					Ready:        4,
					NotStarted:   3,
					Suspended:    1,
					BeingDeleted: 1,
					Unready: api.RegisteredUnreadyNodeCount{
						Total:           2,
						ResourceUnready: 1,
					},
				},
				LongUnregistered: 1,
				Unregistered:     2,
			},
			LastProbeTime:      metav1.Date(2023, 11, 24, 04, 28, 19, 48988, time.UTC),
			LastTransitionTime: metav1.Date(2023, 11, 23, 14, 52, 02, 11000, time.UTC),
		},
		ScaleUp: api.ClusterScaleUpCondition{
			Status:             "NoActivity",
			LastProbeTime:      metav1.Date(2023, 11, 24, 04, 28, 19, 48988, time.UTC),
			LastTransitionTime: metav1.Date(2023, 11, 23, 14, 52, 02, 11000, time.UTC),
		},
		ScaleDown: api.ScaleDownCondition{
			Status:             "NoCandidates",
			Candidates:         2,
			LastProbeTime:      metav1.Date(2023, 11, 24, 04, 28, 19, 48988, time.UTC),
			LastTransitionTime: metav1.Date(2023, 11, 23, 14, 52, 02, 11000, time.UTC),
		},
	},
	NodeGroups: []api.NodeGroupStatus{{
		Name: "sample-node-group",
		Health: api.NodeGroupHealthCondition{
			Status: "Healthy",
			NodeCounts: api.NodeCount{
				Registered: api.RegisteredNodeCount{
					Total:        11,
					Ready:        4,
					NotStarted:   3,
					Suspended:    1,
					BeingDeleted: 1,
					Unready: api.RegisteredUnreadyNodeCount{
						Total:           2,
						ResourceUnready: 1,
					},
				},
				LongUnregistered: 1,
				Unregistered:     2,
			},
			CloudProviderTarget: 8,
			MinSize:             2,
			MaxSize:             12,
			LastProbeTime:       metav1.Date(2023, 11, 24, 04, 28, 19, 48988, time.UTC),
			LastTransitionTime:  metav1.Date(2023, 11, 23, 14, 52, 02, 11000, time.UTC),
		},
		ScaleUp: api.NodeGroupScaleUpCondition{
			Status: "Backoff",
			BackoffInfo: api.BackoffInfo{
				ErrorCode:    "QUOTA_EXCEEDED",
				ErrorMessage: "Instance 'sample-node-group-40ce0341-t28s' creation failed: Quota 'CPUS' exceeded. Limit: 57.0 in region us-central1.",
			},
			LastProbeTime:      metav1.Date(2023, 11, 24, 04, 28, 19, 48988, time.UTC),
			LastTransitionTime: metav1.Date(2023, 11, 23, 14, 52, 02, 11000, time.UTC),
		},
		ScaleDown: api.ScaleDownCondition{
			Status:             "NoCandidates",
			Candidates:         2,
			LastProbeTime:      metav1.Date(2023, 11, 24, 04, 28, 19, 48988, time.UTC),
			LastTransitionTime: metav1.Date(2023, 11, 23, 14, 52, 02, 11000, time.UTC),
		},
	}},
}

func TestWriteStatusConfigMapMarshal(t *testing.T) {
	const statusYamlTestFile = "status_test.yaml"
	ti := setUpTest(t)
	want, err := ioutil.ReadFile(statusYamlTestFile)
	if err != nil {
		t.Fatalf("Failed to Marshal %s: %v", statusYamlTestFile, err)
	}
	result, err := WriteStatusConfigMap(ti.client, ti.namespace, status, nil, "my-cool-configmap", time.Date(2023, 11, 24, 4, 28, 19, 546750398, time.UTC))
	if err != nil {
		t.Fatalf("Expected WriteStatusConfigMap not to return error, got: %v", err)
	}
	assert.YAMLEq(t, string(want), result.Data["status"])
}

func TestMarshalAndTruncateStatus(t *testing.T) {
	testNodeGroups := []api.NodeGroupStatus{
		{Name: "ng-1-healthy", Health: api.NodeGroupHealthCondition{Status: api.ClusterAutoscalerHealthy}, ScaleUp: api.NodeGroupScaleUpCondition{Status: api.ClusterAutoscalerNoActivity}, ScaleDown: api.ScaleDownCondition{Status: api.ClusterAutoscalerNoCandidates}},
		{Name: "ng-2-unhealthy", Health: api.NodeGroupHealthCondition{Status: api.ClusterAutoscalerUnhealthy}},
		{Name: "ng-3-scaleup-backoff", Health: api.NodeGroupHealthCondition{Status: api.ClusterAutoscalerHealthy}, ScaleUp: api.NodeGroupScaleUpCondition{Status: api.ClusterAutoscalerBackoff}},
		{Name: "ng-4-scaleup-inprogress", Health: api.NodeGroupHealthCondition{Status: api.ClusterAutoscalerHealthy}, ScaleUp: api.NodeGroupScaleUpCondition{Status: api.ClusterAutoscalerInProgress}},
		{Name: "ng-5-scaledown-candidates", Health: api.NodeGroupHealthCondition{Status: api.ClusterAutoscalerHealthy}, ScaleDown: api.ScaleDownCondition{Status: api.ClusterAutoscalerCandidatesPresent}},
	}

	testCases := []struct {
		name                 string
		nodeGroups           []api.NodeGroupStatus
		maxSize              int
		expectedLengths      int
		expectedNamesInOrder []string
	}{
		{
			name:                 "No truncation needed",
			nodeGroups:           testNodeGroups,
			maxSize:              100000,
			expectedLengths:      5,
			expectedNamesInOrder: []string{"ng-1-healthy", "ng-2-unhealthy", "ng-3-scaleup-backoff", "ng-4-scaleup-inprogress", "ng-5-scaledown-candidates"},
		},
		{
			name:                 "Truncation limits to 2 elements",
			nodeGroups:           testNodeGroups,
			maxSize:              1600,
			expectedLengths:      2,
			expectedNamesInOrder: []string{"ng-2-unhealthy", "ng-3-scaleup-backoff"},
		},
		{
			name:                 "Exactly one NodeGroup fits",
			nodeGroups:           testNodeGroups,
			maxSize:              1500,
			expectedLengths:      1,
			expectedNamesInOrder: []string{"ng-2-unhealthy"},
		},
		{
			name:                 "Max size too small for even 0 node groups",
			nodeGroups:           testNodeGroups,
			maxSize:              10,
			expectedLengths:      0,
			expectedNamesInOrder: []string{},
		},
		{
			name:                 "Empty NodeGroups",
			nodeGroups:           []api.NodeGroupStatus{},
			maxSize:              100000,
			expectedLengths:      0,
			expectedNamesInOrder: []string{},
		},
		{
			name: "Stable sort test: same priority elements retain original order",
			nodeGroups: []api.NodeGroupStatus{
				{Name: "ng-a-healthy", Health: api.NodeGroupHealthCondition{Status: api.ClusterAutoscalerHealthy}},
				{Name: "ng-b-healthy", Health: api.NodeGroupHealthCondition{Status: api.ClusterAutoscalerHealthy}},
				{Name: "ng-c-healthy", Health: api.NodeGroupHealthCondition{Status: api.ClusterAutoscalerHealthy}},
			},
			maxSize:              100000,
			expectedLengths:      3,
			expectedNamesInOrder: []string{"ng-a-healthy", "ng-b-healthy", "ng-c-healthy"},
		},
		{
			name: "Stable sort test with truncation",
			nodeGroups: []api.NodeGroupStatus{
				{Name: "ng-a-healthy", Health: api.NodeGroupHealthCondition{Status: api.ClusterAutoscalerHealthy}},
				{Name: "ng-b-healthy", Health: api.NodeGroupHealthCondition{Status: api.ClusterAutoscalerHealthy}},
				{Name: "ng-c-healthy", Health: api.NodeGroupHealthCondition{Status: api.ClusterAutoscalerHealthy}},
			},
			maxSize:              1500, // fits exactly 1 item based on our byte sizing logic
			expectedLengths:      1,
			expectedNamesInOrder: []string{"ng-a-healthy"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testStatus := api.ClusterAutoscalerStatus{
				NodeGroups: make([]api.NodeGroupStatus, len(tc.nodeGroups)),
			}
			copy(testStatus.NodeGroups, tc.nodeGroups)

			b, err := marshalAndTruncateStatus(testStatus, tc.maxSize)
			assert.NoError(t, err)

			// If the max size is extremely small (like 10), even 0 node groups will exceed it,
			// but the function should still return the 0-node-group YAML (which is ~430 bytes).
			// So we only enforce length checks if maxSize is reasonably large.
			if tc.maxSize >= 500 {
				assert.True(t, len(b) <= tc.maxSize, "Output YAML size must be under the limit")
			}

			var out api.ClusterAutoscalerStatus
			err = yaml.Unmarshal(b, &out)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectedLengths, len(out.NodeGroups))
			for i, expectedName := range tc.expectedNamesInOrder {
				assert.Equal(t, expectedName, out.NodeGroups[i].Name)
			}
		})
	}
}
