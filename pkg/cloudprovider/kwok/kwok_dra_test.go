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

package kwok

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

func TestNodeResourceSlices(t *testing.T) {
	nodeName := "dra-node"
	testCases := []struct {
		testName       string
		annotations    map[string]string
		expectedSlices []*resourceapi.ResourceSlice
		expectedErr    bool
	}{
		{
			testName:       "no DRA annotations",
			annotations:    map[string]string{},
			expectedSlices: nil,
			expectedErr:    false,
		},
		{
			testName: "driver and device count set",
			annotations: map[string]string{
				NGDRADriverAnnotation:      "gpu.example.com",
				NGDRADeviceCountAnnotation: "2",
			},
			expectedSlices: []*resourceapi.ResourceSlice{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "dra-node-gpu.example.com"},
					Spec: resourceapi.ResourceSliceSpec{
						Driver:   "gpu.example.com",
						NodeName: &nodeName,
						Pool:     resourceapi.ResourcePool{Name: "dra-node", ResourceSliceCount: 1},
						Devices:  []resourceapi.Device{{Name: "device-0"}, {Name: "device-1"}},
					},
				},
			},
			expectedErr: false,
		},
		{
			testName: "driver without device count",
			annotations: map[string]string{
				NGDRADriverAnnotation: "gpu.example.com",
			},
			expectedSlices: nil,
			expectedErr:    true,
		},
		{
			testName: "device count without driver",
			annotations: map[string]string{
				NGDRADeviceCountAnnotation: "2",
			},
			expectedSlices: nil,
			expectedErr:    true,
		},
		{
			testName: "device count is not a number",
			annotations: map[string]string{
				NGDRADriverAnnotation:      "gpu.example.com",
				NGDRADeviceCountAnnotation: "two",
			},
			expectedSlices: nil,
			expectedErr:    true,
		},
		{
			testName: "device count is zero",
			annotations: map[string]string{
				NGDRADriverAnnotation:      "gpu.example.com",
				NGDRADeviceCountAnnotation: "0",
			},
			expectedSlices: nil,
			expectedErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			node := &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName, Annotations: tc.annotations}}
			slices, err := nodeResourceSlices(node)
			assert.Equal(t, tc.expectedErr, err != nil)
			assert.Equal(t, tc.expectedSlices, slices)
		})
	}
}

func TestTemplateNodeInfoDRA(t *testing.T) {
	templateNodeName := "template-node-ng"
	testCases := []struct {
		testName       string
		annotations    map[string]string
		expectedSlices []*resourceapi.ResourceSlice
		expectedErr    bool
	}{
		{
			testName: "template node with DRA devices",
			annotations: map[string]string{
				NGDRADriverAnnotation:      "gpu.example.com",
				NGDRADeviceCountAnnotation: "4",
			},
			expectedSlices: []*resourceapi.ResourceSlice{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "template-node-ng-gpu.example.com"},
					Spec: resourceapi.ResourceSliceSpec{
						Driver:   "gpu.example.com",
						NodeName: &templateNodeName,
						Pool:     resourceapi.ResourcePool{Name: "template-node-ng", ResourceSliceCount: 1},
						Devices: []resourceapi.Device{
							{Name: "device-0"}, {Name: "device-1"}, {Name: "device-2"}, {Name: "device-3"},
						},
					},
				},
			},
			expectedErr: false,
		},
		{
			testName:       "template node without DRA devices",
			annotations:    map[string]string{},
			expectedSlices: nil,
			expectedErr:    false,
		},
		{
			testName: "template node with invalid DRA device count",
			annotations: map[string]string{
				NGDRADriverAnnotation:      "gpu.example.com",
				NGDRADeviceCountAnnotation: "zero",
			},
			expectedSlices: nil,
			expectedErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			ng := NodeGroup{
				name:       "ng",
				kubeClient: fake.NewClientset(),
				lister:     kube_util.NewTestNodeLister(nil),
				nodeTemplate: &apiv1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: templateNodeName, Annotations: tc.annotations},
				},
				minSize:    0,
				targetSize: 0,
				maxSize:    3,
			}

			ti, err := ng.TemplateNodeInfo(context.Background())
			assert.Equal(t, tc.expectedErr, err != nil)
			assert.Equal(t, tc.expectedSlices, localResourceSlices(ti))
		})
	}
}

// createdSlice summarizes a ResourceSlice created by IncreaseSize, relative to the node it was created for.
type createdSlice struct {
	driver      string
	deviceCount int
	forNode     bool
	ownedByNode bool
}

func TestIncreaseSizeResourceSlices(t *testing.T) {
	testCases := []struct {
		testName          string
		annotations       map[string]string
		expectedNodeCount int
		expectedSlices    []createdSlice
		expectedErr       bool
	}{
		{
			testName: "template node with DRA devices",
			annotations: map[string]string{
				NGDRADriverAnnotation:      "gpu.example.com",
				NGDRADeviceCountAnnotation: "4",
			},
			expectedNodeCount: 1,
			expectedSlices: []createdSlice{
				{driver: "gpu.example.com", deviceCount: 4, forNode: true, ownedByNode: true},
			},
			expectedErr: false,
		},
		{
			testName:          "template node without DRA devices",
			annotations:       map[string]string{},
			expectedNodeCount: 1,
			expectedSlices:    nil,
			expectedErr:       false,
		},
		{
			testName: "template node with invalid DRA device count",
			annotations: map[string]string{
				NGDRADriverAnnotation:      "gpu.example.com",
				NGDRADeviceCountAnnotation: "zero",
			},
			expectedNodeCount: 0,
			expectedSlices:    nil,
			expectedErr:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			fakeClient := fake.NewClientset()
			ng := NodeGroup{
				name:       "ng",
				kubeClient: fakeClient,
				lister:     kube_util.NewTestNodeLister(nil),
				nodeTemplate: &apiv1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "template-node-ng",
						UID:         types.UID("template-uid"),
						Annotations: tc.annotations,
					},
				},
				minSize:    0,
				targetSize: 0,
				maxSize:    3,
			}

			err := ng.IncreaseSize(context.Background(), 1)
			assert.Equal(t, tc.expectedErr, err != nil)

			nodes, err := fakeClient.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
			assert.NoError(t, err)
			slices, err := fakeClient.ResourceV1().ResourceSlices().List(context.Background(), metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedNodeCount, len(nodes.Items))
			assert.Equal(t, tc.expectedSlices, summarizeCreatedSlices(slices.Items, nodes.Items))
		})
	}
}

// localResourceSlices returns the ResourceSlices of the given NodeInfo, or nil if there is no NodeInfo.
func localResourceSlices(nodeInfo *framework.NodeInfo) []*resourceapi.ResourceSlice {
	if nodeInfo == nil {
		return nil
	}
	return nodeInfo.LocalResourceSlices
}

// summarizeCreatedSlices describes each slice relative to the single created node, so test cases can
// specify expectations without knowing the node's randomly generated name.
func summarizeCreatedSlices(slices []resourceapi.ResourceSlice, nodes []apiv1.Node) []createdSlice {
	if len(slices) == 0 || len(nodes) != 1 {
		return nil
	}
	node := nodes[0]
	nodeOwner := []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID}}
	var result []createdSlice
	for _, slice := range slices {
		result = append(result, createdSlice{
			driver:      slice.Spec.Driver,
			deviceCount: len(slice.Spec.Devices),
			forNode:     slice.Spec.NodeName != nil && *slice.Spec.NodeName == node.Name && slice.Spec.Pool.Name == node.Name,
			ownedByNode: assert.ObjectsAreEqual(nodeOwner, slice.OwnerReferences),
		})
	}
	return result
}
