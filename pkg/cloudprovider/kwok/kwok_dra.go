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
	"fmt"
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// nodeResourceSlices returns the node-local ResourceSlices exposed by the given node, based on the
// NGDRADriverAnnotation and NGDRADeviceCountAnnotation annotations copied from its template node.
// KWOK nodes have no kubelet, so no DRA driver can publish ResourceSlices for them. Instead, the kwok
// provider simulates the driver. It returns nil if the node doesn't declare any DRA devices.
func nodeResourceSlices(node *apiv1.Node) ([]*resourceapi.ResourceSlice, error) {
	driver := node.Annotations[NGDRADriverAnnotation]
	deviceCount := node.Annotations[NGDRADeviceCountAnnotation]
	if driver == "" && deviceCount == "" {
		return nil, nil
	}
	if driver == "" || deviceCount == "" {
		return nil, fmt.Errorf("node '%s' must set both '%s' and '%s' annotations", node.Name, NGDRADriverAnnotation, NGDRADeviceCountAnnotation)
	}
	count, err := strconv.Atoi(deviceCount)
	if err != nil || count <= 0 {
		return nil, fmt.Errorf("annotation '%s' on node '%s' must be a positive integer, got '%s'", NGDRADeviceCountAnnotation, node.Name, deviceCount)
	}

	devices := make([]resourceapi.Device, 0, count)
	for i := range count {
		devices = append(devices, resourceapi.Device{Name: fmt.Sprintf("device-%d", i)})
	}
	nodeName := node.Name
	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%s", node.Name, driver),
		},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   driver,
			NodeName: &nodeName,
			Pool: resourceapi.ResourcePool{
				Name:               node.Name,
				ResourceSliceCount: 1,
			},
			Devices: devices,
		},
	}
	return []*resourceapi.ResourceSlice{slice}, nil
}

// createResourceSlices publishes the ResourceSlices for a newly created node. The slices are owned
// by the node, so they are garbage collected together with it.
func (nodeGroup *NodeGroup) createResourceSlices(node *apiv1.Node) error {
	if node == nil {
		return nil
	}
	slices, err := nodeResourceSlices(node)
	if err != nil {
		return err
	}
	for _, slice := range slices {
		slice.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       node.Name,
				UID:        node.UID,
			},
		}
		_, err := nodeGroup.kubeClient.ResourceV1().ResourceSlices().Create(context.Background(), slice, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}
