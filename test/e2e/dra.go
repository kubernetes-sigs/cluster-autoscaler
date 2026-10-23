//go:build e2e

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

package e2e

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient"
)

// The DRA tests run on the kind-worker-dra node group of the KWOK chart (kwok/charts/templates/configmap.yaml).
// KWOK nodes have no kubelet, so no DRA driver publishes their devices. Instead, PublishDRAResourceSlices
// publishes a ResourceSlice with draDevicesPerNode devices of draDriverName for every kind-worker-dra node.
const (
	draNodeGroup       = "kind-worker-dra"
	draDriverName      = "gpu.example.com"
	draDevicesPerNode  = 4
	draDeviceClassName = "gpu"
	// draNodeGroupMinSize is the min-count of kind-worker-dra. Cluster Autoscaler keeps this many baseline
	// nodes (--enforce-node-group-min-size in the Makefile) and builds the node group template from them.
	draNodeGroupMinSize = 1
)

// PublishDRAResourceSlices publishes the devices of every draNodeGroup node, like the kubelet plugin of a
// real DRA driver would: one ResourceSlice per node. Nodes that already have it are skipped, so tests
// call it again after Cluster Autoscaler adds nodes.
func PublishDRAResourceSlices(ctx context.Context, client klient.Client) error {
	nodes, err := listNodeGroupNodes(ctx, client, draNodeGroup)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		err = client.Resources().Create(ctx, resourceSliceForNode(node))
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create ResourceSlice for node %s: %w", node.Name, err)
		}
	}
	return nil
}

// resourceSliceForNode returns the ResourceSlice with the devices of the given node. It is named after the
// node, so it can only be created once per node, and owned by the node, so it is deleted together with it.
func resourceSliceForNode(node corev1.Node) *resourceapi.ResourceSlice {
	devices := make([]resourceapi.Device, 0, draDevicesPerNode)
	for i := range draDevicesPerNode {
		devices = append(devices, resourceapi.Device{Name: fmt.Sprintf("device-%d", i)})
	}
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: node.Name + "-" + draDriverName,
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID},
			},
		},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   draDriverName,
			NodeName: &node.Name,
			Pool: resourceapi.ResourcePool{
				Name:               node.Name,
				ResourceSliceCount: 1,
			},
			Devices: devices,
		},
	}
}

// listNodeGroupNodes returns the nodes of the given nodeGroup.
func listNodeGroupNodes(ctx context.Context, client klient.Client, nodeGroup string) ([]corev1.Node, error) {
	nodeList := &corev1.NodeList{}
	err := client.Resources().List(ctx, nodeList)
	if err != nil {
		return nil, err
	}
	nodes := []corev1.Node{}
	for _, node := range nodeList.Items {
		if node.Labels[testCfg.NodeGroupLabelKey] != nodeGroup {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// EnsureDeviceClass creates the DeviceClass that selects the devices of draNodeGroup nodes, if it doesn't exist yet.
func EnsureDeviceClass(ctx context.Context, client klient.Client) error {
	deviceClass := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: draDeviceClassName,
		},
		Spec: resourceapi.DeviceClassSpec{
			Selectors: []resourceapi.DeviceSelector{
				{
					CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf("device.driver == %q", draDriverName),
					},
				},
			},
		},
	}
	err := client.Resources().Create(ctx, deviceClass)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// NewTestResourceClaimTemplate creates a ResourceClaimTemplate requesting the given number of devices
// from the DRA DeviceClass.
func NewTestResourceClaimTemplate(name, namespace string, devices int64) *resourceapi.ResourceClaimTemplate {
	return &resourceapi.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: resourceapi.ResourceClaimTemplateSpec{
			Spec: resourceapi.ResourceClaimSpec{
				Devices: resourceapi.DeviceClaim{
					Requests: []resourceapi.DeviceRequest{
						{
							Name: "devices",
							Exactly: &resourceapi.ExactDeviceRequest{
								DeviceClassName: draDeviceClassName,
								AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
								Count:           devices,
							},
						},
					},
				},
			},
		},
	}
}

// NewTestDRAPod creates a baseline test pod that runs on draNodeGroup and gets its devices from a
// ResourceClaim created from the given ResourceClaimTemplate.
func NewTestDRAPod(name, namespace, claimTemplateName string) *corev1.Pod {
	pod := NewTestPod(name, namespace)
	addDRAClaim(&pod.Spec, claimTemplateName)
	return pod
}

// NewTestDRAReplicaSet creates a test ReplicaSet whose pods run on draNodeGroup and each get their devices
// from a ResourceClaim created from the given ResourceClaimTemplate. If antiAffinity is set, at most one
// of its pods can run on a node.
func NewTestDRAReplicaSet(name, namespace string, replicas int32, claimTemplateName string, antiAffinity bool) *appsv1.ReplicaSet {
	rs := NewTestReplicaSet(name, namespace, replicas, 0.05, 0.02, "app", name)
	addDRAClaim(&rs.Spec.Template.Spec, claimTemplateName)
	if !antiAffinity {
		return rs
	}
	rs.Spec.Template.Spec.Affinity = &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app": name,
						},
					},
					TopologyKey: corev1.LabelHostname,
				},
			},
		},
	}
	return rs
}

// addDRAClaim makes the pod run on draNodeGroup and get its devices from a ResourceClaim created
// from the given ResourceClaimTemplate.
func addDRAClaim(spec *corev1.PodSpec, claimTemplateName string) {
	spec.NodeSelector = map[string]string{
		testCfg.NodeGroupLabelKey: draNodeGroup,
	}
	spec.ResourceClaims = []corev1.PodResourceClaim{
		{
			Name:                      "devices",
			ResourceClaimTemplateName: &claimTemplateName,
		},
	}
	spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{
		{
			Name: "devices",
		},
	}
}
