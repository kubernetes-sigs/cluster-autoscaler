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

package common

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient"
)

const (
	pauseImage = "registry.k8s.io/pause:3.9"
)

// NewTestPod creates a test pod configuration using the active TestConfig.
// Defaults to modest baseline requests (500m CPU, 500Mi memory) when exact node-proportional
// sizing is not required, ensuring the pod is non-BestEffort and triggers standard scheduling.
func NewTestPod(name, namespace string) *corev1.Pod {
	return NewTestPodWithResources(name, namespace, "500m", "500Mi")
}

// NewTestPodWithResources creates a test pod configuration with custom CPU and memory requests.
func NewTestPodWithResources(name, namespace, cpu, memory string) *corev1.Pod {
	return NewTestPodWithConfig(GetTestConfig(), name, namespace, cpu, memory)
}

// NewTestPodWithConfig creates a test pod configured for the specified TestConfig.
func NewTestPodWithConfig(cfg *TestConfig, name, namespace, cpu, memory string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app": name,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "pause",
					Image: pauseImage,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(cpu),
							corev1.ResourceMemory: resource.MustParse(memory),
						},
					},
				},
			},
			Tolerations: cfg.Tolerations,
		},
	}
	if cfg.NodeGroup != "" && cfg.NodeGroupLabelKey != "" {
		pod.Spec.NodeSelector = map[string]string{
			cfg.NodeGroupLabelKey: cfg.NodeGroup,
		}
	}
	return pod
}

// MatchNodeForConfig checks whether a node belongs to the target node group in TestConfig.
// If NodeGroup is empty (e.g. generic cluster-wide GCE test), it matches any schedulable worker node.
func MatchNodeForConfig(node *corev1.Node, cfg *TestConfig) bool {
	if cfg.NodeGroup != "" && cfg.NodeGroupLabelKey != "" {
		return node.Labels[cfg.NodeGroupLabelKey] == cfg.NodeGroup
	}
	if node.Spec.Unschedulable {
		return false
	}
	for _, taint := range node.Spec.Taints {
		if taint.Key == "node-role.kubernetes.io/control-plane" || taint.Key == "node-role.kubernetes.io/master" {
			return false
		}
	}
	return true
}

// IsNodeReady returns true if the node has NodeReady condition True.
func IsNodeReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// ResolveNodeCPU determines the allocatable CPU in millicores of a single node in the target group.
//
// Universal Proportional CPU Sizing:
// KWOK E2E tests start with 0 worker nodes (12000m CPU template), whereas GCE E2E tests start with N
// existing worker nodes (e.g. 3900m allocatable CPU on e2-standard-4). By detecting a live node's actual
// allocatable CPU (or falling back to cfg.NodeCPU when 0 nodes exist) and sizing test pods as exact
// percentages of a single node's capacity (via CalculateCPURequest), the standard Kubernetes scheduler
// deterministically places pods across nodes without needing manual node cordoning or pod anti-affinity hacks.
func ResolveNodeCPU(ctx context.Context, client klient.Client, cfg *TestConfig) int64 {
	nodeList := &corev1.NodeList{}
	if err := client.Resources().List(ctx, nodeList); err == nil {
		for i := range nodeList.Items {
			node := &nodeList.Items[i]
			if MatchNodeForConfig(node, cfg) && IsNodeReady(node) {
				if cpu, ok := node.Status.Allocatable[corev1.ResourceCPU]; ok && cpu.MilliValue() > 0 {
					return cpu.MilliValue()
				}
			}
		}
	}
	return cfg.NodeCPU
}

// CleanUpNodeGroup deletes all fake nodes for the given nodeGroup (on KWOK) and waits until count is 0.
func CleanUpNodeGroup(ctx context.Context, client klient.Client, nodeGroup string) error {
	cfg := GetTestConfig()
	if cfg.Provider != ProviderKWOK {
		return nil
	}
	nodeList := &corev1.NodeList{}
	err := client.Resources().List(ctx, nodeList)
	if err != nil {
		return err
	}
	for _, node := range nodeList.Items {
		if node.Labels[cfg.NodeGroupLabelKey] == nodeGroup {
			n := node
			_ = client.Resources().Delete(ctx, &n)
		}
	}
	return WaitForNodeCount(ctx, client, nodeGroup, 0, 30*time.Second)
}

// TeardownPodAndNodeGroup deletes the test pods, waits for them to be deleted,
// waits for Cluster Autoscaler to scale down the node naturally (to keep CA state synchronized),
// and performs forced node cleanup if CA didn't scale down in time (on KWOK).
func TeardownPodAndNodeGroup(ctx context.Context, client klient.Client, pods []*corev1.Pod, nodeGroup string) {
	cfg := GetTestConfig()
	for _, pod := range pods {
		if pod != nil {
			_ = client.Resources().Delete(ctx, pod)
		}
	}
	_ = WaitForPodsDeleted(ctx, client, pods, cfg.PodDeletionTimeout)
	if cfg.Provider == ProviderKWOK {
		// Allow CA to scale down the empty node naturally
		_ = WaitForNodeCount(ctx, client, nodeGroup, 0, 45*time.Second)
		_ = CleanUpNodeGroup(ctx, client, nodeGroup)
	}
}

// CountNodeGroupNodes returns the number of nodes currently matching the nodeGroup.
func CountNodeGroupNodes(ctx context.Context, client klient.Client, nodeGroup string) (int, error) {
	cfg := GetTestConfig()
	nodeList := &corev1.NodeList{}
	err := client.Resources().List(ctx, nodeList)
	if err != nil {
		return 0, err
	}
	count := 0
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if nodeGroup != "" && cfg.NodeGroupLabelKey != "" {
			if node.Labels[cfg.NodeGroupLabelKey] == nodeGroup {
				count++
			}
		} else if MatchNodeForConfig(node, cfg) {
			count++
		}
	}
	return count, nil
}

// CountReadyNodesForConfig returns the number of Ready nodes matching the TestConfig.
func CountReadyNodesForConfig(ctx context.Context, client klient.Client, cfg *TestConfig) (int, error) {
	nodeList := &corev1.NodeList{}
	err := client.Resources().List(ctx, nodeList)
	if err != nil {
		return 0, err
	}
	count := 0
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if MatchNodeForConfig(node, cfg) && IsNodeReady(node) {
			count++
		}
	}
	return count, nil
}
