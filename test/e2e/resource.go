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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient"
)

// NewTestPod creates a baseline test pod requesting 5% of a single node's available CPU
// and 2% of a single node's available memory (~600m CPU, ~655Mi memory on KWOK).
func NewTestPod(name, namespace string) *corev1.Pod {
	return NewTestPodWithResourceFraction(name, namespace, 0.05, 0.02)
}

// NewTestPodWithResourceFraction creates a test pod requesting the specified fractions
// (0.0 < fraction <= 1.0) of a single node's available CPU (NodeCPU) and memory (NodeMemory).
func NewTestPodWithResourceFraction(name, namespace string, cpuFraction, memFraction float64) *corev1.Pod {
	return NewTestPodWithResources(
		name,
		namespace,
		testCfg.CalculateCPURequest(cpuFraction),
		testCfg.CalculateMemoryRequest(memFraction),
	)
}

// NewTestPodWithResources creates a test pod configuration with custom CPU and memory requests.
func NewTestPodWithResources(name, namespace, cpu, memory string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app": name,
			},
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: new(int64),
			Containers: []corev1.Container{
				{
					Name:  "fake-container",
					Image: "fake-image",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(cpu),
							corev1.ResourceMemory: resource.MustParse(memory),
						},
					},
				},
			},
			NodeSelector: map[string]string{
				testCfg.NodeGroupLabelKey: testCfg.NodeGroup,
			},
			Tolerations: testCfg.Tolerations,
		},
	}
}

// CleanUpNodeGroup deletes all fake nodes for the given nodeGroup on KWOK and waits until count is 0.
func CleanUpNodeGroup(ctx context.Context, client klient.Client, nodeGroup string) error {
	if testCfg.Provider != ProviderKWOK {
		return WaitForNodeCount(ctx, client, nodeGroup, 0, testCfg.ScaleDownTimeout)
	}
	nodeList := &corev1.NodeList{}
	err := client.Resources().List(ctx, nodeList)
	if err != nil {
		return err
	}
	for _, node := range nodeList.Items {
		if node.Labels[testCfg.NodeGroupLabelKey] == nodeGroup {
			n := node
			_ = client.Resources().Delete(ctx, &n)
		}
	}
	return WaitForNodeCount(ctx, client, nodeGroup, 0, 30*time.Second)
}

// TeardownPodAndNodeGroup deletes the test pods, waits for them to be deleted,
// and waits for Cluster Autoscaler to scale down the node group.
func TeardownPodAndNodeGroup(ctx context.Context, client klient.Client, pods []*corev1.Pod, nodeGroup string) {
	for _, pod := range pods {
		if pod != nil {
			_ = client.Resources().Delete(ctx, pod)
		}
	}
	_ = WaitForPodsDeleted(ctx, client, pods, testCfg.PodDeletionTimeout)
	if testCfg.Provider == ProviderKWOK {
		// Allow CA to scale down the empty node naturally, then force-clean any leftover fake nodes.
		_ = WaitForNodeCount(ctx, client, nodeGroup, 0, 45*time.Second)
		_ = CleanUpNodeGroup(ctx, client, nodeGroup)
	} else {
		// For non-KWOK cloud providers, wait for CA to scale down naturally using the provider-configured timeout.
		_ = WaitForNodeCount(ctx, client, nodeGroup, 0, testCfg.ScaleDownTimeout)
	}
}

// CountNodeGroupNodes returns the number of nodes currently matching the nodeGroup.
func CountNodeGroupNodes(ctx context.Context, client klient.Client, nodeGroup string) (int, error) {
	nodeList := &corev1.NodeList{}
	err := client.Resources().List(ctx, nodeList)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, node := range nodeList.Items {
		if node.Labels[testCfg.NodeGroupLabelKey] == nodeGroup {
			count++
		}
	}
	return count, nil
}

const (
	expendablePriorityClassName = "expendable-priority"
	highPriorityClassName       = "high-priority"
	expendablePriorityValue     = int32(-15)
	highPriorityValue           = int32(1000)
)

// EnsurePriorityClasses creates the expendable and high priority classes for testing.
func EnsurePriorityClasses(ctx context.Context, client klient.Client) error {
	for name, val := range map[string]int32{
		expendablePriorityClassName: expendablePriorityValue,
		highPriorityClassName:       highPriorityValue,
	} {
		pc := &schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Value: val,
		}
		_ = client.Resources().Create(ctx, pc)
	}
	return nil
}

// DeletePriorityClasses cleans up priority classes created for testing.
func DeletePriorityClasses(ctx context.Context, client klient.Client) {
	for _, name := range []string{expendablePriorityClassName, highPriorityClassName} {
		pc := &schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
		}
		_ = client.Resources().Delete(ctx, pc)
	}
}

// NewTestPodWithPriority creates a baseline test pod configuration with a specific PriorityClassName.
func NewTestPodWithPriority(name, namespace, priorityClassName string) *corev1.Pod {
	pod := NewTestPod(name, namespace)
	pod.Spec.PriorityClassName = priorityClassName
	return pod
}

// NewTestReplicaSet creates a test ReplicaSet requesting the specified CPU and memory fractions
// per replica with the safe-to-evict annotation.
func NewTestReplicaSet(name, namespace string, replicas int32, cpuFraction, memFraction float64, labelKey, labelVal string) *appsv1.ReplicaSet {
	labels := map[string]string{
		labelKey: labelVal,
	}
	cpu := testCfg.CalculateCPURequest(cpuFraction)
	memory := testCfg.CalculateMemoryRequest(memFraction)
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
					},
				},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: new(int64),
					Containers: []corev1.Container{
						{
							Name:  "fake-container",
							Image: "fake-image",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(cpu),
									corev1.ResourceMemory: resource.MustParse(memory),
								},
							},
						},
					},
					NodeSelector: map[string]string{
						testCfg.NodeGroupLabelKey: testCfg.NodeGroup,
					},
					Tolerations: testCfg.Tolerations,
				},
			},
		},
	}
}

// DeletePodsWithLabel deletes all pods in namespace matching the given label key and value.
func DeletePodsWithLabel(ctx context.Context, client klient.Client, namespace, labelKey, labelVal string) error {
	podList := &corev1.PodList{}
	err := client.Resources(namespace).List(ctx, podList)
	if err != nil {
		return err
	}
	for _, pod := range podList.Items {
		if pod.Labels[labelKey] == labelVal {
			p := pod
			_ = client.Resources().Delete(ctx, &p)
		}
	}
	return nil
}
