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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/wait"
)

const (
	podSchedulingTimeout = 2 * time.Minute
	podDeletionTimeout   = 2 * time.Minute
	nodeReadyTimeout     = 2 * time.Minute
	scaleDownTimeout     = 4 * time.Minute
)

// WaitForPodsScheduled waits until all specified pods are assigned to nodes.
func WaitForPodsScheduled(ctx context.Context, client klient.Client, pods []*corev1.Pod, timeout time.Duration) error {
	return wait.For(func(ctx context.Context) (done bool, err error) {
		for _, pod := range pods {
			if pod == nil {
				continue
			}
			p := &corev1.Pod{}
			err := client.Resources().Get(ctx, pod.Name, pod.Namespace, p)
			if err != nil {
				return false, err
			}
			if p.Spec.NodeName == "" {
				return false, nil
			}
		}
		return true, nil
	}, wait.WithTimeout(timeout), wait.WithContext(ctx))
}

// WaitForPodScheduled waits until the pod is assigned to a node.
func WaitForPodScheduled(ctx context.Context, client klient.Client, pod *corev1.Pod, timeout time.Duration) error {
	return WaitForPodsScheduled(ctx, client, []*corev1.Pod{pod}, timeout)
}

// WaitForPodsDeleted waits until all specified pods are deleted.
func WaitForPodsDeleted(ctx context.Context, client klient.Client, pods []*corev1.Pod, timeout time.Duration) error {
	return wait.For(func(ctx context.Context) (done bool, err error) {
		for _, pod := range pods {
			if pod == nil {
				continue
			}
			p := &corev1.Pod{}
			err := client.Resources().Get(ctx, pod.Name, pod.Namespace, p)
			if err == nil {
				return false, nil
			}
			if !apierrors.IsNotFound(err) {
				return false, err
			}
		}
		return true, nil
	}, wait.WithTimeout(timeout), wait.WithContext(ctx))
}

// WaitForPodDeleted waits until the pod is deleted.
func WaitForPodDeleted(ctx context.Context, client klient.Client, pod *corev1.Pod, timeout time.Duration) error {
	return WaitForPodsDeleted(ctx, client, []*corev1.Pod{pod}, timeout)
}

// WaitForNodeCount waits until the number of nodes with the specified nodeGroup matches expected count.
func WaitForNodeCount(ctx context.Context, client klient.Client, nodeGroup string, expectedCount int, timeout time.Duration) error {
	return wait.For(func(ctx context.Context) (done bool, err error) {
		count, err := CountNodeGroupNodes(ctx, client, nodeGroup)
		if err != nil {
			return false, err
		}
		return count == expectedCount, nil
	}, wait.WithTimeout(timeout), wait.WithContext(ctx))
}

// WaitForNodesAtLeast waits until the number of nodes in a nodeGroup is at least expectedCount.
func WaitForNodesAtLeast(ctx context.Context, client klient.Client, nodeGroup string, expectedCount int, timeout time.Duration) error {
	return wait.For(func(ctx context.Context) (done bool, err error) {
		count, err := CountNodeGroupNodes(ctx, client, nodeGroup)
		if err != nil {
			return false, err
		}
		return count >= expectedCount, nil
	}, wait.WithTimeout(timeout), wait.WithContext(ctx))
}

// WaitForNodesReady waits until at least expectedCount nodes in a nodeGroup have Ready condition True.
func WaitForNodesReady(ctx context.Context, client klient.Client, nodeGroup string, expectedCount int, timeout time.Duration) error {
	return wait.For(func(ctx context.Context) (done bool, err error) {
		nodeList := &corev1.NodeList{}
		err = client.Resources().List(ctx, nodeList)
		if err != nil {
			return false, err
		}
		readyCount := 0
		for _, node := range nodeList.Items {
			if node.Labels[nodeGroupLabelKey] == nodeGroup {
				for _, condition := range node.Status.Conditions {
					if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
						readyCount++
						break
					}
				}
			}
		}
		return readyCount >= expectedCount, nil
	}, wait.WithTimeout(timeout), wait.WithContext(ctx))
}
