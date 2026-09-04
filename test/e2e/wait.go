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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
)

const (
	podSchedulingTimeout = 2 * time.Minute
	podDeletionTimeout   = 2 * time.Minute
	nodeReadyTimeout     = 2 * time.Minute
	scaleDownTimeout     = 4 * time.Minute
)

// WaitForPodScheduled waits until the pod is assigned to a node.
func WaitForPodScheduled(ctx context.Context, client klient.Client, pod *corev1.Pod, timeout time.Duration) error {
	return wait.For(conditions.New(client.Resources()).ResourceMatch(pod, func(object k8s.Object) bool {
		p := object.(*corev1.Pod)
		return p.Spec.NodeName != ""
	}), wait.WithTimeout(timeout), wait.WithContext(ctx))
}

// WaitForPodDeleted waits until the pod is deleted.
func WaitForPodDeleted(ctx context.Context, client klient.Client, pod *corev1.Pod, timeout time.Duration) error {
	return wait.For(conditions.New(client.Resources()).ResourceDeleted(pod), wait.WithTimeout(timeout), wait.WithContext(ctx))
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

// WaitForNodeCountConsistently waits for duration while asserting that the node count remains expectedCount.
func WaitForNodeCountConsistently(ctx context.Context, client klient.Client, nodeGroup string, expectedCount int, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			count, err := CountNodeGroupNodes(ctx, client, nodeGroup)
			if err != nil {
				return err
			}
			if count != expectedCount {
				return fmt.Errorf("expected node count %d, got %d", expectedCount, count)
			}
			return nil
		case <-ticker.C:
			count, err := CountNodeGroupNodes(ctx, client, nodeGroup)
			if err != nil {
				return err
			}
			if count != expectedCount {
				return fmt.Errorf("node count deviated: expected %d, got %d", expectedCount, count)
			}
		}
	}
}

// WaitForPodsWithLabelScheduled waits until at least expectedCount pods matching the label are assigned to a node.
func WaitForPodsWithLabelScheduled(ctx context.Context, client klient.Client, namespace, labelKey, labelVal string, expectedCount int, timeout time.Duration) error {
	return wait.For(func(ctx context.Context) (done bool, err error) {
		podList := &corev1.PodList{}
		err = client.Resources(namespace).List(ctx, podList)
		if err != nil {
			return false, err
		}
		scheduledCount := 0
		for _, pod := range podList.Items {
			if pod.Labels[labelKey] == labelVal && pod.Spec.NodeName != "" {
				scheduledCount++
			}
		}
		return scheduledCount >= expectedCount, nil
	}, wait.WithTimeout(timeout), wait.WithContext(ctx))
}

// WaitForPodEvent waits until an event with the given reason for a pod matching podPrefix appears in the namespace.
func WaitForPodEvent(ctx context.Context, client klient.Client, namespace, podPrefix, reason string, timeout time.Duration) error {
	return wait.For(func(ctx context.Context) (done bool, err error) {
		events := &corev1.EventList{}
		err = client.Resources(namespace).List(ctx, events)
		if err != nil {
			return false, err
		}
		for _, event := range events.Items {
			if strings.HasPrefix(event.InvolvedObject.Name, podPrefix) && event.Reason == reason {
				return true, nil
			}
		}
		return false, nil
	}, wait.WithTimeout(timeout), wait.WithContext(ctx))
}
