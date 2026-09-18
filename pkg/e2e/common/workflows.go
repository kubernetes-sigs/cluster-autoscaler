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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/wait"
)

// RunScaleDownReschedulingPodAllowedByPDB executes the common scale-down with PDB rescheduling test logic.
// Works across both KWOK and GCE environments.
func RunScaleDownReschedulingPodAllowedByPDB(ctx context.Context, client klient.Client, ns string, cfg *TestConfig) error {
	// Measure initial ready worker nodes to establish the cluster baseline:
	// - KWOK starts with 0 worker nodes -> baselineCount = 1 (we scale up 1 baseline node in Phase 1).
	// - GCE starts with N existing worker nodes (e.g. 3 nodes) -> baselineCount = N.
	initialCount, err := CountReadyNodesForConfig(ctx, client, cfg)
	if err != nil {
		return fmt.Errorf("failed to count initial ready nodes: %v", err)
	}
	baselineCount := initialCount
	if baselineCount < 1 {
		baselineCount = 1
	}

	// Auto-detect single-node allocatable CPU from live cluster nodes (or fallback to cfg.NodeCPU
	// when 0 nodes exist) so all test pod CPU requests are sized proportionally to node capacity.
	nodeCPU := ResolveNodeCPU(ctx, client, cfg)
	cfgCopy := *cfg
	cfgCopy.NodeCPU = nodeCPU

	// Proportional CPU Requests (replaces hardcoded millicores like 8000m/5000m/1000m):
	// - fillerPods (70% CPU each): 70% + 70% = 140% > 100%, guaranteeing at most 1 filler pod per node.
	// - pdbPod1 (10% CPU): 70% + 10% = 80% <= 100%, co-schedules alongside a filler pod on a baseline node.
	// - pdbPod2 (35% CPU): 70% + 35% = 105% > 100%, cannot fit with any filler pod -> forces scale-up;
	//   once filler pods are deleted, 10% + 35% = 45% <= 100%, allowing both PDB pods to fit on 1 node.
	pdbPod1 := NewTestPodWithConfig(&cfgCopy, "pdb-pod-1", ns, cfgCopy.CalculateCPURequest(0.10), "500Mi")
	pdbPod1.Labels["app"] = "pdb-test"
	pdbPod1.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	pdbPod2 := NewTestPodWithConfig(&cfgCopy, "pdb-pod-2", ns, cfgCopy.CalculateCPURequest(0.35), "500Mi")
	pdbPod2.Labels["app"] = "pdb-test"
	pdbPod2.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	// Create an array of filler pods (1 per baseline node) so that if the cluster starts
	// with N existing nodes (e.g. GCE), all N nodes are occupied before triggering scale-up.
	fillerPods := make([]*corev1.Pod, baselineCount)
	for i := 0; i < baselineCount; i++ {
		fillerPods[i] = NewTestPodWithConfig(&cfgCopy, fmt.Sprintf("filler-pod-%d", i), ns, cfgCopy.CalculateCPURequest(0.70), "500Mi")
	}

	minAvailable := intstr.FromInt(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pdb-test-budget",
			Namespace: ns,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "pdb-test",
				},
			},
			MinAvailable: &minAvailable,
		},
	}

	allPods := append([]*corev1.Pod{pdbPod1, pdbPod2}, fillerPods...)
	defer func() {
		_ = client.Resources().Delete(ctx, pdb)
		TeardownPodAndNodeGroup(ctx, client, allPods, cfg.NodeGroup)
	}()

	// Create PDB allowing at most 1 disruption (minAvailable=1 across 2 replicas).
	if err := client.Resources().Create(ctx, pdb); err != nil {
		return fmt.Errorf("failed to create PDB: %v", err)
	}

	// Phase 1: Occupy baseline capacity and schedule pdbPod1.
	// - Create baselineCount filler pods requesting 70% CPU each. Since 70% + 70% = 140% > 100%,
	//   Kubernetes schedules exactly 1 filler pod per baseline node without needing cordoning hacks.
	// - Create pdbPod1 requesting 10% CPU. Since 70% + 10% = 80% <= 100%, pdbPod1 co-schedules
	//   onto one of the baseline nodes alongside a filler pod.
	step1Pods := append([]*corev1.Pod{pdbPod1}, fillerPods...)
	for _, pod := range step1Pods {
		if err := client.Resources().Create(ctx, pod); err != nil {
			return fmt.Errorf("failed to create pod %s: %v", pod.Name, err)
		}
	}

	if err := WaitForNodesReadyForConfig(ctx, client, cfg, baselineCount, cfg.NodeReadyTimeout); err != nil {
		return fmt.Errorf("cluster did not reach baseline %d ready nodes: %v", baselineCount, err)
	}
	if err := WaitForPodsScheduled(ctx, client, step1Pods, cfg.PodSchedulingTimeout); err != nil {
		return fmt.Errorf("initial pods were not scheduled: %v", err)
	}

	// Phase 2: Trigger scale-up with pdbPod2.
	// - Create pdbPod2 requesting 35% CPU. Every baseline node already holds a 70% filler pod
	//   (70% + 35% = 105% > 100%), so pdbPod2 cannot fit on any existing node.
	// - Cluster Autoscaler provisions 1 additional node (reaching baselineCount + 1 ready nodes).
	if err := client.Resources().Create(ctx, pdbPod2); err != nil {
		return fmt.Errorf("failed to create pod %s: %v", pdbPod2.Name, err)
	}

	if err := WaitForNodesReadyForConfig(ctx, client, cfg, baselineCount+1, cfg.NodeReadyTimeout); err != nil {
		return fmt.Errorf("cluster did not scale up to %d nodes: %v", baselineCount+1, err)
	}
	if err := WaitForPodScheduled(ctx, client, pdbPod2, cfg.PodSchedulingTimeout); err != nil {
		return fmt.Errorf("pod %s was not scheduled: %v", pdbPod2.Name, err)
	}

	// Phase 3: Free capacity by deleting filler pods and verify PDB-allowed drain + scale-down.
	// - Delete all 70% filler pods, leaving only pdbPod1 (10% CPU) and pdbPod2 (35% CPU) across
	//   baselineCount + 1 nodes. Together they require only 45% CPU and can fit on a single node.
	// - Because the PDB allows 1 disruption (minAvailable=1 with 2 running replicas), Cluster Autoscaler
	//   drains/evicts pdbPod2 onto a baseline node and scales down the unneeded node (<= baselineCount).
	for _, fp := range fillerPods {
		if err := client.Resources().Delete(ctx, fp); err != nil {
			return fmt.Errorf("failed to delete filler pod %s: %v", fp.Name, err)
		}
	}
	_ = WaitForPodsDeleted(ctx, client, fillerPods, cfg.PodDeletionTimeout)

	if err := WaitForNodesAtMostForConfig(ctx, client, cfg, baselineCount, cfg.ScaleDownTimeout); err != nil {
		return fmt.Errorf("cluster did not scale down from %d to <= %d nodes: %v", baselineCount+1, baselineCount, err)
	}

	return nil
}

// RunScaleDownUnneededNode executes the common scale-down of an unneeded node test logic.
func RunScaleDownUnneededNode(ctx context.Context, client klient.Client, ns string, cfg *TestConfig) error {
	// Measure initial ready worker nodes (0 on KWOK, N on GCE).
	initialCount, err := CountReadyNodesForConfig(ctx, client, cfg)
	if err != nil {
		return fmt.Errorf("failed to count initial ready nodes: %v", err)
	}

	// Resolve single-node allocatable CPU so each pod requests 70% of node capacity
	// (70% + 70% = 140% > 100%, guaranteeing 1 pod per node).
	nodeCPU := ResolveNodeCPU(ctx, client, cfg)
	cfgCopy := *cfg
	cfgCopy.NodeCPU = nodeCPU

	// Create 1 filler pod per existing node (on GCE) so existing nodes are occupied,
	// plus 1 scaleDownPod to force Cluster Autoscaler to provision 1 new node (initialCount + 1).
	fillerPods := make([]*corev1.Pod, initialCount)
	for i := 0; i < initialCount; i++ {
		fillerPods[i] = NewTestPodWithConfig(&cfgCopy, fmt.Sprintf("baseline-filler-%d", i), ns, cfgCopy.CalculateCPURequest(0.70), "500Mi")
	}

	scaleDownPod := NewTestPodWithConfig(&cfgCopy, "scaledown-test-pod", ns, cfgCopy.CalculateCPURequest(0.70), "500Mi")
	allPods := append(fillerPods, scaleDownPod)
	defer func() {
		TeardownPodAndNodeGroup(ctx, client, allPods, cfg.NodeGroup)
	}()

	// Phase 1: Create filler pods + scaleDownPod to trigger scale-up to initialCount + 1 nodes.
	for _, pod := range allPods {
		if err := client.Resources().Create(ctx, pod); err != nil {
			return fmt.Errorf("failed to create pod %s: %v", pod.Name, err)
		}
	}

	if err := WaitForNodesReadyForConfig(ctx, client, cfg, initialCount+1, cfg.NodeReadyTimeout); err != nil {
		return fmt.Errorf("node did not become ready (expected %d): %v", initialCount+1, err)
	}
	if err := WaitForPodsScheduled(ctx, client, allPods, cfg.PodSchedulingTimeout); err != nil {
		return fmt.Errorf("pods were not scheduled: %v", err)
	}

	// Phase 2: Delete scaleDownPod so the newly provisioned node becomes completely unneeded.
	if err := client.Resources().Delete(ctx, scaleDownPod); err != nil {
		return fmt.Errorf("failed to delete pod: %v", err)
	}
	_ = WaitForPodDeleted(ctx, client, scaleDownPod, cfg.PodDeletionTimeout)

	// Phase 3: Wait for Cluster Autoscaler to scale down back to <= initialCount nodes.
	if err := WaitForNodesAtMostForConfig(ctx, client, cfg, initialCount, cfg.ScaleDownTimeout); err != nil {
		return fmt.Errorf("node was not scaled down to %d after pod deletion: %v", initialCount, err)
	}

	return nil
}

// RunScaleUp executes the common cluster scale-up test logic.
func RunScaleUp(ctx context.Context, client klient.Client, ns string, cfg *TestConfig) error {
	initialCount, err := CountReadyNodesForConfig(ctx, client, cfg)
	if err != nil {
		return fmt.Errorf("failed to count initial ready nodes: %v", err)
	}

	nodeCPU := ResolveNodeCPU(ctx, client, cfg)
	cfgCopy := *cfg
	cfgCopy.NodeCPU = nodeCPU

	fillerPods := make([]*corev1.Pod, initialCount)
	for i := 0; i < initialCount; i++ {
		fillerPods[i] = NewTestPodWithConfig(&cfgCopy, fmt.Sprintf("scaleup-filler-%d", i), ns, cfgCopy.CalculateCPURequest(0.70), "500Mi")
	}

	triggerPod := NewTestPodWithConfig(&cfgCopy, "fake-pod", ns, cfgCopy.CalculateCPURequest(0.70), "100Mi")
	allPods := append(fillerPods, triggerPod)
	defer func() {
		TeardownPodAndNodeGroup(ctx, client, allPods, cfg.NodeGroup)
	}()

	for _, pod := range allPods {
		if err := client.Resources().Create(ctx, pod); err != nil {
			return fmt.Errorf("failed to create pod %s: %v", pod.Name, err)
		}
	}

	// Verify TriggeredScaleUp event is emitted
	err = wait.For(func(ctx context.Context) (done bool, err error) {
		var events corev1.EventList
		if err := client.Resources(ns).List(ctx, &events); err != nil {
			return false, err
		}
		for _, e := range events.Items {
			if e.InvolvedObject.Name == triggerPod.Name && e.Reason == "TriggeredScaleUp" {
				return true, nil
			}
		}
		return false, nil
	}, wait.WithTimeout(cfg.PodSchedulingTimeout), wait.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("TriggeredScaleUp event not found: %v", err)
	}

	if err := WaitForPodScheduled(ctx, client, triggerPod, cfg.PodSchedulingTimeout); err != nil {
		return fmt.Errorf("pod not scheduled: %v", err)
	}

	if err := WaitForNodesReadyForConfig(ctx, client, cfg, initialCount+1, cfg.NodeReadyTimeout); err != nil {
		return fmt.Errorf("node not created (expected %d): %v", initialCount+1, err)
	}

	return nil
}
