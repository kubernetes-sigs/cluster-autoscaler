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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestScaleDownReschedulingPodAllowedByPDB(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Pods protected by PDB, sized as fractions of a single node's available resources (NodeCPU, NodeMemory)
	// so the test works across both KWOK (default 12 cores, 32Gi) and cloud providers with different node sizes:
	// - pdbPod1 (10% CPU) + fillerPod (80% CPU) = 90% CPU (fits on 1st node, leaving 10% free).
	// - pdbPod2 (20% CPU) cannot fit on the 1st node (90% + 20% = 110% > 100%), forcing a 2nd node.
	// - Once fillerPod is deleted, pdbPod1 (10%) + pdbPod2 (20%) = 30% CPU (both fit on 1 node).
	pdbPod1 := NewTestPodWithResourceFraction("pdb-pod-1", ns, 0.10, 0.02)
	pdbPod1.Labels["app"] = "pdb-test"
	pdbPod1.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	pdbPod2 := NewTestPodWithResourceFraction("pdb-pod-2", ns, 0.20, 0.02)
	pdbPod2.Labels["app"] = "pdb-test"
	pdbPod2.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	fillerPod := NewTestPodWithResourceFraction("filler-pod", ns, 0.80, 0.02)

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

	feature := features.New("Scale Down When Rescheduling A Pod Is Required And PDB Allows For It").
		Assess("scale down when rescheduling a pod is required and pdb allows for it", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Create PDB allowing at most 1 pod disrupted (minAvailable=1 with 2 replicas)
			err = client.Resources().Create(ctx, pdb)
			if err != nil {
				t.Fatalf("failed to create PDB: %v", err)
			}

			// Create pdbPod1 and fillerPod to fill the first node
			for _, pod := range []*corev1.Pod{pdbPod1, fillerPod} {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			// Wait for the first node to become ready and pods scheduled
			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 1, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}
			err = WaitForPodsScheduled(ctx, client, []*corev1.Pod{pdbPod1, fillerPod}, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pods were not scheduled: %v", err)
			}

			// Create pdbPod2 which cannot fit on the first node, forcing scale-up of a second node.
			err = client.Resources().Create(ctx, pdbPod2)
			if err != nil {
				t.Fatalf("failed to create pod %s: %v", pdbPod2.Name, err)
			}

			// Wait for both nodes to become ready and pdbPod2 scheduled on the second node
			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 2, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}
			err = WaitForPodScheduled(ctx, client, pdbPod2, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod %s was not scheduled: %v", pdbPod2.Name, err)
			}

			// Delete filler pod so the first node now has plenty of room
			err = client.Resources().Delete(ctx, fillerPod)
			if err != nil {
				t.Fatalf("failed to delete filler pod: %v", err)
			}
			_ = WaitForPodDeleted(ctx, client, fillerPod, testCfg.PodDeletionTimeout)

			// Cluster Autoscaler should drain pdbPod2 (allowed by PDB) and scale down to 1 node
			err = WaitForNodeCount(ctx, client, testCfg.NodeGroup, 1, testCfg.ScaleDownTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale down from 2 nodes to 1 node: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, pdb)
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pdbPod1, fillerPod, pdbPod2}, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleDownReschedulingPodPreventedByPDB(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Pods protected by PDB, sized as fractions of a single node's available resources:
	// - pdbPod1 (10% CPU) + fillerPod (80% CPU) = 90% CPU on the 1st node.
	// - pdbPod2 (20% CPU) cannot fit on the 1st node, forcing a 2nd node.
	// - After fillerPod is deleted, both pods could fit on 1 node, but PDB minAvailable=2 blocks drain.
	pdbPod1 := NewTestPodWithResourceFraction("pdb-prev-pod-1", ns, 0.10, 0.02)
	pdbPod1.Labels["app"] = "pdb-prev-test"
	pdbPod1.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	pdbPod2 := NewTestPodWithResourceFraction("pdb-prev-pod-2", ns, 0.20, 0.02)
	pdbPod2.Labels["app"] = "pdb-prev-test"
	pdbPod2.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	fillerPod := NewTestPodWithResourceFraction("pdb-prev-filler-pod", ns, 0.80, 0.02)

	minAvailable := intstr.FromInt(2)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pdb-prev-test-budget",
			Namespace: ns,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "pdb-prev-test",
				},
			},
			MinAvailable: &minAvailable,
		},
	}

	feature := features.New("Scale Down When Rescheduling A Pod Is Prevented By PDB").
		Assess("do not scale down when rescheduling a pod is prevented by pdb", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Create PDB allowing 0 disruptions (minAvailable=2 with 2 replicas)
			err = client.Resources().Create(ctx, pdb)
			if err != nil {
				t.Fatalf("failed to create PDB: %v", err)
			}

			// Create pdbPod1 and fillerPod to fill the first node
			for _, pod := range []*corev1.Pod{pdbPod1, fillerPod} {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			// Wait for the first node to become ready and pods scheduled
			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 1, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}
			err = WaitForPodsScheduled(ctx, client, []*corev1.Pod{pdbPod1, fillerPod}, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pods were not scheduled: %v", err)
			}

			// Create pdbPod2 which cannot fit on the first node, forcing scale-up of a second node.
			err = client.Resources().Create(ctx, pdbPod2)
			if err != nil {
				t.Fatalf("failed to create pod %s: %v", pdbPod2.Name, err)
			}

			// Wait for both nodes to become ready and pdbPod2 scheduled on the second node
			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 2, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}
			err = WaitForPodScheduled(ctx, client, pdbPod2, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod %s was not scheduled: %v", pdbPod2.Name, err)
			}

			// Delete filler pod so the first node now has plenty of room
			err = client.Resources().Delete(ctx, fillerPod)
			if err != nil {
				t.Fatalf("failed to delete filler pod: %v", err)
			}
			_ = WaitForPodDeleted(ctx, client, fillerPod, testCfg.PodDeletionTimeout)

			// Cluster Autoscaler should NOT scale down node 2 because PDB prevents evicting pdbPod2
			err = WaitForNodeCountConsistently(ctx, client, testCfg.NodeGroup, 2, 20*time.Second)
			if err != nil {
				t.Fatalf("cluster scaled down despite PDB blocking drain: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, pdb)
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pdbPod1, fillerPod, pdbPod2}, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleDownDrainingMultiplePodsOneByOnePDB(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// 2 pods on the node to be drained (20% CPU each), managed by a ReplicaSet so that when CA evicts one pod,
	// the ReplicaSet controller replaces it, satisfying PDB so the second pod can then be evicted.
	rs := NewTestReplicaSet("pdb-multidrain-rs", ns, 2, 0.20, 0.02, "app", "pdb-multidrain-test")

	// Filler pod consumes 90% CPU on the first node, leaving only 10% free,
	// forcing both RS pods (20% CPU each) onto a second node.
	fillerPod := NewTestPodWithResourceFraction("pdb-multidrain-filler", ns, 0.90, 0.02)

	// minAvailable: 1 out of 2 pods means at most 1 disruption is allowed at a time.
	minAvailable := intstr.FromInt(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pdb-multidrain-budget",
			Namespace: ns,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "pdb-multidrain-test",
				},
			},
			MinAvailable: &minAvailable,
		},
	}

	feature := features.New("Scale Down By Draining Multiple Pods One By One As Dictated By PDB").
		Assess("scale down by draining multiple pods one by one as dictated by pdb", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Create PDB allowing at most 1 disruption at a time
			err = client.Resources().Create(ctx, pdb)
			if err != nil {
				t.Fatalf("failed to create PDB: %v", err)
			}

			// Create filler pod first to scale up node 1 and consume 90% CPU
			err = client.Resources().Create(ctx, fillerPod)
			if err != nil {
				t.Fatalf("failed to create filler pod: %v", err)
			}

			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 1, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}

			err = WaitForPodScheduled(ctx, client, fillerPod, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("filler pod was not scheduled: %v", err)
			}

			// Create ReplicaSet with 2 pods (20% CPU each).
			// Since node 1 only has 10% CPU free, neither fits, forcing CA to scale up node 2.
			err = client.Resources().Create(ctx, rs)
			if err != nil {
				t.Fatalf("failed to create ReplicaSet: %v", err)
			}

			// Wait for cluster to scale up to 2 nodes
			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 2, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}

			// Wait for both RS pods to be scheduled (on node 2)
			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", "pdb-multidrain-test", 2, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("ReplicaSet pods were not scheduled: %v", err)
			}

			// Delete filler pod so node 1 has 100% CPU free to receive the RS pods
			err = client.Resources().Delete(ctx, fillerPod)
			if err != nil {
				t.Fatalf("failed to delete filler pod: %v", err)
			}
			_ = WaitForPodDeleted(ctx, client, fillerPod, testCfg.PodDeletionTimeout)

			// Cluster Autoscaler should drain the 2 pods on node 2 sequentially and scale down to 1 node
			err = WaitForNodeCount(ctx, client, testCfg.NodeGroup, 1, testCfg.ScaleDownTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale down from 2 nodes to 1 node: %v", err)
			}

			// Verify both RS pods are scheduled on the remaining node
			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", "pdb-multidrain-test", 2, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("ReplicaSet pods were not rescheduled to remaining node: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, pdb)
			_ = client.Resources().Delete(ctx, rs)
			_ = DeletePodsWithLabel(ctx, client, ns, "app", "pdb-multidrain-test")
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{fillerPod}, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleDownDrainingSystemPodsWithPDB(t *testing.T) {
	testNs := testEnv.EnvConf().Namespace()
	systemNs := "kube-system"

	// Companion pod in test namespace (10% CPU)
	companionPod := NewTestPodWithResourceFraction("system-pdb-companion-pod", testNs, 0.10, 0.02)

	// System pod placed in kube-system namespace (20% CPU)
	systemPod := NewTestPodWithResourceFraction("system-pdb-pod", systemNs, 0.20, 0.02)
	systemPod.Labels["app"] = "system-pdb-test"
	systemPod.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	// Filler pod in test namespace consumes 80% CPU on the first node (leaving 10% CPU free)
	// forcing systemPod (20% CPU) onto a second node.
	fillerPod := NewTestPodWithResourceFraction("system-pdb-filler-pod", testNs, 0.80, 0.02)

	minAvailable := intstr.FromInt(0)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "system-pdb-budget",
			Namespace: systemNs,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "system-pdb-test",
				},
			},
			MinAvailable: &minAvailable,
		},
	}

	feature := features.New("Scale Down By Draining System Pods With PDB").
		Assess("scale down by draining system pods with pdb", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Create PDB in kube-system allowing disruption
			err = client.Resources().Create(ctx, pdb)
			if err != nil {
				t.Fatalf("failed to create PDB in kube-system: %v", err)
			}

			// Create companionPod and fillerPod to fill the first node
			for _, pod := range []*corev1.Pod{companionPod, fillerPod} {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			// Wait for the first node to become ready and pods scheduled
			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 1, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}
			err = WaitForPodsScheduled(ctx, client, []*corev1.Pod{companionPod, fillerPod}, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pods were not scheduled: %v", err)
			}

			// Create systemPod which cannot fit on the first node, forcing scale-up of a second node.
			err = client.Resources().Create(ctx, systemPod)
			if err != nil {
				t.Fatalf("failed to create pod %s: %v", systemPod.Name, err)
			}

			// Wait for both nodes to become ready and systemPod scheduled on the second node
			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 2, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}
			err = WaitForPodScheduled(ctx, client, systemPod, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod %s was not scheduled: %v", systemPod.Name, err)
			}

			// Delete filler pod so the first node now has plenty of room
			err = client.Resources().Delete(ctx, fillerPod)
			if err != nil {
				t.Fatalf("failed to delete filler pod: %v", err)
			}
			_ = WaitForPodDeleted(ctx, client, fillerPod, testCfg.PodDeletionTimeout)

			// Cluster Autoscaler should drain systemPod (allowed because of PDB in kube-system) and scale down to 1 node
			err = WaitForNodeCount(ctx, client, testCfg.NodeGroup, 1, testCfg.ScaleDownTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale down from 2 nodes to 1 node: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, pdb)
			_ = client.Resources().Delete(ctx, systemPod)
			_ = WaitForPodDeleted(ctx, client, systemPod, testCfg.PodDeletionTimeout)
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{companionPod, fillerPod}, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}
