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

	// Pods protected by PDB. Both fit on 1 node (1000m and 2000m on 12-core node).
	pdbPod1 := NewTestPodWithResources("pdb-pod-1", ns, "1000m", "500Mi")
	pdbPod1.Labels["app"] = "pdb-test"
	pdbPod1.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	pdbPod2 := NewTestPodWithResources("pdb-pod-2", ns, "2000m", "500Mi")
	pdbPod2.Labels["app"] = "pdb-test"
	pdbPod2.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	// Filler pod consumes 10 cores on the first node, leaving only 1 core free (12 - 1 - 10 = 1 core)
	// which prevents pdbPod2 (2 cores) from fitting, forcing it to trigger scale-up of a second node.
	fillerPod := NewTestPodWithResources("filler-pod", ns, "10000m", "500Mi")

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

			// Step 1: Create pdbPod1 and fillerPod to fill the first node
			for _, pod := range []*corev1.Pod{pdbPod1, fillerPod} {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			// Wait for the first node to become ready and pods scheduled
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}
			for _, pod := range []*corev1.Pod{pdbPod1, fillerPod} {
				err = WaitForPodScheduled(ctx, client, pod, podSchedulingTimeout)
				if err != nil {
					t.Fatalf("pod %s was not scheduled: %v", pod.Name, err)
				}
			}

			// Step 2: Create pdbPod2 which cannot fit on the first node (needs 2 cores, only 1 core free)
			// forcing scale-up of a second node.
			err = client.Resources().Create(ctx, pdbPod2)
			if err != nil {
				t.Fatalf("failed to create pod %s: %v", pdbPod2.Name, err)
			}

			// Wait for both nodes to become ready and pdbPod2 scheduled on the second node
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 2, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}
			err = WaitForPodScheduled(ctx, client, pdbPod2, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod %s was not scheduled: %v", pdbPod2.Name, err)
			}

			// Step 3: Delete filler pod so the first node now has plenty of room (11 cores free)
			err = client.Resources().Delete(ctx, fillerPod)
			if err != nil {
				t.Fatalf("failed to delete filler pod: %v", err)
			}
			_ = WaitForPodDeleted(ctx, client, fillerPod, podDeletionTimeout)

			// Step 4: Cluster Autoscaler should drain pdbPod2 (allowed by PDB) and scale down to 1 node
			err = WaitForNodeCount(ctx, client, defaultNodeGroup, 1, scaleDownTimeout)
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
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pdbPod1, fillerPod, pdbPod2}, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleDownReschedulingPodPreventedByPDB(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Pods protected by PDB. Both fit on 1 node (1000m and 2000m on 12-core node).
	pdbPod1 := NewTestPodWithResources("pdb-prev-pod-1", ns, "1000m", "500Mi")
	pdbPod1.Labels["app"] = "pdb-prev-test"
	pdbPod1.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	pdbPod2 := NewTestPodWithResources("pdb-prev-pod-2", ns, "2000m", "500Mi")
	pdbPod2.Labels["app"] = "pdb-prev-test"
	pdbPod2.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	// Filler pod consumes 10 cores on the first node, leaving only 1 core free (12 - 1 - 10 = 1 core)
	// which prevents pdbPod2 (2 cores) from fitting, forcing it to trigger scale-up of a second node.
	fillerPod := NewTestPodWithResources("pdb-prev-filler-pod", ns, "10000m", "500Mi")

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

			// Step 1: Create pdbPod1 and fillerPod to fill the first node
			for _, pod := range []*corev1.Pod{pdbPod1, fillerPod} {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			// Wait for the first node to become ready and pods scheduled
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}
			for _, pod := range []*corev1.Pod{pdbPod1, fillerPod} {
				err = WaitForPodScheduled(ctx, client, pod, podSchedulingTimeout)
				if err != nil {
					t.Fatalf("pod %s was not scheduled: %v", pod.Name, err)
				}
			}

			// Step 2: Create pdbPod2 which cannot fit on the first node (needs 2 cores, only 1 core free)
			// forcing scale-up of a second node.
			err = client.Resources().Create(ctx, pdbPod2)
			if err != nil {
				t.Fatalf("failed to create pod %s: %v", pdbPod2.Name, err)
			}

			// Wait for both nodes to become ready and pdbPod2 scheduled on the second node
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 2, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}
			err = WaitForPodScheduled(ctx, client, pdbPod2, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod %s was not scheduled: %v", pdbPod2.Name, err)
			}

			// Step 3: Delete filler pod so the first node now has plenty of room (11 cores free)
			err = client.Resources().Delete(ctx, fillerPod)
			if err != nil {
				t.Fatalf("failed to delete filler pod: %v", err)
			}
			_ = WaitForPodDeleted(ctx, client, fillerPod, podDeletionTimeout)

			// Step 4: Cluster Autoscaler should NOT scale down node 2 because PDB prevents evicting pdbPod2
			err = WaitForNodeCountConsistently(ctx, client, defaultNodeGroup, 2, 20*time.Second)
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
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pdbPod1, fillerPod, pdbPod2}, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleDownDrainingMultiplePodsOneByOnePDB(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// 2 pods on the node to be drained, managed by a ReplicaSet so that when CA evicts one pod,
	// the ReplicaSet controller replaces it, satisfying PDB so the second pod can then be evicted.
	rs := NewTestReplicaSet("pdb-multidrain-rs", ns, 2, "2000m", "500Mi", "app", "pdb-multidrain-test")

	// Filler pod consumes remaining capacity on the first node (11 cores out of 12)
	// forcing both RS pods onto a second node.
	fillerPod := NewTestPodWithResources("pdb-multidrain-filler", ns, "11000m", "500Mi")

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

			// Step 1: Create filler pod first to scale up node 1 and consume 11 cores
			err = client.Resources().Create(ctx, fillerPod)
			if err != nil {
				t.Fatalf("failed to create filler pod: %v", err)
			}

			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}

			err = WaitForPodScheduled(ctx, client, fillerPod, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("filler pod was not scheduled: %v", err)
			}

			// Step 2: Create ReplicaSet with 2 pods (2000m CPU each).
			// Since node 1 only has 1000m free, neither fits, forcing CA to scale up node 2.
			err = client.Resources().Create(ctx, rs)
			if err != nil {
				t.Fatalf("failed to create ReplicaSet: %v", err)
			}

			// Wait for cluster to scale up to 2 nodes
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 2, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}

			// Wait for both RS pods to be scheduled (on node 2)
			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", "pdb-multidrain-test", 2, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("ReplicaSet pods were not scheduled: %v", err)
			}

			// Step 3: Delete filler pod so node 1 has all 12 cores free to receive the RS pods
			err = client.Resources().Delete(ctx, fillerPod)
			if err != nil {
				t.Fatalf("failed to delete filler pod: %v", err)
			}
			_ = WaitForPodDeleted(ctx, client, fillerPod, podDeletionTimeout)

			// Step 4: Cluster Autoscaler should drain the 2 pods on node 2 sequentially and scale down to 1 node
			err = WaitForNodeCount(ctx, client, defaultNodeGroup, 1, scaleDownTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale down from 2 nodes to 1 node: %v", err)
			}

			// Verify both RS pods are scheduled on the remaining node
			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", "pdb-multidrain-test", 2, podSchedulingTimeout)
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
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{fillerPod}, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleDownDrainingSystemPodsWithPDB(t *testing.T) {
	testNs := testEnv.EnvConf().Namespace()
	systemNs := "kube-system"

	// Companion pod in test namespace
	companionPod := NewTestPodWithResources("system-pdb-companion-pod", testNs, "1000m", "500Mi")

	// System pod placed in kube-system namespace
	systemPod := NewTestPodWithResources("system-pdb-pod", systemNs, "2000m", "500Mi")
	systemPod.Labels["app"] = "system-pdb-test"
	systemPod.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	// Filler pod in test namespace consumes 10 cores on the first node (leaving 12 - 1 - 10 = 1 core free)
	// forcing systemPod (2 cores) onto a second node.
	fillerPod := NewTestPodWithResources("system-pdb-filler-pod", testNs, "10000m", "500Mi")

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

			// Step 1: Create companionPod and fillerPod to fill the first node
			for _, pod := range []*corev1.Pod{companionPod, fillerPod} {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			// Wait for the first node to become ready and pods scheduled
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}
			for _, pod := range []*corev1.Pod{companionPod, fillerPod} {
				err = WaitForPodScheduled(ctx, client, pod, podSchedulingTimeout)
				if err != nil {
					t.Fatalf("pod %s was not scheduled: %v", pod.Name, err)
				}
			}

			// Step 2: Create systemPod which cannot fit on the first node (needs 2 cores, only 1 core free)
			// forcing scale-up of a second node.
			err = client.Resources().Create(ctx, systemPod)
			if err != nil {
				t.Fatalf("failed to create pod %s: %v", systemPod.Name, err)
			}

			// Wait for both nodes to become ready and systemPod scheduled on the second node
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 2, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}
			err = WaitForPodScheduled(ctx, client, systemPod, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod %s was not scheduled: %v", systemPod.Name, err)
			}

			// Step 3: Delete filler pod so the first node now has plenty of room (11 cores free)
			err = client.Resources().Delete(ctx, fillerPod)
			if err != nil {
				t.Fatalf("failed to delete filler pod: %v", err)
			}
			_ = WaitForPodDeleted(ctx, client, fillerPod, podDeletionTimeout)

			// Step 4: Cluster Autoscaler should drain systemPod (allowed because of PDB in kube-system) and scale down to 1 node
			err = WaitForNodeCount(ctx, client, defaultNodeGroup, 1, scaleDownTimeout)
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
			_ = WaitForPodDeleted(ctx, client, systemPod, podDeletionTimeout)
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{companionPod, fillerPod}, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}
