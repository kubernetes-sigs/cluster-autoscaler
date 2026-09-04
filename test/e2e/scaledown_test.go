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
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestScaleDownUnneededNode(t *testing.T) {
	pod := NewTestPod("scaledown-test-pod", testEnv.EnvConf().Namespace())

	feature := features.New("Scale Down Unneeded Node").
		Assess("scale down empty node after pod deletion", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Step 1: Create pod to trigger scale up
			err = client.Resources().Create(ctx, pod)
			if err != nil {
				t.Fatalf("failed to create pod: %v", err)
			}

			err = WaitForPodScheduled(ctx, client, pod, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod not scheduled: %v", err)
			}

			// Wait for node count to increase to 1 and be Ready
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("node did not become ready: %v", err)
			}

			// Step 2: Delete pod to make node unneeded
			err = client.Resources().Delete(ctx, pod)
			if err != nil {
				t.Fatalf("failed to delete pod: %v", err)
			}
			_ = WaitForPodDeleted(ctx, client, pod, podDeletionTimeout)

			// Step 3: Wait for scale down to delete the unneeded node back to 0
			err = WaitForNodeCount(ctx, client, defaultNodeGroup, 0, scaleDownTimeout)
			if err != nil {
				t.Fatalf("node was not scaled down after pod deletion: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pod}, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleDownExpendablePodRunning(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Initial workload to trigger scale-up of 1 node
	triggerPod := NewTestPodWithResources("priority-trigger-pod", ns, "1000m", "500Mi")

	// Expendable pod with priority < -10 (cutoff is -10)
	expendablePod := NewTestPodWithPriority("expendable-pod", ns, "1000m", "500Mi", expendablePriorityClassName)
	expendablePod.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	feature := features.New("Scale Down When Expendable Pod Is Running").
		Assess("scale down node running only expendable pod", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Ensure PriorityClasses are present
			err = EnsurePriorityClasses(ctx, client)
			if err != nil {
				t.Fatalf("failed to ensure priority classes: %v", err)
			}

			// Step 1: Create trigger pod to scale up to 1 node
			err = client.Resources().Create(ctx, triggerPod)
			if err != nil {
				t.Fatalf("failed to create trigger pod: %v", err)
			}

			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}

			err = WaitForPodScheduled(ctx, client, triggerPod, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("trigger pod was not scheduled: %v", err)
			}

			// Step 2: Create expendable pod on the existing node
			err = client.Resources().Create(ctx, expendablePod)
			if err != nil {
				t.Fatalf("failed to create expendable pod: %v", err)
			}

			err = WaitForPodScheduled(ctx, client, expendablePod, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("expendable pod was not scheduled: %v", err)
			}

			// Step 3: Delete trigger pod. Now node 1 runs ONLY the expendable pod.
			err = client.Resources().Delete(ctx, triggerPod)
			if err != nil {
				t.Fatalf("failed to delete trigger pod: %v", err)
			}
			_ = WaitForPodDeleted(ctx, client, triggerPod, podDeletionTimeout)

			// Step 4: Cluster Autoscaler should consider the node unneeded and scale down to 0
			err = WaitForNodeCount(ctx, client, defaultNodeGroup, 0, scaleDownTimeout)
			if err != nil {
				t.Fatalf("node with only expendable pod was not scaled down: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{triggerPod, expendablePod}, defaultNodeGroup)
			DeletePriorityClasses(ctx, client)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestNotScaleDownNonExpendablePodRunning(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Pod with non-expendable (high) priority
	nonExpendablePod := NewTestPodWithPriority("non-expendable-pod", ns, "1000m", "500Mi", highPriorityClassName)
	nonExpendablePod.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	feature := features.New("Scale Down When Non Expendable Pod Is Running").
		Assess("do not scale down node when non expendable pod is running", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Ensure PriorityClasses are present
			err = EnsurePriorityClasses(ctx, client)
			if err != nil {
				t.Fatalf("failed to ensure priority classes: %v", err)
			}

			// Step 1: Create non-expendable pod to scale up 1 node
			err = client.Resources().Create(ctx, nonExpendablePod)
			if err != nil {
				t.Fatalf("failed to create non-expendable pod: %v", err)
			}

			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}

			err = WaitForPodScheduled(ctx, client, nonExpendablePod, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("non-expendable pod was not scheduled: %v", err)
			}

			// Step 2: The node is needed because a non-expendable pod is running; verify it does not scale down
			err = WaitForNodeCountConsistently(ctx, client, defaultNodeGroup, 1, 20*time.Second)
			if err != nil {
				t.Fatalf("node was unexpectedly scaled down while running non-expendable pod: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{nonExpendablePod}, defaultNodeGroup)
			DeletePriorityClasses(ctx, client)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}
