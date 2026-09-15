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
