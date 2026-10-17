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
	resourceapi "k8s.io/api/resource/v1"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestScaleUpPodsRequestDRAResources(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Enough pods requesting 1 device each to use all devices of exactly 2 nodes
	expectedNodes := 2
	replicas := int32(expectedNodes * testCfg.DRADevicesPerNode)
	claimTemplate := NewTestResourceClaimTemplate("dra-scale-up-claim", ns, 1)
	rs := NewTestDRAReplicaSet("dra-scale-up-rs", ns, replicas, claimTemplate.Name, false)

	feature := features.New("Scale Up When Pods Request DRA Resources").
		Assess("scale up when pods request DRA resources", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			err = EnsureDeviceClass(ctx, client)
			if err != nil {
				t.Fatalf("failed to ensure device class: %v", err)
			}

			err = client.Resources().Create(ctx, claimTemplate)
			if err != nil {
				t.Fatalf("failed to create ResourceClaimTemplate: %v", err)
			}

			err = client.Resources().Create(ctx, rs)
			if err != nil {
				t.Fatalf("failed to create ReplicaSet: %v", err)
			}

			err = WaitForNodesReady(ctx, client, testCfg.DRANodeGroup, expectedNodes, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to %d nodes: %v", expectedNodes, err)
			}

			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", rs.Name, int(replicas), testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("DRA pods were not scheduled: %v", err)
			}

			err = WaitForNodeCount(ctx, client, testCfg.DRANodeGroup, expectedNodes, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("expected exactly %d nodes: %v", expectedNodes, err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, rs)
			_ = DeletePodsWithLabel(ctx, client, ns, "app", rs.Name)
			TeardownPodAndNodeGroup(ctx, client, nil, testCfg.DRANodeGroup)
			_ = client.Resources().Delete(ctx, claimTemplate)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleUpPodRequestsMoreDRADevicesThanNodeCapacity(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Pod requesting one more device than a single node exposes, so it cannot fit on any node
	claimTemplate := NewTestResourceClaimTemplate("dra-oversized-claim", ns, int64(testCfg.DRADevicesPerNode+1))
	pod := NewTestDRAPod("dra-oversized-pod", ns, claimTemplate.Name)

	feature := features.New("Scale Up When Pod Requests More DRA Devices Than Node Capacity").
		Assess("do not scale up if pod requests more DRA devices than node capacity", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			err = EnsureDeviceClass(ctx, client)
			if err != nil {
				t.Fatalf("failed to ensure device class: %v", err)
			}

			err = client.Resources().Create(ctx, claimTemplate)
			if err != nil {
				t.Fatalf("failed to create ResourceClaimTemplate: %v", err)
			}

			err = client.Resources().Create(ctx, pod)
			if err != nil {
				t.Fatalf("failed to create pod: %v", err)
			}

			// Cluster Autoscaler should evaluate the pod and decide not to scale up
			err = WaitForPodEvent(ctx, client, pod, "NotTriggerScaleUp", testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("NotTriggerScaleUp event not found: %v", err)
			}

			err = WaitForNodeCountConsistently(ctx, client, testCfg.DRANodeGroup, 0, testCfg.NoScaleUpWindow)
			if err != nil {
				t.Fatalf("cluster scaled up for a pod requesting more devices than a node has: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pod}, testCfg.DRANodeGroup)
			_ = client.Resources().Delete(ctx, claimTemplate)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleDownWithDRANodeDraining(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// 2 pods requesting 1 device each with anti-affinity to each other, forcing a scale-up to 2 nodes
	antiAffinityClaimTemplate := NewTestResourceClaimTemplate("dra-anti-affinity-claim", ns, 1)
	antiAffinityRS := NewTestDRAReplicaSet("dra-anti-affinity-rs", ns, 2, antiAffinityClaimTemplate.Name, true)

	// 2 pods requesting 2 devices each. With an anti-affinity pod using 1 of the 4 devices on each node,
	// at most one of them fits per node, so each node gets one.
	drainClaimTemplate := NewTestResourceClaimTemplate("dra-drain-claim", ns, 2)
	drainRS := NewTestDRAReplicaSet("dra-drain-rs", ns, 2, drainClaimTemplate.Name, false)

	feature := features.New("Scale Down With DRA Node Draining").
		Assess("scale down with DRA node draining", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			err = EnsureDeviceClass(ctx, client)
			if err != nil {
				t.Fatalf("failed to ensure device class: %v", err)
			}

			for _, claimTemplate := range []*resourceapi.ResourceClaimTemplate{antiAffinityClaimTemplate, drainClaimTemplate} {
				err = client.Resources().Create(ctx, claimTemplate)
				if err != nil {
					t.Fatalf("failed to create ResourceClaimTemplate %s: %v", claimTemplate.Name, err)
				}
			}

			err = client.Resources().Create(ctx, antiAffinityRS)
			if err != nil {
				t.Fatalf("failed to create anti-affinity ReplicaSet: %v", err)
			}

			err = WaitForNodesReady(ctx, client, testCfg.DRANodeGroup, 2, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}

			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", antiAffinityRS.Name, 2, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("anti-affinity pods were not scheduled: %v", err)
			}

			// Add one pod using 2 devices to each node, so each node uses 3 of its 4 devices
			err = client.Resources().Create(ctx, drainRS)
			if err != nil {
				t.Fatalf("failed to create ReplicaSet: %v", err)
			}

			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", drainRS.Name, 2, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("DRA pods were not scheduled: %v", err)
			}

			// Remove the anti-affinity pods. Each node now uses 2 of its 4 devices, so both pods fit on one node
			// and Cluster Autoscaler should drain the other node.
			err = client.Resources().Delete(ctx, antiAffinityRS)
			if err != nil {
				t.Fatalf("failed to delete anti-affinity ReplicaSet: %v", err)
			}
			err = DeletePodsWithLabel(ctx, client, ns, "app", antiAffinityRS.Name)
			if err != nil {
				t.Fatalf("failed to delete anti-affinity pods: %v", err)
			}

			err = WaitForNodeCount(ctx, client, testCfg.DRANodeGroup, 1, testCfg.ScaleDownTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale down from 2 nodes to 1 node: %v", err)
			}

			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", drainRS.Name, 2, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("DRA pods were not rescheduled to the remaining node: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, antiAffinityRS)
			_ = client.Resources().Delete(ctx, drainRS)
			_ = DeletePodsWithLabel(ctx, client, ns, "app", antiAffinityRS.Name)
			_ = DeletePodsWithLabel(ctx, client, ns, "app", drainRS.Name)
			TeardownPodAndNodeGroup(ctx, client, nil, testCfg.DRANodeGroup)
			_ = client.Resources().Delete(ctx, antiAffinityClaimTemplate)
			_ = client.Resources().Delete(ctx, drainClaimTemplate)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}
