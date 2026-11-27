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

	resourceapi "k8s.io/api/resource/v1"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

// setupDRA creates the DRA DeviceClass, waits for the baseline draNodeGroup node created by Cluster
// Autoscaler, and publishes its ResourceSlice. Cluster Autoscaler builds the draNodeGroup template from
// the baseline node, with its ResourceSlice, right away: it only uses nodes that have been Ready for its
// 1m stabilization delay, and KWOK nodes keep the old Ready lastTransitionTime of the template node.
func setupDRA() features.Func {
	return func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		client, err := cfg.NewClient()
		if err != nil {
			t.Fatal(err)
		}
		err = EnsureDeviceClass(ctx, client)
		if err != nil {
			t.Fatalf("failed to ensure device class: %v", err)
		}
		err = WaitForNodesReady(ctx, client, draNodeGroup, draNodeGroupMinSize, testCfg.NodeReadyTimeout)
		if err != nil {
			t.Fatalf("baseline %s node is not ready: %v", draNodeGroup, err)
		}
		err = PublishDRAResourceSlices(ctx, client)
		if err != nil {
			t.Fatalf("failed to publish DRA ResourceSlices: %v", err)
		}
		return ctx
	}
}

// teardownDRA waits for Cluster Autoscaler to scale draNodeGroup back down to its baseline node and deletes
// the claim templates. The test workloads must already be deleted.
func teardownDRA(ctx context.Context, t *testing.T, cfg *envconf.Config, claimTemplates ...*resourceapi.ResourceClaimTemplate) {
	client, err := cfg.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	err = WaitForNodeCount(ctx, client, draNodeGroup, draNodeGroupMinSize, testCfg.ScaleDownTimeout)
	if err != nil {
		t.Errorf("%s did not scale back down to %d node: %v", draNodeGroup, draNodeGroupMinSize, err)
	}
	for _, claimTemplate := range claimTemplates {
		_ = client.Resources().Delete(ctx, claimTemplate)
	}
}

func TestScaleUpPodsRequestDRAResources(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Enough pods requesting 1 device each to use all devices of the baseline nodes and exactly 2 new nodes
	expectedNodes := draNodeGroupMinSize + 2
	replicas := int32(expectedNodes * draDevicesPerNode)
	claimTemplate := NewTestResourceClaimTemplate("dra-scale-up-claim", ns, 1)
	rs := NewTestDRAReplicaSet("dra-scale-up-rs", ns, replicas, claimTemplate.Name, false)

	feature := features.New("Scale Up When Pods Request DRA Resources").
		Setup(setupDRA()).
		Assess("scale up when pods request DRA resources", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			err = client.Resources().Create(ctx, claimTemplate)
			if err != nil {
				t.Fatalf("failed to create ResourceClaimTemplate: %v", err)
			}

			err = client.Resources().Create(ctx, rs)
			if err != nil {
				t.Fatalf("failed to create ReplicaSet: %v", err)
			}

			err = WaitForNodesReady(ctx, client, draNodeGroup, expectedNodes, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to %d nodes: %v", expectedNodes, err)
			}

			err = PublishDRAResourceSlices(ctx, client)
			if err != nil {
				t.Fatalf("failed to publish DRA ResourceSlices for the new nodes: %v", err)
			}

			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", rs.Name, int(replicas), testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("DRA pods were not scheduled: %v", err)
			}

			err = WaitForNodeCount(ctx, client, draNodeGroup, expectedNodes, testCfg.NodeReadyTimeout)
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
			teardownDRA(ctx, t, cfg, claimTemplate)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleUpPodRequestsMoreDRADevicesThanNodeCapacity(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Pod requesting one more device than a single node exposes, so it cannot fit on any node
	claimTemplate := NewTestResourceClaimTemplate("dra-oversized-claim", ns, int64(draDevicesPerNode+1))
	pod := NewTestDRAPod("dra-oversized-pod", ns, claimTemplate.Name)

	feature := features.New("Scale Up When Pod Requests More DRA Devices Than Node Capacity").
		Setup(setupDRA()).
		Assess("do not scale up if pod requests more DRA devices than node capacity", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
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

			err = WaitForNodeCountConsistently(ctx, client, draNodeGroup, draNodeGroupMinSize, testCfg.NoScaleUpWindow)
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
			_ = client.Resources().Delete(ctx, pod)
			_ = WaitForPodDeleted(ctx, client, pod, testCfg.PodDeletionTimeout)
			teardownDRA(ctx, t, cfg, claimTemplate)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleDownWithDRANodeDraining(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Pods requesting 1 device each with anti-affinity to each other, one per baseline node plus one more,
	// forcing a scale-up by 1 node
	increasedNodes := draNodeGroupMinSize + 1
	antiAffinityClaimTemplate := NewTestResourceClaimTemplate("dra-anti-affinity-claim", ns, 1)
	antiAffinityRS := NewTestDRAReplicaSet("dra-anti-affinity-rs", ns, int32(increasedNodes), antiAffinityClaimTemplate.Name, true)

	// Pods requesting 2 devices each, one per node. With an anti-affinity pod using 1 of the 4 devices on
	// each node, at most one of them fits per node, so each node gets one.
	drainClaimTemplate := NewTestResourceClaimTemplate("dra-drain-claim", ns, 2)
	drainRS := NewTestDRAReplicaSet("dra-drain-rs", ns, int32(increasedNodes), drainClaimTemplate.Name, false)

	feature := features.New("Scale Down With DRA Node Draining").
		Setup(setupDRA()).
		Assess("scale down with DRA node draining", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
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

			err = WaitForNodesReady(ctx, client, draNodeGroup, increasedNodes, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to %d nodes: %v", increasedNodes, err)
			}

			err = PublishDRAResourceSlices(ctx, client)
			if err != nil {
				t.Fatalf("failed to publish DRA ResourceSlices for the new node: %v", err)
			}

			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", antiAffinityRS.Name, increasedNodes, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("anti-affinity pods were not scheduled: %v", err)
			}

			// Add one pod using 2 devices to each node, so each node uses 3 of its 4 devices
			err = client.Resources().Create(ctx, drainRS)
			if err != nil {
				t.Fatalf("failed to create ReplicaSet: %v", err)
			}

			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", drainRS.Name, increasedNodes, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("DRA pods were not scheduled: %v", err)
			}

			// Remove the anti-affinity pods. Each node now uses 2 of its 4 devices, so the pods of two nodes
			// fit on one node and Cluster Autoscaler should drain one node.
			err = client.Resources().Delete(ctx, antiAffinityRS)
			if err != nil {
				t.Fatalf("failed to delete anti-affinity ReplicaSet: %v", err)
			}
			err = DeletePodsWithLabel(ctx, client, ns, "app", antiAffinityRS.Name)
			if err != nil {
				t.Fatalf("failed to delete anti-affinity pods: %v", err)
			}

			err = WaitForNodeCount(ctx, client, draNodeGroup, draNodeGroupMinSize, testCfg.ScaleDownTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale down from %d to %d nodes: %v", increasedNodes, draNodeGroupMinSize, err)
			}

			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", drainRS.Name, increasedNodes, testCfg.PodSchedulingTimeout)
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
			teardownDRA(ctx, t, cfg, antiAffinityClaimTemplate, drainClaimTemplate)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}
