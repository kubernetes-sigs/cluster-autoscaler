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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

const (
	draDeviceClassName = "gpu"
	draDeviceCapacity  = 4
)

// ReserveDRA creates a ReplicaSet where pods request DRA devices via an Ephemeral ResourceClaimTemplate.
// It configures pod anti-affinity when onePodPerNode is true.
// Returns a cleanup function to tear down the ReplicaSet, pods, and ResourceClaimTemplate.
func ReserveDRA(ctx context.Context, client klient.Client, ns, id string, replicas int, deviceClass string, devicesPerPod int, onePodPerNode bool) (func(context.Context) error, error) {
	claimName := "test-claim"

	template := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: ns,
		},
		Spec: resourcev1.ResourceClaimTemplateSpec{
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{
						{
							Name: "req-1",
							Exactly: &resourcev1.ExactDeviceRequest{
								DeviceClassName: deviceClass,
								AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
								Count:           int64(devicesPerPod),
							},
						},
					},
				},
			},
		},
	}

	err := client.Resources().Create(ctx, template)
	if err != nil {
		return nil, fmt.Errorf("failed to create ResourceClaimTemplate: %w", err)
	}

	rep := int32(replicas)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: ns,
			Labels: map[string]string{
				"name": id,
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &rep,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"name": id,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"name": id,
					},
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
					},
				},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{
						nodeGroupLabelKey: defaultNodeGroup,
					},
					Tolerations: []corev1.Toleration{
						{
							Key:      kwokTaintKey,
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "fake-container",
							Image: "fake-image",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("100Mi"),
								},
								Claims: []corev1.ResourceClaim{
									{
										Name: claimName,
									},
								},
							},
						},
					},
					ResourceClaims: []corev1.PodResourceClaim{
						{
							Name:                      claimName,
							ResourceClaimTemplateName: &id,
						},
					},
				},
			},
		},
	}

	if onePodPerNode {
		rs.Spec.Template.Spec.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"name": id,
							},
						},
						TopologyKey: "kubernetes.io/hostname",
					},
				},
			},
		}
	}

	err = client.Resources().Create(ctx, rs)
	if err != nil {
		_ = client.Resources().Delete(ctx, template)
		return nil, fmt.Errorf("failed to create ReplicaSet: %w", err)
	}

	cleanup := func(cleanupCtx context.Context) error {
		_ = client.Resources().Delete(cleanupCtx, rs)
		_ = DeletePodsWithLabel(cleanupCtx, client, ns, "name", id)
		_ = client.Resources().Delete(cleanupCtx, template)
		return nil
	}

	return cleanup, nil
}

// TestScaleUpPodsRequestDRAResources tests that Cluster Autoscaler scales up when pods request DRA resources.
func TestScaleUpPodsRequestDRAResources(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	feature := features.New("Cluster Autoscaler Scale Up With DRA").
		Assess("scale up when pods request DRA resources", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Check if DRA DeviceClass is available; skip if not installed, matching upstream behavior
			deviceClass := &resourcev1.DeviceClass{}
			err = client.Resources().Get(ctx, draDeviceClassName, "", deviceClass)
			if err != nil {
				t.Skipf("skipping test: DRA driver not installed (DeviceClass %q not found): %v", draDeviceClassName, err)
				return ctx
			}

			cleanup, err := ReserveDRA(ctx, client, ns, "dra-scale-up", 1, draDeviceClassName, 1, false)
			if err != nil {
				t.Fatalf("failed to reserve DRA resources: %v", err)
			}
			t.Cleanup(func() {
				_ = cleanup(context.Background())
			})

			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}

			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "name", "dra-scale-up", 1, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("DRA pod was not scheduled: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleUpPodRequestsMoreDRADevicesThanNodeCapacity tests that CA does not scale up if pod requests more DRA devices than node capacity.
func TestScaleUpPodRequestsMoreDRADevicesThanNodeCapacity(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	feature := features.New("Cluster Autoscaler Scale Up With Oversized DRA").
		Assess("shouldn't scale up if pod requests more DRA devices than node capacity", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Check if DRA DeviceClass is available; skip if not installed, matching upstream behavior
			deviceClass := &resourcev1.DeviceClass{}
			err = client.Resources().Get(ctx, draDeviceClassName, "", deviceClass)
			if err != nil {
				t.Skipf("skipping test: DRA driver not installed (DeviceClass %q not found): %v", draDeviceClassName, err)
				return ctx
			}

			cleanup, err := ReserveDRA(ctx, client, ns, "oversized-dra-pod", 1, draDeviceClassName, draDeviceCapacity+1, false)
			if err != nil {
				t.Fatalf("failed to reserve oversized DRA resources: %v", err)
			}
			t.Cleanup(func() {
				_ = cleanup(context.Background())
			})

			// Wait for NotTriggerScaleUp event
			err = WaitForPodEvent(ctx, client, ns, "oversized-dra-pod", "NotTriggerScaleUp", 1*time.Minute)
			if err != nil {
				t.Fatalf("expected NotTriggerScaleUp event: %v", err)
			}

			// Verify cluster size remains 0
			err = WaitForNodeCountConsistently(ctx, client, defaultNodeGroup, 0, 10*time.Second)
			if err != nil {
				t.Fatalf("cluster size changed: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleDownWithDRANodeDraining tests that Cluster Autoscaler correctly scales down with DRA node draining.
func TestScaleDownWithDRANodeDraining(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	feature := features.New("Cluster Autoscaler Scale Down With DRA Node Draining").
		Assess("should correctly scale down with DRA node draining", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Check if DRA DeviceClass is available; skip if not installed, matching upstream behavior
			deviceClass := &resourcev1.DeviceClass{}
			err = client.Resources().Get(ctx, draDeviceClassName, "", deviceClass)
			if err != nil {
				t.Skipf("skipping test: DRA driver not installed (DeviceClass %q not found): %v", draDeviceClassName, err)
				return ctx
			}

			increasedNodeCount := 2

			// Scale cluster up by creating antiAffinity pods that request 1 device (forces 1 pod per node)
			cleanupAntiAffinityDRA, err := ReserveDRA(ctx, client, ns, "dra-antiaffinity", increasedNodeCount, draDeviceClassName, 1, true)
			if err != nil {
				t.Fatalf("failed to create anti-affinity DRA pods: %v", err)
			}
			t.Cleanup(func() {
				_ = cleanupAntiAffinityDRA(context.Background())
			})

			// Wait for cluster to scale up to 2 nodes
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, increasedNodeCount, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to %d nodes: %v", increasedNodeCount, err)
			}

			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "name", "dra-antiaffinity", increasedNodeCount, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("anti-affinity DRA pods were not scheduled: %v", err)
			}

			// Add increasedNodeCount pods, each reserving 2 devices.
			// Each node will have 1 antiAffinity pod (1 device) + 1 workload pod (2 devices) = 3/4 devices (75% utilization).
			cleanupDRA, err := ReserveDRA(ctx, client, ns, "dra-workload", increasedNodeCount, draDeviceClassName, 2, false)
			if err != nil {
				t.Fatalf("failed to create workload DRA pods: %v", err)
			}
			t.Cleanup(func() {
				_ = cleanupDRA(context.Background())
			})

			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "name", "dra-workload", increasedNodeCount, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("workload DRA pods were not scheduled: %v", err)
			}

			// Remove the pods that increased the cluster size
			err = cleanupAntiAffinityDRA(ctx)
			if err != nil {
				t.Fatalf("failed to clean up anti-affinity DRA pods: %v", err)
			}

			// The unneeded nodes should be removed by draining the DRA pods, scaling cluster down to 1 node
			err = WaitForNodeCount(ctx, client, defaultNodeGroup, 1, scaleDownTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale down to 1 node with DRA node draining: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}
