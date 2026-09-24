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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/testutil"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

const (
	bufferStatusTimeout  = 1 * time.Minute
	bufferScaleUpTimeout = 2 * time.Minute
)

func TestCapacityBufferScaleUp(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Each buffer replica requests 5 CPU, so only 2 replicas fit on a single
	// 12-core kwok node. 3 replicas therefore require 2 new nodes.
	const (
		bufferReplicas = int32(3)
		expectedNodes  = 2
	)

	podTemplate := &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "buffer-pod-template",
			Namespace: ns,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"app": "buffer-pod"},
			},
			Spec: NewTestPodWithResources("buffer-pod", ns, "5000m", "500Mi").Spec,
		},
	}

	buffer := testutil.NewBuffer(
		testutil.WithName("test-buffer"),
		testutil.WithNamespace[*cbv1beta1.CapacityBuffer](ns),
		testutil.WithActiveProvisioningStrategy(),
		testutil.WithReplicas(bufferReplicas),
	)

	feature := features.New("Capacity Buffer Scale Up").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			if err := cbv1beta1.AddToScheme(client.Resources().GetScheme()); err != nil {
				t.Fatalf("failed to register CapacityBuffer scheme: %v", err)
			}
			return ctx
		}).
		Assess("buffer status is reconciled", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			if err := client.Resources().Create(ctx, podTemplate); err != nil {
				t.Fatalf("failed to create pod template: %v", err)
			}
			if err := client.Resources().Create(ctx, buffer); err != nil {
				t.Fatalf("failed to create capacity buffer: %v", err)
			}

			var lastErr error
			err = waitForBuffer(ctx, client, buffer, bufferStatusTimeout, func(b *cbv1beta1.CapacityBuffer) bool {
				lastErr = validateBufferStatus(b, bufferReplicas)
				return lastErr == nil
			})
			if err != nil {
				t.Fatalf("capacity buffer status not reconciled: %v, last validation error: %v", err, lastErr)
			}
			return ctx
		}).
		Assess("buffer triggers scale up", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			if err := waitForBufferEvent(ctx, client, buffer, "TriggeredScaleUp", bufferScaleUpTimeout); err != nil {
				t.Fatalf("TriggeredScaleUp event for capacity buffer not found: %v", err)
			}
			if err := WaitForNodesReady(ctx, client, defaultNodeGroup, expectedNodes, nodeReadyTimeout); err != nil {
				t.Fatalf("expected %d ready nodes in node group %q: %v", expectedNodes, defaultNodeGroup, err)
			}
			return ctx
		}).
		Assess("buffer ready replicas are reconciled after scale up", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			var lastReadyReplicas *int32
			err = waitForBuffer(ctx, client, buffer, bufferStatusTimeout, func(b *cbv1beta1.CapacityBuffer) bool {
				lastReadyReplicas = b.Status.ReadyReplicas
				return lastReadyReplicas != nil && *lastReadyReplicas == bufferReplicas
			})
			if err != nil {
				t.Fatalf("expected status.readyReplicas to be %d, last observed: %s, err: %v", bufferReplicas, formatInt32Ptr(lastReadyReplicas), err)
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, buffer)
			_ = client.Resources().Delete(ctx, podTemplate)
			TeardownPodAndNodeGroup(ctx, client, nil, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// validateBufferStatus checks that the buffer status fields set by the capacity
// buffer controller are correctly reconciled.
func validateBufferStatus(b *cbv1beta1.CapacityBuffer, expectedReplicas int32) error {
	if !meta.IsStatusConditionTrue(b.Status.Conditions, capacitybuffer.ReadyForProvisioningCondition) {
		return fmt.Errorf("condition %s is not True, conditions: %+v", capacitybuffer.ReadyForProvisioningCondition, b.Status.Conditions)
	}
	if b.Status.Replicas == nil || *b.Status.Replicas != expectedReplicas {
		return fmt.Errorf("expected status.replicas to be %d, got %s", expectedReplicas, formatInt32Ptr(b.Status.Replicas))
	}
	if b.Status.PodTemplateRef == nil || b.Status.PodTemplateRef.Name == "" {
		return fmt.Errorf("status.podTemplateRef is not set")
	}
	if b.Status.PodTemplateGeneration == nil {
		return fmt.Errorf("status.podTemplateGeneration is not set")
	}
	if b.Status.ProvisioningStrategy == nil || *b.Status.ProvisioningStrategy != capacitybuffer.ActiveProvisioningStrategy {
		return fmt.Errorf("expected status.provisioningStrategy to be %q, got %v", capacitybuffer.ActiveProvisioningStrategy, b.Status.ProvisioningStrategy)
	}
	return nil
}

// waitForBuffer polls the given buffer until the condition function returns true.
func waitForBuffer(ctx context.Context, client klient.Client, buffer *cbv1beta1.CapacityBuffer, timeout time.Duration, condition func(*cbv1beta1.CapacityBuffer) bool) error {
	return wait.For(func(ctx context.Context) (done bool, err error) {
		b := &cbv1beta1.CapacityBuffer{}
		if err := client.Resources().Get(ctx, buffer.Name, buffer.Namespace, b); err != nil {
			return false, err
		}
		return condition(b), nil
	}, wait.WithTimeout(timeout), wait.WithInterval(2*time.Second), wait.WithContext(ctx))
}

// waitForBufferEvent waits until an event with the given reason is recorded for the buffer.
func waitForBufferEvent(ctx context.Context, client klient.Client, buffer *cbv1beta1.CapacityBuffer, reason string, timeout time.Duration) error {
	return wait.For(func(ctx context.Context) (done bool, err error) {
		events := &corev1.EventList{}
		if err := client.Resources(buffer.Namespace).List(ctx, events); err != nil {
			return false, err
		}
		for _, event := range events.Items {
			if event.InvolvedObject.Kind == capacitybuffer.CapacityBufferKind &&
				event.InvolvedObject.Name == buffer.Name &&
				event.Reason == reason {
				return true, nil
			}
		}
		return false, nil
	}, wait.WithTimeout(timeout), wait.WithContext(ctx))
}

func formatInt32Ptr(v *int32) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d", *v)
}
