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
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestClusterAutoscaling(t *testing.T) {
	pod := NewTestPod("fake-pod", testEnv.EnvConf().Namespace())

	scaleUpFeature := features.New("Cluster Autoscaler Scale Up").
		Assess("scale up when a pod is pending", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Create the pending pod
			err = client.Resources().Create(ctx, pod)
			if err != nil {
				t.Fatalf("failed to create pod: %v", err)
			}

			// Wait for TriggeredScaleUp event
			err = wait.For(func(ctx context.Context) (done bool, err error) {
				events := &corev1.EventList{}
				err = client.Resources(pod.Namespace).List(ctx, events)
				if err != nil {
					return false, err
				}
				for _, event := range events.Items {
					if event.InvolvedObject.Name == pod.Name && event.Reason == "TriggeredScaleUp" {
						return true, nil
					}
				}
				return false, nil
			}, wait.WithTimeout(testCfg.PodSchedulingTimeout), wait.WithContext(ctx))
			if err != nil {
				t.Fatalf("TriggeredScaleUp event not found: %v", err)
			}

			// Wait for the pod to be scheduled
			err = wait.For(conditions.New(client.Resources()).ResourceMatch(pod, func(object k8s.Object) bool {
				p := object.(*corev1.Pod)
				return p.Spec.NodeName != ""
			}), wait.WithTimeout(testCfg.PodSchedulingTimeout), wait.WithContext(ctx))
			if err != nil {
				t.Fatalf("pod not scheduled: %v", err)
			}

			// Verify new node is created
			nodeList := &corev1.NodeList{}
			err = wait.For(func(ctx context.Context) (done bool, err error) {
				err = client.Resources().List(ctx, nodeList)
				if err != nil {
					return false, err
				}
				for _, node := range nodeList.Items {
					if node.Labels[testCfg.NodeGroupLabelKey] == testCfg.NodeGroup {
						return true, nil
					}
				}
				return false, nil
			}, wait.WithTimeout(testCfg.NodeReadyTimeout), wait.WithContext(ctx))
			if err != nil {
				t.Fatalf("%s node not created: %v", testCfg.NodeGroup, err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pod}, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, scaleUpFeature)
}
