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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestClusterAutoscaling(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "fake-pod",
			Namespace: testEnv.EnvConf().Namespace(),
			Labels: map[string]string{
				"app": "fake-pod",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "fake-container",
					Image: "fake-image",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("100Mi"),
						},
					},
				},
			},
			NodeSelector: map[string]string{
				nodeGroupLabelKey: defaultNodeGroup,
			},
			Tolerations: []corev1.Toleration{
				{
					Key:      "kwok-provider",
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
		},
	}

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
			}, wait.WithTimeout(2*time.Minute), wait.WithContext(ctx))
			if err != nil {
				t.Fatalf("TriggeredScaleUp event not found: %v", err)
			}

			// Wait for the pod to be scheduled
			err = wait.For(conditions.New(client.Resources()).ResourceMatch(pod, func(object k8s.Object) bool {
				p := object.(*corev1.Pod)
				return p.Spec.NodeName != ""
			}), wait.WithTimeout(2*time.Minute), wait.WithContext(ctx))
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
					if node.Labels[nodeGroupLabelKey] == defaultNodeGroup {
						return true, nil
					}
				}
				return false, nil
			}, wait.WithTimeout(2*time.Minute), wait.WithContext(ctx))
			if err != nil {
				t.Fatalf("kind-worker node not created: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			// Delete the pod
			_ = client.Resources().Delete(ctx, pod)
			// Delete the node so that each test keeps the cluster clean
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, scaleUpFeature)
}

// TestScaleUpPendingPodTooLarge verifies CA doesn't increase cluster size if pending pod is too large for any node group.
func TestScaleUpPendingPodTooLarge(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	// Pod requests 100 CPUs (node allocatable is 12 CPUs)
	pod := NewTestPodWithResources("too-large-pod", ns, "100", "100Mi")

	feature := features.New("Scale Up Pending Pod Too Large").
		Assess("shouldn't increase cluster size if pending pod is too large", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			err = client.Resources().Create(ctx, pod)
			if err != nil {
				t.Fatalf("failed to create pod: %v", err)
			}

			// Wait for NotTriggerScaleUp event
			err = WaitForPodEvent(ctx, client, ns, pod.Name, "NotTriggerScaleUp", 1*time.Minute)
			if err != nil {
				t.Fatalf("expected NotTriggerScaleUp event: %v", err)
			}

			// Verify that cluster size is not changed
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
			_ = client.Resources().Delete(ctx, pod)
			_ = WaitForPodDeleted(ctx, client, pod, podDeletionTimeout)
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleUpNoAdditionalScaleUpsDuringProcessing verifies CA does not trigger extra scale-ups once target nodes are added.
func TestScaleUpNoAdditionalScaleUpsDuringProcessing(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	pod := NewTestPodWithResources("stabilize-pod", ns, "500m", "500Mi")

	feature := features.New("Scale Up No Additional Scale-Ups").
		Assess("shouldn't trigger additional scale-ups during processing scale-up", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			err = client.Resources().Create(ctx, pod)
			if err != nil {
				t.Fatalf("failed to create pod: %v", err)
			}

			// Cluster should scale up to exactly 1 node
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not reach 1 node: %v", err)
			}

			err = WaitForPodScheduled(ctx, client, pod, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod not scheduled: %v", err)
			}

			// Verify that cluster size remains consistently 1 (no extra scale-ups triggered)
			err = WaitForNodeCountConsistently(ctx, client, defaultNodeGroup, 1, 15*time.Second)
			if err != nil {
				t.Fatalf("unexpected additional scale-up triggered: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, pod)
			_ = WaitForPodDeleted(ctx, client, pod, podDeletionTimeout)
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleUpHostPortConflict verifies CA increases cluster size when pods conflict on host ports.
func TestScaleUpHostPortConflict(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	pod1 := NewTestPodWithHostPort("hostport-pod-1", ns, "100m", "100Mi", 8080)
	pod2 := NewTestPodWithHostPort("hostport-pod-2", ns, "100m", "100Mi", 8080)

	feature := features.New("Scale Up Host Port Conflict").
		Assess("should increase cluster size if pods are pending due to host port conflict", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Create both pods with the same hostPort
			for _, pod := range []*corev1.Pod{pod1, pod2} {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			// CA should scale up 2 nodes because hostPort cannot be shared on 1 node
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 2, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}

			for _, pod := range []*corev1.Pod{pod1, pod2} {
				err = WaitForPodScheduled(ctx, client, pod, podSchedulingTimeout)
				if err != nil {
					t.Fatalf("pod %s was not scheduled: %v", pod.Name, err)
				}
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pod1, pod2}, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleUpPodAntiAffinity verifies CA increases cluster size when pods have mutual anti-affinity.
func TestScaleUpPodAntiAffinity(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	pod1 := NewTestPodWithAntiAffinity("anti-affinity-pod-1", ns, "100m", "100Mi", "anti-affinity", "yes")
	pod2 := NewTestPodWithAntiAffinity("anti-affinity-pod-2", ns, "100m", "100Mi", "anti-affinity", "yes")

	feature := features.New("Scale Up Pod Anti-Affinity").
		Assess("should increase cluster size if pods are pending due to pod anti-affinity", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			for _, pod := range []*corev1.Pod{pod1, pod2} {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			// CA should scale up to 2 nodes due to anti-affinity
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 2, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}

			for _, pod := range []*corev1.Pod{pod1, pod2} {
				err = WaitForPodScheduled(ctx, client, pod, podSchedulingTimeout)
				if err != nil {
					t.Fatalf("pod %s was not scheduled: %v", pod.Name, err)
				}
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pod1, pod2}, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleUpEmptyDirVolume verifies CA increases cluster size when pending pod requests EmptyDir volume.
func TestScaleUpEmptyDirVolume(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	pod1 := NewTestPodWithAntiAffinity("emptydir-base-pod", ns, "100m", "100Mi", "anti-affinity-emptydir", "yes")
	pod2 := NewTestPodWithEmptyDirAndAntiAffinity("emptydir-volume-pod", ns, "100m", "100Mi", "anti-affinity-emptydir", "yes")

	feature := features.New("Scale Up EmptyDir Volume").
		Assess("should increase cluster size if pod requesting EmptyDir volume is pending", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			for _, pod := range []*corev1.Pod{pod1, pod2} {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 2, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}

			for _, pod := range []*corev1.Pod{pod1, pod2} {
				err = WaitForPodScheduled(ctx, client, pod, podSchedulingTimeout)
				if err != nil {
					t.Fatalf("pod %s was not scheduled: %v", pod.Name, err)
				}
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pod1, pod2}, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleUpExpendablePodCreated verifies CA does not scale up when an expendable pod is created.
func TestScaleUpExpendablePodCreated(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	pod := NewTestPodWithPriority("expendable-pod", ns, "500m", "500Mi", expendablePriorityClassName)

	feature := features.New("Scale Up Expendable Pod Created").
		Assess("shouldn't scale up when expendable pod is created", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			_ = EnsurePriorityClasses(ctx, client)

			err = client.Resources().Create(ctx, pod)
			if err != nil {
				t.Fatalf("failed to create expendable pod: %v", err)
			}

			// Cluster size must remain 0
			err = WaitForNodeCountConsistently(ctx, client, defaultNodeGroup, 0, 15*time.Second)
			if err != nil {
				t.Fatalf("cluster scaled up for expendable pod: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, pod)
			_ = WaitForPodDeleted(ctx, client, pod, podDeletionTimeout)
			DeletePriorityClasses(ctx, client)
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleUpNonExpendablePodCreated verifies CA scales up when a non-expendable pod is created.
func TestScaleUpNonExpendablePodCreated(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	pod := NewTestPodWithPriority("non-expendable-pod", ns, "500m", "500Mi", highPriorityClassName)

	feature := features.New("Scale Up Non Expendable Pod Created").
		Assess("should scale up when non expendable pod is created", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			_ = EnsurePriorityClasses(ctx, client)

			err = client.Resources().Create(ctx, pod)
			if err != nil {
				t.Fatalf("failed to create non-expendable pod: %v", err)
			}

			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}

			err = WaitForPodScheduled(ctx, client, pod, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod not scheduled: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, pod)
			_ = WaitForPodDeleted(ctx, client, pod, podDeletionTimeout)
			DeletePriorityClasses(ctx, client)
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleUpExpendablePodPreempted verifies CA does not scale up when an expendable pod is preempted.
func TestScaleUpExpendablePodPreempted(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	basePod := NewTestPodWithResources("preemption-base-pod", ns, "100m", "100Mi")
	expendableRS := NewTestReplicaSetWithPriority("expendable-rs", ns, 1, "10000m", "100Mi", "app", "expendable-rs", expendablePriorityClassName)
	highPriorityPod := NewTestPodWithPriority("preempting-high-priority-pod", ns, "10000m", "100Mi", highPriorityClassName)

	feature := features.New("Scale Up Expendable Pod Preempted").
		Assess("shouldn't scale up when expendable pod is preempted", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			_ = EnsurePriorityClasses(ctx, client)

			// Step 1: Scale up 1 node with basePod
			err = client.Resources().Create(ctx, basePod)
			if err != nil {
				t.Fatalf("failed to create base pod: %v", err)
			}
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}
			err = WaitForPodScheduled(ctx, client, basePod, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("base pod not scheduled: %v", err)
			}

			// Step 2: Schedule expendable ReplicaSet pod on the node
			err = client.Resources().Create(ctx, expendableRS)
			if err != nil {
				t.Fatalf("failed to create expendable RS: %v", err)
			}
			err = WaitForPodsWithLabelScheduled(ctx, client, ns, "app", "expendable-rs", 1, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("expendable pod was not scheduled: %v", err)
			}

			// Step 3: Create high-priority pod that preempts the expendable pod on the node
			err = client.Resources().Create(ctx, highPriorityPod)
			if err != nil {
				t.Fatalf("failed to create high-priority pod: %v", err)
			}
			err = WaitForPodScheduled(ctx, client, highPriorityPod, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("high-priority pod was not scheduled: %v", err)
			}

			// Step 4: The preempted expendable pod is recreated by RS and stays unschedulable.
			// CA must NOT scale up for the preempted expendable pod. Node count remains 1.
			err = WaitForNodeCountConsistently(ctx, client, defaultNodeGroup, 1, 15*time.Second)
			if err != nil {
				t.Fatalf("cluster scaled up for preempted expendable pod: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, expendableRS)
			_ = DeletePodsWithLabel(ctx, client, ns, "app", "expendable-rs")
			_ = client.Resources().Delete(ctx, highPriorityPod)
			_ = client.Resources().Delete(ctx, basePod)
			_ = WaitForPodDeleted(ctx, client, basePod, podDeletionTimeout)
			DeletePriorityClasses(ctx, client)
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleUpUnprocessedPodUnschedulable verifies CA scales up when unprocessed pod is created with bypassed scheduler.
func TestScaleUpUnprocessedPodUnschedulable(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	pod := NewTestPodWithScheduler("unprocessed-unschedulable-pod", ns, "500m", "500Mi", nonExistingBypassedSchedulerName)

	feature := features.New("Scale Up Unprocessed Pod Unschedulable").
		Assess("should scale up when unprocessed pod is created and is going to be unschedulable", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			err = client.Resources().Create(ctx, pod)
			if err != nil {
				t.Fatalf("failed to create unprocessed pod: %v", err)
			}

			// Cluster should scale up to 1 node because CA bypasses scheduler check for this scheduler
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node for bypassed scheduler pod: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, pod)
			_ = WaitForPodDeleted(ctx, client, pod, podDeletionTimeout)
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleUpUnprocessedPodSchedulable verifies CA does not scale up when unprocessed pod is schedulable on existing nodes.
func TestScaleUpUnprocessedPodSchedulable(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	basePod := NewTestPodWithResources("schedulable-base-pod", ns, "100m", "100Mi")
	unprocessedPod := NewTestPodWithScheduler("unprocessed-schedulable-pod", ns, "500m", "500Mi", nonExistingBypassedSchedulerName)

	feature := features.New("Scale Up Unprocessed Pod Schedulable").
		Assess("shouldn't scale up when unprocessed pod is created and is going to be schedulable", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Step 1: Scale up 1 node
			err = client.Resources().Create(ctx, basePod)
			if err != nil {
				t.Fatalf("failed to create base pod: %v", err)
			}
			err = WaitForNodesReady(ctx, client, defaultNodeGroup, 1, nodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}
			err = WaitForPodScheduled(ctx, client, basePod, podSchedulingTimeout)
			if err != nil {
				t.Fatalf("base pod not scheduled: %v", err)
			}

			// Step 2: Create unprocessed pod that fits on the existing node (~11.9 cores free)
			err = client.Resources().Create(ctx, unprocessedPod)
			if err != nil {
				t.Fatalf("failed to create unprocessed pod: %v", err)
			}

			// Step 3: Verify node count remains 1 (CA does not scale up since pod is schedulable)
			err = WaitForNodeCountConsistently(ctx, client, defaultNodeGroup, 1, 15*time.Second)
			if err != nil {
				t.Fatalf("cluster scaled up unexpectedly: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, unprocessedPod)
			_ = client.Resources().Delete(ctx, basePod)
			_ = WaitForPodDeleted(ctx, client, basePod, podDeletionTimeout)
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

// TestScaleUpUnprocessedPodSchedulerNotBypassed verifies CA does not scale up when unprocessed pod targets a non-bypassed scheduler.
func TestScaleUpUnprocessedPodSchedulerNotBypassed(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	nonBypassedScheduler := "non-existing-custom-scheduler"
	pod := NewTestPodWithScheduler("unprocessed-not-bypassed-pod", ns, "500m", "500Mi", nonBypassedScheduler)

	feature := features.New("Scale Up Unprocessed Pod Scheduler Not Bypassed").
		Assess("shouldn't scale up when unprocessed pod is created and scheduler is not specified to be bypassed", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			err = client.Resources().Create(ctx, pod)
			if err != nil {
				t.Fatalf("failed to create pod: %v", err)
			}

			// Verify node count remains 0 because the scheduler is not bypassed
			err = WaitForNodeCountConsistently(ctx, client, defaultNodeGroup, 0, 15*time.Second)
			if err != nil {
				t.Fatalf("cluster scaled up for non-bypassed scheduler pod: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, pod)
			_ = WaitForPodDeleted(ctx, client, pod, podDeletionTimeout)
			_ = CleanUpNodeGroup(ctx, client, defaultNodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}
