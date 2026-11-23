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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

const (
	// draDriverName must remain constant (with 1 pool per node) across all DRA E2E tests
	// sharing testCfg.DRANodeGroup so that DraCustomResourcesProcessor.FilterOutNodesWithUnreadyResources
	// considers new seed nodes ready and allows MixedTemplateNodeInfoProvider to refresh the cached template.
	draDriverName       = "dra.e2e.example.com"
	draSliceLabelKey    = "dra-e2e-driver"
	draPodClaimName     = "device-claim"
	draClaimRequestName = "req-0"
)

// draTestEnv manages a dedicated KWOK node group seeded with a manual DRA node and a
// background reconciler that attaches matching ResourceSlices to any nodes KWOK scales up.
type draTestEnv struct {
	nodeGroup      string
	seedNode       *corev1.Node
	anchorPod      *corev1.Pod
	deviceClasses  []*resourcev1.DeviceClass
	claimTemplates []*resourcev1.ResourceClaimTemplate
	cancelWatcher  context.CancelFunc
	watcherDone    chan struct{}
}

func newDRATestEnv() *draTestEnv {
	return &draTestEnv{
		nodeGroup: testCfg.DRANodeGroup,
	}
}

// setup creates a manually seeded KWOK node with ResourceSlices in testCfg.DRANodeGroup so CA's
// TemplateNodeInfoRegistry caches DRA LocalResourceSlices instead of falling back to KWOK's
// slice-less TemplateNodeInfo(), and starts a background reconciler to publish matching
// ResourceSlices for newly scaled-up nodes.
func (d *draTestEnv) setup(ctx context.Context, t *testing.T, client klient.Client, namespace string, devices []resourcev1.Device) {
	t.Helper()

	if err := resourcev1.AddToScheme(client.Resources().GetScheme()); err != nil {
		t.Fatalf("failed to register resource.k8s.io/v1 scheme: %v", err)
	}

	// Clean any leftover ResourceSlices from previous runs before creating the seed node.
	deleteDRAResourceSlices(ctx, client)

	seedNodeName := envconf.RandomName("dra-seed", 14)
	seedNode := newDRASeedNode(seedNodeName, d.nodeGroup)
	if err := client.Resources().Create(ctx, seedNode); err != nil {
		t.Fatalf("failed to create DRA seed node %s: %v", seedNodeName, err)
	}
	d.seedNode = seedNode

	// Create the seed node's ResourceSlice before marking it Ready so CA never observes
	// a Ready seed node missing its expected ResourceSlice.
	if err := ensureNodeResourceSlice(ctx, client, seedNode, devices); err != nil {
		t.Fatalf("failed to create ResourceSlice for seed node %s: %v", seedNodeName, err)
	}

	// Backdate Ready LastTransitionTime by >1m so MixedTemplateNodeInfoProvider immediately
	// treats the seed node as a goodCandidate (bypassing its 1m stabilizationDelay).
	if err := ensureSeedNodeReadyAndStable(ctx, client, seedNodeName); err != nil {
		t.Fatalf("failed to set stable Ready status on seed node %s: %v", seedNodeName, err)
	}

	// Pin an anchor pod consuming 90% CPU/memory on the seed node (leaving 10% headroom
	// for kube system pods, while still blocking 15%-CPU DRA test pods from scheduling or
	// relocating onto it). Because it is a regular non-DaemonSet pod with no DRA claims,
	// SanitizedTemplateNodeInfoFromNodeInfo strips it from the cached template.
	anchorPod := newDRAAnchorPod(seedNodeName+"-anchor", namespace, seedNodeName)
	if err := client.Resources().Create(ctx, anchorPod); err != nil {
		t.Fatalf("failed to create anchor pod on seed node %s: %v", seedNodeName, err)
	}
	d.anchorPod = anchorPod

	// Start a background reconciler that creates ResourceSlices for any KWOK nodes created
	// when CA scales up testCfg.DRANodeGroup.
	watcherCtx, cancel := context.WithCancel(ctx)
	d.cancelWatcher = cancel
	d.watcherDone = make(chan struct{})
	go func() {
		defer close(d.watcherDone)
		reconcileDRAResourceSlices(watcherCtx, client, d.nodeGroup, devices)
	}()
}

func (d *draTestEnv) createDeviceClass(ctx context.Context, t *testing.T, client klient.Client, name, celExpr string) *resourcev1.DeviceClass {
	t.Helper()
	dc := &resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: resourcev1.DeviceClassSpec{
			Selectors: []resourcev1.DeviceSelector{
				{
					CEL: &resourcev1.CELDeviceSelector{
						Expression: celExpr,
					},
				},
			},
		},
	}
	if err := client.Resources().Create(ctx, dc); err != nil {
		t.Fatalf("failed to create DeviceClass %s: %v", name, err)
	}
	d.deviceClasses = append(d.deviceClasses, dc)
	return dc
}

func (d *draTestEnv) createClaimTemplate(ctx context.Context, t *testing.T, client klient.Client, name, namespace, deviceClassName string, count int64) *resourcev1.ResourceClaimTemplate {
	t.Helper()
	rct := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: resourcev1.ResourceClaimTemplateSpec{
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{
						{
							Name: draClaimRequestName,
							Exactly: &resourcev1.ExactDeviceRequest{
								DeviceClassName: deviceClassName,
								AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
								Count:           count,
							},
						},
					},
				},
			},
		},
	}
	if err := client.Resources().Create(ctx, rct); err != nil {
		t.Fatalf("failed to create ResourceClaimTemplate %s: %v", name, err)
	}
	d.claimTemplates = append(d.claimTemplates, rct)
	return rct
}

func (d *draTestEnv) teardown(ctx context.Context, client klient.Client, pods []*corev1.Pod, replicaSets []*appsv1.ReplicaSet) {
	if d.cancelWatcher != nil {
		d.cancelWatcher()
		<-d.watcherDone
	}

	for _, rs := range replicaSets {
		if rs != nil {
			_ = client.Resources().Delete(ctx, rs)
		}
	}

	allPods := append([]*corev1.Pod{}, pods...)
	if d.anchorPod != nil {
		allPods = append(allPods, d.anchorPod)
	}
	for _, pod := range allPods {
		if pod != nil {
			_ = client.Resources().Delete(ctx, pod)
		}
	}
	_ = WaitForPodsDeleted(ctx, client, allPods, testCfg.PodDeletionTimeout)

	for _, rct := range d.claimTemplates {
		if rct != nil {
			_ = client.Resources().Delete(ctx, rct)
		}
	}
	for _, dc := range d.deviceClasses {
		if dc != nil {
			_ = client.Resources().Delete(ctx, dc)
		}
	}

	_ = CleanUpNodeGroup(ctx, client, d.nodeGroup)
	deleteDRAResourceSlices(ctx, client)
}

func newDRASeedNode(name, nodeGroup string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				testCfg.NodeGroupLabelKey: nodeGroup,
				corev1.LabelHostname:      name,
				corev1.LabelOSStable:      "linux",
				corev1.LabelArchStable:    "amd64",
			},
			Annotations: map[string]string{
				"cluster-autoscaler.kwok.nodegroup/name":               nodeGroup,
				"kwok.x-k8s.io/node":                                   "fake",
				"cluster-autoscaler.kubernetes.io/scale-down-disabled": "true",
			},
		},
		Spec: corev1.NodeSpec{
			ProviderID: "kwok:" + name,
			Taints: []corev1.Taint{
				{
					Key:    "kwok-provider",
					Value:  "true",
					Effect: corev1.TaintEffectNoSchedule,
				},
			},
		},
	}
}

func ensureSeedNodeReadyAndStable(ctx context.Context, client klient.Client, nodeName string) error {
	applyBackdatedStatus := func() error {
		return wait.For(func(ctx context.Context) (bool, error) {
			node := &corev1.Node{}
			if err := client.Resources().Get(ctx, nodeName, "", node); err != nil {
				return false, err
			}
			now := metav1.Now()
			past := metav1.NewTime(now.Add(-2 * time.Minute))
			resources := corev1.ResourceList{
				corev1.ResourceCPU:              testCfg.NodeCPU.DeepCopy(),
				corev1.ResourceMemory:           resource.MustParse("32781516Ki"),
				corev1.ResourceEphemeralStorage: resource.MustParse("959786032Ki"),
				corev1.ResourcePods:             resource.MustParse("110"),
			}
			node.Status.Phase = corev1.NodeRunning
			node.Status.Capacity = resources
			node.Status.Allocatable = resources.DeepCopy()
			node.Status.Conditions = []corev1.NodeCondition{
				{
					Type:               corev1.NodeMemoryPressure,
					Status:             corev1.ConditionFalse,
					LastHeartbeatTime:  now,
					LastTransitionTime: past,
					Reason:             "KubeletHasSufficientMemory",
					Message:            "kubelet has sufficient memory available",
				},
				{
					Type:               corev1.NodeDiskPressure,
					Status:             corev1.ConditionFalse,
					LastHeartbeatTime:  now,
					LastTransitionTime: past,
					Reason:             "KubeletHasNoDiskPressure",
					Message:            "kubelet has no disk pressure",
				},
				{
					Type:               corev1.NodePIDPressure,
					Status:             corev1.ConditionFalse,
					LastHeartbeatTime:  now,
					LastTransitionTime: past,
					Reason:             "KubeletHasSufficientPID",
					Message:            "kubelet has sufficient PID available",
				},
				{
					Type:               corev1.NodeReady,
					Status:             corev1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: past,
					Reason:             "KubeletReady",
					Message:            "kubelet is posting ready status",
				},
			}
			err := client.Resources().UpdateStatus(ctx, node)
			if apierrors.IsConflict(err) {
				return false, nil
			}
			return err == nil, err
		}, wait.WithTimeout(15*time.Second), wait.WithInterval(200*time.Millisecond), wait.WithImmediate(), wait.WithContext(ctx))
	}

	if err := applyBackdatedStatus(); err != nil {
		return err
	}

	// Verify kwok-stage-fast's node-initialize stage did not race and reset Ready.LastTransitionTime.
	return wait.For(func(ctx context.Context) (bool, error) {
		node := &corev1.Node{}
		if err := client.Resources().Get(ctx, nodeName, "", node); err != nil {
			return false, err
		}
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
				if time.Since(cond.LastTransitionTime.Time) > 70*time.Second {
					return true, nil
				}
				break
			}
		}
		if err := applyBackdatedStatus(); err != nil {
			return false, err
		}
		return false, nil
	}, wait.WithTimeout(15*time.Second), wait.WithInterval(500*time.Millisecond), wait.WithContext(ctx))
}

func newDRAAnchorPod(name, namespace, seedNodeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app": name,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:                      seedNodeName,
			TerminationGracePeriodSeconds: new(int64),
			Containers: []corev1.Container{
				{
					Name:  "anchor",
					Image: "fake-image",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(testCfg.CalculateCPURequest(0.90)),
							corev1.ResourceMemory: resource.MustParse(testCfg.CalculateMemoryRequest(0.90)),
						},
					},
				},
			},
			Tolerations: testCfg.Tolerations,
		},
	}
}

func newDRATestPod(name, namespace, claimTemplateName string) *corev1.Pod {
	// Request 15% CPU so the pod cannot fit alongside the 90%-CPU anchor pod on the seed node,
	// while still allowing multiple pods to binpack on newly scaled-up DRA nodes.
	pod := NewTestPodWithResourceFraction(name, namespace, 0.15, 0.02)
	pod.Spec.NodeSelector = map[string]string{
		testCfg.NodeGroupLabelKey: testCfg.DRANodeGroup,
	}
	pod.Spec.ResourceClaims = []corev1.PodResourceClaim{
		{
			Name:                      draPodClaimName,
			ResourceClaimTemplateName: ptr.To(claimTemplateName),
		},
	}
	pod.Spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{
		{
			Name: draPodClaimName,
		},
	}
	return pod
}

func newDRATestReplicaSet(name, namespace string, replicas int32, claimTemplateName, labelKey, labelVal string) *appsv1.ReplicaSet {
	rs := NewTestReplicaSet(name, namespace, replicas, 0.15, 0.02, labelKey, labelVal)
	rs.Spec.Template.Spec.NodeSelector = map[string]string{
		testCfg.NodeGroupLabelKey: testCfg.DRANodeGroup,
	}
	rs.Spec.Template.Spec.ResourceClaims = []corev1.PodResourceClaim{
		{
			Name:                      draPodClaimName,
			ResourceClaimTemplateName: ptr.To(claimTemplateName),
		},
	}
	rs.Spec.Template.Spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{
		{
			Name: draPodClaimName,
		},
	}
	return rs
}

func draDevices(count int, attrs map[resourcev1.QualifiedName]resourcev1.DeviceAttribute) []resourcev1.Device {
	devices := make([]resourcev1.Device, 0, count)
	for i := 0; i < count; i++ {
		var copiedAttrs map[resourcev1.QualifiedName]resourcev1.DeviceAttribute
		if len(attrs) > 0 {
			copiedAttrs = make(map[resourcev1.QualifiedName]resourcev1.DeviceAttribute, len(attrs))
			for k, v := range attrs {
				copiedAttrs[k] = *v.DeepCopy()
			}
		}
		devices = append(devices, resourcev1.Device{
			Name:       fmt.Sprintf("dev-%d", i),
			Attributes: copiedAttrs,
		})
	}
	return devices
}

func ensureNodeResourceSlice(ctx context.Context, client klient.Client, node *corev1.Node, devices []resourcev1.Device) error {
	copiedDevices := make([]resourcev1.Device, len(devices))
	for i := range devices {
		devices[i].DeepCopyInto(&copiedDevices[i])
	}

	slice := &resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: node.Name + "-slice",
			Labels: map[string]string{
				draSliceLabelKey: draDriverName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Node",
					Name:       node.Name,
					UID:        node.UID,
				},
			},
		},
		Spec: resourcev1.ResourceSliceSpec{
			Driver:   draDriverName,
			NodeName: ptr.To(node.Name),
			Pool: resourcev1.ResourcePool{
				Name:               node.Name + "-pool",
				Generation:         1,
				ResourceSliceCount: 1,
			},
			Devices: copiedDevices,
		},
	}
	err := client.Resources().Create(ctx, slice)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func reconcileDRAResourceSlices(ctx context.Context, client klient.Client, nodeGroup string, devices []resourcev1.Device) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	reconcileOnce := func() {
		nodeList := &corev1.NodeList{}
		if err := client.Resources().List(ctx, nodeList); err != nil {
			return
		}
		sliceList := &resourcev1.ResourceSliceList{}
		if err := client.Resources().List(ctx, sliceList); err != nil {
			return
		}
		existingSlices := make(map[string]struct{}, len(sliceList.Items))
		for i := range sliceList.Items {
			existingSlices[sliceList.Items[i].Name] = struct{}{}
		}
		for i := range nodeList.Items {
			node := &nodeList.Items[i]
			if node.Labels[testCfg.NodeGroupLabelKey] != nodeGroup || node.DeletionTimestamp != nil {
				continue
			}
			if _, exists := existingSlices[node.Name+"-slice"]; exists {
				continue
			}
			_ = ensureNodeResourceSlice(ctx, client, node, devices)
		}
	}

	reconcileOnce()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconcileOnce()
		}
	}
}

func deleteDRAResourceSlices(ctx context.Context, client klient.Client) {
	sliceList := &resourcev1.ResourceSliceList{}
	if err := client.Resources().List(ctx, sliceList); err != nil {
		return
	}
	for i := range sliceList.Items {
		s := &sliceList.Items[i]
		if s.Labels[draSliceLabelKey] == draDriverName || s.Spec.Driver == draDriverName {
			_ = client.Resources().Delete(ctx, s)
		}
	}
}

func TestDRAScaleUpAndScaleDownEmptyNode(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	draEnv := newDRATestEnv()
	dcName := envconf.RandomName("dra-class", 14)
	rctName := envconf.RandomName("dra-rct", 14)
	pod := newDRATestPod("dra-basic-pod", ns, rctName)

	feature := features.New("DRA Scale Up And Scale Down Empty Node").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			draEnv.setup(ctx, t, client, ns, draDevices(1, nil))
			draEnv.createDeviceClass(ctx, t, client, dcName, fmt.Sprintf("device.driver == %q", draDriverName))
			draEnv.createClaimTemplate(ctx, t, client, rctName, ns, dcName, 1)
			return ctx
		}).
		Assess("scale up for DRA pod and scale down empty node after deletion", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			if err := client.Resources().Create(ctx, pod); err != nil {
				t.Fatalf("failed to create DRA pod: %v", err)
			}

			if err := WaitForPodEvent(ctx, client, pod, "TriggeredScaleUp", testCfg.PodSchedulingTimeout); err != nil {
				t.Fatalf("TriggeredScaleUp event not found: %v", err)
			}

			// Expect 2 nodes total: 1 seed node + 1 newly scaled-up DRA node.
			if err := WaitForNodesReady(ctx, client, draEnv.nodeGroup, 2, testCfg.NodeReadyTimeout); err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes in %s: %v", draEnv.nodeGroup, err)
			}

			if err := WaitForPodScheduled(ctx, client, pod, testCfg.PodSchedulingTimeout); err != nil {
				t.Fatalf("DRA pod was not scheduled: %v", err)
			}

			scheduledPod := &corev1.Pod{}
			if err := client.Resources().Get(ctx, pod.Name, pod.Namespace, scheduledPod); err != nil {
				t.Fatalf("failed to fetch scheduled DRA pod: %v", err)
			}
			if scheduledPod.Spec.NodeName == draEnv.seedNode.Name {
				t.Fatalf("expected DRA pod to schedule on newly scaled-up node, got seed node %s", draEnv.seedNode.Name)
			}

			// Delete the DRA pod and verify CA scales down the empty DRA node back to 1 (seed node only).
			if err := client.Resources().Delete(ctx, pod); err != nil {
				t.Fatalf("failed to delete DRA pod: %v", err)
			}
			if err := WaitForPodDeleted(ctx, client, pod, testCfg.PodDeletionTimeout); err != nil {
				t.Fatalf("DRA pod was not deleted: %v", err)
			}

			if err := WaitForNodeCount(ctx, client, draEnv.nodeGroup, 1, testCfg.ScaleDownTimeout); err != nil {
				t.Fatalf("empty DRA node was not scaled down back to seed node: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			draEnv.teardown(ctx, client, []*corev1.Pod{pod}, nil)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestDRAScaleUpMultiplePodsBinpacking(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	draEnv := newDRATestEnv()
	dcName := envconf.RandomName("dra-class", 14)
	rctName := envconf.RandomName("dra-rct", 14)

	// Each node exposes 2 devices and each pod requests 1 device, so 3 pods require exactly
	// 2 new nodes (3 nodes total including the occupied seed node).
	var pods []*corev1.Pod
	for i := 1; i <= 3; i++ {
		pods = append(pods, newDRATestPod(fmt.Sprintf("dra-binpack-pod-%d", i), ns, rctName))
	}

	feature := features.New("DRA Scale Up Multiple Pods Binpacking").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			draEnv.setup(ctx, t, client, ns, draDevices(2, nil))
			draEnv.createDeviceClass(ctx, t, client, dcName, fmt.Sprintf("device.driver == %q", draDriverName))
			draEnv.createClaimTemplate(ctx, t, client, rctName, ns, dcName, 1)
			return ctx
		}).
		Assess("binpack 3 single-device pods onto exactly 2 new two-device nodes", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			for _, pod := range pods {
				if err := client.Resources().Create(ctx, pod); err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			if err := WaitForNodesReady(ctx, client, draEnv.nodeGroup, 3, testCfg.NodeReadyTimeout); err != nil {
				t.Fatalf("cluster did not scale up to 3 nodes in %s: %v", draEnv.nodeGroup, err)
			}

			if err := WaitForPodsScheduled(ctx, client, pods, testCfg.PodSchedulingTimeout); err != nil {
				t.Fatalf("DRA pods were not scheduled: %v", err)
			}

			if err := WaitForNodeCountConsistently(ctx, client, draEnv.nodeGroup, 3, testCfg.NoScaleUpWindow); err != nil {
				t.Fatalf("unexpected node count change after DRA binpacking scale-up: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			draEnv.teardown(ctx, client, pods, nil)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestDRAScaleUpUnsatisfiableClaim(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	draEnv := newDRATestEnv()
	dcName := envconf.RandomName("dra-class", 14)
	rctName := envconf.RandomName("dra-rct", 14)
	pod := newDRATestPod("dra-unsatisfiable-pod", ns, rctName)

	feature := features.New("DRA Scale Up Unsatisfiable Claim").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			// Seed nodes offer model="gpu-a", while the DeviceClass requires model="gpu-b".
			attrs := map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
				"model": {StringValue: ptr.To("gpu-a")},
			}
			draEnv.setup(ctx, t, client, ns, draDevices(1, attrs))
			celExpr := fmt.Sprintf("device.driver == %q && device.attributes[%q].model == %q", draDriverName, draDriverName, "gpu-b")
			draEnv.createDeviceClass(ctx, t, client, dcName, celExpr)
			draEnv.createClaimTemplate(ctx, t, client, rctName, ns, dcName, 1)
			return ctx
		}).
		Assess("do not scale up when no template device satisfies the DRA claim", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			if err := client.Resources().Create(ctx, pod); err != nil {
				t.Fatalf("failed to create DRA pod: %v", err)
			}

			if err := WaitForPodEvent(ctx, client, pod, "NotTriggerScaleUp", testCfg.PodSchedulingTimeout); err != nil {
				t.Fatalf("NotTriggerScaleUp event not found: %v", err)
			}

			if err := WaitForNodeCountConsistently(ctx, client, draEnv.nodeGroup, 1, testCfg.NoScaleUpWindow); err != nil {
				t.Fatalf("cluster unexpectedly scaled up for unsatisfiable DRA claim: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			draEnv.teardown(ctx, client, []*corev1.Pod{pod}, nil)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestDRAScaleDownDrainRelocation(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()
	draEnv := newDRATestEnv()
	dcName := envconf.RandomName("dra-class", 14)
	rctName := envconf.RandomName("dra-rct", 14)

	const (
		rsLabelKey = "app"
		rsLabelVal = "dra-drain-rs"
	)

	// Each node exposes 2 devices. We place:
	// 1. fillerPod (1 device) -> triggers Node A (2 nodes total).
	// 2. ReplicaSet with 1 replica (1 device, safe-to-evict=true) -> lands on Node A (2/2 devices).
	// 3. Scale ReplicaSet to 2 replicas -> triggers Node B (3 nodes total), 2nd replica lands on Node B (1/2 devices).
	// 4. Delete fillerPod -> Node A and Node B each run 1 ReplicaSet pod (1/2 = 0.5 <= scale-down threshold).
	// 5. CA drains one node, relocates its DRA pod onto the other DRA node, and scales down to 2 nodes.
	fillerPod := newDRATestPod("dra-drain-filler", ns, rctName)
	rs := newDRATestReplicaSet("dra-drain-rs", ns, 1, rctName, rsLabelKey, rsLabelVal)

	feature := features.New("DRA Scale Down Drain And Relocation").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			draEnv.setup(ctx, t, client, ns, draDevices(2, nil))
			draEnv.createDeviceClass(ctx, t, client, dcName, fmt.Sprintf("device.driver == %q", draDriverName))
			draEnv.createClaimTemplate(ctx, t, client, rctName, ns, dcName, 1)
			return ctx
		}).
		Assess("drain underutilized DRA node and relocate pod onto another DRA node", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Step 1: Create fillerPod to scale up Node A (2 nodes total: seed + Node A).
			if err := client.Resources().Create(ctx, fillerPod); err != nil {
				t.Fatalf("failed to create filler pod: %v", err)
			}
			if err := WaitForNodesReady(ctx, client, draEnv.nodeGroup, 2, testCfg.NodeReadyTimeout); err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}
			if err := WaitForPodScheduled(ctx, client, fillerPod, testCfg.PodSchedulingTimeout); err != nil {
				t.Fatalf("filler pod was not scheduled: %v", err)
			}

			// Step 2: Create ReplicaSet with 1 replica; it fits on Node A's second device.
			if err := client.Resources().Create(ctx, rs); err != nil {
				t.Fatalf("failed to create ReplicaSet: %v", err)
			}
			if err := WaitForPodsWithLabelScheduled(ctx, client, ns, rsLabelKey, rsLabelVal, 1, testCfg.PodSchedulingTimeout); err != nil {
				t.Fatalf("first ReplicaSet DRA pod was not scheduled: %v", err)
			}

			// Step 3: Scale ReplicaSet to 2 replicas; Node A is full (2/2 devices), so CA adds Node B.
			err = wait.For(func(ctx context.Context) (bool, error) {
				current := &appsv1.ReplicaSet{}
				if err := client.Resources().Get(ctx, rs.Name, rs.Namespace, current); err != nil {
					return false, err
				}
				current.Spec.Replicas = ptr.To(int32(2))
				err := client.Resources().Update(ctx, current)
				if apierrors.IsConflict(err) {
					return false, nil
				}
				return err == nil, err
			}, wait.WithTimeout(15*time.Second), wait.WithInterval(200*time.Millisecond), wait.WithImmediate(), wait.WithContext(ctx))
			if err != nil {
				t.Fatalf("failed to scale ReplicaSet to 2 replicas: %v", err)
			}

			if err := WaitForNodesReady(ctx, client, draEnv.nodeGroup, 3, testCfg.NodeReadyTimeout); err != nil {
				t.Fatalf("cluster did not scale up to 3 nodes: %v", err)
			}
			if err := WaitForPodsWithLabelScheduled(ctx, client, ns, rsLabelKey, rsLabelVal, 2, testCfg.PodSchedulingTimeout); err != nil {
				t.Fatalf("second ReplicaSet DRA pod was not scheduled: %v", err)
			}

			// Step 4: Delete fillerPod so both DRA nodes have 1/2 devices allocated (50% <= threshold).
			if err := client.Resources().Delete(ctx, fillerPod); err != nil {
				t.Fatalf("failed to delete filler pod: %v", err)
			}
			if err := WaitForPodDeleted(ctx, client, fillerPod, testCfg.PodDeletionTimeout); err != nil {
				t.Fatalf("filler pod was not deleted: %v", err)
			}

			// Step 5: Wait for CA to drain one DRA node and scale down from 3 nodes to 2 (seed + 1 DRA node).
			if err := WaitForNodeCount(ctx, client, draEnv.nodeGroup, 2, testCfg.ScaleDownTimeout); err != nil {
				t.Fatalf("cluster did not consolidate DRA nodes from 3 to 2: %v", err)
			}

			// Verify both ReplicaSet pods are scheduled on the single remaining non-seed DRA node.
			err = wait.For(func(ctx context.Context) (bool, error) {
				podList := &corev1.PodList{}
				if err := client.Resources(ns).List(ctx, podList); err != nil {
					return false, err
				}
				scheduledNodes := map[string]int{}
				scheduledCount := 0
				for _, p := range podList.Items {
					if p.Labels[rsLabelKey] == rsLabelVal && p.DeletionTimestamp == nil && p.Spec.NodeName != "" {
						scheduledNodes[p.Spec.NodeName]++
						scheduledCount++
					}
				}
				if scheduledCount != 2 || len(scheduledNodes) != 1 {
					return false, nil
				}
				_, onSeed := scheduledNodes[draEnv.seedNode.Name]
				return !onSeed, nil
			}, wait.WithTimeout(testCfg.PodSchedulingTimeout), wait.WithInterval(1*time.Second), wait.WithImmediate(), wait.WithContext(ctx))
			if err != nil {
				t.Fatalf("expected both ReplicaSet DRA pods to be consolidated onto one non-seed node: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, rs)
			_ = DeletePodsWithLabel(ctx, client, ns, rsLabelKey, rsLabelVal)
			draEnv.teardown(ctx, client, []*corev1.Pod{fillerPod}, nil)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}
