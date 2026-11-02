/*
Copyright 2023 The Kubernetes Authors.

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

package actuation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/budgets"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/deletiontracker"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/status"
	. "sigs.k8s.io/cluster-autoscaler/pkg/core/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type testIteration struct {
	toSchedule            []*budgets.NodeGroupView
	toAbort               []*budgets.NodeGroupView
	toScheduleAfterAbort  []*budgets.NodeGroupView
	drain                 bool
	pods                  map[string][]*apiv1.Pod
	failedPodDrain        map[string]bool
	wantScheduleErrs      map[string]bool
	wantEvictedPods       []string
	wantDeleted           int
	wantNodeDeleteResults map[string]status.NodeDeleteResult
}

type errorOptionsNodeGroup struct {
	*testprovider.TestNodeGroup
	err error
}

func (ng *errorOptionsNodeGroup) GetOptions(context.Context, config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	return nil, ng.err
}

func TestScheduleDeletion(t *testing.T) {
	testNg := testprovider.NewTestNodeGroup("test", 100, 0, 3, true, false, "n1-standard-2", nil, nil)
	atomic2 := sizedNodeGroup("atomic-2", 2, true, false)
	atomic4 := sizedNodeGroup("atomic-4", 4, true, false)
	errNg := &errorOptionsNodeGroup{
		TestNodeGroup: sizedNodeGroup("err-ng", 1, true, false),
		err:           fmt.Errorf("options unavailable"),
	}

	atomic4Pods := map[string][]*apiv1.Pod{
		"atomic-4-node-0": removablePods(1, "atomic-4-node-0"),
		"atomic-4-node-1": removablePods(1, "atomic-4-node-1"),
		"atomic-4-node-2": removablePods(1, "atomic-4-node-2"),
		"atomic-4-node-3": removablePods(1, "atomic-4-node-3"),
	}

	testCases := []struct {
		name       string
		iterations []testIteration
	}{
		{
			name: "no nodes",
			iterations: []testIteration{{
				toSchedule: []*budgets.NodeGroupView{},
			}},
		},
		{
			name: "individual nodes are deleted right away",
			iterations: []testIteration{{
				toSchedule:           generateNodeGroupViewList(testNg, 0, 3),
				toAbort:              generateNodeGroupViewList(testNg, 3, 6),
				toScheduleAfterAbort: generateNodeGroupViewList(testNg, 6, 9),
				wantDeleted:          6,
				wantNodeDeleteResults: map[string]status.NodeDeleteResult{
					"test-node-3": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
					"test-node-4": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
					"test-node-5": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
				},
			}},
		},
		{
			name: "whole atomic node groups deleted",
			iterations: []testIteration{{
				toSchedule: mergeLists(
					generateNodeGroupViewList(atomic4, 0, 1),
					generateNodeGroupViewList(atomic2, 0, 1),
					generateNodeGroupViewList(atomic4, 1, 2),
					generateNodeGroupViewList(atomic2, 1, 2),
					generateNodeGroupViewList(atomic4, 2, 4),
				),
				wantDeleted: 6,
			}},
		},
		{
			name: "atomic node group aborted in the process",
			iterations: []testIteration{{
				toSchedule: mergeLists(
					generateNodeGroupViewList(atomic4, 0, 1),
					generateNodeGroupViewList(atomic2, 0, 1),
					generateNodeGroupViewList(atomic4, 1, 2),
					generateNodeGroupViewList(atomic2, 1, 2),
				),
				toAbort:              generateNodeGroupViewList(atomic4, 2, 3),
				toScheduleAfterAbort: generateNodeGroupViewList(atomic4, 3, 4),
				wantScheduleErrs: map[string]bool{
					"atomic-4-node-3": true,
				},
				wantDeleted: 2,
				wantNodeDeleteResults: map[string]status.NodeDeleteResult{
					"atomic-4-node-0": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
					"atomic-4-node-1": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
					"atomic-4-node-2": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
					"atomic-4-node-3": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
				},
			}},
		},
		{
			name: "atomic node group aborted, next time successful",
			iterations: []testIteration{
				{
					toSchedule:           generateNodeGroupViewList(atomic4, 0, 2),
					toAbort:              generateNodeGroupViewList(atomic4, 2, 3),
					toScheduleAfterAbort: generateNodeGroupViewList(atomic4, 3, 4),
					wantScheduleErrs: map[string]bool{
						"atomic-4-node-3": true,
					},
					wantNodeDeleteResults: map[string]status.NodeDeleteResult{
						"atomic-4-node-0": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
						"atomic-4-node-1": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
						"atomic-4-node-2": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
						"atomic-4-node-3": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
					},
				},
				{
					toSchedule:  generateNodeGroupViewList(atomic4, 0, 4),
					wantDeleted: 4,
				},
			},
		},
		{
			name: "atomic node group drain failure returns error and skips eviction on subsequent nodes",
			iterations: []testIteration{
				{
					toSchedule:           generateNodeGroupViewList(atomic4, 0, 3),
					toScheduleAfterAbort: generateNodeGroupViewList(atomic4, 3, 4),
					drain:                true,
					pods:                 atomic4Pods,
					failedPodDrain: map[string]bool{
						"atomic-4-node-2-pod-0": true,
					},
					wantScheduleErrs: map[string]bool{
						"atomic-4-node-2": true,
						"atomic-4-node-3": true,
					},
					wantEvictedPods: []string{
						"atomic-4-node-0-pod-0",
						"atomic-4-node-1-pod-0",
					},
					wantNodeDeleteResults: map[string]status.NodeDeleteResult{
						"atomic-4-node-0": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
						"atomic-4-node-1": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
						"atomic-4-node-2": {
							ResultType: status.NodeDeleteErrorFailedToEvictPods,
							Err:        cmpopts.AnyError,
							PodEvictionResults: map[string]status.PodEvictionResult{
								"atomic-4-node-2-pod-0": {Pod: removablePod("atomic-4-node-2-pod-0", "atomic-4-node-2"), TimedOut: true, Err: cmpopts.AnyError},
							},
						},
						"atomic-4-node-3": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
					},
				},
				{
					toSchedule: generateNodeGroupViewList(atomic4, 0, 4),
					drain:      true,
					pods:       atomic4Pods,
					wantEvictedPods: []string{
						"atomic-4-node-0-pod-0",
						"atomic-4-node-1-pod-0",
						"atomic-4-node-2-pod-0",
						"atomic-4-node-3-pod-0",
					},
					wantDeleted: 4,
				},
			},
		},
		{
			name: "GetOptions error returns error and aborts node deletion",
			iterations: []testIteration{{
				toSchedule: generateNodeGroupViewList(errNg, 0, 1),
				wantScheduleErrs: map[string]bool{
					"err-ng-node-0": true,
				},
				wantNodeDeleteResults: map[string]status.NodeDeleteResult{
					"err-ng-node-0": {ResultType: status.NodeDeleteErrorInternal, Err: cmpopts.AnyError},
				},
			}},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			provider := testprovider.NewTestCloudProviderBuilder().WithOnScaleDown(func(nodeGroup string, node string) error {
				return nil
			}).Build()

			var currentFailedPodDrain map[string]bool
			var gotEvictedPods []string
			fakeClient := &fake.Clientset{}
			fakeClient.Fake.AddReactor("get", "pods", func(core.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewNotFound(apiv1.Resource("pod"), "missing")
			})
			fakeClient.Fake.AddReactor("create", "pods", func(action core.Action) (bool, runtime.Object, error) {
				eviction := action.(core.CreateAction).GetObject().(*policyv1beta1.Eviction)
				if currentFailedPodDrain[eviction.Name] {
					return true, nil, fmt.Errorf("simulated eviction error")
				}
				gotEvictedPods = append(gotEvictedPods, eviction.Name)
				return true, nil, nil
			})

			batcher := &countingBatcher{}
			tracker := deletiontracker.NewNodeDeletionTracker(0)
			opts := config.AutoscalingOptions{}
			podLister := kube_util.NewTestPodLister([]*apiv1.Pod{})
			pdbLister := kube_util.NewTestPodDisruptionBudgetLister([]*policyv1.PodDisruptionBudget{})
			dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
			if err != nil {
				t.Fatalf("Couldn't create daemonset lister")
			}
			registry := kube_util.NewListerRegistry(nil, nil, podLister, pdbLister, dsLister, nil, nil, nil, nil)
			autoscalingCtx, err := NewScaleTestAutoscalingContext(opts, fakeClient, registry, provider, nil, nil, nil)
			if err != nil {
				t.Fatalf("Couldn't set up autoscaling context: %v", err)
			}
			scheduler := NewGroupDeletionScheduler(&autoscalingCtx, tracker, batcher, Evictor{
				EvictionRetryTime:                0,
				PodEvictionHeadroom:              DefaultPodEvictionHeadroom,
				shutdownGracePeriodByPodPriority: SingleRuleDrainConfig(0),
			})

			for i, ti := range tc.iterations {
				currentFailedPodDrain = ti.failedPodDrain
				gotEvictedPods = nil

				allBuckets := append(append(ti.toSchedule, ti.toAbort...), ti.toScheduleAfterAbort...)
				for _, bucket := range allBuckets {
					switch ng := bucket.Group.(type) {
					case *testprovider.TestNodeGroup:
						ng.SetCloudProvider(provider)
					case *errorOptionsNodeGroup:
						ng.TestNodeGroup.SetCloudProvider(provider)
					}
					provider.InsertNodeGroup(bucket.Group)
					for _, node := range bucket.Nodes {
						provider.AddNode(bucket.Group.Id(), node)
					}
				}

				// ResetAndReportMetrics should be called before each scale-down phase
				scheduler.ResetAndReportMetrics()
				tracker.ClearResultsNotNewerThan(time.Now())

				ctx := test.GetTestContext(t)
				if err := scheduleAll(ctx, ti.toSchedule, scheduler, tracker, ti.drain, ti.pods, ti.wantScheduleErrs); err != nil {
					t.Fatal(err)
				}
				for _, bucket := range ti.toAbort {
					for _, node := range bucket.Nodes {
						nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError}
						tracker.StartDeletion(bucket.Group.Id(), node.Name)
						scheduler.AbortNodeDeletionDueToError(ctx, node, bucket.Group.Id(), ti.drain, "simulated abort", nodeDeleteResult)
					}
				}
				if err := scheduleAll(ctx, ti.toScheduleAfterAbort, scheduler, tracker, ti.drain, ti.pods, ti.wantScheduleErrs); err != nil {
					t.Fatal(err)
				}

				if batcher.addedNodes != ti.wantDeleted {
					t.Errorf("Incorrect number of deleted nodes in iteration %v, want %v but got %v", i, ti.wantDeleted, batcher.addedNodes)
				}
				if diff := cmp.Diff(ti.wantEvictedPods, gotEvictedPods, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("Evicted pods diff in iteration %v (-want +got):\n%s", i, diff)
				}
				gotDeletionResult, _ := tracker.DeletionResults()
				if diff := cmp.Diff(ti.wantNodeDeleteResults, gotDeletionResult, cmpopts.EquateEmpty(), cmpopts.EquateErrors()); diff != "" {
					t.Errorf("NodeDeleteResults diff in iteration %v (-want +got):\n%s", i, diff)
				}

				for _, bucket := range allBuckets {
					provider.DeleteNodeGroup(bucket.Group.Id())
					for _, node := range bucket.Nodes {
						provider.DeleteNode(node)
					}
				}
			}
		})
	}
}

type countingBatcher struct {
	addedNodes int
}

func (b *countingBatcher) AddNodes(ctx context.Context, nodes []*apiv1.Node, nodeGroup cloudprovider.NodeGroup, drain bool) {
	b.addedNodes += len(nodes)
}

func scheduleAll(ctx context.Context, toSchedule []*budgets.NodeGroupView, scheduler *GroupDeletionScheduler, tracker *deletiontracker.NodeDeletionTracker, drain bool, pods map[string][]*apiv1.Pod, wantScheduleErrs map[string]bool) error {
	for _, bucket := range toSchedule {
		bucketSize, err := bucket.Group.TargetSize(ctx)
		if err != nil {
			return fmt.Errorf("failed to get target size for node group %q: %s", bucket.Group.Id(), err)
		}
		for _, node := range bucket.Nodes {
			if drain {
				tracker.StartDeletionWithDrain(bucket.Group.Id(), node.Name)
			} else {
				tracker.StartDeletion(bucket.Group.Id(), node.Name)
			}
			gotErr := scheduler.ScheduleDeletion(ctx, framework.NewTestNodeInfo(node, pods[node.Name]...), bucket.Group, bucketSize, drain)
			if wantErr := wantScheduleErrs[node.Name]; (gotErr != nil) != wantErr {
				return fmt.Errorf("ScheduleDeletion(%q) returned err=%v, wantErr=%v", node.Name, gotErr, wantErr)
			}
		}
	}
	return nil
}

func mergeLists(lists ...[]*budgets.NodeGroupView) []*budgets.NodeGroupView {
	merged := []*budgets.NodeGroupView{}
	for _, l := range lists {
		merged = append(merged, l...)
	}
	return merged
}
