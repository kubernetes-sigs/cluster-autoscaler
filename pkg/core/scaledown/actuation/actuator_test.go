/*
Copyright 2022 The Kubernetes Authors.

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
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/budgets"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/deletiontracker"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/status"
	. "sigs.k8s.io/cluster-autoscaler/pkg/core/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/observers/nodegroupchange"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupconfig"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/utilization"
	caerrors "sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/expiring"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type nodeGroupViewInfo struct {
	nodeGroupName string
	from          int
	to            int
}

type scaleDownNodeInfo struct {
	name        string
	nodeGroup   string
	evictedPods []*apiv1.Pod
	utilInfo    utilization.Info
}

type scaleDownStatusInfo struct {
	result          status.ScaleDownResult
	scaledDownNodes []scaleDownNodeInfo
}

type startDeletionTestCase struct {
	defaultOnly               bool // Set to true to only run default deletion logic tests.
	forcedOnly                bool // Set to true to only run forced deletion logic tests.
	nodeGroups                map[string]*testprovider.TestNodeGroup
	emptyNodes                []nodeGroupViewInfo
	drainNodes                []nodeGroupViewInfo
	pods                      map[string][]*apiv1.Pod
	failedPodDrain            map[string]bool
	failedNodeDeletion        map[string]bool
	failedNodeTaint           map[string]bool
	failedNodeReport          map[string]bool // Nodes left out of the ClusterSnapshot, so their utilization can't be computed.
	wantStatus                scaleDownStatusInfo
	wantErr                   error
	wantDeletedPods           []string
	wantDeletedNodes          []string
	wantTaintUpdates          map[string][][]apiv1.Taint
	wantNodeDeleteResults     map[string]status.NodeDeleteResult
	dynamicDeleteDelayEnabled bool // Set to true to enable DynamicNodeDeleteDelayAfterTaintEnabled.
	wantLatencySamples        int  // Number of latencies the actuator should add to pastLatencies.
}

func getStartDeletionTestCases(ignoreDaemonSetsUtilization bool, force bool, suffix string) map[string]startDeletionTestCase {
	toBeDeletedTaint := apiv1.Taint{Key: taints.ToBeDeletedTaint, Effect: apiv1.TaintEffectNoSchedule}

	dsUtilInfo := generateUtilInfo(2./8., 2./8.)

	if ignoreDaemonSetsUtilization {
		dsUtilInfo = generateUtilInfo(0./8., 0./8.)
	}

	testCases := map[string]startDeletionTestCase{
		"nothing to delete": {
			emptyNodes: nil,
			drainNodes: nil,
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNoNodeDeleted,
			},
		},
		"empty node deletion": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{
				{"test", 0, 2},
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:      "test-node-0",
						nodeGroup: "test",
						utilInfo:  generateUtilInfo(0, 0),
					},
					{
						name:      "test-node-1",
						nodeGroup: "test",
						utilInfo:  generateUtilInfo(0, 0),
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0", "test-node-1"},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteOk},
			},
		},
		"empty atomic node deletion": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"atomic-2": sizedNodeGroup("atomic-2", 2, true, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{
				{"atomic-2", 0, 2},
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:      "atomic-2-node-0",
						nodeGroup: "atomic-2",
						utilInfo:  generateUtilInfo(0, 0),
					},
					{
						name:      "atomic-2-node-1",
						nodeGroup: "atomic-2",
						utilInfo:  generateUtilInfo(0, 0),
					},
				},
			},
			wantDeletedNodes: []string{
				"atomic-2-node-0",
				"atomic-2-node-1",
			},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"atomic-2-node-0": {
					{toBeDeletedTaint},
				},
				"atomic-2-node-1": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"atomic-2-node-0": {ResultType: status.NodeDeleteOk},
				"atomic-2-node-1": {ResultType: status.NodeDeleteOk},
			},
		},
		"deletion with drain": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			drainNodes: []nodeGroupViewInfo{
				{"test", 0, 2},
			},
			pods: map[string][]*apiv1.Pod{
				"test-node-0": removablePods(2, "test-node-0"),
				"test-node-1": removablePods(2, "test-node-1"),
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "test-node-0",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-0"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
					{
						name:        "test-node-1",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-1"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0", "test-node-1"},
			wantDeletedPods:  []string{"test-node-0-pod-0", "test-node-0-pod-1", "test-node-1-pod-0", "test-node-1-pod-1"},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteOk},
			},
		},
		"empty and drain deletion work correctly together": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{
				{"test", 0, 2},
			},
			drainNodes: []nodeGroupViewInfo{
				{"test", 2, 4},
			},
			pods: map[string][]*apiv1.Pod{
				"test-node-2": removablePods(2, "test-node-2"),
				"test-node-3": removablePods(2, "test-node-3"),
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:      "test-node-0",
						nodeGroup: "test",
						utilInfo:  generateUtilInfo(0, 0),
					},
					{
						name:      "test-node-1",
						nodeGroup: "test",
						utilInfo:  generateUtilInfo(0, 0),
					},
					{
						name:        "test-node-2",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-2"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
					{
						name:        "test-node-3",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-3"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
				},
			},
			wantDeletedNodes: []string{
				"test-node-0",
				"test-node-1",
				"test-node-2", "test-node-3"},
			wantDeletedPods: []string{"test-node-2-pod-0", "test-node-2-pod-1", "test-node-3-pod-0", "test-node-3-pod-1"},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
				"test-node-2": {
					{toBeDeletedTaint},
				},
				"test-node-3": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteOk},
				"test-node-2": {ResultType: status.NodeDeleteOk},
				"test-node-3": {ResultType: status.NodeDeleteOk},
			},
		},
		"two atomic groups can be scaled down together": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"atomic-2-mixed": sizedNodeGroup("atomic-2-mixed", 2, true, ignoreDaemonSetsUtilization),
				"atomic-2-drain": sizedNodeGroup("atomic-2-drain", 2, true, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{
				{"atomic-2-mixed", 1, 2},
			},
			drainNodes: []nodeGroupViewInfo{
				{"atomic-2-mixed", 0, 1},
				{"atomic-2-drain", 0, 2},
			},
			pods: map[string][]*apiv1.Pod{
				"atomic-2-mixed-node-0": removablePods(2, "atomic-2-mixed-node-0"),
				"atomic-2-drain-node-0": removablePods(1, "atomic-2-drain-node-0"),
				"atomic-2-drain-node-1": removablePods(2, "atomic-2-drain-node-1"),
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "atomic-2-mixed-node-1",
						nodeGroup:   "atomic-2-mixed",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "atomic-2-mixed-node-0",
						nodeGroup:   "atomic-2-mixed",
						evictedPods: removablePods(2, "atomic-2-mixed-node-0"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
					{
						name:        "atomic-2-drain-node-0",
						nodeGroup:   "atomic-2-drain",
						evictedPods: removablePods(1, "atomic-2-drain-node-0"),
						utilInfo:    generateUtilInfo(1./8., 1./8.),
					},
					{
						name:        "atomic-2-drain-node-1",
						nodeGroup:   "atomic-2-drain",
						evictedPods: removablePods(2, "atomic-2-drain-node-1"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
				},
			},
			wantDeletedNodes: []string{"atomic-2-mixed-node-0", "atomic-2-mixed-node-1", "atomic-2-drain-node-0", "atomic-2-drain-node-1"},
			wantDeletedPods:  []string{"atomic-2-mixed-node-0-pod-0", "atomic-2-mixed-node-0-pod-1", "atomic-2-drain-node-0-pod-0", "atomic-2-drain-node-1-pod-0", "atomic-2-drain-node-1-pod-1"},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"atomic-2-mixed-node-0": {
					{toBeDeletedTaint},
				},
				"atomic-2-mixed-node-1": {
					{toBeDeletedTaint},
				},
				"atomic-2-drain-node-0": {
					{toBeDeletedTaint},
				},
				"atomic-2-drain-node-1": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"atomic-2-mixed-node-0": {ResultType: status.NodeDeleteOk},
				"atomic-2-mixed-node-1": {ResultType: status.NodeDeleteOk},
				"atomic-2-drain-node-0": {ResultType: status.NodeDeleteOk},
				"atomic-2-drain-node-1": {ResultType: status.NodeDeleteOk},
			},
		},
		"atomic empty and drain deletion work correctly together": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"atomic-4": sizedNodeGroup("atomic-4", 4, true, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{
				{"atomic-4", 0, 2},
			},
			drainNodes: []nodeGroupViewInfo{
				{"atomic-4", 2, 4},
			},
			pods: map[string][]*apiv1.Pod{
				"atomic-4-node-2": removablePods(2, "atomic-4-node-2"),
				"atomic-4-node-3": removablePods(2, "atomic-4-node-3"),
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "atomic-4-node-0",
						nodeGroup:   "atomic-4",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "atomic-4-node-1",
						nodeGroup:   "atomic-4",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "atomic-4-node-2",
						nodeGroup:   "atomic-4",
						evictedPods: removablePods(2, "atomic-4-node-2"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
					{
						name:        "atomic-4-node-3",
						nodeGroup:   "atomic-4",
						evictedPods: removablePods(2, "atomic-4-node-3"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
				},
			},
			wantDeletedNodes: []string{"atomic-4-node-0", "atomic-4-node-1", "atomic-4-node-2", "atomic-4-node-3"},
			wantDeletedPods:  []string{"atomic-4-node-2-pod-0", "atomic-4-node-2-pod-1", "atomic-4-node-3-pod-0", "atomic-4-node-3-pod-1"},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"atomic-4-node-0": {
					{toBeDeletedTaint},
				},
				"atomic-4-node-1": {
					{toBeDeletedTaint},
				},
				"atomic-4-node-2": {
					{toBeDeletedTaint},
				},
				"atomic-4-node-3": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"atomic-4-node-0": {ResultType: status.NodeDeleteOk},
				"atomic-4-node-1": {ResultType: status.NodeDeleteOk},
				"atomic-4-node-2": {ResultType: status.NodeDeleteOk},
				"atomic-4-node-3": {ResultType: status.NodeDeleteOk},
			},
		},
		"failure to taint empty node does not stop deletion of other nodes": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{{"test", 0, 4}},
			drainNodes: []nodeGroupViewInfo{{"test", 4, 5}},
			pods: map[string][]*apiv1.Pod{
				"test-node-4": removablePods(2, "test-node-4"),
			},
			failedNodeTaint: map[string]bool{"test-node-2": true},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:      "test-node-0",
						nodeGroup: "test",
						utilInfo:  generateUtilInfo(0, 0),
					},
					{
						name:      "test-node-1",
						nodeGroup: "test",
						utilInfo:  generateUtilInfo(0, 0),
					},
					{
						name:      "test-node-3",
						nodeGroup: "test",
						utilInfo:  generateUtilInfo(0, 0),
					},
					{
						name:        "test-node-4",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-4"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0", "test-node-1", "test-node-3", "test-node-4"},
			wantDeletedPods:  []string{"test-node-4-pod-0", "test-node-4-pod-1"},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
				"test-node-3": {
					{toBeDeletedTaint},
				},
				"test-node-4": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteOk},
				"test-node-3": {ResultType: status.NodeDeleteOk},
				"test-node-4": {ResultType: status.NodeDeleteOk},
			},
		},
		"failure to taint all nodes stops deletion": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{{"test", 0, 2}},
			drainNodes: []nodeGroupViewInfo{{"test", 2, 3}},
			pods: map[string][]*apiv1.Pod{
				"test-node-2": removablePods(2, "test-node-2"),
			},
			failedNodeTaint: map[string]bool{
				"test-node-0": true,
				"test-node-1": true,
				"test-node-2": true,
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownError,
			},
			wantErr: cmpopts.AnyError,
		},
		"nodes whose utilization can't be computed are still reported and deleted": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{{"test", 0, 2}},
			failedNodeReport: map[string]bool{
				"test-node-1": true,
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:      "test-node-0",
						nodeGroup: "test",
						utilInfo:  generateUtilInfo(0, 0),
					},
					{
						name:      "test-node-1",
						nodeGroup: "test",
						// Node is missing from the snapshot, so utilization is left empty.
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0", "test-node-1"},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteOk},
			},
		},
		"failure to taint empty atomic node stops atomic group deletion and cleans already applied taints": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test":     sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
				"atomic-4": sizedNodeGroup("atomic-4", 4, true, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{{"atomic-4", 0, 4}},
			drainNodes: []nodeGroupViewInfo{{"test", 4, 5}},
			pods: map[string][]*apiv1.Pod{
				"test-node-4": removablePods(2, "test-node-4"),
			},
			failedNodeTaint: map[string]bool{"atomic-4-node-2": true},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "test-node-4",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-4"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
				},
			},
			wantDeletedNodes: []string{"test-node-4"},
			wantDeletedPods:  []string{"test-node-4-pod-0", "test-node-4-pod-1"},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"atomic-4-node-0": {
					{toBeDeletedTaint},
					{},
				},
				"atomic-4-node-1": {
					{toBeDeletedTaint},
					{},
				},
				"atomic-4-node-3": {
					{toBeDeletedTaint},
					{},
				},
				"test-node-4": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-4": {ResultType: status.NodeDeleteOk},
			},
		},
		"failure to taint drain node does not stop deletion of other nodes": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{{"test", 0, 2}},
			drainNodes: []nodeGroupViewInfo{{"test", 2, 6}},
			pods: map[string][]*apiv1.Pod{
				"test-node-2": removablePods(2, "test-node-2"),
				"test-node-3": removablePods(2, "test-node-3"),
				"test-node-4": removablePods(2, "test-node-4"),
				"test-node-5": removablePods(2, "test-node-5"),
			},
			failedNodeTaint: map[string]bool{"test-node-2": true},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "test-node-0",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "test-node-1",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "test-node-3",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-3"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
					{
						name:        "test-node-4",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-4"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
					{
						name:        "test-node-5",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-5"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0", "test-node-1", "test-node-3", "test-node-4", "test-node-5"},
			wantDeletedPods: []string{
				"test-node-3-pod-0", "test-node-3-pod-1",
				"test-node-4-pod-0", "test-node-4-pod-1",
				"test-node-5-pod-0", "test-node-5-pod-1",
			},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
				"test-node-3": {
					{toBeDeletedTaint},
				},
				"test-node-4": {
					{toBeDeletedTaint},
				},
				"test-node-5": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteOk},
				"test-node-3": {ResultType: status.NodeDeleteOk},
				"test-node-4": {ResultType: status.NodeDeleteOk},
				"test-node-5": {ResultType: status.NodeDeleteOk},
			},
		},
		"failure to taint drain atomic node stops atomic group deletion and cleans already applied taints": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test":     sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
				"atomic-6": sizedNodeGroup("atomic-6", 6, true, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{{"test", 0, 2}},
			drainNodes: []nodeGroupViewInfo{{"atomic-6", 0, 6}},
			pods: map[string][]*apiv1.Pod{
				"atomic-6-node-0": removablePods(2, "atomic-6-node-0"),
				"atomic-6-node-1": removablePods(2, "atomic-6-node-1"),
				"atomic-6-node-2": removablePods(2, "atomic-6-node-2"),
				"atomic-6-node-3": removablePods(2, "atomic-6-node-3"),
				"atomic-6-node-4": removablePods(2, "atomic-6-node-4"),
				"atomic-6-node-5": removablePods(2, "atomic-6-node-5"),
			},
			failedNodeTaint: map[string]bool{"atomic-6-node-2": true},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "test-node-0",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "test-node-1",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0", "test-node-1"},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
				"atomic-6-node-0": {
					{toBeDeletedTaint},
					{},
				},
				"atomic-6-node-1": {
					{toBeDeletedTaint},
					{},
				},
				"atomic-6-node-3": {
					{toBeDeletedTaint},
					{},
				},
				"atomic-6-node-4": {
					{toBeDeletedTaint},
					{},
				},
				"atomic-6-node-5": {
					{toBeDeletedTaint},
					{},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteOk},
			},
		},
		"failure to taint drain node in mixed atomic group cleans empty node taint and aborts atomic group": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test":     sizedNodeGroup("test", 1, false, ignoreDaemonSetsUtilization),
				"atomic-2": sizedNodeGroup("atomic-2", 2, true, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{
				{"atomic-2", 0, 1},
				{"test", 0, 1},
			},
			drainNodes: []nodeGroupViewInfo{
				{"atomic-2", 1, 2},
			},
			pods: map[string][]*apiv1.Pod{
				"atomic-2-node-1": removablePods(2, "atomic-2-node-1"),
			},
			failedNodeTaint: map[string]bool{"atomic-2-node-1": true},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:      "test-node-0",
						nodeGroup: "test",
						utilInfo:  generateUtilInfo(0, 0),
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0"},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"atomic-2-node-0": {
					{toBeDeletedTaint},
					{},
				},
				"test-node-0": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
			},
		},
		"failure to taint drain node in mixed atomic group with no other groups returns error": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"atomic-2": sizedNodeGroup("atomic-2", 2, true, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{
				{"atomic-2", 0, 1},
			},
			drainNodes: []nodeGroupViewInfo{
				{"atomic-2", 1, 2},
			},
			pods: map[string][]*apiv1.Pod{
				"atomic-2-node-1": removablePods(2, "atomic-2-node-1"),
			},
			failedNodeTaint: map[string]bool{"atomic-2-node-1": true},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownError,
			},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"atomic-2-node-0": {
					{toBeDeletedTaint},
					{},
				},
			},
			wantErr: cmpopts.AnyError,
		},
		"nodes that failed drain are correctly reported in results": {
			defaultOnly: true,
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			drainNodes: []nodeGroupViewInfo{{"test", 0, 4}},
			pods: map[string][]*apiv1.Pod{
				"test-node-0": removablePods(3, "test-node-0"),
				"test-node-1": removablePods(3, "test-node-1"),
				"test-node-2": removablePods(3, "test-node-2"),
				"test-node-3": removablePods(3, "test-node-3"),
			},
			failedPodDrain: map[string]bool{
				"test-node-0-pod-0": true,
				"test-node-0-pod-1": true,
				"test-node-2-pod-1": true,
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "test-node-0",
						nodeGroup:   "test",
						evictedPods: removablePods(3, "test-node-0"),
						utilInfo:    generateUtilInfo(3./8., 3./8.),
					},
					{
						name:        "test-node-1",
						nodeGroup:   "test",
						evictedPods: removablePods(3, "test-node-1"),
						utilInfo:    generateUtilInfo(3./8., 3./8.),
					},
					{
						name:        "test-node-2",
						nodeGroup:   "test",
						evictedPods: removablePods(3, "test-node-2"),
						utilInfo:    generateUtilInfo(3./8., 3./8.),
					},
					{
						name:        "test-node-3",
						nodeGroup:   "test",
						evictedPods: removablePods(3, "test-node-3"),
						utilInfo:    generateUtilInfo(3./8., 3./8.),
					},
				},
			},
			wantDeletedNodes: []string{"test-node-1", "test-node-3"},
			wantDeletedPods: []string{
				"test-node-0-pod-2",
				"test-node-1-pod-0", "test-node-1-pod-1", "test-node-1-pod-2",
				"test-node-2-pod-0", "test-node-2-pod-2",
				"test-node-3-pod-0", "test-node-3-pod-1", "test-node-3-pod-2",
			},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
					{},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
				"test-node-2": {
					{toBeDeletedTaint},
					{},
				},
				"test-node-3": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {
					ResultType: status.NodeDeleteErrorFailedToEvictPods,
					Err:        cmpopts.AnyError,
					PodEvictionResults: map[string]status.PodEvictionResult{
						"test-node-0-pod-0": {Pod: removablePod("test-node-0-pod-0", "test-node-0"), Err: cmpopts.AnyError, TimedOut: true},
						"test-node-0-pod-1": {Pod: removablePod("test-node-0-pod-1", "test-node-0"), Err: cmpopts.AnyError, TimedOut: true},
						"test-node-0-pod-2": {Pod: removablePod("test-node-0-pod-2", "test-node-0")},
					},
				},
				"test-node-1": {ResultType: status.NodeDeleteOk},
				"test-node-2": {
					ResultType: status.NodeDeleteErrorFailedToEvictPods,
					Err:        cmpopts.AnyError,
					PodEvictionResults: map[string]status.PodEvictionResult{
						"test-node-2-pod-0": {Pod: removablePod("test-node-2-pod-0", "test-node-2")},
						"test-node-2-pod-1": {Pod: removablePod("test-node-2-pod-1", "test-node-2"), Err: cmpopts.AnyError, TimedOut: true},
						"test-node-2-pod-2": {Pod: removablePod("test-node-2-pod-2", "test-node-2")},
					},
				},
				"test-node-3": {ResultType: status.NodeDeleteOk},
			},
		},
		"nodes that failed drain are forcefully deleted": {
			forcedOnly: true,
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			drainNodes: []nodeGroupViewInfo{{"test", 0, 4}},
			pods: map[string][]*apiv1.Pod{
				"test-node-0": removablePods(3, "test-node-0"),
				"test-node-1": removablePods(3, "test-node-1"),
				"test-node-2": removablePods(3, "test-node-2"),
				"test-node-3": removablePods(3, "test-node-3"),
			},
			failedPodDrain: map[string]bool{
				"test-node-0-pod-0": true,
				"test-node-0-pod-1": true,
				"test-node-2-pod-1": true,
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "test-node-0",
						nodeGroup:   "test",
						evictedPods: removablePods(3, "test-node-0"),
						utilInfo:    generateUtilInfo(3./8., 3./8.),
					},
					{
						name:        "test-node-1",
						nodeGroup:   "test",
						evictedPods: removablePods(3, "test-node-1"),
						utilInfo:    generateUtilInfo(3./8., 3./8.),
					},
					{
						name:        "test-node-2",
						nodeGroup:   "test",
						evictedPods: removablePods(3, "test-node-2"),
						utilInfo:    generateUtilInfo(3./8., 3./8.),
					},
					{
						name:        "test-node-3",
						nodeGroup:   "test",
						evictedPods: removablePods(3, "test-node-3"),
						utilInfo:    generateUtilInfo(3./8., 3./8.),
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0", "test-node-1", "test-node-2", "test-node-3"},
			wantDeletedPods: []string{
				"test-node-0-pod-2",
				"test-node-1-pod-0", "test-node-1-pod-1", "test-node-1-pod-2",
				"test-node-2-pod-0", "test-node-2-pod-2",
				"test-node-3-pod-0", "test-node-3-pod-1", "test-node-3-pod-2",
			},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
				"test-node-2": {
					{toBeDeletedTaint},
				},
				"test-node-3": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteOk},
				"test-node-2": {ResultType: status.NodeDeleteOk},
				"test-node-3": {ResultType: status.NodeDeleteOk},
			},
		},
		"nodes that failed deletion are correctly reported in results": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{{"test", 0, 2}},
			drainNodes: []nodeGroupViewInfo{{"test", 2, 4}},
			pods: map[string][]*apiv1.Pod{
				"test-node-2": removablePods(2, "test-node-2"),
				"test-node-3": removablePods(2, "test-node-3"),
			},
			failedNodeDeletion: map[string]bool{
				"test-node-1": true,
				"test-node-3": true,
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "test-node-0",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "test-node-1",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "test-node-2",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-2"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
					{
						name:        "test-node-3",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-3"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0", "test-node-2"},
			wantDeletedPods: []string{
				"test-node-2-pod-0", "test-node-2-pod-1",
				"test-node-3-pod-0", "test-node-3-pod-1",
			},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
					{},
				},
				"test-node-2": {
					{toBeDeletedTaint},
				},
				"test-node-3": {
					{toBeDeletedTaint},
					{},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
				"test-node-2": {ResultType: status.NodeDeleteOk},
				"test-node-3": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
			},
		},
		"DS pods are evicted from empty nodes, but don't block deletion on error": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{{"test", 0, 2}},
			pods: map[string][]*apiv1.Pod{
				"test-node-0": generateDsPods(2, "test-node-0"),
				"test-node-1": generateDsPods(2, "test-node-1"),
			},
			failedPodDrain: map[string]bool{"test-node-1-ds-pod-0": true},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "test-node-0",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    dsUtilInfo,
					},
					{
						name:        "test-node-1",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    dsUtilInfo,
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0", "test-node-1"},
			wantDeletedPods:  []string{"test-node-0-ds-pod-0", "test-node-0-ds-pod-1", "test-node-1-ds-pod-1"},
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteOk},
			},
		},
		"DS pods and deletion with drain": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			drainNodes: []nodeGroupViewInfo{{"test", 0, 2}},
			pods: map[string][]*apiv1.Pod{
				"test-node-0": generateDsPods(2, "test-node-0"),
				"test-node-1": generateDsPods(2, "test-node-1"),
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:      "test-node-0",
						nodeGroup: "test",
						// this is nil because DaemonSetEvictionForOccupiedNodes is
						// not enabled for drained nodes in this test suite
						evictedPods: nil,
						utilInfo:    dsUtilInfo,
					},
					{
						name:      "test-node-1",
						nodeGroup: "test",
						// this is nil because DaemonSetEvictionForOccupiedNodes is
						// not enabled for drained nodes in this test suite
						evictedPods: nil,
						utilInfo:    dsUtilInfo,
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0", "test-node-1"},
			// same as evicted pods
			wantDeletedPods: nil,
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteOk},
			},
		},
		"DS pods and empty and drain deletion work correctly together": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{{"test", 0, 2}},
			drainNodes: []nodeGroupViewInfo{{"test", 2, 4}},
			pods: map[string][]*apiv1.Pod{
				"test-node-2": removablePods(2, "test-node-2"),
				"test-node-3": generateDsPods(2, "test-node-3"),
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "test-node-0",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "test-node-1",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "test-node-2",
						nodeGroup:   "test",
						evictedPods: removablePods(2, "test-node-2"),
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
					{
						name:        "test-node-3",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    dsUtilInfo,
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0", "test-node-1", "test-node-2", "test-node-3"},
			// same as evicted pods
			wantDeletedPods: nil,
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
				},
				"test-node-2": {
					{toBeDeletedTaint},
				},
				"test-node-3": {
					{toBeDeletedTaint},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteOk},
				"test-node-1": {ResultType: status.NodeDeleteOk},
				"test-node-2": {ResultType: status.NodeDeleteOk},
				"test-node-3": {ResultType: status.NodeDeleteOk},
			},
		},
		"nodes with pods are not deleted if the node is passed as empty": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test": sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{{"test", 0, 2}},
			pods: map[string][]*apiv1.Pod{
				"test-node-0": removablePods(2, "test-node-0"),
				"test-node-1": removablePods(2, "test-node-1"),
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "test-node-0",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
					{
						name:        "test-node-1",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
				},
			},
			wantDeletedNodes: nil,
			wantDeletedPods:  nil,
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
					{},
				},
				"test-node-1": {
					{toBeDeletedTaint},
					{},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0": {ResultType: status.NodeDeleteErrorInternal, Err: cmpopts.AnyError},
				"test-node-1": {ResultType: status.NodeDeleteErrorInternal, Err: cmpopts.AnyError},
			},
		},
		"atomic nodes with pods are not deleted if the node is passed as empty": {
			nodeGroups: map[string]*testprovider.TestNodeGroup{
				"test":     sizedNodeGroup("test", 3, false, ignoreDaemonSetsUtilization),
				"atomic-2": sizedNodeGroup("atomic-2", 2, true, ignoreDaemonSetsUtilization),
			},
			emptyNodes: []nodeGroupViewInfo{{"test", 0, 2}, {"atomic-2", 0, 2}},
			pods: map[string][]*apiv1.Pod{
				"test-node-1":     removablePods(2, "test-node-1"),
				"atomic-2-node-1": removablePods(2, "atomic-2-node-1"),
			},
			wantStatus: scaleDownStatusInfo{
				result: status.ScaleDownNodeDeleteStarted,
				scaledDownNodes: []scaleDownNodeInfo{
					{
						name:        "test-node-0",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "test-node-1",
						nodeGroup:   "test",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
					{
						name:        "atomic-2-node-0",
						nodeGroup:   "atomic-2",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(0, 0),
					},
					{
						name:        "atomic-2-node-1",
						nodeGroup:   "atomic-2",
						evictedPods: nil,
						utilInfo:    generateUtilInfo(2./8., 2./8.),
					},
				},
			},
			wantDeletedNodes: []string{"test-node-0"},
			wantDeletedPods:  nil,
			wantTaintUpdates: map[string][][]apiv1.Taint{
				"test-node-0": {
					{toBeDeletedTaint},
				},
				"test-node-1": {
					{toBeDeletedTaint},
					{},
				},
				"atomic-2-node-0": {
					{toBeDeletedTaint},
					{},
				},
				"atomic-2-node-1": {
					{toBeDeletedTaint},
					{},
				},
			},
			wantNodeDeleteResults: map[string]status.NodeDeleteResult{
				"test-node-0":     {ResultType: status.NodeDeleteOk},
				"test-node-1":     {ResultType: status.NodeDeleteErrorInternal, Err: cmpopts.AnyError},
				"atomic-2-node-0": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: cmpopts.AnyError},
				"atomic-2-node-1": {ResultType: status.NodeDeleteErrorInternal, Err: cmpopts.AnyError},
			},
		},
	}

	filteredTestCases := map[string]startDeletionTestCase{}
	for k, v := range testCases {
		if force && v.defaultOnly {
			continue
		}
		if !force && v.forcedOnly {
			continue
		}
		filteredTestCases[k+" "+suffix] = v
	}

	return filteredTestCases
}

func runStartDeletionTest(t *testing.T, tc startDeletionTestCase, force bool) {
	ctx := GetTestContext(t)
	// Insert all nodes into a map to support live node updates and GETs.
	emptyNodeGroupViews, drainNodeGroupViews := []*budgets.NodeGroupView{}, []*budgets.NodeGroupView{}
	allEmptyNodes, allDrainNodes := []*apiv1.Node{}, []*apiv1.Node{}
	nodesByName := make(map[string]*apiv1.Node)
	nodesLock := sync.Mutex{}
	for _, ngvInfo := range tc.emptyNodes {
		ngv := generateNodeGroupViewList(tc.nodeGroups[ngvInfo.nodeGroupName], ngvInfo.from, ngvInfo.to)
		emptyNodeGroupViews = append(emptyNodeGroupViews, ngv...)
	}
	for _, bucket := range emptyNodeGroupViews {
		allEmptyNodes = append(allEmptyNodes, bucket.Nodes...)
		for _, node := range bucket.Nodes {
			nodesByName[node.Name] = node
		}
	}

	for _, ngvInfo := range tc.drainNodes {
		ngv := generateNodeGroupViewList(tc.nodeGroups[ngvInfo.nodeGroupName], ngvInfo.from, ngvInfo.to)
		drainNodeGroupViews = append(drainNodeGroupViews, ngv...)
	}
	for _, bucket := range drainNodeGroupViews {
		allDrainNodes = append(allDrainNodes, bucket.Nodes...)
		for _, node := range bucket.Nodes {
			nodesByName[node.Name] = node
		}
	}

	// Set up a fake k8s client to hook and verify certain actions.
	fakeClient := &fake.Clientset{}
	type nodeTaints struct {
		nodeName string
		taints   []apiv1.Taint
	}
	allUpdatesCount := 0
	for _, updates := range tc.wantTaintUpdates {
		allUpdatesCount += len(updates)
	}
	// Nothing reads taintUpdates until all deletion results are in, and the update reactor sends to it while holding
	// nodesLock, so the buffer has to fit every update. The extra room lets unexpected updates show up in the diff
	// instead of blocking the reactor.
	taintUpdates := make(chan nodeTaints, allUpdatesCount+2*len(nodesByName))
	deletedNodes := make(chan string, 10)
	deletedPods := make(chan string, 10)

	ds := generateDaemonSet()

	// We're faking the whole k8s client, and some of the code needs to get live nodes and pods, so GET on nodes and pods has to be set up.
	fakeClient.Fake.AddReactor("get", "nodes", func(action core.Action) (bool, runtime.Object, error) {
		nodesLock.Lock()
		defer nodesLock.Unlock()
		getAction := action.(core.GetAction)
		node, found := nodesByName[getAction.GetName()]
		if !found {
			return true, nil, fmt.Errorf("node %q not found", getAction.GetName())
		}
		return true, node.DeepCopy(), nil
	})
	fakeClient.Fake.AddReactor("get", "pods",
		func(action core.Action) (bool, runtime.Object, error) {
			return true, nil, errors.NewNotFound(apiv1.Resource("pod"), "whatever")
		})
	// Hook node update to gather all taint updates, and to fail the update for certain nodes to simulate errors.
	fakeClient.Fake.AddReactor("update", "nodes",
		func(action core.Action) (bool, runtime.Object, error) {
			nodesLock.Lock()
			defer nodesLock.Unlock()
			update := action.(core.UpdateAction)
			obj := update.GetObject().(*apiv1.Node)
			// Checking if a node has ToBeDeletedTaint allows reactor to fail attempts to add the taint, without failing attempts to untaint it.
			if tc.failedNodeTaint[obj.Name] && taints.HasToBeDeletedTaint(obj) {
				return true, nil, fmt.Errorf("SIMULATED ERROR: won't taint")
			}
			nt := nodeTaints{
				nodeName: obj.Name,
			}
			for _, taint := range obj.Spec.Taints {
				nt.taints = append(nt.taints, taint)
			}
			taintUpdates <- nt
			nodesByName[obj.Name] = obj.DeepCopy()
			return true, obj, nil
		})
	// Hook eviction creation to gather which pods were evicted, and to fail the eviction for certain pods to simulate errors.
	fakeClient.Fake.AddReactor("create", "pods",
		func(action core.Action) (bool, runtime.Object, error) {
			createAction := action.(core.CreateAction)
			if createAction == nil {
				return false, nil, nil
			}
			eviction := createAction.GetObject().(*policyv1beta1.Eviction)
			if eviction == nil {
				return false, nil, nil
			}
			if tc.failedPodDrain[eviction.Name] {
				return true, nil, fmt.Errorf("SIMULATED ERROR: won't evict")
			}
			deletedPods <- eviction.Name
			return true, nil, nil
		})

	// Hook node deletion at the level of cloud provider, to gather which nodes were deleted, and to fail the deletion for
	// certain nodes to simulate errors.
	provider := testprovider.NewTestCloudProviderBuilder().WithOnScaleDown(func(nodeGroup string, node string) error {
		if tc.failedNodeDeletion[node] {
			return fmt.Errorf("SIMULATED ERROR: won't remove node")
		}
		deletedNodes <- node
		return nil
	}).Build()
	for _, bucket := range emptyNodeGroupViews {
		bucket.Group.(*testprovider.TestNodeGroup).SetCloudProvider(provider)
		provider.InsertNodeGroup(bucket.Group)
		for _, node := range bucket.Nodes {
			provider.AddNode(bucket.Group.Id(), node)
		}
	}
	for _, bucket := range drainNodeGroupViews {
		bucket.Group.(*testprovider.TestNodeGroup).SetCloudProvider(provider)
		provider.InsertNodeGroup(bucket.Group)
		for _, node := range bucket.Nodes {
			provider.AddNode(bucket.Group.Id(), node)
		}
	}

	// Set up other needed structures and options.
	opts := config.AutoscalingOptions{
		MaxScaleDownParallelism:                 10,
		MaxDrainParallelism:                     5,
		MaxPodEvictionTime:                      0,
		DaemonSetEvictionForEmptyNodes:          true,
		DynamicNodeDeleteDelayAfterTaintEnabled: tc.dynamicDeleteDelayEnabled,
	}

	allPods := []*apiv1.Pod{}

	for _, pods := range tc.pods {
		allPods = append(allPods, pods...)
	}

	podLister := kube_util.NewTestPodLister(allPods)
	pdbLister := kube_util.NewTestPodDisruptionBudgetLister([]*policyv1.PodDisruptionBudget{})
	dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{ds})
	if err != nil {
		t.Fatalf("Couldn't create daemonset lister")
	}

	nodeLister := kube_util.NewDynamicTestNodeLister(fakeClient)
	registry := kube_util.NewListerRegistry(nodeLister, nil, podLister, pdbLister, dsLister, nil, nil, nil, nil)
	autoscalingCtx, err := NewScaleTestAutoscalingContext(opts, fakeClient, registry, provider, nil, nil, nil)
	if err != nil {
		t.Fatalf("Couldn't set up autoscaling context: %v", err)
	}
	scaleStateNotifier := nodegroupchange.NewNodeGroupChangeObserversList()
	_ = clusterstate.NewClusterStateRegistry(provider, autoscalingCtx.LogRecorder, NewBackoff(), nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 15 * time.Minute}), autoscalingCtx.TemplateNodeInfoRegistry, clusterstate.WithScaleStateNotifier(scaleStateNotifier))
	for _, bucket := range emptyNodeGroupViews {
		for _, node := range bucket.Nodes {
			if tc.failedNodeReport[node.Name] {
				continue
			}
			err := autoscalingCtx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node, tc.pods[node.Name]...))
			if err != nil {
				t.Fatalf("Couldn't add node %q to snapshot: %v", node.Name, err)
			}
		}
	}
	for _, bucket := range drainNodeGroupViews {
		for _, node := range bucket.Nodes {
			pods, found := tc.pods[node.Name]
			if !found {
				t.Fatalf("Drain node %q doesn't have pods defined in the test case.", node.Name)
			}
			err := autoscalingCtx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...))
			if err != nil {
				t.Fatalf("Couldn't add node %q to snapshot: %v", node.Name, err)
			}
		}
	}

	wantScaleDownNodes := []*status.ScaleDownNode{}
	for _, scaleDownNodeInfo := range tc.wantStatus.scaledDownNodes {
		statusScaledDownNode := &status.ScaleDownNode{
			Node:        generateNode(scaleDownNodeInfo.name),
			NodeGroup:   tc.nodeGroups[scaleDownNodeInfo.nodeGroup],
			EvictedPods: scaleDownNodeInfo.evictedPods,
			UtilInfo:    scaleDownNodeInfo.utilInfo,
		}
		wantScaleDownNodes = append(wantScaleDownNodes, statusScaledDownNode)
	}

	// Create Actuator, run StartDeletion, and verify the error.
	ndt := deletiontracker.NewNodeDeletionTracker(0)
	ndb := NewNodeDeletionBatcher(&autoscalingCtx, scaleStateNotifier, ndt, 0*time.Second)
	legacyFlagDrainConfig := SingleRuleDrainConfig(autoscalingCtx.MaxGracefulTerminationSec)
	evictor := Evictor{EvictionRetryTime: 0, PodEvictionHeadroom: DefaultPodEvictionHeadroom, shutdownGracePeriodByPodPriority: legacyFlagDrainConfig, fullDsEviction: force}
	actuator := Actuator{
		autoscalingCtx: &autoscalingCtx, nodeDeletionTracker: ndt,
		nodeDeletionScheduler: NewGroupDeletionScheduler(&autoscalingCtx, ndt, ndb, evictor),
		pastLatencies:         expiring.NewList(),
		budgetProcessor:       budgets.NewScaleDownBudgetProcessor(&autoscalingCtx),
		configGetter:          nodegroupconfig.NewDefaultNodeGroupConfigProcessor(autoscalingCtx.NodeGroupDefaults),
	}

	var gotResult status.ScaleDownResult
	var gotScaleDownNodes []*status.ScaleDownNode
	var gotErr error
	if force {
		gotResult, gotScaleDownNodes, gotErr = actuator.StartForceDeletion(ctx, allEmptyNodes, allDrainNodes)
	} else {
		gotResult, gotScaleDownNodes, gotErr = actuator.StartDeletion(ctx, allEmptyNodes, allDrainNodes)
	}

	if diff := cmp.Diff(tc.wantErr, gotErr, cmpopts.EquateErrors()); diff != "" {
		t.Errorf("StartDeletion error diff (-want +got):\n%s", diff)
	}

	// Verify ScaleDownResult looks as expected.
	if diff := cmp.Diff(tc.wantStatus.result, gotResult); diff != "" {
		t.Errorf("StartDeletion result diff (-want +got):\n%s", diff)
	}

	// Verify ScaleDownNodes looks as expected.
	ignoreSdNodeOrder := cmpopts.SortSlices(func(a, b *status.ScaleDownNode) bool { return a.Node.Name < b.Node.Name })
	cmpNg := cmp.Comparer(func(a, b *testprovider.TestNodeGroup) bool { return a.Id() == b.Id() })
	statusCmpOpts := cmp.Options{ignoreSdNodeOrder, cmpNg, cmpopts.EquateEmpty()}
	if diff := cmp.Diff(wantScaleDownNodes, gotScaleDownNodes, statusCmpOpts); diff != "" {
		t.Errorf("StartDeletion scaled down nodes diff (-want +got):\n%s", diff)
	}

	// Verify that all expected nodes were deleted using the cloud provider hook.
	var gotDeletedNodes []string
nodesLoop:
	for i := 0; i < len(tc.wantDeletedNodes); i++ {
		select {
		case deletedNode := <-deletedNodes:
			gotDeletedNodes = append(gotDeletedNodes, deletedNode)
		case <-time.After(3 * time.Second):
			t.Errorf("Timeout while waiting for deleted nodes.")
			break nodesLoop
		}
	}
	ignoreStrOrder := cmpopts.SortSlices(func(a, b string) bool { return a < b })
	if diff := cmp.Diff(tc.wantDeletedNodes, gotDeletedNodes, ignoreStrOrder); diff != "" {
		t.Errorf("deletedNodes diff (-want +got):\n%s", diff)
	}

	// Verify that all expected pods were deleted using the fake k8s client hook.
	var gotDeletedPods []string
podsLoop:
	for i := 0; i < len(tc.wantDeletedPods); i++ {
		select {
		case deletedPod := <-deletedPods:
			gotDeletedPods = append(gotDeletedPods, deletedPod)
		case <-time.After(3 * time.Second):
			t.Errorf("Timeout while waiting for deleted pods.")
			break podsLoop
		}
	}
	if diff := cmp.Diff(tc.wantDeletedPods, gotDeletedPods, ignoreStrOrder); diff != "" {
		t.Errorf("deletedPods diff (-want +got):\n%s", diff)
	}

	// Wait for all expected deletions (including failed drains/deletions that clean up taints before
	// calling EndDeletion) to be reported in NodeDeletionTracker before asserting on taint updates.
	err = waitForDeletionResultsCount(actuator.nodeDeletionTracker, len(tc.wantNodeDeleteResults), 3*time.Second, 5*time.Millisecond)
	if err != nil {
		t.Errorf("Timeout while waiting for node deletion results")
	}

	// Verify the number of taint latency samples. The dynamic delay is computed once per batch, in the background,
	// and the deleteNodesAsync goroutines (all finished above) wait for it.
	actuator.pastLatenciesMu.Lock()
	gotLatencySamples := len(actuator.pastLatencies.ToSlice())
	actuator.pastLatenciesMu.Unlock()
	if gotLatencySamples != tc.wantLatencySamples {
		t.Errorf("pastLatencies: want %d samples, got %d", tc.wantLatencySamples, gotLatencySamples)
	}

	// Gather node deletion results for deletions started in the previous call, and verify that they look as expected.
	nodeDeleteResults, _ := actuator.DeletionResults()
	if diff := cmp.Diff(tc.wantNodeDeleteResults, nodeDeleteResults, cmpopts.EquateEmpty(), cmpopts.EquateErrors()); diff != "" {
		t.Errorf("NodeDeleteResults diff (-want +got):\n%s", diff)
	}

	// Collect all taint updates recorded by the fake k8s client hook.
	// Because StartDeletion (including cleanTaintsSync) and all background EndDeletion calls have completed above,
	// all taint updates have already been pushed to the buffered taintUpdates channel.
	gotTaintUpdates := make(map[string][][]apiv1.Taint)
extraTaintsLoop:
	for {
		select {
		case taintUpdate := <-taintUpdates:
			gotTaintUpdates[taintUpdate.nodeName] = append(gotTaintUpdates[taintUpdate.nodeName], taintUpdate.taints)
		default:
			break extraTaintsLoop
		}
	}
	// After the first taint failure in an atomic node group, the group's remaining nodes are skipped. Which siblings
	// were already tainted (and then untainted) and which were skipped depends on worker scheduling, so a sibling
	// expected to be tainted and untainted is also accepted with no taint updates at all.
	wantTaintUpdates := make(map[string][][]apiv1.Taint, len(tc.wantTaintUpdates))
	for nodeName, updates := range tc.wantTaintUpdates {
		_, startedDeletion := tc.wantNodeDeleteResults[nodeName]
		if !startedDeletion && len(tc.failedNodeTaint) > 0 && len(updates) == 2 && len(updates[1]) == 0 {
			if _, wasTainted := gotTaintUpdates[nodeName]; !wasTainted {
				continue
			}
		}
		wantTaintUpdates[nodeName] = updates
	}
	startupTaintValue := cmpopts.IgnoreFields(apiv1.Taint{}, "Value")
	if diff := cmp.Diff(wantTaintUpdates, gotTaintUpdates, startupTaintValue, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("taintUpdates diff (-want +got):\n%s", diff)
	}
}

func TestStartDeletion(t *testing.T) {
	testSets := []map[string]startDeletionTestCase{
		// IgnoreDaemonSetsUtilization is false
		getStartDeletionTestCases(false, false, "testNg1"),
		// IgnoreDaemonSetsUtilization is true
		getStartDeletionTestCases(true, false, "testNg2"),
	}

	for _, testSet := range testSets {
		for tn, tc := range testSet {
			t.Run(tn, func(t *testing.T) {
				runStartDeletionTest(t, tc, false)
			})
		}
	}
}

func TestStartDeletionWithDynamicNodeDeleteDelay(t *testing.T) {
	testCases := getStartDeletionTestCases(false, false, "testNgDyn")
	testsToRun := []struct {
		name               string
		wantLatencySamples int
	}{
		// The delay is computed once per batch, so there's one sample whenever any deletion starts.
		{name: "empty node deletion testNgDyn", wantLatencySamples: 1},
		{name: "empty and drain deletion work correctly together testNgDyn", wantLatencySamples: 1},
		{name: "failure to taint empty node does not stop deletion of other nodes testNgDyn", wantLatencySamples: 1},
		{name: "failure to taint drain node does not stop deletion of other nodes testNgDyn", wantLatencySamples: 1},
		{name: "failure to taint drain atomic node stops atomic group deletion and cleans already applied taints testNgDyn", wantLatencySamples: 1},
		{name: "failure to taint drain node in mixed atomic group cleans empty node taint and aborts atomic group testNgDyn", wantLatencySamples: 1},
		{name: "failure to taint drain node in mixed atomic group with no other groups returns error testNgDyn", wantLatencySamples: 0},
		{name: "failure to taint all nodes stops deletion testNgDyn", wantLatencySamples: 0},
		{name: "nodes whose utilization can't be computed are still reported and deleted testNgDyn", wantLatencySamples: 1},
	}

	for _, tt := range testsToRun {
		tc, ok := testCases[tt.name]
		if !ok {
			t.Fatalf("test case %q not found", tt.name)
		}
		tc.dynamicDeleteDelayEnabled = true
		tc.wantLatencySamples = tt.wantLatencySamples
		t.Run(tt.name, func(t *testing.T) {
			runStartDeletionTest(t, tc, false)
		})
	}
}

// newTaintNodesTestActuator returns an Actuator whose fake client fails updates for nodes in failingNodes,
// and a counter of update attempts.
func newTaintNodesTestActuator(t *testing.T, nodes []*apiv1.Node, failingNodes map[string]bool) (*Actuator, func() int) {
	t.Helper()
	var mu sync.Mutex
	nodesByName := make(map[string]*apiv1.Node, len(nodes))
	for _, n := range nodes {
		nodesByName[n.Name] = n
	}
	updateAttempts := 0
	fakeClient := &fake.Clientset{}
	fakeClient.Fake.AddReactor("get", "nodes", func(action core.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		return true, nodesByName[action.(core.GetAction).GetName()].DeepCopy(), nil
	})
	fakeClient.Fake.AddReactor("update", "nodes", func(action core.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		updateAttempts++
		obj := action.(core.UpdateAction).GetObject().(*apiv1.Node)
		if failingNodes[obj.Name] {
			return true, nil, fmt.Errorf("simulated taint error")
		}
		nodesByName[obj.Name] = obj.DeepCopy()
		return true, obj, nil
	})

	opts := config.AutoscalingOptions{MaxScaleDownParallelism: len(nodes)}
	registry := kube_util.NewListerRegistry(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	autoscalingCtx, err := NewScaleTestAutoscalingContext(opts, fakeClient, registry, provider, nil, nil, nil)
	if err != nil {
		t.Fatalf("Couldn't set up autoscaling context: %v", err)
	}
	return &Actuator{autoscalingCtx: &autoscalingCtx}, func() int {
		mu.Lock()
		defer mu.Unlock()
		return updateAttempts
	}
}

func TestTaintNodesAtomicFailFast(t *testing.T) {
	ctx := GetTestContext(t)
	nodes := generateNodes(0, 20, "atomic-20")
	failingNodes := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		failingNodes[n.Name] = true
	}
	actuator, updateAttempts := newTaintNodesTestActuator(t, nodes, failingNodes)
	tracker := NewUpdateLatencyTracker(nil, nodeNames(nodes))
	bucket := &budgets.NodeGroupView{Group: sizedNodeGroup("atomic-20", 20, true, false), Nodes: nodes, BatchSize: len(nodes)}

	got := actuator.taintNodesSync(ctx, []*budgets.NodeGroupView{bucket}, nil, tracker)

	if len(got.empty) != 0 || len(got.drain) != 0 || len(got.nodesToClean) != 0 {
		t.Errorf("want no tainted views and no nodes to clean, got %d empty views, %d drain views, %d nodes to clean", len(got.empty), len(got.drain), len(got.nodesToClean))
	}
	if got.nodesCount != len(nodes) {
		t.Errorf("want nodesCount %d, got %d", len(nodes), got.nodesCount)
	}
	if got.failedCount == 0 {
		t.Errorf("want at least 1 failed node, got 0")
	}
	// Only the workers that were already past the skip check when the first taint failed can still attempt a taint.
	if attempts := updateAttempts(); attempts > maxConcurrentNodesTainting {
		t.Errorf("want at most %d taint attempts before the rest of the atomic group is skipped, got %d", maxConcurrentNodesTainting, attempts)
	}
	if len(tracker.notStarted) != 0 || len(tracker.started) != 0 || len(tracker.finished) != 0 {
		t.Errorf("want every node dropped from the latency tracker, got %d not started, %d started, %d finished", len(tracker.notStarted), len(tracker.started), len(tracker.finished))
	}
}

func TestTaintNodesLatencyTrackerBookkeeping(t *testing.T) {
	ctx := GetTestContext(t)
	atomicNodes := generateNodes(0, 4, "atomic-4")
	testNodes := generateNodes(0, 3, "test")
	allNodes := append(append([]*apiv1.Node{}, atomicNodes...), testNodes...)
	failingNodes := map[string]bool{"atomic-4-node-2": true, "test-node-1": true}
	actuator, _ := newTaintNodesTestActuator(t, allNodes, failingNodes)
	tracker := NewUpdateLatencyTracker(nil, nodeNames(allNodes))

	atomicGroup := sizedNodeGroup("atomic-4", 4, true, false)
	emptyToDelete := []*budgets.NodeGroupView{
		{Group: atomicGroup, Nodes: atomicNodes[:2], BatchSize: 4},
		{Group: sizedNodeGroup("test", 3, false, false), Nodes: testNodes},
	}
	drainToDelete := []*budgets.NodeGroupView{
		{Group: atomicGroup, Nodes: atomicNodes[2:], BatchSize: 4},
	}

	got := actuator.taintNodesSync(ctx, emptyToDelete, drainToDelete, tracker)

	if got.nodesCount != len(allNodes) || got.failedCount != 2 {
		t.Errorf("want nodesCount %d and 2 failed nodes, got %d and %d", len(allNodes), got.nodesCount, got.failedCount)
	}
	if len(got.empty) != 1 || got.empty[0].Group.Id() != "test" {
		t.Fatalf("want only the non-atomic empty view, got %d empty views", len(got.empty))
	}
	if diff := cmp.Diff([]string{"test-node-0", "test-node-2"}, nodeNames(got.empty[0].Nodes)); diff != "" {
		t.Errorf("tainted non-atomic nodes diff (-want +got):\n%s", diff)
	}
	if len(got.drain) != 0 {
		t.Errorf("want no drain views, the atomic group failed, got %d", len(got.drain))
	}
	// Which atomic siblings got tainted before atomic-4-node-2 failed depends on scheduling.
	cleanable := map[string]bool{"atomic-4-node-0": true, "atomic-4-node-1": true, "atomic-4-node-3": true}
	for _, node := range got.nodesToClean {
		if !cleanable[node.Name] {
			t.Errorf("unexpected node to clean: %s", node.Name)
		}
	}
	// Only the nodes that will be deleted are left in the tracker. The tracker isn't started, so none of them finished.
	if len(tracker.notStarted) != 0 || len(tracker.finished) != 0 {
		t.Errorf("want no nodes not started or finished, got %d not started, %d finished", len(tracker.notStarted), len(tracker.finished))
	}
	var gotTracked []string
	for name := range tracker.started {
		gotTracked = append(gotTracked, name)
	}
	if diff := cmp.Diff([]string{"test-node-0", "test-node-2"}, gotTracked, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("tracked nodes diff (-want +got):\n%s", diff)
	}
}

func TestStartForceDeletion(t *testing.T) {
	testSets := []map[string]startDeletionTestCase{
		// IgnoreDaemonSetsUtilization is false
		getStartDeletionTestCases(false, true, "testNg1"),
		// IgnoreDaemonSetsUtilization is true
		getStartDeletionTestCases(true, true, "testNg2"),
	}

	for _, testSet := range testSets {
		for tn, tc := range testSet {
			t.Run(tn, func(t *testing.T) {
				runStartDeletionTest(t, tc, true)
			})
		}
	}
}

func TestStartDeletionInBatchBasic(t *testing.T) {
	deleteInterval := 1 * time.Second

	for _, test := range []struct {
		name                   string
		deleteCalls            int
		numNodesToDelete       map[string][]int //per node group and per call
		failedRequests         map[string]bool  //per node group
		wantSuccessfulDeletion map[string]int   //per node group
	}{
		{
			name:        "Succesfull deletion for all node group",
			deleteCalls: 1,
			numNodesToDelete: map[string][]int{
				"test-ng-1": {4},
				"test-ng-2": {5},
				"test-ng-3": {1},
			},
			wantSuccessfulDeletion: map[string]int{
				"test-ng-1": 4,
				"test-ng-2": 5,
				"test-ng-3": 1,
			},
		},
		{
			name:        "Node deletion failed for one group",
			deleteCalls: 1,
			numNodesToDelete: map[string][]int{
				"test-ng-1": {4},
				"test-ng-2": {5},
				"test-ng-3": {1},
			},
			failedRequests: map[string]bool{
				"test-ng-1": true,
			},
			wantSuccessfulDeletion: map[string]int{
				"test-ng-1": 0,
				"test-ng-2": 5,
				"test-ng-3": 1,
			},
		},
		{
			name:        "Node deletion failed for one group two times",
			deleteCalls: 2,
			numNodesToDelete: map[string][]int{
				"test-ng-1": {4, 3},
				"test-ng-2": {5},
				"test-ng-3": {1},
			},
			failedRequests: map[string]bool{
				"test-ng-1": true,
			},
			wantSuccessfulDeletion: map[string]int{
				"test-ng-1": 0,
				"test-ng-2": 5,
				"test-ng-3": 1,
			},
		},
		{
			name:        "Node deletion failed for all groups",
			deleteCalls: 2,
			numNodesToDelete: map[string][]int{
				"test-ng-1": {4, 3},
				"test-ng-2": {5},
				"test-ng-3": {1},
			},
			failedRequests: map[string]bool{
				"test-ng-1": true,
				"test-ng-2": true,
				"test-ng-3": true,
			},
			wantSuccessfulDeletion: map[string]int{
				"test-ng-1": 0,
				"test-ng-2": 0,
				"test-ng-3": 0,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test := test
			gotFailedRequest := func(nodeGroupId string) bool {
				val, _ := test.failedRequests[nodeGroupId]
				return val
			}
			deletedResult := make(chan string)
			fakeClient := &fake.Clientset{}
			provider := testprovider.NewTestCloudProviderBuilder().WithOnScaleDown(func(nodeGroupId string, node string) error {
				if gotFailedRequest(nodeGroupId) {
					return fmt.Errorf("SIMULATED ERROR: won't remove node")
				}
				deletedResult <- nodeGroupId
				return nil
			}).Build()
			// 2d array represent the waves of pushing nodes to delete.
			deleteNodes := [][]*apiv1.Node{}

			for i := 0; i < test.deleteCalls; i++ {
				deleteNodes = append(deleteNodes, []*apiv1.Node{})
			}
			testNg1 := testprovider.NewTestNodeGroup("test-ng-1", 0, 100, 3, true, false, "n1-standard-2", nil, nil)
			testNg2 := testprovider.NewTestNodeGroup("test-ng-2", 0, 100, 3, true, false, "n1-standard-2", nil, nil)
			testNg3 := testprovider.NewTestNodeGroup("test-ng-3", 0, 100, 3, true, false, "n1-standard-2", nil, nil)
			testNg := map[string]*testprovider.TestNodeGroup{
				"test-ng-1": testNg1,
				"test-ng-2": testNg2,
				"test-ng-3": testNg3,
			}

			for ngName, numNodes := range test.numNodesToDelete {
				ng := testNg[ngName]
				provider.InsertNodeGroup(ng)
				ng.SetCloudProvider(provider)
				for i, num := range numNodes {
					singleBucketList := generateNodeGroupViewList(ng, 0, num)
					bucket := singleBucketList[0]
					deleteNodes[i] = append(deleteNodes[i], bucket.Nodes...)
					for _, node := range bucket.Nodes {
						provider.AddNode(bucket.Group.Id(), node)
					}
				}
			}
			opts := config.AutoscalingOptions{
				MaxScaleDownParallelism:        10,
				MaxDrainParallelism:            5,
				MaxPodEvictionTime:             0,
				DaemonSetEvictionForEmptyNodes: true,
			}

			podLister := kube_util.NewTestPodLister([]*apiv1.Pod{})
			pdbLister := kube_util.NewTestPodDisruptionBudgetLister([]*policyv1.PodDisruptionBudget{})
			registry := kube_util.NewListerRegistry(nil, nil, podLister, pdbLister, nil, nil, nil, nil, nil)
			autoscalingCtx, err := NewScaleTestAutoscalingContext(opts, fakeClient, registry, provider, nil, nil, nil)
			if err != nil {
				t.Fatalf("Couldn't set up autoscaling context: %v", err)
			}
			scaleStateNotifier := nodegroupchange.NewNodeGroupChangeObserversList()
			_ = clusterstate.NewClusterStateRegistry(provider, autoscalingCtx.LogRecorder, NewBackoff(), nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 15 * time.Minute}), autoscalingCtx.TemplateNodeInfoRegistry, clusterstate.WithScaleStateNotifier(scaleStateNotifier))
			ndt := deletiontracker.NewNodeDeletionTracker(0)
			ndb := NewNodeDeletionBatcher(&autoscalingCtx, scaleStateNotifier, ndt, deleteInterval)
			legacyFlagDrainConfig := SingleRuleDrainConfig(autoscalingCtx.MaxGracefulTerminationSec)
			evictor := Evictor{EvictionRetryTime: 0, PodEvictionHeadroom: DefaultPodEvictionHeadroom, shutdownGracePeriodByPodPriority: legacyFlagDrainConfig}
			actuator := Actuator{
				autoscalingCtx: &autoscalingCtx, nodeDeletionTracker: ndt,
				nodeDeletionScheduler: NewGroupDeletionScheduler(&autoscalingCtx, ndt, ndb, evictor),
				budgetProcessor:       budgets.NewScaleDownBudgetProcessor(&autoscalingCtx),
			}

			for _, nodes := range deleteNodes {
				actuator.StartDeletion(context.Background(), nodes, []*apiv1.Node{})
				time.Sleep(deleteInterval)
			}
			wantDeletedNodes := 0
			for _, num := range test.wantSuccessfulDeletion {
				wantDeletedNodes += num
			}
			gotDeletedNodes := map[string]int{
				"test-ng-1": 0,
				"test-ng-2": 0,
				"test-ng-3": 0,
			}
			for i := 0; i < wantDeletedNodes; i++ {
				select {
				case ngId := <-deletedResult:
					gotDeletedNodes[ngId]++
				case <-time.After(1 * time.Second):
					t.Errorf("Timeout while waiting for deleted nodes.")
					break
				}
			}
			if diff := cmp.Diff(test.wantSuccessfulDeletion, gotDeletedNodes); diff != "" {
				t.Errorf("Successful deleteions per node group diff (-want +got):\n%s", diff)
			}
		})
	}
}

func sizedNodeGroup(id string, size int, atomic, ignoreDaemonSetUtil bool) *testprovider.TestNodeGroup {
	ng := testprovider.NewTestNodeGroup(id, 1000, 0, size, true, false, "n1-standard-2", nil, nil)
	ng.SetOptions(&config.NodeGroupAutoscalingOptions{
		ZeroOrMaxNodeScaling:        atomic,
		IgnoreDaemonSetsUtilization: ignoreDaemonSetUtil,
	})
	return ng
}

func generateNodes(from, to int, prefix string) []*apiv1.Node {
	var result []*apiv1.Node
	for i := from; i < to; i++ {
		name := fmt.Sprintf("node-%d", i)
		if prefix != "" {
			name = prefix + "-" + name
		}
		result = append(result, generateNode(name))
	}
	return result
}

func generateNodeGroupViewList(ng cloudprovider.NodeGroup, from, to int) []*budgets.NodeGroupView {
	return []*budgets.NodeGroupView{
		{
			Group: ng,
			Nodes: generateNodes(from, to, ng.Id()),
		},
	}
}

func generateNode(name string) *apiv1.Node {
	return &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: apiv1.NodeStatus{
			Allocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("8"),
				apiv1.ResourceMemory: resource.MustParse("8G"),
			},
		},
	}
}

func removablePods(count int, prefix string) []*apiv1.Pod {
	var result []*apiv1.Pod
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("pod-%d", i)
		if prefix != "" {
			name = prefix + "-" + name
		}
		result = append(result, removablePod(name, prefix))
	}
	return result
}

func removablePod(name string, node string) *apiv1.Pod {
	return &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Annotations: map[string]string{
				"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
			},
		},
		Spec: apiv1.PodSpec{
			NodeName: node,
			Containers: []apiv1.Container{
				{
					Name: "test-container",
					Resources: apiv1.ResourceRequirements{
						Requests: map[apiv1.ResourceName]resource.Quantity{
							apiv1.ResourceCPU:    resource.MustParse("1"),
							apiv1.ResourceMemory: resource.MustParse("1G"),
						},
					},
				},
			},
		},
	}
}

func generateDsPods(count int, node string) []*apiv1.Pod {

	var result []*apiv1.Pod
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("ds-pod-%d", i)
		result = append(result, generateDsPod(name, node))
	}
	return result
}

func generateDsPod(name string, node string) *apiv1.Pod {
	pod := removablePod(fmt.Sprintf("%s-%s", node, name), node)
	pod.OwnerReferences = GenerateOwnerReferences("ds", "DaemonSet", "apps/v1", "some-uid")
	return pod
}

func generateDaemonSet() *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ds",
			Namespace: "default",
			SelfLink:  "/apiv1s/apps/v1/namespaces/default/daemonsets/ds",
		},
	}
}

func generateUtilInfo(cpuUtil, memUtil float64) utilization.Info {
	var higherUtilName apiv1.ResourceName
	var higherUtilVal float64
	if cpuUtil > memUtil {
		higherUtilName = apiv1.ResourceCPU
		higherUtilVal = cpuUtil
	} else {
		higherUtilName = apiv1.ResourceMemory
		higherUtilVal = memUtil
	}
	return utilization.Info{
		CpuUtil:      cpuUtil,
		MemUtil:      memUtil,
		ResourceName: higherUtilName,
		Utilization:  higherUtilVal,
	}
}

func waitForDeletionResultsCount(ndt *deletiontracker.NodeDeletionTracker, resultsCount int, timeout, retryTime time.Duration) error {
	// This is quite ugly, but shouldn't matter much since in most cases there shouldn't be a need to wait at all, and
	// the function should return quickly after the first if check.
	// An alternative could be to turn NodeDeletionTracker into an interface, and use an implementation which allows
	// synchronizing calls to EndDeletion in the test code.
	for retryUntil := time.Now().Add(timeout); time.Now().Before(retryUntil); time.Sleep(retryTime) {
		if results, _ := ndt.DeletionResults(); len(results) == resultsCount {
			return nil
		}
	}
	return fmt.Errorf("timed out while waiting for node deletion results")
}

func TestSummarizeTaintErrors(t *testing.T) {
	// taintNode wraps API errors like this.
	wrap := func(err error) error { return caerrors.ToAutoscalerError(caerrors.ApiCallError, err) }
	nodeResource := apiv1.Resource("nodes")
	testCases := []struct {
		name        string
		failedNodes map[string]error
		wantSummary map[string]int
	}{
		{
			name: "mixed errors",
			failedNodes: map[string]error{
				"n1": wrap(errors.NewTooManyRequests("throttled", 1)),
				"n2": wrap(errors.NewTooManyRequests("throttled", 1)),
				"n3": wrap(errors.NewServiceUnavailable("unavailable")),
				"n4": wrap(errors.NewConflict(nodeResource, "n4", fmt.Errorf("conflict"))),
				"n5": wrap(fmt.Errorf("connection refused")),
			},
			wantSummary: map[string]int{
				"Conflict(409)":           1,
				"OTHER":                   1,
				"ServiceUnavailable(503)": 1,
				"TooManyRequests(429)":    2,
			},
		},
		{
			name: "5xx without a known reason uses the HTTP status text",
			failedNodes: map[string]error{
				"n1": wrap(&errors.StatusError{ErrStatus: metav1.Status{Code: http.StatusBadGateway}}),
			},
			wantSummary: map[string]int{
				"Bad Gateway(502)": 1,
			},
		},
		{
			name: "internal error",
			failedNodes: map[string]error{
				"n1": wrap(errors.NewInternalError(fmt.Errorf("boom"))),
			},
			wantSummary: map[string]int{
				"InternalError(500)": 1,
			},
		},
		{
			name: "API error wrapped with fmt.Errorf keeps its code",
			failedNodes: map[string]error{
				"n1": wrap(fmt.Errorf("failed to get node n1: %w", errors.NewTooManyRequests("throttled", 1))),
			},
			wantSummary: map[string]int{
				"TooManyRequests(429)": 1,
			},
		},
		{
			name:        "empty map",
			failedNodes: map[string]error{},
			wantSummary: map[string]int{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotSummary := summarizeTaintErrors(tc.failedNodes)
			if diff := cmp.Diff(tc.wantSummary, gotSummary, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("summary diff (-want +got):\n%s", diff)
			}
		})
	}
}
