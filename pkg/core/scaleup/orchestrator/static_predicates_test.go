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

package orchestrator

import (
	stdcontext "context"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	scheduler_config_latest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/equivalence"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestStaticPredicatesErrorAgreesWithFramework(t *testing.T) {
	taintedNode := benchNode("tainted", "type-c.large")
	taintedNode.Spec.Taints = []apiv1.Taint{{Key: "dedicated", Value: "x", Effect: apiv1.TaintEffectNoSchedule}}
	preferNoScheduleNode := benchNode("prefer", "type-c.large")
	preferNoScheduleNode.Spec.Taints = []apiv1.Taint{{Key: "soft", Effect: apiv1.TaintEffectPreferNoSchedule}}

	tolerating := func(pod *apiv1.Pod) {
		pod.Spec.Tolerations = []apiv1.Toleration{{Key: "dedicated", Operator: apiv1.TolerationOpExists}}
	}
	withNodeSelector := func(pod *apiv1.Pod) {
		pod.Spec.NodeSelector = map[string]string{benchInstanceTypeLabel: "type-c.xlarge"}
	}

	for _, tc := range []struct {
		name          string
		pod           *apiv1.Pod
		node          *apiv1.Node
		wantPredicate string
	}{
		{name: "excluded instance type", pod: BuildTestPod("p", 100, 100, withInstanceTypeExclusion("type-a.large")), node: benchNode("n", "type-a.large"), wantPredicate: "NodeAffinity"},
		{name: "allowed instance type", pod: BuildTestPod("p", 100, 100, withInstanceTypeExclusion("type-a.large")), node: benchNode("n", "type-c.large")},
		{name: "node selector mismatch", pod: BuildTestPod("p", 100, 100, withNodeSelector), node: benchNode("n", "type-c.large"), wantPredicate: "NodeAffinity"},
		{name: "node selector match", pod: BuildTestPod("p", 100, 100, withNodeSelector), node: benchNode("n", "type-c.xlarge")},
		{name: "untolerated taint", pod: BuildTestPod("p", 100, 100), node: taintedNode, wantPredicate: "TaintToleration"},
		{name: "tolerated taint", pod: BuildTestPod("p", 100, 100, tolerating), node: taintedNode},
		{name: "PreferNoSchedule is not a filter", pod: BuildTestPod("p", 100, 100), node: preferNoScheduleNode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := staticPredicatesError(klog.Background(), tc.pod, tc.node)

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(tc.node)))
			frameworkErr := snapshot.CheckPredicates(tc.pod, tc.node.Name)

			if tc.wantPredicate == "" {
				assert.Nil(t, got)
				return
			}
			if assert.NotNil(t, got) && assert.NotNil(t, frameworkErr, "pre-check rejected a pod the framework accepts") {
				assert.Equal(t, clustersnapshot.FailingPredicateError, got.Type())
				assert.Equal(t, tc.wantPredicate, got.FailingPredicateName())
				assert.Equal(t, frameworkErr.FailingPredicateName(), got.FailingPredicateName())
				assert.Equal(t, frameworkErr.FailingPredicateReasons(), got.FailingPredicateReasons())
			}
		})
	}
}

func TestSchedulablePodGroupsSkipsStaticallyUnschedulable(t *testing.T) {
	snapshot, handle, err := testsnapshot.NewTestSnapshotAndHandle()
	assert.NoError(t, err)
	o := &ScaleUpOrchestrator{autoscalingCtx: &ca_context.AutoscalingContext{ClusterSnapshot: snapshot, FrameworkHandle: handle}}

	excluded := &equivalence.PodGroup{Pods: []*apiv1.Pod{BuildTestPod("excluded", 100, 100, withInstanceTypeExclusion("type-a.large"))}, SchedulingErrors: map[string]status.Reasons{}}
	plain := &equivalence.PodGroup{Pods: []*apiv1.Pod{BuildTestPod("plain", 100, 100)}, SchedulingErrors: map[string]status.Reasons{}}
	nodeGroup := testprovider.NewTestNodeGroup("ng", 10, 0, 0, true, false, "type-a.large", nil, nil)
	nodeInfo := framework.NewNodeInfo(benchNode("template", "type-a.large"), nil)

	got := o.SchedulablePodGroups(stdcontext.Background(), []*equivalence.PodGroup{excluded, plain}, nodeGroup, nodeInfo)

	assert.Len(t, got, 1)
	assert.Equal(t, plain.Pods, got[0].Pods)
	assert.True(t, plain.Schedulable)
	assert.Equal(t, []string{"ng"}, plain.SchedulableGroups)
	assert.False(t, excluded.Schedulable)
	if assert.Contains(t, excluded.SchedulingErrors, "ng") {
		assert.Equal(t, []string{"node(s) didn't match Pod's node affinity/selector"}, excluded.SchedulingErrors["ng"].Reasons())
	}

	// When every group is statically rejected, nothing is returned and the snapshot is untouched.
	excluded.SchedulingErrors = map[string]status.Reasons{}
	assert.Empty(t, o.SchedulablePodGroups(stdcontext.Background(), []*equivalence.PodGroup{excluded}, nodeGroup, nodeInfo))
	nodeInfos, err := snapshot.ListNodeInfos()
	assert.NoError(t, err)
	assert.Empty(t, nodeInfos)
}

// With a scheduler profile that doesn't run NodeAffinity, the pre-check must not be used: here
// the framework accepts an "excluded" template.
func TestSchedulablePodGroupsCustomSchedulerProfileSkipsPreCheck(t *testing.T) {
	cfg, err := scheduler_config_latest.Default()
	assert.NoError(t, err)
	plugins := cfg.Profiles[0].Plugins
	plugins.PreFilter.Disabled = append(plugins.PreFilter.Disabled, schedulerconfig.Plugin{Name: names.NodeAffinity})
	plugins.Filter.Disabled = append(plugins.Filter.Disabled, schedulerconfig.Plugin{Name: names.NodeAffinity})
	handle, err := framework.NewHandle(stdcontext.Background(), informers.NewSharedInformerFactory(clientsetfake.NewSimpleClientset(), 0), cfg, false, false)
	assert.NoError(t, err)
	snapshot := predicate.NewPredicateSnapshot(store.NewBasicSnapshotStore(), handle, false, 1, false, 0)

	o := &ScaleUpOrchestrator{autoscalingCtx: &ca_context.AutoscalingContext{
		ClusterSnapshot:    snapshot,
		FrameworkHandle:    handle,
		AutoscalingOptions: config.AutoscalingOptions{SchedulerConfig: cfg},
	}}
	excluded := &equivalence.PodGroup{Pods: []*apiv1.Pod{BuildTestPod("excluded", 100, 100, withInstanceTypeExclusion("type-a.large"))}, SchedulingErrors: map[string]status.Reasons{}}
	nodeGroup := testprovider.NewTestNodeGroup("ng", 10, 0, 0, true, false, "type-a.large", nil, nil)
	nodeInfo := framework.NewNodeInfo(benchNode("template", "type-a.large"), nil)

	got := o.SchedulablePodGroups(stdcontext.Background(), []*equivalence.PodGroup{excluded}, nodeGroup, nodeInfo)
	assert.Len(t, got, 1)
	assert.True(t, excluded.Schedulable)
	assert.Empty(t, excluded.SchedulingErrors)
}

func TestStaticPredicatesUsable(t *testing.T) {
	newHandle := func(mutate func(*schedulerconfig.Plugins)) *framework.Handle {
		cfg, err := scheduler_config_latest.Default()
		assert.NoError(t, err)
		if mutate != nil {
			mutate(cfg.Profiles[0].Plugins)
		}
		handle, err := framework.NewHandle(stdcontext.Background(), informers.NewSharedInformerFactory(clientsetfake.NewSimpleClientset(), 0), cfg, false, false)
		assert.NoError(t, err)
		return handle
	}
	assert.False(t, staticPredicatesUsable(nil))
	assert.True(t, staticPredicatesUsable(newHandle(nil)), "default profile")
	assert.False(t, staticPredicatesUsable(newHandle(func(p *schedulerconfig.Plugins) {
		p.Filter.Disabled = append(p.Filter.Disabled, schedulerconfig.Plugin{Name: names.TaintToleration})
	})), "TaintToleration disabled")
	assert.False(t, staticPredicatesUsable(newHandle(func(p *schedulerconfig.Plugins) {
		p.Filter.Disabled = append(p.Filter.Disabled, schedulerconfig.Plugin{Name: names.NodeAffinity})
	})), "NodeAffinity disabled")
	reordered := newHandle(func(p *schedulerconfig.Plugins) {
		p.Filter.Disabled = []schedulerconfig.Plugin{{Name: "*"}}
		p.Filter.Enabled = []schedulerconfig.Plugin{{Name: names.NodeAffinity}, {Name: names.TaintToleration}}
	})
	var filters []string
	for _, plugin := range reordered.Framework.ListPlugins().Filter.Enabled {
		filters = append(filters, plugin.Name)
	}
	assert.Equal(t, []string{names.NodeAffinity, names.TaintToleration}, filters)
	assert.False(t, staticPredicatesUsable(reordered), "TaintToleration after NodeAffinity")
}

// A pod that also fails another PreFilter plugin (here a missing PVC) is still rejected on a
// template it can't use, with the pre-check's reason instead of the other plugin's.
func TestStaticPredicatesErrorWithOtherPreFilterFailure(t *testing.T) {
	pod := BuildTestPod("pvc", 100, 100, withInstanceTypeExclusion("type-a.large"))
	pod.Spec.Volumes = []apiv1.Volume{{Name: "data", VolumeSource: apiv1.VolumeSource{PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{ClaimName: "missing"}}}}
	node := benchNode("n", "type-a.large")

	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(node)))
	frameworkErr := snapshot.CheckPredicates(pod, node.Name)
	got := staticPredicatesError(klog.Background(), pod, node)
	if assert.NotNil(t, frameworkErr) && assert.NotNil(t, got) {
		assert.Equal(t, "NodeAffinity", got.FailingPredicateName())
		assert.NotEqual(t, "NodeAffinity", frameworkErr.FailingPredicateName(), "the framework reports the PreFilter failure")
	}
}
