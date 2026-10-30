/*
Copyright 2017 The Kubernetes Authors.

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

package daemonset

import (
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/labels"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/daemon"
	"k8s.io/utils/ptr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDaemonSetPodsForNode(t *testing.T) {
	node := BuildTestNode("node", 1000, 1000)
	SetNodeReadyState(node, true, time.Now())
	nodeInfo := framework.NewTestNodeInfo(node)

	ds1 := newDaemonSet("ds1", "0.1", "100M", nil)
	ds1.UID = "ds1-uid"
	ds2 := newDaemonSet("ds2", "0.1", "100M", map[string]string{"foo": "bar"})
	ds3 := newDaemonSet("ds1", "4", "4000M", nil)

	{
		daemonSets, err := GetDaemonSetPodsForNode(nodeInfo, []*appsv1.DaemonSet{ds1, ds2})

		require.NoError(t, err)
		require.Len(t, daemonSets, 1)
		assert.True(t, strings.HasPrefix(daemonSets[0].Name, "ds1-pod-"))
		assert.Equal(t, "node", daemonSets[0].Spec.NodeName)
		controllerRef := metav1.GetControllerOf(daemonSets[0].Pod)
		require.NotNil(t, controllerRef)
		assert.Equal(t, ds1.UID, controllerRef.UID)
	}
	{
		daemonSets, err := GetDaemonSetPodsForNode(nodeInfo, []*appsv1.DaemonSet{ds1})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(daemonSets))
	}
	{
		daemonSets, err := GetDaemonSetPodsForNode(nodeInfo, []*appsv1.DaemonSet{ds2})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(daemonSets))
	}
	{
		daemonSets, err := GetDaemonSetPodsForNode(nodeInfo, []*appsv1.DaemonSet{ds3})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(daemonSets))
	}
	{
		daemonSets, err := GetDaemonSetPodsForNode(nodeInfo, []*appsv1.DaemonSet{})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(daemonSets))
	}
}

func TestEvictedPodsFilter(t *testing.T) {
	testCases := []struct {
		name            string
		pods            map[string]string
		evictionDefault bool
		expectedPods    []string
	}{
		{
			name: "all pods evicted by default",
			pods: map[string]string{
				"p1": "",
				"p2": "",
				"p3": "",
			},
			evictionDefault: true,
			expectedPods:    []string{"p1", "p2", "p3"},
		},
		{
			name: "no pods evicted by default",
			pods: map[string]string{
				"p1": "",
				"p2": "",
				"p3": "",
			},
			evictionDefault: false,
			expectedPods:    []string{},
		},
		{
			name: "all pods evicted by default, one opt-out",
			pods: map[string]string{
				"p1": "",
				"p2": "false",
				"p3": "",
			},
			evictionDefault: true,
			expectedPods:    []string{"p1", "p3"},
		},
		{
			name: "no pods evicted by default, one opt-in",
			pods: map[string]string{
				"p1": "",
				"p2": "true",
				"p3": "",
			},
			evictionDefault: false,
			expectedPods:    []string{"p2"},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var dsPods []*apiv1.Pod
			for n, av := range tc.pods {
				p := BuildTestPod(n, 100, 0)
				if av != "" {
					p.Annotations[EnableDsEvictionKey] = av
				}
				dsPods = append(dsPods, p)
			}
			pte := PodsToEvict(dsPods, tc.evictionDefault)
			got := make([]string, len(pte))
			for i, p := range pte {
				got[i] = p.Name
			}
			assert.ElementsMatch(t, got, tc.expectedPods)
		})
	}
}

func newDaemonSet(name, cpu, memory string, selector map[string]string) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"name": "simple-daemon", "type": "production"}},
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"name": "simple-daemon", "type": "production"},
				},
				Spec: apiv1.PodSpec{
					NodeSelector: selector,
					Containers: []apiv1.Container{
						{
							Image: "foo/bar",
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceCPU:    resource.MustParse(cpu),
									apiv1.ResourceMemory: resource.MustParse(memory),
								},
							},
						},
					},
				},
			},
		},
	}
}

func withNodeAffinity(terms ...apiv1.NodeSelectorTerm) func(*apiv1.PodSpec) {
	return func(spec *apiv1.PodSpec) {
		spec.Affinity = &apiv1.Affinity{NodeAffinity: &apiv1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{NodeSelectorTerms: terms},
		}}
	}
}

func nodeSelectorTerm(key string, operator apiv1.NodeSelectorOperator, values ...string) apiv1.NodeSelectorTerm {
	return apiv1.NodeSelectorTerm{MatchExpressions: []apiv1.NodeSelectorRequirement{{Key: key, Operator: operator, Values: values}}}
}

func TestCompiledDaemonSetMatchesDaemonController(t *testing.T) {
	testCases := []struct {
		name       string
		modifySpec func(*apiv1.PodSpec)
		nodeLabels map[string]string
		nodeTaints []apiv1.Taint
	}{
		{name: "no constraints"},
		{
			name:       "node selector matches",
			modifySpec: func(spec *apiv1.PodSpec) { spec.NodeSelector = map[string]string{"pool": "a"} },
			nodeLabels: map[string]string{"pool": "a"},
		},
		{
			name:       "node selector does not match",
			modifySpec: func(spec *apiv1.PodSpec) { spec.NodeSelector = map[string]string{"pool": "a"} },
			nodeLabels: map[string]string{"pool": "b"},
		},
		{
			name:       "second affinity term matches",
			modifySpec: withNodeAffinity(nodeSelectorTerm("pool", apiv1.NodeSelectorOpIn, "x"), nodeSelectorTerm("tier", apiv1.NodeSelectorOpExists)),
			nodeLabels: map[string]string{"tier": "1"},
		},
		{
			name:       "NotIn affinity excludes node",
			modifySpec: withNodeAffinity(nodeSelectorTerm("pool", apiv1.NodeSelectorOpNotIn, "a")),
			nodeLabels: map[string]string{"pool": "a"},
		},
		{
			name:       "node name does not match",
			modifySpec: func(spec *apiv1.PodSpec) { spec.NodeName = "other-node" },
		},
		{
			name:       "untolerated NoSchedule taint",
			nodeTaints: []apiv1.Taint{{Key: "dedicated", Value: "gpu", Effect: apiv1.TaintEffectNoSchedule}},
		},
		{
			name: "tolerated NoSchedule taint",
			modifySpec: func(spec *apiv1.PodSpec) {
				spec.Tolerations = []apiv1.Toleration{{Key: "dedicated", Operator: apiv1.TolerationOpExists}}
			},
			nodeTaints: []apiv1.Taint{{Key: "dedicated", Value: "gpu", Effect: apiv1.TaintEffectNoSchedule}},
		},
		{
			name:       "untolerated NoExecute taint",
			nodeTaints: []apiv1.Taint{{Key: "dedicated", Value: "gpu", Effect: apiv1.TaintEffectNoExecute}},
		},
		{
			name:       "PreferNoSchedule taint is ignored",
			nodeTaints: []apiv1.Taint{{Key: "dedicated", Value: "gpu", Effect: apiv1.TaintEffectPreferNoSchedule}},
		},
		{
			name:       "default not-ready toleration",
			nodeTaints: []apiv1.Taint{{Key: apiv1.TaintNodeNotReady, Effect: apiv1.TaintEffectNoExecute}},
		},
		{
			name:       "default unschedulable toleration",
			nodeTaints: []apiv1.Taint{{Key: apiv1.TaintNodeUnschedulable, Effect: apiv1.TaintEffectNoSchedule}},
		},
		{
			name:       "host network DaemonSet tolerates network-unavailable",
			modifySpec: func(spec *apiv1.PodSpec) { spec.HostNetwork = true },
			nodeTaints: []apiv1.Taint{{Key: apiv1.TaintNodeNetworkUnavailable, Effect: apiv1.TaintEffectNoSchedule}},
		},
		{
			name:       "non-host-network DaemonSet does not tolerate network-unavailable",
			nodeTaints: []apiv1.Taint{{Key: apiv1.TaintNodeNetworkUnavailable, Effect: apiv1.TaintEffectNoSchedule}},
		},
		{
			name:       "node name matches",
			modifySpec: func(spec *apiv1.PodSpec) { spec.NodeName = "node" },
		},
		{
			name: "existing default toleration is updated",
			modifySpec: func(spec *apiv1.PodSpec) {
				spec.Tolerations = []apiv1.Toleration{{
					Key:               apiv1.TaintNodeNotReady,
					Operator:          apiv1.TolerationOpExists,
					Effect:            apiv1.TaintEffectNoExecute,
					TolerationSeconds: ptr.To(int64(300)),
				}}
			},
			nodeTaints: []apiv1.Taint{{Key: apiv1.TaintNodeNotReady, Effect: apiv1.TaintEffectNoExecute}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ds := newDaemonSet("ds", "100m", "100M", nil)
			if tc.modifySpec != nil {
				tc.modifySpec(&ds.Spec.Template.Spec)
			}
			node := BuildTestNode("node", 1000, 1000, WithNodeLabels(tc.nodeLabels))
			node.Spec.Taints = tc.nodeTaints
			compiled := CompileDaemonSets([]*appsv1.DaemonSet{ds})[0]

			wantShouldRun, _ := daemon.NodeShouldRunDaemonPod(klog.Background(), node, ds)
			assert.Equal(t, wantShouldRun, compiled.ShouldRunOnNode(node))

			wantPod := daemon.NewPod(ds, ds.Spec.Template.Spec.NodeName)
			assert.Equal(t, wantPod.Spec, compiled.TemplatePod().Spec)
			assert.Equal(t, wantPod.Namespace, compiled.TemplatePod().Namespace)
			assert.Equal(t, wantPod.Labels, compiled.TemplatePod().Labels)
		})
	}
}

func TestCompileDaemonSetsDoesNotModifyInput(t *testing.T) {
	ds := newDaemonSet("ds", "100m", "100M", nil)
	ds.Spec.Template.Spec.Tolerations = []apiv1.Toleration{{Key: "custom", Operator: apiv1.TolerationOpExists}}
	original := ds.DeepCopy()

	compiled := CompileDaemonSets([]*appsv1.DaemonSet{ds})[0]
	compiled.TemplatePod().Labels["changed"] = "true"
	compiled.TemplatePod().Spec.Containers[0].Image = "changed"
	compiled.TemplatePod().Spec.Tolerations[0].Key = "changed"

	assert.Equal(t, original, ds)
}

func TestIsPreemptingSystemNodeCritical(t *testing.T) {
	testCases := []struct {
		name              string
		priorityClassName string
		preemptionPolicy  *apiv1.PreemptionPolicy
		want              bool
	}{
		{
			name: "no priority class",
			want: false,
		},
		{
			name:              "other priority class",
			priorityClassName: "high-priority",
			want:              false,
		},
		{
			name:              "system-node-critical with default policy",
			priorityClassName: labels.SystemNodeCriticalLabel,
			want:              true,
		},
		{
			name:              "system-node-critical preempting",
			priorityClassName: labels.SystemNodeCriticalLabel,
			preemptionPolicy:  ptr.To(apiv1.PreemptLowerPriority),
			want:              true,
		},
		{
			name:              "system-node-critical never preempting",
			priorityClassName: labels.SystemNodeCriticalLabel,
			preemptionPolicy:  ptr.To(apiv1.PreemptNever),
			want:              false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ds := newDaemonSet("ds", "100m", "100M", nil)
			ds.Spec.Template.Spec.PriorityClassName = tc.priorityClassName
			ds.Spec.Template.Spec.PreemptionPolicy = tc.preemptionPolicy
			assert.Equal(t, tc.want, CompileDaemonSets([]*appsv1.DaemonSet{ds})[0].IsPreemptingSystemNodeCritical())
		})
	}
}
