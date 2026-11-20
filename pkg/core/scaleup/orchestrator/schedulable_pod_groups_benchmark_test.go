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
	"fmt"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/equivalence"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const benchInstanceTypeLabel = "node.kubernetes.io/instance-type"

// withHostnameAntiAffinity adds a required hostname anti-affinity on app=<app>, like the
// "one pod per node" isolation used by many workloads. These are what make the
// InterPodAffinity PreFilter expensive: it scans every pod in the cluster.
func withHostnameAntiAffinity(app string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels["app"] = app
		if pod.Spec.Affinity == nil {
			pod.Spec.Affinity = &apiv1.Affinity{}
		}
		pod.Spec.Affinity.PodAntiAffinity = &apiv1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []apiv1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
				TopologyKey:   apiv1.LabelHostname,
			}},
		}
	}
}

// withInstanceTypeExclusion adds a required node affinity term excluding the given instance types.
func withInstanceTypeExclusion(instanceTypes ...string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		if pod.Spec.Affinity == nil {
			pod.Spec.Affinity = &apiv1.Affinity{}
		}
		pod.Spec.Affinity.NodeAffinity = &apiv1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
				NodeSelectorTerms: []apiv1.NodeSelectorTerm{{
					MatchExpressions: []apiv1.NodeSelectorRequirement{{
						Key:      benchInstanceTypeLabel,
						Operator: apiv1.NodeSelectorOpNotIn,
						Values:   instanceTypes,
					}},
				}},
			},
		}
	}
}

func benchNode(name, instanceType string) *apiv1.Node {
	node := BuildTestNode(name, 48000, 192*1024*1024*1024)
	node.Labels = map[string]string{
		apiv1.LabelHostname:    name,
		benchInstanceTypeLabel: instanceType,
	}
	SetNodeReadyState(node, true, node.CreationTimestamp.Time)
	return node
}

// withDaemonSetNodePin adds the node affinity the DaemonSet controller sets on its pods.
func withDaemonSetNodePin(nodeName string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		pod.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet", Name: "ds", Controller: boolPtr(true)}}
		pod.Spec.Affinity = &apiv1.Affinity{NodeAffinity: &apiv1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{NodeSelectorTerms: []apiv1.NodeSelectorTerm{{
				MatchFields: []apiv1.NodeSelectorRequirement{{Key: "metadata.name", Operator: apiv1.NodeSelectorOpIn, Values: []string{nodeName}}},
			}}},
		}}
	}
}

func boolPtr(b bool) *bool { return &b }

// benchCluster describes a cluster shape. "large" mirrors a representative large cluster: ~2.5k nodes,
// ~9 pods per node of which ~7 are DaemonSet pods, one node agent pod with hostname
// anti-affinity on every node, and ~1 workload pod with hostname anti-affinity spread over
// ~125 distinct selectors; ~220 node groups and typically 1-40 unschedulable pods.
type benchCluster struct {
	name                  string
	nodes                 int
	daemonSetPodsPerNode  int
	agentPerNode          bool
	workloadPodsPerNode   int
	antiAffinitySelectors int
	pendingGroups         int
}

var benchClusters = []benchCluster{
	{name: "small", nodes: 100, daemonSetPodsPerNode: 0, workloadPodsPerNode: 10, antiAffinitySelectors: 10, pendingGroups: 10},
	{name: "large", nodes: 2500, daemonSetPodsPerNode: 7, agentPerNode: true, workloadPodsPerNode: 1, antiAffinitySelectors: 124, pendingGroups: 10},
}

func (c benchCluster) build() ([]*apiv1.Node, []*apiv1.Pod) {
	var nodes []*apiv1.Node
	var pods []*apiv1.Pod
	for i := 0; i < c.nodes; i++ {
		node := benchNode(fmt.Sprintf("node-%d", i), "type-c.large")
		nodes = append(nodes, node)
		for j := 0; j < c.daemonSetPodsPerNode; j++ {
			pods = append(pods, BuildTestPod(fmt.Sprintf("ds-%d-%d", i, j), 100, 1024, WithNodeName(node.Name), withDaemonSetNodePin(node.Name)))
		}
		if c.agentPerNode {
			pods = append(pods, BuildTestPod(fmt.Sprintf("agent-%d", i), 100, 1024, WithNodeName(node.Name), withHostnameAntiAffinity("agent")))
		}
		for j := 0; j < c.workloadPodsPerNode; j++ {
			app := fmt.Sprintf("app-%d", (i*c.workloadPodsPerNode+j)%c.antiAffinitySelectors)
			pods = append(pods, BuildTestPod(fmt.Sprintf("p-%d-%d", i, j), 100, 1024, WithNodeName(node.Name), withHostnameAntiAffinity(app)))
		}
	}
	return nodes, pods
}

// BenchmarkSchedulablePodGroups measures one SchedulablePodGroups call (scale-up runs it once
// per node group on every loop) for a template node that the
// pending pods can't use (excluded instance type) and one they can.
func BenchmarkSchedulablePodGroups(b *testing.B) {
	for _, c := range benchClusters {
		nodes, pods := c.build()
		snapshot, handle, err := testsnapshot.NewTestSnapshotAndHandle()
		if err != nil {
			b.Fatal(err)
		}
		if err := snapshot.SetClusterState(stdcontext.Background(), nodes, pods, nil, nil); err != nil {
			b.Fatal(err)
		}
		o := &ScaleUpOrchestrator{autoscalingCtx: &ca_context.AutoscalingContext{ClusterSnapshot: snapshot, FrameworkHandle: handle}}

		newGroups := func() []*equivalence.PodGroup {
			var groups []*equivalence.PodGroup
			for g := 0; g < c.pendingGroups; g++ {
				pod := BuildTestPod(fmt.Sprintf("pending-%d", g), 100, 1024,
					withHostnameAntiAffinity(fmt.Sprintf("app-%d", g%c.antiAffinitySelectors)),
					withInstanceTypeExclusion("type-a.large", "type-b.large"))
				groups = append(groups, &equivalence.PodGroup{Pods: []*apiv1.Pod{pod}, SchedulingErrors: map[string]status.Reasons{}})
			}
			return groups
		}

		for _, tmpl := range []struct {
			name, instanceType string
		}{
			{name: "excluded", instanceType: "type-a.large"},
			{name: "allowed", instanceType: "type-c.xlarge"},
		} {
			nodeGroup := testprovider.NewTestNodeGroup("ng-"+tmpl.instanceType, 100, 0, 0, true, false, tmpl.instanceType, nil, nil)
			nodeInfo := framework.NewNodeInfo(benchNode("template-"+tmpl.instanceType, tmpl.instanceType), nil)
			b.Run(fmt.Sprintf("%s/nodes=%d/pods=%d/groups=%d/template=%s", c.name, len(nodes), len(pods), c.pendingGroups, tmpl.name), func(b *testing.B) {
				groups := newGroups()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					o.SchedulablePodGroups(stdcontext.Background(), groups, nodeGroup, nodeInfo)
				}
			})
		}
	}
}
