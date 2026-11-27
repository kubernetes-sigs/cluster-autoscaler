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

package predicate

import (
	"context"
	"fmt"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func benchAntiAffinity(app string) *apiv1.Affinity {
	return &apiv1.Affinity{PodAntiAffinity: &apiv1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []apiv1.PodAffinityTerm{{
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
			TopologyKey:   apiv1.LabelHostname,
		}},
	}}
}

// BenchmarkSetClusterStateRepeated mimics the autoscaler loop: SetClusterState is called
// every iteration with the same (informer-cached) Node and Pod objects.
//
// "large" mirrors a representative large cluster: ~2.5k nodes, ~9 pods per node:
// 7 DaemonSet pods (pinned with a metadata.name node affinity), one agent pod with hostname
// anti-affinity, and one workload pod with hostname anti-affinity over ~125 selectors.
func BenchmarkSetClusterStateRepeated(b *testing.B) {
	for _, tc := range []struct {
		name                        string
		nodes, dsPods, workloadPods int
		agent                       bool
	}{
		{name: "small", nodes: 100, workloadPods: 20},
		{name: "large", nodes: 2500, dsPods: 7, workloadPods: 1, agent: true},
	} {
		nodes := clustersnapshot.CreateTestNodes(tc.nodes)
		var pods []*apiv1.Pod
		addPod := func(name, nodeName string, affinity *apiv1.Affinity, labels map[string]string) {
			pod := BuildTestPod(name, 100, 1024, WithNodeName(nodeName))
			pod.ResourceVersion = "1"
			pod.Labels = labels
			pod.Spec.Affinity = affinity
			pods = append(pods, pod)
		}
		for i, node := range nodes {
			for j := 0; j < tc.dsPods; j++ {
				addPod(fmt.Sprintf("ds-%d-%d", i, j), node.Name, &apiv1.Affinity{NodeAffinity: &apiv1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{NodeSelectorTerms: []apiv1.NodeSelectorTerm{{
						MatchFields: []apiv1.NodeSelectorRequirement{{Key: "metadata.name", Operator: apiv1.NodeSelectorOpIn, Values: []string{node.Name}}},
					}}},
				}}, map[string]string{"ds": fmt.Sprint(j)})
			}
			if tc.agent {
				addPod(fmt.Sprintf("agent-%d", i), node.Name, benchAntiAffinity("agent"), map[string]string{"app": "agent"})
			}
			for j := 0; j < tc.workloadPods; j++ {
				app := fmt.Sprintf("app-%d", (i*tc.workloadPods+j)%124)
				addPod(fmt.Sprintf("p-%d-%d", i, j), node.Name, benchAntiAffinity(app), map[string]string{"app": app})
			}
		}
		for _, storeName := range []string{"basic", "delta"} {
			fwHandle, err := framework.NewTestFrameworkHandle()
			if err != nil {
				b.Fatal(err)
			}
			snapshotStore := clustersnapshot.ClusterSnapshotStore(store.NewBasicSnapshotStore())
			if storeName == "delta" {
				snapshotStore = store.NewDeltaSnapshotStore()
			}
			// DRA and CSI off, parallelism 16.
			snapshot := NewPredicateSnapshot(snapshotStore, fwHandle, false, 16, false, 0)
			b.Run(fmt.Sprintf("%s/%s/nodes=%d/pods=%d", tc.name, storeName, tc.nodes, len(pods)), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if err := snapshot.SetClusterState(context.Background(), nodes, pods, nil, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
