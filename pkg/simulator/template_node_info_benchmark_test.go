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

package simulator

import (
	"context"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/daemonset"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// BenchmarkSanitizedTemplateNodeInfoFromNodeInfo mirrors one loop of template building in a
// large cluster: one template per node group (220), each getting the DaemonSet pods (35
// DaemonSets with node affinity and tolerations) that would run on it. "uncached" uses the
// existing functions, "cached" a DaemonSetPodsCache like MixedTemplateNodeInfoProvider.
func BenchmarkSanitizedTemplateNodeInfoFromNodeInfo(b *testing.B) {
	const nodeGroups, daemonSetCount = 220, 35
	var templates []*framework.NodeInfo
	for i := 0; i < nodeGroups; i++ {
		node := BuildTestNode(fmt.Sprintf("template-%d", i), 48000, 192*1024*1024*1024)
		node.Labels = map[string]string{apiv1.LabelArchStable: []string{"amd64", "arm64"}[i%2], "example.com/pool": fmt.Sprintf("pool-%d", i%4)}
		node.Spec.Taints = []apiv1.Taint{{Key: "example.com/pool", Value: fmt.Sprintf("pool-%d", i%4), Effect: apiv1.TaintEffectNoSchedule}}
		templates = append(templates, framework.NewTestNodeInfo(node))
	}
	var daemonSets []*appsv1.DaemonSet
	for i := 0; i < daemonSetCount; i++ {
		daemonSets = append(daemonSets, &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("ds-%d", i), Namespace: "kube-system", UID: types.UID(fmt.Sprintf("ds-%d", i)), Generation: 1},
			Spec: appsv1.DaemonSetSpec{Template: apiv1.PodTemplateSpec{Spec: apiv1.PodSpec{
				Containers:  []apiv1.Container{{Name: "c", Resources: apiv1.ResourceRequirements{Requests: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("100m"), apiv1.ResourceMemory: resource.MustParse("128Mi")}}}},
				Tolerations: []apiv1.Toleration{{Key: "example.com/pool", Operator: apiv1.TolerationOpExists}},
				Affinity: &apiv1.Affinity{NodeAffinity: &apiv1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
					NodeSelectorTerms: []apiv1.NodeSelectorTerm{{MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: apiv1.LabelArchStable, Operator: apiv1.NodeSelectorOpIn, Values: []string{"amd64", "arm64"}},
						{Key: "example.com/pool", Operator: apiv1.NodeSelectorOpNotIn, Values: []string{fmt.Sprintf("pool-%d", i%5)}},
					}}},
				}}},
			}}},
		})
	}
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		opts TemplateNodeInfoOptions
	}{
		{name: "uncached"},
		{name: "cached", opts: TemplateNodeInfoOptions{DaemonSetPodsCache: daemonset.NewPodsForNodeCache()}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for j, template := range templates {
					if _, err := SanitizedTemplateNodeInfoFromNodeInfoWithOptions(ctx, template, fmt.Sprintf("ng-%d", j), daemonSets, true, taints.TaintConfig{}, tc.opts); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
