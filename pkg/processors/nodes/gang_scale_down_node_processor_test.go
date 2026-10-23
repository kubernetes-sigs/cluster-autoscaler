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

package nodes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestGangScaleDownNodeProcessor(t *testing.T) {
	gangGroup := &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "gang", Namespace: "default"},
		Spec: schedulingv1beta1.PodGroupSpec{SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{
			Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: 3},
		}},
	}
	basicGroup := &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "basic", Namespace: "default"},
		Spec: schedulingv1beta1.PodGroupSpec{SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{
			Basic: &schedulingv1beta1.BasicSchedulingPolicy{},
		}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).WithObjects(gangGroup, basicGroup).Build()

	inGroup := func(name string) func(*apiv1.Pod) {
		return func(pod *apiv1.Pod) {
			pod.Spec.SchedulingGroup = &apiv1.PodSchedulingGroup{PodGroupName: &name}
		}
	}
	empty := BuildTestNode("empty", 1000, 1000)
	plain := BuildTestNode("plain", 1000, 1000)
	withGang := BuildTestNode("with-gang", 1000, 1000)
	withBasic := BuildTestNode("with-basic", 1000, 1000)
	notInSnapshot := BuildTestNode("not-in-snapshot", 1000, 1000)
	pods := []*apiv1.Pod{
		BuildTestPod("p1", 100, 100, WithNodeName(plain.Name)),
		BuildTestPod("p2", 100, 100, WithNodeName(withGang.Name)),
		BuildTestPod("p3", 100, 100, WithNodeName(withGang.Name), inGroup("gang")),
		BuildTestPod("p4", 100, 100, WithNodeName(withBasic.Name), inGroup("basic")),
	}
	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	clustersnapshot.InitializeClusterSnapshotOrDie(t, snapshot, []*apiv1.Node{empty, plain, withGang, withBasic}, pods)
	autoscalingCtx := &ca_context.AutoscalingContext{ClusterSnapshot: snapshot}
	all := []*apiv1.Node{empty, plain, withGang, withBasic, notInSnapshot}

	processor := NewGangScaleDownNodeProcessor(kubeClient)

	candidates, err := processor.GetScaleDownCandidates(t.Context(), autoscalingCtx, all)
	assert.NoError(t, err)
	assert.Equal(t, []*apiv1.Node{empty, plain, withBasic, notInSnapshot}, candidates)

	destinations, err := processor.GetPodDestinationCandidates(autoscalingCtx, all)
	assert.NoError(t, err)
	assert.Equal(t, all, destinations)

	// Without a client the processor is a no-op.
	candidates, err = NewGangScaleDownNodeProcessor(nil).GetScaleDownCandidates(t.Context(), autoscalingCtx, all)
	assert.NoError(t, err)
	assert.Equal(t, all, candidates)
}
