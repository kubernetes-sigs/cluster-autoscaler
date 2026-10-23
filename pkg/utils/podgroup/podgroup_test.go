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

package podgroup

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func newPodGroup(name string, policy schedulingv1beta1.PodGroupSchedulingPolicy) *schedulingv1beta1.PodGroup {
	return &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       schedulingv1beta1.PodGroupSpec{SchedulingPolicy: policy},
	}
}

func gangPolicy(minCount int32) schedulingv1beta1.PodGroupSchedulingPolicy {
	return schedulingv1beta1.PodGroupSchedulingPolicy{Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: minCount}}
}

func basicPolicy() schedulingv1beta1.PodGroupSchedulingPolicy {
	return schedulingv1beta1.PodGroupSchedulingPolicy{Basic: &schedulingv1beta1.BasicSchedulingPolicy{}}
}

// newFakeClient returns a client serving the given PodGroups.
func newFakeClient(podGroups ...*schedulingv1beta1.PodGroup) client.Client {
	builder := fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme)
	for _, podGroup := range podGroups {
		builder = builder.WithObjects(podGroup)
	}
	return builder.Build()
}

// withPodGroup sets the pod's scheduling group reference.
func withPodGroup(name string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		pod.Spec.SchedulingGroup = &apiv1.PodSchedulingGroup{PodGroupName: &name}
	}
}

func TestIsGangPod(t *testing.T) {
	kubeClient := newFakeClient(
		newPodGroup("gang", gangPolicy(4)),
		newPodGroup("gang-of-one", gangPolicy(1)),
		newPodGroup("basic", basicPolicy()),
	)
	// A scheme without the scheduling API stands in for a cluster that does
	// not serve PodGroups.
	clientWithoutAPI := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()
	terminalGangPod := BuildTestPod("terminal", 100, 100, withPodGroup("gang"))
	terminalGangPod.Status.Phase = apiv1.PodSucceeded

	for _, tc := range []struct {
		name       string
		pod        *apiv1.Pod
		kubeClient client.Reader
		want       bool
	}{
		{
			name:       "pod without scheduling group",
			pod:        BuildTestPod("plain", 100, 100),
			kubeClient: kubeClient,
		},
		{
			name:       "member of a gang",
			pod:        BuildTestPod("p", 100, 100, withPodGroup("gang")),
			kubeClient: kubeClient,
			want:       true,
		},
		{
			name:       "member of a gang of one",
			pod:        BuildTestPod("p", 100, 100, withPodGroup("gang-of-one")),
			kubeClient: kubeClient,
		},
		{
			name:       "member of a basic group",
			pod:        BuildTestPod("p", 100, 100, withPodGroup("basic")),
			kubeClient: kubeClient,
		},
		{
			name:       "terminal gang pod",
			pod:        terminalGangPod,
			kubeClient: kubeClient,
		},
		{
			name:       "unresolvable group is conservatively a gang",
			pod:        BuildTestPod("p", 100, 100, withPodGroup("missing")),
			kubeClient: kubeClient,
			want:       true,
		},
		{
			name:       "no client means no gangs",
			pod:        BuildTestPod("p", 100, 100, withPodGroup("gang")),
			kubeClient: nil,
		},
		{
			name:       "no PodGroup API means no gangs",
			pod:        BuildTestPod("p", 100, 100, withPodGroup("gang")),
			kubeClient: clientWithoutAPI,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, NewResolver(tc.kubeClient).IsGangPod(t.Context(), tc.pod))
		})
	}
}

func TestNodeHasGangPods(t *testing.T) {
	kubeClient := newFakeClient(newPodGroup("gang", gangPolicy(2)))
	node := BuildTestNode("n", 1000, 1000)
	plain := BuildTestPod("plain", 100, 100)
	member := BuildTestPod("member", 100, 100, withPodGroup("gang"))

	resolver := NewResolver(kubeClient)
	assert.False(t, resolver.NodeHasGangPods(t.Context(), framework.NewTestNodeInfo(node)))
	assert.False(t, resolver.NodeHasGangPods(t.Context(), framework.NewTestNodeInfo(node, plain)))
	assert.True(t, resolver.NodeHasGangPods(t.Context(), framework.NewTestNodeInfo(node, plain, member)))
}

func TestResolverCachesPerPodGroup(t *testing.T) {
	podGroup := newPodGroup("gang", gangPolicy(2))
	kubeClient := newFakeClient(podGroup)
	resolver := NewResolver(kubeClient)
	member := BuildTestPod("member", 100, 100, withPodGroup("gang"))
	assert.True(t, resolver.IsGangPod(t.Context(), member))

	// The group is no longer a gang, but the verdict was cached for this iteration.
	podGroup.Spec.SchedulingPolicy = basicPolicy()
	assert.NoError(t, kubeClient.Update(t.Context(), podGroup))
	assert.True(t, resolver.IsGangPod(t.Context(), member))
	assert.False(t, NewResolver(kubeClient).IsGangPod(t.Context(), member))
}

func TestResolverStopsLookingUpAfterMissingAPI(t *testing.T) {
	resolver := NewResolver(fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build())
	assert.True(t, resolver.Enabled())
	assert.False(t, resolver.IsGangPod(t.Context(), BuildTestPod("p", 100, 100, withPodGroup("gang"))))
	assert.False(t, resolver.Enabled())
}

func TestGet(t *testing.T) {
	gang := newPodGroup("gang", gangPolicy(2))
	kubeClient := newFakeClient(gang)
	resolver := NewResolver(kubeClient)

	got, err := resolver.Get(t.Context(), BuildTestPod("plain", 100, 100))
	assert.NoError(t, err)
	assert.Nil(t, got)

	got, err = resolver.Get(t.Context(), BuildTestPod("member", 100, 100, withPodGroup("gang")))
	assert.NoError(t, err)
	assert.Equal(t, gang.Name, got.Name)

	got, err = resolver.Get(t.Context(), BuildTestPod("orphan", 100, 100, withPodGroup("missing")))
	assert.Error(t, err)
	assert.Nil(t, got)
}
