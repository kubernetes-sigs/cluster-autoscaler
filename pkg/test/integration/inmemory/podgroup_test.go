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

package inmemory

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakecloudprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/drain"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const podGroupName = "pod-group"

func gangPodGroup(minCount int32) *schedulingv1beta1.PodGroup {
	return &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: podGroupName, Namespace: "default"},
		Spec: schedulingv1beta1.PodGroupSpec{
			SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{
				Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: minCount},
			},
		},
	}
}

func basicPodGroup() *schedulingv1beta1.PodGroup {
	return &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: podGroupName, Namespace: "default"},
		Spec: schedulingv1beta1.PodGroupSpec{
			SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{
				Basic: &schedulingv1beta1.BasicSchedulingPolicy{},
			},
		},
	}
}

// TestScaleDown_NodesWithPodGroupMembers checks which PodGroup memberships
// protect a node from scale-down. A node group of two nodes may shrink to one;
// one node hosts a single, replicated pod that is a member of a PodGroup and
// would fit on the other node, so only its membership can keep the node group
// at two nodes.
func TestScaleDown_NodesWithPodGroupMembers(t *testing.T) {
	for _, tc := range []struct {
		name            string
		podGroupSupport bool
		// podGroup is the PodGroup the pod references; nil means it does not exist.
		podGroup *schedulingv1beta1.PodGroup
		wantSize int
	}{
		{
			name:            "member of a gang blocks scale-down",
			podGroupSupport: true,
			podGroup:        gangPodGroup(2),
			wantSize:        2,
		},
		{
			name:            "member of a gang is ignored when PodGroup support is disabled",
			podGroupSupport: false,
			podGroup:        gangPodGroup(2),
			wantSize:        1,
		},
		{
			name:            "member of a gang of one does not block scale-down",
			podGroupSupport: true,
			podGroup:        gangPodGroup(1),
			wantSize:        1,
		},
		{
			name:            "member of a basic PodGroup does not block scale-down",
			podGroupSupport: true,
			podGroup:        basicPodGroup(),
			wantSize:        1,
		},
		{
			name:            "member of a missing PodGroup conservatively blocks scale-down",
			podGroupSupport: true,
			podGroup:        nil,
			wantSize:        2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stepDuration := 10 * time.Second
			config := integration.NewTestConfig().
				WithOverrides(
					integration.WithScaleDownUnneededTime(unneededTime),
					integration.WithPodGroupSupport(tc.podGroupSupport),
				)
			options := config.ResolveOptions()
			infra := integration.SetupInfrastructure(t)
			fakes := infra.Fakes

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := test.GetTestContextWithCancel(t)
				defer synctestutils.TearDown(cancel)

				if tc.podGroup != nil {
					assert.NoError(t, infra.Manager.GetClient().Create(ctx, tc.podGroup))
				}

				autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
				assert.NoError(t, err)

				// Two underutilized nodes in a node group that may shrink to one.
				template := test.BuildTestNode("ng1-template", 1000, 1000, test.IsReady(true))
				fakes.CloudProvider.AddNodeGroup("ng1", fakecloudprovider.WithNodes(template, 2), fakecloudprovider.WithNGSize(1, 10))

				// The member is replicated and small, so it could be moved to
				// the other node. Only its PodGroup membership can protect its node.
				member := test.SetRSPodSpec(test.BuildScheduledTestPod("member", 100, 100, "ng1-node-0"), "rs")
				member.Spec.SchedulingGroup = &apiv1.PodSchedulingGroup{PodGroupName: new(podGroupName)}
				fakes.K8s.AddPod(member)

				// The other node hosts a pod that must not be evicted, so it is
				// never a scale-down candidate and the member's node is the only
				// node scale-down could remove.
				blocker := test.BuildScheduledTestPod("blocker", 100, 100, "ng1-node-1")
				blocker.Annotations = map[string]string{drain.PodSafeToEvictKey: "false"}
				fakes.K8s.AddPod(blocker)

				// One loop to find the node unneeded, another after the unneeded
				// time elapses to act on it.
				synctestutils.MustRunOnceAfter(ctx, t, autoscaler, stepDuration)
				synctestutils.MustRunOnceAfter(ctx, t, autoscaler, unneededTime+time.Nanosecond)

				size, err := fakes.CloudProvider.GetNodeGroup("ng1").TargetSize(ctx)
				assert.NoError(t, err)
				assert.Equal(t, tc.wantSize, size)
			})
		})
	}
}
