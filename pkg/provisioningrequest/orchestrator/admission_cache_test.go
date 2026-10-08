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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	prfake "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/clientset/versioned/fake"
	prlisters "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/listers/autoscaling.x-k8s.io/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/provreq"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestStaleAdmissionCacheDoesNotDoubleBookCapacity(t *testing.T) {
	for _, maxSize := range []int{2, 4} {
		t.Run(fmt.Sprintf("%d-node-limit", maxSize), func(t *testing.T) {
			ctx := t.Context()
			requests, _ := adaptiveRequests(t, []int32{1, 1, 1, 1})
			requestCache := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			templateCache := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			var objects []runtime.Object
			for _, request := range requests {
				objects = append(objects, request.DeepCopy())
				require.NoError(t, requestCache.Add(request.DeepCopy()))
				for _, template := range request.PodTemplates {
					require.NoError(t, templateCache.Add(template.DeepCopy()))
				}
			}
			api := prfake.NewSimpleClientset(objects...)
			client := provreqclient.NewProvisioningRequestClient(api,
				prlisters.NewProvisioningRequestLister(requestCache), corelisters.NewPodTemplateLister(templateCache))
			nodes := []*apiv1.Node{BuildTestNode("pool-node-0", 100, 10), BuildTestNode("pool-node-1", 100, 10)}
			for _, node := range nodes {
				SetNodeReadyState(node, true, time.Now().Add(-time.Minute))
			}
			var resizes []int
			env := setupTestEnvironment(t, client, nodes, func(_ string, delta int) error {
				resizes = append(resizes, delta)
				return nil
			}, false, batchTestOptions{
				bestEffortAtomic: true, maxBatchSize: 2,
				nodeGroups: []batchTestNodeGroup{{name: "pool", maxSize: maxSize, nodes: nodes}},
			})
			booking := provreq.NewProvReqProcessor(client, "")
			run := func() *status.ScaleUpStatus {
				clustersnapshot.InitializeClusterSnapshotOrDie(t, env.clusterSnapshot, nodes, nil)
				injected, err := env.podsInjector.Process(ctx, nil, nil)
				require.NoError(t, err)
				require.Len(t, injected, 2)
				_, err = booking.Process(ctx, &ca_context.AutoscalingContext{ClusterSnapshot: env.clusterSnapshot}, injected)
				require.NoError(t, err)
				st, scaleErr := env.orchestrator.ScaleUp(ctx, injected, nodes, nil, env.nodeInfos, false)
				require.NoError(t, scaleErr)
				return st
			}
			require.Equal(t, status.ScaleUpNotNeeded, run().Result)
			stored, err := client.ProvisioningRequestsNoCache()
			require.NoError(t, err)
			require.Equal(t, 2, NumProvisioningRequestsWithCondition(stored, v1.Provisioned, metav1.ConditionTrue))
			for _, raw := range requestCache.List() {
				assert.False(t, apimeta.IsStatusConditionTrue(raw.(*v1.ProvisioningRequest).Status.Conditions, v1.Provisioned))
			}

			st := run()
			stored, err = client.ProvisioningRequestsNoCache()
			require.NoError(t, err)
			assert.Equal(t, maxSize, NumProvisioningRequestsWithCondition(stored, v1.Provisioned, metav1.ConditionTrue))
			if maxSize == 2 {
				assert.Equal(t, status.ScaleUpNoOptionsAvailable, st.Result)
				assert.Empty(t, resizes)
			} else {
				assert.Equal(t, status.ScaleUpSuccessful, st.Result)
				assert.Equal(t, []int{2}, resizes, "the second batch needs its own capacity")
			}
		})
	}
}
