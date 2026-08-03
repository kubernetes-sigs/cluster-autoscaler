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

package provreq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/conditions"
	provreqpods "sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
)

func TestGetCheckCapacityBatchCarriesMaterializedClaims(t *testing.T) {
	pr := provisioningRequestWithTemplateClaim("gpu-template", 2)
	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, pr)
	builder := provreqpods.NewSimulationWorkloadBuilder(injectorTemplateLister(t,
		&resourcev1.ResourceClaimTemplate{ObjectMeta: metav1.ObjectMeta{Name: "gpu-template", Namespace: "ns"}},
	))
	injector := NewProvisioningRequestPodsInjector(client, time.Minute, 10*time.Minute, 100, true, "", builder)

	batch, err := injector.GetCheckCapacityBatch(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.NotNil(t, batch[0].Workload)
	require.Len(t, batch[0].Workload.Pods, 2)
	require.Len(t, batch[0].Workload.Claims, 2)
	assert.NotEqual(t, batch[0].Workload.Claims[0].Name, batch[0].Workload.Claims[1].Name)
	for i, pod := range batch[0].Workload.Pods {
		require.Len(t, pod.Status.ResourceClaimStatuses, 1)
		assert.Equal(t, batch[0].Workload.Claims[i].Name, ptr.Deref(pod.Status.ResourceClaimStatuses[0].ResourceClaimName, ""))
	}
}

func TestGetCheckCapacityBatchMarksMissingTemplateFailed(t *testing.T) {
	pr := provisioningRequestWithTemplateClaim("missing-template", 1)
	client := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, pr)
	injector := NewProvisioningRequestPodsInjector(client, time.Minute, 10*time.Minute, 100, true, "",
		provreqpods.NewSimulationWorkloadBuilder(injectorTemplateLister(t)))

	batch, err := injector.GetCheckCapacityBatch(t.Context(), 1)
	require.NoError(t, err)
	assert.Empty(t, batch)
	updated, err := client.ProvisioningRequestNoCache(pr.Namespace, pr.Name)
	require.NoError(t, err)
	failed := apimeta.FindStatusCondition(updated.Status.Conditions, provreqv1.Failed)
	require.NotNil(t, failed)
	assert.Equal(t, conditions.FailedToCreatePodsReason, failed.Reason)
}

func provisioningRequestWithTemplateClaim(templateName string, count int) *provreqwrapper.ProvisioningRequest {
	pr := testProvisioningRequestWithCondition("test-pr", count, provreqv1.ProvisioningClassCheckCapacity)
	pr.PodTemplates[0].Template.Spec.ResourceClaims = []corev1.PodResourceClaim{
		{Name: "gpu", ResourceClaimTemplateName: ptr.To(templateName)},
	}
	return pr
}

func injectorTemplateLister(t *testing.T, templates ...*resourcev1.ResourceClaimTemplate) resourcelisters.ResourceClaimTemplateLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, template := range templates {
		require.NoError(t, indexer.Add(template))
	}
	return resourcelisters.NewResourceClaimTemplateLister(indexer)
}
