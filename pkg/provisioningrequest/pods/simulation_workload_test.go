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

package pods

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	drautils "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/utils"
)

func TestSimulationWorkloadBuilderForProvisioningRequestMaterializesEveryPod(t *testing.T) {
	claimTemplate := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-template", Namespace: "test-ns"},
	}
	podTemplate := &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-template", Namespace: "test-ns"},
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				ResourceClaims: []corev1.PodResourceClaim{
					{Name: "gpu", ResourceClaimTemplateName: ptr.To(claimTemplate.Name)},
				},
			},
		},
	}
	pr := &provreqv1.ProvisioningRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pr", Namespace: "test-ns", UID: types.UID("pr-uid")},
		Spec: provreqv1.ProvisioningRequestSpec{
			ProvisioningClassName: "test-class",
			PodSets: []provreqv1.PodSet{
				{Count: 3, PodTemplateRef: provreqv1.Reference{Name: podTemplate.Name}},
			},
		},
	}
	builder := NewSimulationWorkloadBuilder(newResourceClaimTemplateLister(t, claimTemplate))
	wrapped := provreqwrapper.NewProvisioningRequest(pr, []*corev1.PodTemplate{podTemplate})

	workload, err := builder.ForProvisioningRequest(wrapped)
	require.NoError(t, err)
	require.Len(t, workload.Pods, 3)
	require.Len(t, workload.Claims, 3)

	claimNames := make(map[string]struct{})
	for i := range workload.Pods {
		pod := workload.Pods[i]
		claim := workload.Claims[i]
		require.Len(t, pod.Status.ResourceClaimStatuses, 1)
		assert.Equal(t, claim.Name, ptr.Deref(pod.Status.ResourceClaimStatuses[0].ResourceClaimName, ""))
		claimNames[claim.Name] = struct{}{}
	}
	assert.Len(t, claimNames, 3)
	replayed, err := builder.ForProvisioningRequest(wrapped)
	require.NoError(t, err)
	assert.Equal(t, workload, replayed)
}

func TestSimulationWorkloadBuilderCopiesTemplateData(t *testing.T) {
	claimTemplate := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-template", Namespace: "test-ns"},
		Spec: resourcev1.ResourceClaimTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{"template-label": "original"},
				Annotations: map[string]string{"template-annotation": "original"},
			},
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{
						{Name: "device", Exactly: &resourcev1.ExactDeviceRequest{DeviceClassName: "gpu.example.com"}},
					},
				},
			},
		},
	}
	networkTemplate := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "network-template", Namespace: "test-ns"},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      strings.Repeat("a", 245),
			Namespace: "test-ns",
			UID:       types.UID("pod-uid"),
		},
		Spec: corev1.PodSpec{
			ResourceClaims: []corev1.PodResourceClaim{
				{Name: "gpu", ResourceClaimTemplateName: ptr.To(claimTemplate.Name)},
				{Name: "shared", ResourceClaimName: ptr.To("shared-claim")},
				{Name: "network", ResourceClaimTemplateName: ptr.To(networkTemplate.Name)},
			},
		},
	}
	builder := NewSimulationWorkloadBuilder(newResourceClaimTemplateLister(t, claimTemplate, networkTemplate))

	workload, err := builder.forPods([]*corev1.Pod{pod})
	require.NoError(t, err)
	require.Len(t, workload.Pods, 1)
	require.Len(t, workload.Claims, 2)
	materializedPod := workload.Pods[0]
	claim := workload.Claims[0]
	networkClaim := workload.Claims[1]

	assert.Same(t, pod, materializedPod)
	assert.Equal(t, pod.Spec.ResourceClaims, materializedPod.Spec.ResourceClaims)
	require.Len(t, materializedPod.Status.ResourceClaimStatuses, 2)
	assert.Equal(t, claim.Name, ptr.Deref(materializedPod.Status.ResourceClaimStatuses[0].ResourceClaimName, ""))
	assert.Equal(t, networkClaim.Name, ptr.Deref(materializedPod.Status.ResourceClaimStatuses[1].ResourceClaimName, ""))
	assert.Equal(t, "network", networkClaim.Annotations[resourcev1.PodResourceClaimAnnotation])
	assert.LessOrEqual(t, len(claim.Name), maxSimulationResourceClaimNameLength)
	assert.Empty(t, validation.IsDNS1123Subdomain(claim.Name))
	assert.Regexp(t, `-[0-9a-f]{16}$`, claim.Name)
	assert.Equal(t, types.UID(claim.Namespace+"/"+claim.Name), claim.UID)
	assert.Equal(t, "original", claim.Labels["template-label"])
	assert.Equal(t, "original", claim.Annotations["template-annotation"])
	assert.Equal(t, "gpu", claim.Annotations[resourcev1.PodResourceClaimAnnotation])
	assert.Equal(t, claimTemplate.Spec.Spec, claim.Spec)
	require.Len(t, claim.OwnerReferences, 1)
	assert.Equal(t, drautils.PodClaimOwnerReference(pod), claim.OwnerReferences[0])
	require.NoError(t, resourceclaim.IsForPod(pod, claim, false))
	require.NoError(t, resourceclaim.IsForPod(pod, networkClaim, false))

	assert.NotContains(t, claimTemplate.Spec.Annotations, resourcev1.PodResourceClaimAnnotation, "cached template was mutated")
	workload.Claims[0].Labels["template-label"] = "changed"
	workload.Claims[0].Annotations["template-annotation"] = "changed"
	workload.Claims[0].Spec.Devices.Requests[0].Name = "changed"
	assert.Equal(t, "original", claimTemplate.Spec.Labels["template-label"])
	assert.Equal(t, "original", claimTemplate.Spec.Annotations["template-annotation"])
	assert.Equal(t, "device", claimTemplate.Spec.Spec.Devices.Requests[0].Name)
}

func TestSimulationWorkloadBuilderReturnsRuntimeErrors(t *testing.T) {
	claimTemplate := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-template", Namespace: "test-ns"},
	}
	validPod := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "test-ns", UID: types.UID("pod-uid")},
			Spec: corev1.PodSpec{
				ResourceClaims: []corev1.PodResourceClaim{
					{Name: "gpu", ResourceClaimTemplateName: ptr.To(claimTemplate.Name)},
				},
			},
		}
	}

	tests := []struct {
		name    string
		builder *SimulationWorkloadBuilder
		pod     *corev1.Pod
		wantErr string
	}{
		{
			name:    "missing template",
			builder: NewSimulationWorkloadBuilder(newResourceClaimTemplateLister(t)),
			pod:     validPod(),
			wantErr: "could not get ResourceClaimTemplate test-ns/gpu-template",
		},
		{
			name:    "lister not configured",
			builder: NewSimulationWorkloadBuilder(nil),
			pod:     validPod(),
			wantErr: "no ResourceClaimTemplate lister is configured",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.builder.forPods([]*corev1.Pod{test.pod})
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}
}

func TestSimulationWorkloadBuilderPassesDirectClaimsThroughWithoutTemplateLister(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "test-ns", UID: types.UID("pod-uid")},
		Spec: corev1.PodSpec{
			ResourceClaims: []corev1.PodResourceClaim{
				{Name: "shared", ResourceClaimName: ptr.To("shared-claim")},
			},
		},
	}

	workload, err := NewSimulationWorkloadBuilder(nil).forPods([]*corev1.Pod{pod})

	require.NoError(t, err)
	assert.Empty(t, workload.Claims)
	require.Len(t, workload.Pods, 1)
	assert.Equal(t, pod.Spec.ResourceClaims, workload.Pods[0].Spec.ResourceClaims)
	assert.Empty(t, workload.Pods[0].Status.ResourceClaimStatuses)
}

func TestSimulationResourceClaimName(t *testing.T) {
	tests := []struct {
		name             string
		podName          string
		logicalClaimName string
		wantPrefix       string
	}{
		{
			name:             "short names remain intact",
			podName:          "pod",
			logicalClaimName: "gpu",
			wantPrefix:       "pod-gpu",
		},
		{
			name:             "prefix exactly fills available space",
			podName:          strings.Repeat("a", 40),
			logicalClaimName: strings.Repeat("b", 5),
			wantPrefix:       strings.Repeat("a", 40) + "-" + strings.Repeat("b", 5),
		},
		{
			name:             "one extra character triggers proportional truncation",
			podName:          strings.Repeat("a", 40),
			logicalClaimName: strings.Repeat("b", 6),
			wantPrefix:       strings.Repeat("a", 39) + "-" + strings.Repeat("b", 6),
		},
		{
			name:             "both parts are truncated proportionally",
			podName:          strings.Repeat("a", 50),
			logicalClaimName: strings.Repeat("b", 20),
			wantPrefix:       strings.Repeat("a", 32) + "-" + strings.Repeat("b", 13),
		},
		{
			name:             "logical claim keeps at least one character",
			podName:          strings.Repeat("a", 253),
			logicalClaimName: "gpu",
			wantPrefix:       strings.Repeat("a", 44) + "-g",
		},
		{
			name:             "pod keeps at least one character",
			podName:          "p",
			logicalClaimName: strings.Repeat("b", 63),
			wantPrefix:       "p-" + strings.Repeat("b", 44),
		},
		{
			name:             "trailing dot exposed in pod name is removed",
			podName:          strings.Repeat("a", 31) + "." + strings.Repeat("a", 18),
			logicalClaimName: strings.Repeat("b", 20),
			wantPrefix:       strings.Repeat("a", 31) + "-" + strings.Repeat("b", 13),
		},
		{
			name:             "trailing hyphen exposed in claim name is removed",
			podName:          strings.Repeat("a", 20),
			logicalClaimName: strings.Repeat("b", 32) + "-" + strings.Repeat("b", 17),
			wantPrefix:       strings.Repeat("a", 12) + "-" + strings.Repeat("b", 32),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name:      test.podName,
				Namespace: "test-ns",
				UID:       types.UID("pod-uid"),
			}}

			name := simulationResourceClaimName(pod, test.logicalClaimName)
			replayedName := simulationResourceClaimName(pod, test.logicalClaimName)
			require.Greater(t, len(name), simulationResourceClaimHashLength+1)
			prefix := name[:len(name)-simulationResourceClaimHashLength-1]

			assert.Equal(t, test.wantPrefix, prefix)
			assert.Equal(t, len(test.wantPrefix)+simulationResourceClaimHashLength+1, len(name))
			assert.LessOrEqual(t, len(name), maxSimulationResourceClaimNameLength)
			assert.Empty(t, validation.IsDNS1123Subdomain(name))
			assert.Regexp(t, `-[0-9a-f]{16}$`, name)
			assert.Equal(t, name, replayedName, "same identity must recreate the same name")
		})
	}
}

func TestSimulationResourceClaimHashUsesCompleteFieldIdentity(t *testing.T) {
	basePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "pod",
		Namespace: "test-ns",
		UID:       types.UID("pod-uid"),
	}}
	baseHash := simulationResourceClaimHash(basePod, "gpu")

	tests := []struct {
		name             string
		pod              *corev1.Pod
		logicalClaimName string
	}{
		{
			name:             "namespace",
			pod:              &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "other-ns", UID: types.UID("pod-uid")}},
			logicalClaimName: "gpu",
		},
		{
			name:             "pod name",
			pod:              &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other-pod", Namespace: "test-ns", UID: types.UID("pod-uid")}},
			logicalClaimName: "gpu",
		},
		{
			name:             "pod UID",
			pod:              &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "test-ns", UID: types.UID("other-uid")}},
			logicalClaimName: "gpu",
		},
		{
			name:             "logical claim name",
			pod:              basePod,
			logicalClaimName: "network",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.NotEqual(t, baseHash, simulationResourceClaimHash(test.pod, test.logicalClaimName))
		})
	}
	assert.Equal(t, baseHash, simulationResourceClaimHash(basePod, "gpu"))
}

func TestSimulationResourceClaimHashSeparatesIdentityFields(t *testing.T) {
	firstPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ab", Name: "c", UID: types.UID("d")}}
	secondPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "bc", UID: types.UID("d")}}

	// These identities both become "abcde" if their fields are concatenated
	// without boundaries.
	assert.NotEqual(t, simulationResourceClaimHash(firstPod, "e"), simulationResourceClaimHash(secondPod, "e"))
}

func newResourceClaimTemplateLister(t *testing.T, templates ...*resourcev1.ResourceClaimTemplate) resourcelisters.ResourceClaimTemplateLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, template := range templates {
		require.NoError(t, indexer.Add(template))
	}
	return resourcelisters.NewResourceClaimTemplateLister(indexer)
}
