/*
Copyright 2025 The Kubernetes Authors.

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

package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	buffersfake "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/clientset/versioned/fake"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/testutil"
)

func TestClientGetPodTemplate(t *testing.T) {
	podTemplate1 := &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "podTemp1",
			Namespace:  "default",
			Generation: 3,
		},
	}
	podTemplate2 := &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "podTemp2",
			Namespace:  "default",
			Generation: 4,
		},
	}
	tests := []struct {
		name                      string
		objectsInKubernetesClient []runtime.Object
		objectName                string
		expectedValue             *corev1.PodTemplate
		expectError               bool
	}{
		{
			name:                      "simple existing pod template",
			objectsInKubernetesClient: []runtime.Object{podTemplate1, podTemplate2},
			objectName:                podTemplate1.Name,
			expectedValue:             podTemplate1,
			expectError:               false,
		},
		{
			name:                      "simple non existing pod template",
			objectsInKubernetesClient: []runtime.Object{podTemplate1, podTemplate2},
			objectName:                "RandomName",
			expectedValue:             nil,
			expectError:               true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeKubernetesClient := fakeclient.NewSimpleClientset(test.objectsInKubernetesClient...)
			fakeBuffersClient := buffersfake.NewSimpleClientset()
			fakeCapacityBuffersClient, _ := NewCapacityBufferClientFromClients(fakeBuffersClient, fakeKubernetesClient, nil, nil)
			pt, err := fakeCapacityBuffersClient.GetPodTemplate("default", test.objectName)
			assert.Equal(t, err != nil, test.expectError)
			assert.Equal(t, pt, test.expectedValue)
		})
	}
}

func TestPodTemplateRefIndex(t *testing.T) {
	tests := []struct {
		name     string
		buffer   runtime.Object
		wantKeys []string
		missKeys []string
	}{
		{
			name: "spec_pod_template_ref_only",
			buffer: testutil.NewBuffer(
				testutil.WithName("b1"),
				testutil.WithPodTemplateRef("source-pt"),
			),
			wantKeys: []string{"source-pt"},
			missKeys: []string{"capacitybuffer-b1-pod-template"},
		},
		{
			name: "spec_and_status_pod_template_ref",
			buffer: testutil.NewBuffer(
				testutil.WithName("b1"),
				testutil.WithPodTemplateRef("source-pt"),
				testutil.WithStatusPodTemplateRef("capacitybuffer-b1-pod-template"),
			),
			wantKeys: []string{"source-pt", "capacitybuffer-b1-pod-template"},
			missKeys: []string{"other-pt"},
		},
		{
			name: "scalable_ref_with_status_pod_template_ref",
			buffer: testutil.NewBuffer(
				testutil.WithName("b1"),
				testutil.WithScalableRef("apps", "Deployment", "dep-1"),
				testutil.WithStatusPodTemplateRef("capacitybuffer-b1-pod-template"),
			),
			wantKeys: []string{"capacitybuffer-b1-pod-template"},
			missKeys: []string{"dep-1"},
		},
		{
			name: "identical_spec_and_status_pod_template_ref",
			buffer: testutil.NewBuffer(
				testutil.WithName("b1"),
				testutil.WithPodTemplateRef("same-pt"),
				testutil.WithStatusPodTemplateRef("same-pt"),
			),
			wantKeys: []string{"same-pt"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeBuffersClient := buffersfake.NewSimpleClientset(tc.buffer)
			fakeKubernetesClient := fakeclient.NewSimpleClientset()
			cbClient, err := NewCapacityBufferClientFromClients(fakeBuffersClient, fakeKubernetesClient, nil, nil)
			if err != nil {
				t.Fatalf("NewCapacityBufferClientFromClients() unexpected error: %v", err)
			}

			indexer := cbClient.GetBufferInformer().GetIndexer()
			for _, key := range tc.wantKeys {
				got, err := indexer.ByIndex(PodTemplateRefIndex, key)
				if err != nil {
					t.Fatalf("ByIndex(%q) unexpected error: %v", key, err)
				}
				assert.Len(t, got, 1, "ByIndex(%q) item count", key)
			}
			for _, key := range tc.missKeys {
				got, err := indexer.ByIndex(PodTemplateRefIndex, key)
				if err != nil {
					t.Fatalf("ByIndex(%q) unexpected error: %v", key, err)
				}
				assert.Empty(t, got, "ByIndex(%q) item count", key)
			}
		})
	}
}

func TestEnsurePodTemplate_OwnerReferences(t *testing.T) {
	ownerRefs := []metav1.OwnerReference{
		{
			APIVersion: "autoscaling.x-k8s.io/v1beta1",
			Kind:       "CapacityBuffer",
			Name:       "b1",
			UID:        "uid-1",
			Controller: ptr.To(true),
		},
	}
	desired := testutil.NewPodTemplate(
		testutil.WithPodTemplateName("capacitybuffer-b1-pod-template"),
		func(pt *corev1.PodTemplate) {
			pt.OwnerReferences = ownerRefs
		},
	)
	withoutOwnerRefs := desired.DeepCopy()
	withoutOwnerRefs.OwnerReferences = nil

	t.Run("restores_missing_owner_references_when_template_unchanged", func(t *testing.T) {
		fakeK8s := fakeclient.NewSimpleClientset(withoutOwnerRefs)
		cbClient, err := NewCapacityBufferClient(nil, fakeK8s, nil, nil, nil, nil, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("NewCapacityBufferClient() unexpected error: %v", err)
		}

		got, err := cbClient.EnsurePodTemplate(t.Context(), desired.DeepCopy())
		if err != nil {
			t.Fatalf("EnsurePodTemplate() unexpected error: %v", err)
		}
		assert.Equal(t, ownerRefs, got.OwnerReferences)
	})

	t.Run("skips_update_when_template_and_owner_references_match", func(t *testing.T) {
		fakeK8s := fakeclient.NewSimpleClientset(desired.DeepCopy())
		cbClient, err := NewCapacityBufferClient(nil, fakeK8s, nil, nil, nil, nil, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("NewCapacityBufferClient() unexpected error: %v", err)
		}
		fakeK8s.ClearActions()

		_, err = cbClient.EnsurePodTemplate(t.Context(), desired.DeepCopy())
		if err != nil {
			t.Fatalf("EnsurePodTemplate() unexpected error: %v", err)
		}
		for _, action := range fakeK8s.Actions() {
			if action.GetVerb() == "update" || action.GetVerb() == "create" {
				t.Fatalf("EnsurePodTemplate() performed unexpected write action: %+v", action)
			}
		}
	})
}
