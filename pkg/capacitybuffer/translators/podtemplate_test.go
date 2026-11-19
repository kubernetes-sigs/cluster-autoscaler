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

package translator

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	apiv1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
)

func TestGetPodTemplateFromSpec(t *testing.T) {
	testCases := []struct {
		name   string
		pts    *corev1.PodTemplateSpec
		buffer *apiv1.CapacityBuffer
		want   *corev1.PodTemplate
	}{
		{
			name: "basic pod template spec",
			pts: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"app": "foo"},
					Annotations: map[string]string{"anno": "bar"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c1"}},
				},
			},
			buffer: &apiv1.CapacityBuffer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-buffer",
					Namespace: "test-ns",
					UID:       "123-uid",
				},
			},
			want: &corev1.PodTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "capacitybuffer-test-buffer-pod-template",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: capacitybuffer.CapacityBufferApiVersion,
							Kind:       capacitybuffer.CapacityBufferKind,
							Name:       "test-buffer",
							UID:        "123-uid",
							Controller: ptr.To(true),
						},
					},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels:      map[string]string{"app": "foo"},
						Annotations: map[string]string{"anno": "bar"},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "c1"}},
					},
				},
			},
		},
		{
			name: "long buffer name",
			pts: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "foo"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c1"}},
				},
			},
			buffer: &apiv1.CapacityBuffer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "this-is-a-very-long-buffer-name-that-will-exceed-the-maximum-allowed-length-for-a-pod-template-name-which-is-two-hundred-and-fifty-three-characters-in-total-so-it-will-be-truncated-to-fit-the-limit-and-we-need-to-make-sure-it-does-not-exceed-the-limit-whatsoever",
					Namespace: "test-ns",
					UID:       "123-uid",
				},
			},
			want: &corev1.PodTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "capacitybuffer-this-is-a-very-long-buffer-name-that-will-exceed-the-maximum-allowed-length-for-a-pod-template-name-which-is-two-hundred-and-fifty-three-characters-in-total-so--3391c9c8db933e57aa9247c276cd7581d3d99514da0b43a46e7d42d5425dcca4-pod-template",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: capacitybuffer.CapacityBufferApiVersion,
							Kind:       capacitybuffer.CapacityBufferKind,
							Name:       "this-is-a-very-long-buffer-name-that-will-exceed-the-maximum-allowed-length-for-a-pod-template-name-which-is-two-hundred-and-fifty-three-characters-in-total-so-it-will-be-truncated-to-fit-the-limit-and-we-need-to-make-sure-it-does-not-exceed-the-limit-whatsoever",
							UID:        "123-uid",
							Controller: ptr.To(true),
						},
					},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "foo"},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "c1"}},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := getPodTemplateFromSpec(tc.pts, tc.buffer)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestGetPodTemplateNameForBuffer(t *testing.T) {
	testCases := []struct {
		name       string
		bufferName string
		want       string
	}{
		{
			name:       "max_unhashed_length_223",
			bufferName: strings.Repeat("a", 223),
			want:       "capacitybuffer-" + strings.Repeat("a", 223) + "-pod-template",
		},
		{
			name:       "first_hashed_length_224",
			bufferName: strings.Repeat("a", 224),
			want:       "capacitybuffer-" + strings.Repeat("a", 160) + "-de67a84b5ac131bae4461570479cab3560ab15665f5fddc64f9355d5b81c7078-pod-template",
		},
		{
			name:       "max_k8s_name_253_suffix_1",
			bufferName: strings.Repeat("a", 252) + "1",
			want:       "capacitybuffer-" + strings.Repeat("a", 160) + "-93c902e98b4f891bcfad46705803f65ec2cf3e7fc705b321d7e8e079d7cce523-pod-template",
		},
		{
			name:       "max_k8s_name_253_suffix_2",
			bufferName: strings.Repeat("a", 252) + "2",
			want:       "capacitybuffer-" + strings.Repeat("a", 160) + "-cab75d001a7ff8a301696f1939de85def8427a7ddff6663e499425e59a593edd-pod-template",
		},
		{
			name:       "dot_trimmed_hashed_name_has_length_252",
			bufferName: strings.Repeat("a", 159) + "." + strings.Repeat("b", 65),
			want:       "capacitybuffer-" + strings.Repeat("a", 159) + "-f642dc9338800c7e7a9f9367f037b077cfef0f09c4e6d6acfc9128dbb3ade5c5-pod-template",
		},
		{
			name:       "hyphen_at_prefix_boundary_preserved",
			bufferName: strings.Repeat("a", 159) + "-" + strings.Repeat("b", 65),
			want:       "capacitybuffer-" + strings.Repeat("a", 159) + "--6086475d45bfd21e5248e50890da0b7b09dd7eace6750839e838d6f15722e98d-pod-template",
		},
		{
			name:       "crafted_224_char_name_matching_dot_trimmed_output_is_also_hashed",
			bufferName: strings.Repeat("a", 159) + "-f642dc9338800c7e7a9f9367f037b077cfef0f09c4e6d6acfc9128dbb3ade5c5",
			want:       "capacitybuffer-" + strings.Repeat("a", 159) + "--33dbf3d09c127d44a474121880f752bd166a7ec2fbb6aceb9fa11e4dcf73b7e9-pod-template",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := &apiv1.CapacityBuffer{ObjectMeta: metav1.ObjectMeta{Name: tc.bufferName}}
			got := getPodTemplateNameForBuffer(buf)
			assert.Equal(t, tc.want, got)
			assert.Empty(t, validation.IsDNS1123Subdomain(got))
		})
	}
}

func TestGetPodTemplateSpecFromPod(t *testing.T) {
	testCases := []struct {
		name string
		pod  *corev1.Pod
		want *corev1.PodTemplateSpec
	}{
		{
			name: "pod without service account volumes",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"app": "foo"},
					Annotations: map[string]string{"anno": "bar"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c1"}},
				},
			},
			want: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"app": "foo"},
					Annotations: map[string]string{"anno": "bar"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c1"}},
				},
			},
		},
		{
			name: "pod with service account volumes",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"app": "foo"},
					Annotations: map[string]string{"anno": "bar"},
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{Name: "vol-1"},
						{Name: "kube-api-access-abcde"},
					},
					Containers: []corev1.Container{
						{
							Name: "c1",
							VolumeMounts: []corev1.VolumeMount{
								{Name: "vol-1", MountPath: "/vol-1"},
								{Name: "kube-api-access-abcde", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount"},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name: "i1",
							VolumeMounts: []corev1.VolumeMount{
								{Name: "vol-1", MountPath: "/vol-1"},
								{Name: "kube-api-access-abcde", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount"},
							},
						},
					},
				},
			},
			want: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"app": "foo"},
					Annotations: map[string]string{"anno": "bar"},
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{Name: "vol-1"},
					},
					Containers: []corev1.Container{
						{
							Name: "c1",
							VolumeMounts: []corev1.VolumeMount{
								{Name: "vol-1", MountPath: "/vol-1"},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name: "i1",
							VolumeMounts: []corev1.VolumeMount{
								{Name: "vol-1", MountPath: "/vol-1"},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := getPodTemplateSpecFromPod(tc.pod)
			assert.Equal(t, tc.want, got)
		})
	}
}
