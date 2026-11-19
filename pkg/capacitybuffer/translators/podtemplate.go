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

package translator

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiv1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
)

const (
	serviceAccountVolumeNamePrefix = "kube-api-access-"
	podTemplateNamePrefix          = "capacitybuffer-"
	podTemplateNameSuffix          = "-pod-template"
	maxPodTemplateNameLength       = 253
	maxBufferNameLen               = maxPodTemplateNameLength - len(podTemplateNamePrefix) - len(podTemplateNameSuffix)
	unhashedMaxBufferNameLen       = maxBufferNameLen - 2
	bufferNameHashLen              = sha256.Size * 2
	// hashedBufferPrefixLen is the prefix length before trimming a trailing '.',
	// so hashed segments are 225 chars (or 224 when a '.' is trimmed) and
	// unhashedMaxBufferNameLen (223) sits below both to prevent cross-branch
	// collisions.
	hashedBufferPrefixLen = maxBufferNameLen - bufferNameHashLen - 1
)

func getPodTemplateNameForBuffer(buffer *apiv1.CapacityBuffer) string {
	bufferName := buffer.Name
	if len(bufferName) > unhashedMaxBufferNameLen {
		bufferName = hashBufferName(bufferName)
	}

	return podTemplateNamePrefix + bufferName + podTemplateNameSuffix
}

func hashBufferName(name string) string {
	sum := sha256.Sum256([]byte(name))
	prefix := strings.TrimSuffix(name[:hashedBufferPrefixLen], ".")
	return prefix + "-" + hex.EncodeToString(sum[:])
}

func getPodTemplateFromSpec(pts *corev1.PodTemplateSpec, buffer *apiv1.CapacityBuffer) *corev1.PodTemplate {
	return &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getPodTemplateNameForBuffer(buffer),
			Namespace: buffer.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: capacitybuffer.CapacityBufferApiVersion,
					Kind:       capacitybuffer.CapacityBufferKind,
					Name:       buffer.Name,
					UID:        buffer.UID,
					Controller: ptr.To(true),
				},
			},
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      pts.Labels,
				Annotations: pts.Annotations,
			},
			Spec: pts.Spec,
		},
	}
}

func getPodTemplateSpecFromPod(pod *corev1.Pod) *corev1.PodTemplateSpec {
	p := pod.DeepCopy()
	cleanUpServiceAccountVolume(p)
	return &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      p.Labels,
			Annotations: p.Annotations,
		},
		Spec: p.Spec,
	}
}

// cleanUpServiceAccountVolume removes kube-api-access volume from the pod spec.
//
// Pod returned from server dry run call has kube-api-access volume attached
// with a random suffix (kube-api-access-xxxxx). While this does not affect
// the behavior of the buffer, it triggers unnecessary updates of the managed
// pod template, as the volume name is different with every server dry run request.
// This function prevents these redundant updates.
func cleanUpServiceAccountVolume(pod *corev1.Pod) {
	var volumes []corev1.Volume
	for _, volume := range pod.Spec.Volumes {
		if !strings.HasPrefix(volume.Name, serviceAccountVolumeNamePrefix) {
			volumes = append(volumes, volume)
		}
	}
	pod.Spec.Volumes = volumes
	for i, c := range pod.Spec.Containers {
		var mounts []corev1.VolumeMount
		for _, mount := range c.VolumeMounts {
			if !strings.HasPrefix(mount.Name, serviceAccountVolumeNamePrefix) {
				mounts = append(mounts, mount)
			}
		}
		pod.Spec.Containers[i].VolumeMounts = mounts
	}
	for i, c := range pod.Spec.InitContainers {
		var mounts []corev1.VolumeMount
		for _, mount := range c.VolumeMounts {
			if !strings.HasPrefix(mount.Name, serviceAccountVolumeNamePrefix) {
				mounts = append(mounts, mount)
			}
		}
		pod.Spec.InitContainers[i].VolumeMounts = mounts
	}
}
