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
	"crypto/sha256"
	"fmt"
	"maps"
	"strings"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	drautils "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/utils"
)

const (
	// Match the 63-character limit used by the Kubernetes ResourceClaim controller
	// for template-generated claims (pkg/controller/resourceclaim/controller.go).
	// The hash replaces its random API-server suffix so later booking passes
	// reconstruct the same in-memory claim identity.
	maxSimulationResourceClaimNameLength = validation.DNS1123LabelMaxLength
	simulationResourceClaimHashLength    = 16
)

// SimulationWorkload contains virtual Pods and the ResourceClaims which must
// be added to the same ClusterSnapshot transaction before scheduling them.
type SimulationWorkload struct {
	Pods   []*corev1.Pod
	Claims []*resourcev1.ResourceClaim
}

// SimulationWorkloadBuilder expands ProvisioningRequests and materializes
// ResourceClaimTemplate-backed claims without creating Kubernetes API objects.
type SimulationWorkloadBuilder struct {
	resourceClaimTemplateLister resourcelisters.ResourceClaimTemplateLister
}

// NewSimulationWorkloadBuilder creates a builder backed by the shared
// ResourceClaimTemplate informer cache.
func NewSimulationWorkloadBuilder(resourceClaimTemplateLister resourcelisters.ResourceClaimTemplateLister) *SimulationWorkloadBuilder {
	return &SimulationWorkloadBuilder{resourceClaimTemplateLister: resourceClaimTemplateLister}
}

// ForProvisioningRequest expands a ProvisioningRequest and returns the
// complete in-memory workload needed to schedule it.
func (b *SimulationWorkloadBuilder) ForProvisioningRequest(pr *provreqwrapper.ProvisioningRequest) (*SimulationWorkload, error) {
	pods, err := PodsForProvisioningRequest(pr)
	if err != nil {
		return nil, err
	}
	workload, err := b.forPods(pods)
	if err != nil {
		return nil, fmt.Errorf("ProvisioningRequest %s/%s: %w", pr.Namespace, pr.Name, err)
	}
	return workload, nil
}

// forPods materializes ResourceClaimTemplate-backed claims. Callers must
// provide newly created Pods which are safe to update in place.
func (b *SimulationWorkloadBuilder) forPods(pods []*corev1.Pod) (*SimulationWorkload, error) {
	workload := &SimulationWorkload{
		Pods:   make([]*corev1.Pod, 0, len(pods)),
		Claims: make([]*resourcev1.ResourceClaim, 0),
	}

	for _, pod := range pods {
		for claimIndex := range pod.Spec.ResourceClaims {
			podClaim := &pod.Spec.ResourceClaims[claimIndex]
			templateName := podClaim.ResourceClaimTemplateName
			if templateName == nil {
				continue
			}

			if b.resourceClaimTemplateLister == nil {
				return nil, fmt.Errorf("virtual pod %s/%s resource claim %q references ResourceClaimTemplate %s/%s, but no ResourceClaimTemplate lister is configured", pod.Namespace, pod.Name, podClaim.Name, pod.Namespace, *templateName)
			}

			template, err := b.resourceClaimTemplateLister.ResourceClaimTemplates(pod.Namespace).Get(*templateName)
			if err != nil {
				return nil, fmt.Errorf("virtual pod %s/%s resource claim %q could not get ResourceClaimTemplate %s/%s: %w", pod.Namespace, pod.Name, podClaim.Name, pod.Namespace, *templateName, err)
			}

			claimName := simulationResourceClaimName(pod, podClaim.Name)
			pod.Status.ResourceClaimStatuses = append(pod.Status.ResourceClaimStatuses, corev1.PodResourceClaimStatus{
				Name:              podClaim.Name,
				ResourceClaimName: ptr.To(claimName),
			})

			annotations := maps.Clone(template.Spec.Annotations)
			if annotations == nil {
				annotations = make(map[string]string, 1)
			}
			// Preserve metadata added by the Kubernetes ResourceClaim controller.
			annotations[resourcev1.PodResourceClaimAnnotation] = podClaim.Name
			workload.Claims = append(workload.Claims, &resourcev1.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:            claimName,
					Namespace:       pod.Namespace,
					UID:             types.UID(pod.Namespace + "/" + claimName),
					Labels:          maps.Clone(template.Spec.Labels),
					Annotations:     annotations,
					OwnerReferences: []metav1.OwnerReference{drautils.PodClaimOwnerReference(pod)},
				},
				Spec: *template.Spec.Spec.DeepCopy(),
			})
		}
		workload.Pods = append(workload.Pods, pod)
	}

	return workload, nil
}

func simulationResourceClaimName(pod *corev1.Pod, logicalClaimName string) string {
	identity := fmt.Sprintf("%s\x00%s\x00%s\x00%s", pod.Namespace, pod.Name, pod.UID, logicalClaimName)
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))[:simulationResourceClaimHashLength]
	suffix := "-" + hash
	prefix := pod.Name + "-" + logicalClaimName
	maxPrefixLength := maxSimulationResourceClaimNameLength - len(suffix)
	if len(prefix) > maxPrefixLength {
		nameLength := maxPrefixLength - 1
		podNameLength := len(pod.Name) * nameLength / (len(pod.Name) + len(logicalClaimName))
		podNameLength = max(1, min(podNameLength, nameLength-1))
		logicalClaimNameLength := nameLength - podNameLength
		podNamePrefix := strings.TrimRight(pod.Name[:podNameLength], ".-")
		logicalClaimNamePrefix := strings.TrimRight(logicalClaimName[:logicalClaimNameLength], ".-")
		prefix = podNamePrefix + "-" + logicalClaimNamePrefix
	}
	prefix = strings.TrimRight(prefix, ".-")
	if prefix == "" {
		prefix = "simulated-claim"
	}
	return prefix + suffix
}
