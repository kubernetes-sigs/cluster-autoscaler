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

package controllers

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	cbapi "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/testutil"
)

var _ = Describe("CapacityBuffer Controller", func() {
	var namespace string

	BeforeEach(func() {
		By("creating a test namespace")
		testNS := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "test-ns-",
			},
		}
		testNS, err := k8sClient.CoreV1().Namespaces().Create(ctx, testNS, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		namespace = testNS.Name
	})

	AfterEach(func() {
		By("cleaning up the test namespace")
		_ = k8sClient.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	})

	Context("ResourceQuotas Integration", func() {
		SetDefaultEventuallyTimeout(5 * time.Second)
		SetDefaultEventuallyPollingInterval(100 * time.Millisecond)

		BeforeEach(func() {
			By("creating a pod template")
			podTemp := testutil.NewPodTemplate(
				testutil.WithPodTemplateName("pod-temp"),
				testutil.WithNamespace[*corev1.PodTemplate](namespace),
				testutil.WithPodTemplateResources(corev1.ResourceList{"cpu": resource.MustParse("1")}, nil),
			)
			_, err := k8sClient.CoreV1().PodTemplates(namespace).Create(ctx, podTemp, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("creating a resource quota")
			rq := testutil.NewResourceQuota(
				testutil.WithResourceQuotaName("quota"),
				testutil.WithResourceQuotaHard(corev1.ResourceList{"cpu": resource.MustParse("5")}),
				testutil.WithNamespace[*corev1.ResourceQuota](namespace),
			)
			rq, err = k8sClient.CoreV1().ResourceQuotas(namespace).Create(ctx, rq, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("setting resource quota's status")
			rq.Status.Hard = rq.Spec.Hard
			rq.Status.Used = corev1.ResourceList{"cpu": resource.MustParse("0")}
			_, err = k8sClient.CoreV1().ResourceQuotas(namespace).UpdateStatus(ctx, rq, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			By("cleaning up test resources")
			_ = k8sClient.CoreV1().PodTemplates(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = k8sClient.CoreV1().ResourceQuotas(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
		})

		It("should respect k8s ResourceQuotas", func() {
			By("creating a buffer that does not exceed remaining quota")
			b1 := testutil.NewBuffer(
				testutil.WithName("b1"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithPodTemplateRef("pod-temp"),
				testutil.WithReplicas(2),
				testutil.WithActiveProvisioningStrategy(),
			)
			_, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, b1, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("checking that the buffer's replicas were not limited")
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(b.Status.Replicas).To(Equal(new(int32(2))))
				g.Expect(meta.IsStatusConditionFalse(b.Status.Conditions, cbapi.LimitedByQuotasCondition)).To(BeTrue())
			}).Should(Succeed())

			By("creating a buffer that exceeds remaining quota")
			b2 := testutil.NewBuffer(
				testutil.WithName("b2"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithPodTemplateRef("pod-temp"),
				testutil.WithReplicas(4),
				testutil.WithActiveProvisioningStrategy(),
			)
			_, err = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, b2, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("checking that the new buffer's replicas were limited")
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b2", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(b.Status.Replicas).To(Equal(new(int32(3))))
				g.Expect(meta.IsStatusConditionTrue(b.Status.Conditions, cbapi.LimitedByQuotasCondition)).To(BeTrue())
			}).Should(Succeed())

			By("increasing b1's replicas from 2 to 4")
			b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			b.Spec.Replicas = new(int32(4))
			_, err = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Update(ctx, b, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("checking that b1's replicas are still not limited")
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionFalse(b.Status.Conditions, cbapi.LimitedByQuotasCondition)).To(BeTrue())
				g.Expect(b.Status.Replicas).To(Equal(new(int32(4))))
			}).Should(Succeed())

			// buffers are ordered in the order of creation
			By("checking that b2's replicas dropped from 3 to 1")
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b2", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionTrue(b.Status.Conditions, cbapi.LimitedByQuotasCondition)).To(BeTrue())
				g.Expect(b.Status.Replicas).To(Equal(new(int32(1))))
			}).Should(Succeed())

			By("increasing quota from 5 to 10")
			rq, err := k8sClient.CoreV1().ResourceQuotas(namespace).Get(ctx, "quota", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			rq.Status.Hard["cpu"] = resource.MustParse("10")
			Expect(k8sClient.CoreV1().ResourceQuotas(namespace).UpdateStatus(ctx, rq, metav1.UpdateOptions{})).Error().NotTo(HaveOccurred())

			By("checking that b1's replicas are still not limited")
			Consistently(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(b.Status.Replicas).To(Equal(new(int32(4))))
			}, 1*time.Second, 100*time.Millisecond).Should(Succeed())

			By("checking that b2 used the newly available quota")
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b2", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(b.Status.Replicas).To(Equal(new(int32(4))))
				g.Expect(meta.IsStatusConditionFalse(b.Status.Conditions, cbapi.LimitedByQuotasCondition)).To(BeTrue())
			}).Should(Succeed())
		})
	})

	Context("ScalableRef buffers", func() {
		SetDefaultEventuallyTimeout(5 * time.Second)
		SetDefaultEventuallyPollingInterval(100 * time.Millisecond)

		AfterEach(func() {
			By("cleaning up test resources")
			_ = k8sClient.AppsV1().Deployments(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
		})

		It("should scale buffer replicas based on referenced deployment", func() {
			By("creating a deployment")
			dep := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-dep",
					Namespace: namespace,
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: new(int32(10)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "my-dep"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"app": "my-dep"},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "c", Image: "i"}},
						},
					},
				},
			}
			_, err := k8sClient.AppsV1().Deployments(namespace).Create(ctx, dep, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("creating a buffer referencing the deployment with 20%")
			b1 := testutil.NewBuffer(
				testutil.WithName("b1"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithScalableRef("apps", "Deployment", "my-dep"),
				testutil.WithPercentage(20),
				testutil.WithActiveProvisioningStrategy(),
			)
			_, err = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, b1, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("checking that the buffer replicas is 20% of 10 (2)")
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(b.Status.Replicas).To(Equal(new(int32(2))))
			}).Should(Succeed())

			By("updating deployment replicas to 20")
			Eventually(func(g Gomega) {
				d, err := k8sClient.AppsV1().Deployments(namespace).Get(ctx, "my-dep", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				d.Spec.Replicas = new(int32(20))
				_, err = k8sClient.AppsV1().Deployments(namespace).Update(ctx, d, metav1.UpdateOptions{})
				g.Expect(err).NotTo(HaveOccurred())
			}).Should(Succeed())

			By("checking that the buffer replicas is 20% of 20 (4)")
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(b.Status.Replicas).To(Equal(new(int32(4))))
			}).Should(Succeed())
		})
	})

	Context("Cache Updates", func() {
		SetDefaultEventuallyTimeout(5 * time.Second)
		SetDefaultEventuallyPollingInterval(100 * time.Millisecond)

		AfterEach(func() {
			By("cleaning up test resources")
			_ = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
		})

		It("should update reconciliation cache on reconcile", func() {
			By("creating a supported capacity buffer")
			bSupported := testutil.NewBuffer(
				testutil.WithName("supported"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithActiveProvisioningStrategy(),
			)
			bSupported, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, bSupported, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("creating an unsupported capacity buffer")
			bUnsupported := testutil.NewBuffer(
				testutil.WithName("unsupported"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				func(buffer *v1beta1.CapacityBuffer) {
					buffer.Spec.ProvisioningStrategy = new("unsupported-strategy")
				},
			)
			bUnsupported, err = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, bUnsupported, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("checking that both buffers are in the reconciliation cache")
			toleranceRange := 5 * time.Second
			Eventually(func(g Gomega) {
				snapshot := reconciliationCache.Snapshot()
				g.Expect(snapshot[bSupported.UID]).To(BeTemporally("~", clock.Now(), toleranceRange))
				g.Expect(snapshot[bUnsupported.UID]).To(BeTemporally("~", clock.Now(), toleranceRange))
			}).Should(Succeed())
		})
	})

	Context("Managed PodTemplate Reconciliation", func() {
		SetDefaultEventuallyTimeout(5 * time.Second)
		SetDefaultEventuallyPollingInterval(100 * time.Millisecond)

		AfterEach(func() {
			By("cleaning up test resources")
			_ = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = k8sClient.AppsV1().Deployments(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = k8sClient.CoreV1().PodTemplates(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
		})

		It("should immediately revert modifications and deletions of managed PodTemplate for podTemplateRef buffer", func() {
			By("creating a source PodTemplate and CapacityBuffer")
			podTemp := testutil.NewPodTemplate(
				testutil.WithPodTemplateName("source-pt"),
				testutil.WithNamespace[*corev1.PodTemplate](namespace),
				testutil.WithPodTemplateResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}, nil),
			)
			_, err := k8sClient.CoreV1().PodTemplates(namespace).Create(ctx, podTemp, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			buf := testutil.NewBuffer(
				testutil.WithName("b1"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithPodTemplateRef("source-pt"),
				testutil.WithReplicas(2),
				testutil.WithActiveProvisioningStrategy(),
			)
			createdBuf, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, buf, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			var managedPTName string
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionTrue(b.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
				g.Expect(b.Status.PodTemplateRef).NotTo(BeNil())
				g.Expect(b.Status.PodTemplateRef.Name).To(Equal(fmt.Sprintf("capacitybuffer-%s-managed-pod-template", createdBuf.UID)))
				g.Expect(b.Status.PodTemplateGeneration).To(Equal(new(int64(1))))
				managedPTName = b.Status.PodTemplateRef.Name
			}).Should(Succeed())

			By("stripping ownerReferences alone on managed PodTemplate and verifying immediate restoration")
			Eventually(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, managedPTName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				pt.OwnerReferences = nil
				_, err = k8sClient.CoreV1().PodTemplates(namespace).Update(ctx, pt, metav1.UpdateOptions{})
				g.Expect(err).NotTo(HaveOccurred())
			}).Should(Succeed())

			Eventually(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, managedPTName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				ownerRef := metav1.GetControllerOf(pt)
				g.Expect(ownerRef).NotTo(BeNil())
				g.Expect(ownerRef.UID).To(Equal(createdBuf.UID))
				g.Expect(pt.Generation).To(Equal(int64(1)))
			}).Should(Succeed())

			By("modifying managed PodTemplate spec outside the controller")
			var modifiedGen int64
			Eventually(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, managedPTName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				pt.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("999m")
				updated, err := k8sClient.CoreV1().PodTemplates(namespace).Update(ctx, pt, metav1.UpdateOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				modifiedGen = updated.Generation
			}).Should(Succeed())

			By("verifying managed PodTemplate and buffer status reconcile immediately without waiting for resync")
			var revertedGen int64
			var revertedRV string
			var originalUID types.UID
			Eventually(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, managedPTName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pt.Generation).To(BeNumerically(">", modifiedGen))
				g.Expect(pt.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]).To(Equal(resource.MustParse("1")))
				ownerRef := metav1.GetControllerOf(pt)
				g.Expect(ownerRef).NotTo(BeNil())
				g.Expect(ownerRef.UID).To(Equal(createdBuf.UID))
				revertedGen = pt.Generation
				revertedRV = pt.ResourceVersion
				originalUID = pt.UID

				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(b.Status.PodTemplateGeneration).To(Equal(&revertedGen))
			}).Should(Succeed())

			By("verifying controller does not enter a reconciliation write loop after reverting")
			Consistently(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, managedPTName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pt.Generation).To(Equal(revertedGen))
				g.Expect(pt.ResourceVersion).To(Equal(revertedRV))
			}, 500*time.Millisecond, 100*time.Millisecond).Should(Succeed())

			By("deleting managed PodTemplate outside the controller and verifying immediate recreation")
			Expect(k8sClient.CoreV1().PodTemplates(namespace).Delete(ctx, managedPTName, metav1.DeleteOptions{})).To(Succeed())

			Eventually(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, managedPTName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pt.UID).NotTo(Equal(originalUID))
				g.Expect(pt.Generation).To(Equal(int64(1)))
				g.Expect(pt.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]).To(Equal(resource.MustParse("1")))

				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(b.Status.PodTemplateGeneration).To(Equal(new(int64(1))))
			}).Should(Succeed())
		})

		It("should immediately revert modifications of managed PodTemplate for scalableRef buffer", func() {
			By("creating a Deployment and scalableRef CapacityBuffer")
			dep := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-dep",
					Namespace: namespace,
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: new(int32(2)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "my-dep"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"app": "my-dep"},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "c", Image: "original-image"}},
						},
					},
				},
			}
			_, err := k8sClient.AppsV1().Deployments(namespace).Create(ctx, dep, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			buf := testutil.NewBuffer(
				testutil.WithName("b-scalable"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithScalableRef("apps", "Deployment", "my-dep"),
				testutil.WithPercentage(100),
				testutil.WithActiveProvisioningStrategy(),
			)
			_, err = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, buf, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			var managedPTName string
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b-scalable", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionTrue(b.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
				g.Expect(b.Status.PodTemplateRef).NotTo(BeNil())
				g.Expect(b.Status.PodTemplateRef.Name).NotTo(BeEmpty())
				managedPTName = b.Status.PodTemplateRef.Name
			}).Should(Succeed())

			By("modifying managed PodTemplate image outside the controller")
			var modifiedGen int64
			Eventually(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, managedPTName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				pt.Template.Spec.Containers[0].Image = "tampered-image"
				updated, err := k8sClient.CoreV1().PodTemplates(namespace).Update(ctx, pt, metav1.UpdateOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				modifiedGen = updated.Generation
			}).Should(Succeed())

			By("verifying managed PodTemplate reverts image immediately")
			Eventually(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, managedPTName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pt.Generation).To(BeNumerically(">", modifiedGen))
				g.Expect(pt.Template.Spec.Containers[0].Image).To(Equal("original-image"))

				revertedGen := pt.Generation
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b-scalable", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(b.Status.PodTemplateGeneration).To(Equal(&revertedGen))
			}).Should(Succeed())
		})

		It("should isolate managed PodTemplates across 253-char names, pre-existing name-based templates, and same-name recreation", func() {
			sourcePT := testutil.NewPodTemplate(
				testutil.WithPodTemplateName("source-pt"),
				testutil.WithNamespace[*corev1.PodTemplate](namespace),
				testutil.WithPodTemplateResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}, nil),
			)
			_, err := k8sClient.CoreV1().PodTemplates(namespace).Create(ctx, sourcePT, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("pre-creating an unowned PodTemplate with the legacy name-based pattern")
			legacyUserPT := testutil.NewPodTemplate(
				testutil.WithPodTemplateName("capacitybuffer-recreate-buf-pod-template"),
				testutil.WithNamespace[*corev1.PodTemplate](namespace),
				func(pt *corev1.PodTemplate) {
					pt.Template.Spec.Containers[0].Image = "user-kept-image"
				},
			)
			_, err = k8sClient.CoreV1().PodTemplates(namespace).Create(ctx, legacyUserPT, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("creating a 253-char CapacityBuffer and a recreate-buf CapacityBuffer")
			longName := strings.Repeat("x", 253)
			longBuf, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, testutil.NewBuffer(
				testutil.WithName(longName),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithPodTemplateRef("source-pt"),
				testutil.WithReplicas(1),
				testutil.WithActiveProvisioningStrategy(),
			), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			firstBuf, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, testutil.NewBuffer(
				testutil.WithName("recreate-buf"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithPodTemplateRef("source-pt"),
				testutil.WithReplicas(1),
				testutil.WithActiveProvisioningStrategy(),
			), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				bLong, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, longName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionTrue(bLong.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
				g.Expect(bLong.Status.PodTemplateRef).To(Equal(&v1beta1.LocalObjectRef{Name: fmt.Sprintf("capacitybuffer-%s-managed-pod-template", longBuf.UID)}))

				bFirst, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "recreate-buf", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionTrue(bFirst.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
				g.Expect(bFirst.Status.PodTemplateRef).To(Equal(&v1beta1.LocalObjectRef{Name: fmt.Sprintf("capacitybuffer-%s-managed-pod-template", firstBuf.UID)}))
			}).Should(Succeed())

			By("verifying the pre-existing legacy-named PodTemplate was not overwritten")
			untouchedPT, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, "capacitybuffer-recreate-buf-pod-template", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(untouchedPT.OwnerReferences).To(BeEmpty())
			Expect(untouchedPT.Template.Spec.Containers[0].Image).To(Equal("user-kept-image"))

			By("deleting recreate-buf and recreating it with the same name while the first managed PodTemplate remains in the namespace")
			Expect(buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Delete(ctx, "recreate-buf", metav1.DeleteOptions{})).To(Succeed())

			secondBuf, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, testutil.NewBuffer(
				testutil.WithName("recreate-buf"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithPodTemplateRef("source-pt"),
				testutil.WithReplicas(2),
				testutil.WithActiveProvisioningStrategy(),
			), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(secondBuf.UID).NotTo(Equal(firstBuf.UID))

			Eventually(func(g Gomega) {
				bSecond, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "recreate-buf", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionTrue(bSecond.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
				g.Expect(bSecond.Status.PodTemplateRef).To(Equal(&v1beta1.LocalObjectRef{Name: fmt.Sprintf("capacitybuffer-%s-managed-pod-template", secondBuf.UID)}))
			}).Should(Succeed())
		})

		It("should reject managed spec.podTemplateRef, stay ready on finalizer-held delete until recreated, and skip orphan re-adoption", func() {
			sourcePT := testutil.NewPodTemplate(
				testutil.WithPodTemplateName("source-pt"),
				testutil.WithNamespace[*corev1.PodTemplate](namespace),
				testutil.WithPodTemplateResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}, nil),
			)
			_, err := k8sClient.CoreV1().PodTemplates(namespace).Create(ctx, sourcePT, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			b1, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, testutil.NewBuffer(
				testutil.WithName("b1"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithPodTemplateRef("source-pt"),
				testutil.WithReplicas(1),
				testutil.WithActiveProvisioningStrategy(),
			), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			b1ManagedName := fmt.Sprintf("capacitybuffer-%s-managed-pod-template", b1.UID)

			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionTrue(b.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
				g.Expect(b.Status.PodTemplateRef).To(Equal(&v1beta1.LocalObjectRef{Name: b1ManagedName}))
			}).Should(Succeed())

			By("creating b2 pointing spec.podTemplateRef at b1's managed PodTemplate")
			b2, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, testutil.NewBuffer(
				testutil.WithName("b2"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithPodTemplateRef(b1ManagedName),
				testutil.WithReplicas(1),
				testutil.WithActiveProvisioningStrategy(),
			), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			b2ManagedName := fmt.Sprintf("capacitybuffer-%s-managed-pod-template", b2.UID)

			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b2", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionFalse(b.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
				g.Expect(b.Status.PodTemplateRef).To(BeNil())
			}).Should(Succeed())
			_, err = k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, b2ManagedName, metav1.GetOptions{})
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())

			By("updating b1 to self-reference its own managed PodTemplate")
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				b.Spec.PodTemplateRef = &v1beta1.LocalObjectRef{Name: b1ManagedName}
				_, err = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Update(ctx, b, metav1.UpdateOptions{})
				g.Expect(err).NotTo(HaveOccurred())
			}).Should(Succeed())

			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionFalse(b.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
				g.Expect(b.Status.PodTemplateRef).To(BeNil())
			}).Should(Succeed())

			By("restoring b1 to source-pt, holding b1's managed PodTemplate in Terminating with a finalizer, and verifying b1 stays Ready")
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				b.Spec.PodTemplateRef = &v1beta1.LocalObjectRef{Name: "source-pt"}
				_, err = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Update(ctx, b, metav1.UpdateOptions{})
				g.Expect(err).NotTo(HaveOccurred())
			}).Should(Succeed())

			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionTrue(b.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
				g.Expect(b.Status.PodTemplateRef).To(Equal(&v1beta1.LocalObjectRef{Name: b1ManagedName}))
			}).Should(Succeed())

			var terminatingUID types.UID
			Eventually(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, b1ManagedName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				terminatingUID = pt.UID
				pt.Finalizers = []string{"testing.x-k8s.io/hold"}
				_, err = k8sClient.CoreV1().PodTemplates(namespace).Update(ctx, pt, metav1.UpdateOptions{})
				g.Expect(err).NotTo(HaveOccurred())
			}).Should(Succeed())
			Expect(k8sClient.CoreV1().PodTemplates(namespace).Delete(ctx, b1ManagedName, metav1.DeleteOptions{})).To(Succeed())

			Consistently(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionTrue(b.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
				g.Expect(b.Status.PodTemplateRef).To(Equal(&v1beta1.LocalObjectRef{Name: b1ManagedName}))
			}, 300*time.Millisecond, 100*time.Millisecond).Should(Succeed())

			By("clearing the finalizer so DeleteFunc via PodTemplateRefIndex immediately recreates the managed PodTemplate")
			Eventually(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, b1ManagedName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				pt.Finalizers = nil
				_, err = k8sClient.CoreV1().PodTemplates(namespace).Update(ctx, pt, metav1.UpdateOptions{})
				g.Expect(err).NotTo(HaveOccurred())
			}).Should(Succeed())

			Eventually(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, b1ManagedName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pt.UID).NotTo(Equal(terminatingUID))
				g.Expect(pt.DeletionTimestamp).To(BeNil())
			}).Should(Succeed())

			By("deleting b1 with DeletePropagationOrphan and verifying controller neither re-adopts nor allows b2 to reference the orphaned template")
			Expect(buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Delete(ctx, "b1", metav1.DeleteOptions{
				PropagationPolicy: ptr.To(metav1.DeletePropagationOrphan),
			})).To(Succeed())
			Eventually(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, b1ManagedName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				pt.OwnerReferences = nil
				_, err = k8sClient.CoreV1().PodTemplates(namespace).Update(ctx, pt, metav1.UpdateOptions{})
				g.Expect(err).NotTo(HaveOccurred())
			}).Should(Succeed())

			Consistently(func(g Gomega) {
				pt, err := k8sClient.CoreV1().PodTemplates(namespace).Get(ctx, b1ManagedName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pt.OwnerReferences).To(BeEmpty())

				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b2", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(meta.IsStatusConditionFalse(b.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
				g.Expect(b.Status.PodTemplateRef).To(BeNil())
			}, 300*time.Millisecond, 100*time.Millisecond).Should(Succeed())

			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				b.Finalizers = nil
				_, err = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Update(ctx, b, metav1.UpdateOptions{})
				g.Expect(err).NotTo(HaveOccurred())
			}).Should(Succeed())
		})
	})

	Context("Managed PodTemplate ValidatingAdmissionPolicy", func() {
		const (
			caSAUser    = "system:serviceaccount:kube-system:cluster-autoscaler"
			gcSAUser    = "system:serviceaccount:kube-system:generic-garbage-collector"
			regularUser = "jane@example.com"
			vapName     = "cluster-autoscaler-capacitybuffer-managed-podtemplate-protection"
			vapBindName = "cluster-autoscaler-capacitybuffer-managed-podtemplate-protection-binding"
			podTempRole = "podtemplate-full-access"
		)

		var (
			caClient   *kubernetes.Clientset
			gcClient   *kubernetes.Clientset
			userClient *kubernetes.Clientset
		)

		impersonatedClientset := func(username string) *kubernetes.Clientset {
			c := rest.CopyConfig(cfg)
			c.Impersonate = rest.ImpersonationConfig{
				UserName: username,
				Groups:   []string{"system:authenticated"},
			}
			cs, err := kubernetes.NewForConfig(c)
			Expect(err).NotTo(HaveOccurred())
			return cs
		}

		BeforeEach(func() {
			SetDefaultEventuallyTimeout(10 * time.Second)
			SetDefaultEventuallyPollingInterval(100 * time.Millisecond)

			caClient = impersonatedClientset(caSAUser)
			gcClient = impersonatedClientset(gcSAUser)
			userClient = impersonatedClientset(regularUser)

			By("granting full RBAC on podtemplates in the test namespace so denials come solely from ValidatingAdmissionPolicy")
			_, err := k8sClient.RbacV1().Roles(namespace).Create(ctx, &rbacv1.Role{
				ObjectMeta: metav1.ObjectMeta{Name: podTempRole, Namespace: namespace},
				Rules: []rbacv1.PolicyRule{
					{
						APIGroups: []string{""},
						Resources: []string{"podtemplates"},
						Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"},
					},
				},
			}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = k8sClient.RbacV1().RoleBindings(namespace).Create(ctx, &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: podTempRole, Namespace: namespace},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: podTempRole},
				Subjects: []rbacv1.Subject{
					{Kind: rbacv1.UserKind, Name: caSAUser},
					{Kind: rbacv1.UserKind, Name: gcSAUser},
					{Kind: rbacv1.UserKind, Name: regularUser},
				},
			}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("rendering and installing kwok/charts/templates/validatingadmissionpolicy.yaml")
			vapChartPath := filepath.Join("..", "..", "..", "..", "kwok", "charts", "templates", "validatingadmissionpolicy.yaml")
			rawChart, err := os.ReadFile(vapChartPath)
			Expect(err).NotTo(HaveOccurred())

			tpl, err := template.New("vap").Funcs(template.FuncMap{
				"toYaml": func(v any) string { return "app: cluster-autoscaler" },
				"nindent": func(spaces int, s string) string {
					return "\n" + strings.Repeat(" ", spaces) + s
				},
			}).Parse(string(rawChart))
			Expect(err).NotTo(HaveOccurred())

			var rendered bytes.Buffer
			Expect(tpl.Execute(&rendered, map[string]any{
				"Release": map[string]any{
					"Name":      "cluster-autoscaler",
					"Namespace": "kube-system",
				},
				"Values": map[string]any{
					"commonLabels": map[string]any{"app": "cluster-autoscaler"},
				},
			})).To(Succeed())

			decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(rendered.Bytes()), 4096)
			var vap admissionregistrationv1.ValidatingAdmissionPolicy
			Expect(decoder.Decode(&vap)).To(Succeed())
			var binding admissionregistrationv1.ValidatingAdmissionPolicyBinding
			Expect(decoder.Decode(&binding)).To(Succeed())

			_, err = k8sClient.AdmissionregistrationV1().ValidatingAdmissionPolicies().Create(ctx, &vap, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			_, err = k8sClient.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Create(ctx, &binding, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("waiting until the ValidatingAdmissionPolicy is active in kube-apiserver")
			Eventually(func(g Gomega) {
				probe := testutil.NewPodTemplate(
					testutil.WithPodTemplateName("capacitybuffer-vap-probe-managed-pod-template"),
					testutil.WithNamespace[*corev1.PodTemplate](namespace),
				)
				_, err := userClient.CoreV1().PodTemplates(namespace).Create(ctx, probe, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
				g.Expect(k8serrors.IsForbidden(err)).To(BeTrue())
				g.Expect(err.Error()).To(ContainSubstring("ValidatingAdmissionPolicy"))
			}).Should(Succeed())
		})

		AfterEach(func() {
			By("cleaning up ValidatingAdmissionPolicy and test resources")
			_ = k8sClient.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Delete(ctx, vapBindName, metav1.DeleteOptions{})
			_ = k8sClient.AdmissionregistrationV1().ValidatingAdmissionPolicies().Delete(ctx, vapName, metav1.DeleteOptions{})
			Eventually(func() bool {
				_, err1 := k8sClient.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Get(ctx, vapBindName, metav1.GetOptions{})
				_, err2 := k8sClient.AdmissionregistrationV1().ValidatingAdmissionPolicies().Get(ctx, vapName, metav1.GetOptions{})
				return k8serrors.IsNotFound(err1) && k8serrors.IsNotFound(err2)
			}).Should(BeTrue())
			_ = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = k8sClient.CoreV1().PodTemplates(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
		})

		It("enforces CREATE, UPDATE, DELETE, and orphan cleanup rules across user and system identities", func() {
			By("allowing a regular user to create, update, and delete normal and legacy-named PodTemplates, even with a CapacityBuffer ownerReference")
			userOwnedPT := testutil.NewPodTemplate(
				testutil.WithPodTemplateName("capacitybuffer-user-legacy-pod-template"),
				testutil.WithNamespace[*corev1.PodTemplate](namespace),
				func(pt *corev1.PodTemplate) {
					pt.OwnerReferences = []metav1.OwnerReference{
						{
							APIVersion: cbapi.CapacityBufferApiVersion,
							Kind:       cbapi.CapacityBufferKind,
							Name:       "some-buffer",
							UID:        "11111111-1111-1111-1111-111111111111",
							Controller: ptr.To(true),
						},
					}
				},
			)
			createdUserPT, err := userClient.CoreV1().PodTemplates(namespace).Create(ctx, userOwnedPT, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			createdUserPT.Template.Spec.Containers[0].Image = "updated-user-image"
			_, err = userClient.CoreV1().PodTemplates(namespace).Update(ctx, createdUserPT, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(userClient.CoreV1().PodTemplates(namespace).Delete(ctx, createdUserPT.Name, metav1.DeleteOptions{})).To(Succeed())

			By("blocking a regular user from creating a PodTemplate matching ^capacitybuffer-.*-managed-pod-template$")
			spoofPT := testutil.NewPodTemplate(
				testutil.WithPodTemplateName("capacitybuffer-spoof-managed-pod-template"),
				testutil.WithNamespace[*corev1.PodTemplate](namespace),
			)
			_, err = userClient.CoreV1().PodTemplates(namespace).Create(ctx, spoofPT, metav1.CreateOptions{})
			Expect(k8serrors.IsForbidden(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("ValidatingAdmissionPolicy"))

			By("allowing CA ServiceAccount to create and update a CapacityBuffer-managed PodTemplate")
			managedName := "capacitybuffer-22222222-2222-2222-2222-222222222222-managed-pod-template"
			managedPT := testutil.NewPodTemplate(
				testutil.WithPodTemplateName(managedName),
				testutil.WithNamespace[*corev1.PodTemplate](namespace),
				func(pt *corev1.PodTemplate) {
					pt.OwnerReferences = []metav1.OwnerReference{
						{
							APIVersion: cbapi.CapacityBufferApiVersion,
							Kind:       cbapi.CapacityBufferKind,
							Name:       "buf-a",
							UID:        "22222222-2222-2222-2222-222222222222",
							Controller: ptr.To(true),
						},
					}
				},
			)
			createdManagedPT, err := caClient.CoreV1().PodTemplates(namespace).Create(ctx, managedPT, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			createdManagedPT.Template.Spec.Containers[0].Image = "ca-updated-image"
			createdManagedPT, err = caClient.CoreV1().PodTemplates(namespace).Update(ctx, createdManagedPT, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("blocking a regular user from updating, deleting, or delete-collecting the active managed PodTemplate")
			tampered := createdManagedPT.DeepCopy()
			tampered.OwnerReferences = nil
			_, err = userClient.CoreV1().PodTemplates(namespace).Update(ctx, tampered, metav1.UpdateOptions{})
			Expect(k8serrors.IsForbidden(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("ValidatingAdmissionPolicy"))

			err = userClient.CoreV1().PodTemplates(namespace).Delete(ctx, managedName, metav1.DeleteOptions{})
			Expect(k8serrors.IsForbidden(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("ValidatingAdmissionPolicy"))

			err = userClient.CoreV1().PodTemplates(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			Expect(k8serrors.IsForbidden(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("ValidatingAdmissionPolicy"))

			By("allowing generic-garbage-collector to clear ownerReferences (--cascade=orphan), keeping user UPDATE blocked, and allowing user DELETE")
			orphaning := createdManagedPT.DeepCopy()
			orphaning.OwnerReferences = nil
			orphanedPT, err := gcClient.CoreV1().PodTemplates(namespace).Update(ctx, orphaning, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			reattachAttempt := orphanedPT.DeepCopy()
			reattachAttempt.OwnerReferences = createdManagedPT.OwnerReferences
			reattachAttempt.Finalizers = []string{"testing.x-k8s.io/self-lock"}
			_, err = userClient.CoreV1().PodTemplates(namespace).Update(ctx, reattachAttempt, metav1.UpdateOptions{})
			Expect(k8serrors.IsForbidden(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("ValidatingAdmissionPolicy"))

			Expect(userClient.CoreV1().PodTemplates(namespace).Delete(ctx, managedName, metav1.DeleteOptions{})).To(Succeed())
		})
	})

	Context("ReadyReplicas Updates", func() {
		SetDefaultEventuallyTimeout(5 * time.Second)
		SetDefaultEventuallyPollingInterval(100 * time.Millisecond)

		AfterEach(func() {
			By("cleaning up test resources")
			_ = buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
		})

		It("should reconcile buffer readyReplicas when updated", func() {
			By("creating a capacity buffer")
			buffer := testutil.NewBuffer(
				testutil.WithName("b1"),
				testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
				testutil.WithActiveProvisioningStrategy(),
			)
			buffer, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Create(ctx, buffer, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("updating readyReplicas via the controller")
			readyReplicasController.Update(buffer, 3)

			By("waiting until the buffer's readyReplicas get reconciled")
			Eventually(func(g Gomega) {
				b, err := buffersClient.AutoscalingV1beta1().CapacityBuffers(namespace).Get(ctx, "b1", metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(b.Status.ReadyReplicas).To(Equal(new(int32(3))))
			}).Should(Succeed())
		})
	})
})
