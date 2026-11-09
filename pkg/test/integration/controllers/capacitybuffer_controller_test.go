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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
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
				g.Expect(b.Status.PodTemplateRef.Name).NotTo(BeEmpty())
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
	})
})
