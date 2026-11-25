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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	cbapi "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/testutil"
)

const (
	testScalableAPIGroup = "testing.x-k8s.io"
	testScalableKind     = "TestScalable"
	testScalableLabel    = "test-scalable"
)

var testScalableGVK = schema.GroupVersionKind{
	Group:   testScalableAPIGroup,
	Version: "v1",
	Kind:    testScalableKind,
}

// lateScalableGVK is served by a CRD the suite does not install up front. Its group is
// distinct from testScalableAPIGroup, so the REST mapper has never discovered it either.
var lateScalableGVK = schema.GroupVersionKind{
	Group:   "late.testing.x-k8s.io",
	Version: "v1",
	Kind:    "LateScalable",
}

var _ = Describe("CapacityBuffer dynamic ScalableRef watches", func() {
	// The client informers resync every 5 minutes, so anything observed within the
	// Eventually timeout can only have been triggered by a watch event.
	SetDefaultEventuallyTimeout(10 * time.Second)
	SetDefaultEventuallyPollingInterval(100 * time.Millisecond)

	var namespace string

	BeforeEach(func() {
		By("creating a test namespace")
		testNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "test-dynwatch-"}}
		Expect(crClient.Create(ctx, testNS)).To(Succeed())
		namespace = testNS.Name
	})

	AfterEach(func() {
		By("cleaning up test resources")
		Expect(crClient.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("should scale buffer replicas based on a referenced custom resource", func() {
		By("creating a custom scalable resource with 10 replicas")
		createTestScalable(namespace, "test-scalable", 10)

		By("creating a pod the custom resource's scale selector matches")
		createSelectedPod(namespace, "test-scalable-pod")

		By("creating a buffer referencing the custom resource with 20 percent")
		createBuffer(testutil.NewBuffer(
			testutil.WithName("b1"),
			testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
			testutil.WithScalableRef(testScalableAPIGroup, testScalableKind, "test-scalable"),
			testutil.WithPercentage(20),
			testutil.WithActiveProvisioningStrategy(),
		))

		By("checking that the watch was established and the buffer replicas is 20% of 10 (2)")
		Eventually(func(g Gomega) {
			b := getBuffer(g, namespace, "b1")
			g.Expect(b.Status.Replicas).To(Equal(new(int32(2))))
			condition := meta.FindStatusCondition(b.Status.Conditions, cbapi.ScalableRefWatchedCondition)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			g.Expect(condition.Reason).To(Equal(cbapi.WatchEstablishedReason))
		}).Should(Succeed())

		By("scaling the custom resource to 20 replicas")
		scaleTestScalable(namespace, "test-scalable", 20)

		// Nothing else changed, so only an event from the dynamically established watch
		// can have triggered this reconciliation.
		By("checking that the buffer replicas is 20% of 20 (4)")
		Eventually(func(g Gomega) {
			b := getBuffer(g, namespace, "b1")
			g.Expect(b.Status.Replicas).To(Equal(new(int32(4))))
		}).Should(Succeed())
	})

	It("should report an unwatchable kind without affecting other buffers", func() {
		By("creating a pod template")
		podTemplate := testutil.NewPodTemplate(
			testutil.WithPodTemplateName("pod-temp"),
			testutil.WithNamespace[*corev1.PodTemplate](namespace),
		)
		Expect(crClient.Create(ctx, podTemplate)).To(Succeed())

		By("creating a buffer referencing a kind that has no CRD installed")
		createBuffer(testutil.NewBuffer(
			testutil.WithName("unwatchable"),
			testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
			testutil.WithScalableRef(testScalableAPIGroup, "NotInstalled", "whatever"),
			testutil.WithPercentage(20),
			testutil.WithActiveProvisioningStrategy(),
		))

		By("creating a healthy buffer in the same namespace")
		createBuffer(testutil.NewBuffer(
			testutil.WithName("healthy"),
			testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
			testutil.WithPodTemplateRef("pod-temp"),
			testutil.WithReplicas(3),
			testutil.WithActiveProvisioningStrategy(),
		))

		By("checking that the failure is reported on the buffer")
		Eventually(func(g Gomega) {
			b := getBuffer(g, namespace, "unwatchable")
			condition := meta.FindStatusCondition(b.Status.Conditions, cbapi.ScalableRefWatchedCondition)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(condition.Reason).To(Equal(cbapi.UnknownKindReason))
			g.Expect(meta.IsStatusConditionFalse(b.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
		}).Should(Succeed())

		// This is the whole point of keeping watch establishment out of the namespace
		// reconciliation: a buffer that cannot be watched must not stall its neighbours.
		By("checking that the healthy buffer reconciled normally")
		Eventually(func(g Gomega) {
			b := getBuffer(g, namespace, "healthy")
			g.Expect(b.Status.Replicas).To(Equal(new(int32(3))))
			g.Expect(meta.IsStatusConditionTrue(b.Status.Conditions, cbapi.ReadyForProvisioningCondition)).To(BeTrue())
			g.Expect(meta.FindStatusCondition(b.Status.Conditions, cbapi.ScalableRefWatchedCondition)).To(BeNil())
		}).Should(Succeed())
	})

	It("should recover once the referenced kind becomes watchable", func() {
		By("creating a buffer referencing a custom resource that does not exist yet")
		createBuffer(testutil.NewBuffer(
			testutil.WithName("b1"),
			testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
			testutil.WithScalableRef(testScalableAPIGroup, testScalableKind, "created-later"),
			testutil.WithPercentage(50),
			testutil.WithActiveProvisioningStrategy(),
		))

		By("checking that the watch is established even though the object is missing")
		Eventually(func(g Gomega) {
			b := getBuffer(g, namespace, "b1")
			g.Expect(meta.IsStatusConditionTrue(b.Status.Conditions, cbapi.ScalableRefWatchedCondition)).To(BeTrue())
		}).Should(Succeed())

		By("creating the referenced custom resource with 10 replicas")
		createTestScalable(namespace, "created-later", 10)
		createSelectedPod(namespace, "created-later-pod")

		By("checking that the buffer replicas is 50% of 10 (5)")
		Eventually(func(g Gomega) {
			b := getBuffer(g, namespace, "b1")
			g.Expect(b.Status.Replicas).To(Equal(new(int32(5))))
		}).Should(Succeed())
	})

	It("should recover once the CRD of the referenced kind is installed", func() {
		By("creating a buffer referencing a kind whose CRD is not installed yet")
		createBuffer(testutil.NewBuffer(
			testutil.WithName("b1"),
			testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
			testutil.WithScalableRef(lateScalableGVK.Group, lateScalableGVK.Kind, "late-scalable"),
			testutil.WithPercentage(50),
			testutil.WithActiveProvisioningStrategy(),
		))

		By("checking that the kind is reported as unknown")
		Eventually(func(g Gomega) {
			b := getBuffer(g, namespace, "b1")
			condition := meta.FindStatusCondition(b.Status.Conditions, cbapi.ScalableRefWatchedCondition)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Reason).To(Equal(cbapi.UnknownKindReason))
		}).Should(Succeed())

		By("installing the CRD")
		_, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
			CRDs: []*apiextensionsv1.CustomResourceDefinition{newScalableCRD(lateScalableGVK)},
		})
		Expect(err).NotTo(HaveOccurred())

		By("creating the referenced custom resource with 10 replicas")
		createScalable(lateScalableGVK, namespace, "late-scalable", 10)
		createSelectedPod(namespace, "late-scalable-pod")

		By("checking that the watch is established and the buffer replicas is 50% of 10 (5)")
		Eventually(func(g Gomega) {
			b := getBuffer(g, namespace, "b1")
			condition := meta.FindStatusCondition(b.Status.Conditions, cbapi.ScalableRefWatchedCondition)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			g.Expect(condition.Reason).To(Equal(cbapi.WatchEstablishedReason))
			g.Expect(b.Status.Replicas).To(Equal(new(int32(5))))
		}, time.Minute).Should(Succeed())
	})

	It("should keep reconciling when the reference moves to a statically watched kind", func() {
		By("creating a custom scalable resource and a deployment")
		createTestScalable(namespace, "test-scalable", 10)
		createSelectedPod(namespace, "test-scalable-pod")
		createDeployment(namespace, "my-dep", 10)

		By("creating a buffer referencing the custom resource")
		createBuffer(testutil.NewBuffer(
			testutil.WithName("b1"),
			testutil.WithNamespace[*v1beta1.CapacityBuffer](namespace),
			testutil.WithScalableRef(testScalableAPIGroup, testScalableKind, "test-scalable"),
			testutil.WithPercentage(20),
			testutil.WithActiveProvisioningStrategy(),
		))

		By("waiting for the watch to be established")
		Eventually(func(g Gomega) {
			b := getBuffer(g, namespace, "b1")
			g.Expect(meta.IsStatusConditionTrue(b.Status.Conditions, cbapi.ScalableRefWatchedCondition)).To(BeTrue())
		}).Should(Succeed())

		By("pointing the buffer at the deployment instead")
		Eventually(func(g Gomega) {
			b := getBuffer(g, namespace, "b1")
			b.Spec.ScalableRef = &v1beta1.ScalableRef{APIGroup: "apps", Kind: "Deployment", Name: "my-dep"}
			g.Expect(crClient.Update(ctx, b)).To(Succeed())
		}).Should(Succeed())

		By("checking that the buffer is sized from the deployment")
		Eventually(func(g Gomega) {
			b := getBuffer(g, namespace, "b1")
			g.Expect(b.Status.Replicas).To(Equal(new(int32(2))))
		}).Should(Succeed())
	})
})

func createBuffer(buffer *v1beta1.CapacityBuffer) {
	GinkgoHelper()
	Expect(crClient.Create(ctx, buffer)).To(Succeed())
}

func getBuffer(g Gomega, namespace, name string) *v1beta1.CapacityBuffer {
	GinkgoHelper()
	buffer := &v1beta1.CapacityBuffer{}
	g.Expect(crClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, buffer)).To(Succeed())
	return buffer
}

func newTestScalable(namespace, name string) *unstructured.Unstructured {
	return newScalable(testScalableGVK, namespace, name)
}

func newScalable(gvk schema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(gvk)
	object.SetNamespace(namespace)
	object.SetName(name)
	return object
}

// createTestScalable creates a custom resource exposing a scale subresource, with its
// status filled in so that the controller can resolve it.
func createTestScalable(namespace, name string, replicas int64) {
	GinkgoHelper()
	createScalable(testScalableGVK, namespace, name, replicas)
}

// createScalable creates a custom resource of the passed kind exposing a scale
// subresource, with its status filled in so that the controller can resolve it.
func createScalable(gvk schema.GroupVersionKind, namespace, name string, replicas int64) {
	GinkgoHelper()
	object := newScalable(gvk, namespace, name)
	Expect(unstructured.SetNestedField(object.Object, replicas, "spec", "replicas")).To(Succeed())
	Expect(crClient.Create(ctx, object)).To(Succeed())

	Expect(unstructured.SetNestedField(object.Object, replicas, "status", "replicas")).To(Succeed())
	Expect(unstructured.SetNestedField(object.Object, "app="+testScalableLabel, "status", "selector")).To(Succeed())
	Expect(crClient.Status().Update(ctx, object)).To(Succeed())
}

// scaleTestScalable updates the replica count the scale subresource reports.
func scaleTestScalable(namespace, name string, replicas int64) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		object := newTestScalable(namespace, name)
		g.Expect(crClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, object)).To(Succeed())
		g.Expect(unstructured.SetNestedField(object.Object, replicas, "status", "replicas")).To(Succeed())
		g.Expect(crClient.Status().Update(ctx, object)).To(Succeed())
	}).Should(Succeed())
}

func newScalableCRD(gvk schema.GroupVersionKind) *apiextensionsv1.CustomResourceDefinition {
	integer := func(format string) apiextensionsv1.JSONSchemaProps {
		return apiextensionsv1.JSONSchemaProps{Type: "integer", Format: format}
	}
	replicas := integer("int32")
	replicas.Minimum = new(float64(0))
	singular := strings.ToLower(gvk.Kind)
	plural := singular + "s"

	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: plural + "." + gvk.Group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: gvk.Group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:     gvk.Kind,
				ListKind: gvk.Kind + "List",
				Plural:   plural,
				Singular: singular,
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    gvk.Version,
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {
								Type:       "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{"replicas": replicas},
							},
							"status": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"replicas": integer("int32"),
									"selector": {Type: "string"},
								},
							},
						},
					},
				},
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					Scale: &apiextensionsv1.CustomResourceSubresourceScale{
						SpecReplicasPath:   ".spec.replicas",
						StatusReplicasPath: ".status.replicas",
						LabelSelectorPath:  new(".status.selector"),
					},
				},
			}},
		},
	}
}

func createSelectedPod(namespace, name string) {
	GinkgoHelper()
	Expect(crClient.Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": testScalableLabel},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "container", Image: "image"}},
		},
	})).To(Succeed())
}

func createDeployment(namespace, name string, replicas int32) {
	GinkgoHelper()
	labels := map[string]string{"app": name}
	Expect(crClient.Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "container", Image: "image"}},
				},
			},
		},
	})).To(Succeed())
}
