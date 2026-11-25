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

package inmemory

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiv1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	featuretesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
	fakecloudprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// gcePersistentDiskCSIDriver is the name of the GCE Persistent Disk CSI driver.
const gcePersistentDiskCSIDriver = "pd.csi.storage.gke.io"

// TestStaticAutoscaler_NodeWithoutCSINode verifies that a registered Node without a backing CSINode object
// doesn't fail the autoscaler loop when CSINode-aware scheduling is enabled.
func TestStaticAutoscaler_NodeWithoutCSINode(t *testing.T) {
	options := integration.NewTestConfig().
		WithOverrides(integration.WithCSINodeAwareScheduling()).
		ResolveOptions()
	infra := integration.SetupInfrastructure(t)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		require.NoError(t, err)

		// A single ready Node with no CSINode object created for it.
		node := test.BuildTestNode("ng-node-0", 1000, 1000, test.IsReady(true))
		infra.Fakes.CloudProvider.AddNodeGroup("ng", fakecloudprovider.WithNode(node))

		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
	})
}

// TestStaticAutoscaler_NodeWithoutCSINodeIsUpcoming verifies that a node which came up from a scale-up but
// doesn't have a CSINode yet is treated as not ready and substituted in the snapshot with the NodeGroup
// template (which carries the CSI driver) and becomes ready as soon as node starts reporting CSINode
// object with the expected driver registered
func TestStaticAutoscaler_NodeWithoutCSINodeIsUpcoming(t *testing.T) {
	nodeGroup := "csi-ng"
	storageClass := "sc-pd"
	startupTimeout := 5 * time.Minute

	options := integration.NewTestConfig().
		WithOverrides(
			integration.WithCSINodeAwareScheduling(),
			integration.WithMaxNodeStartupTime(startupTimeout),
			integration.WithMaxNodeProvisionTime(startupTimeout),
		).
		ResolveOptions()

	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		require.NoError(t, err)

		// A pending pod using a volume provisioned by the driver, which can only be placed on a node
		// that reports the driver in its CSINode.
		featuretesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.VolumeLimitScaling, true)
		addCSIDriverBlockingScheduling(ctx, t, fakes.KubeClient, gcePersistentDiskCSIDriver)
		addLateBindingStorageClass(ctx, t, fakes.KubeClient, storageClass, gcePersistentDiskCSIDriver)
		pod := test.BuildTestPod("pending", 100, 100, test.MarkUnschedulable(), test.WithPVCVolume("volume", "pvc"))
		addUnboundPVC(ctx, t, fakes.KubeClient, pod.Namespace, "pvc", storageClass)
		fakes.K8s.AddPod(pod)

		// Create an empty NodeGroup whose template declares the CSI driver.
		templateNode := test.BuildTestNode(nodeGroup+"-template", 1000, 1000, test.IsReady(true))
		templateNode.CreationTimestamp = metav1.NewTime(time.Now())
		templateCsiNode := csiNodeWithDriver(templateNode.Name, gcePersistentDiskCSIDriver)
		template := framework.NewTestNodeInfo(templateNode).SetCSINode(templateCsiNode)
		fakes.CloudProvider.AddNodeGroup(nodeGroup, fakecloudprovider.WithTemplate(template))

		fetchTargetSize := func() int {
			size, err := fakes.CloudProvider.GetNodeGroup(nodeGroup).TargetSize(ctx)
			require.NoError(t, err)
			return size
		}

		// The Pod fits the template, so CA scales up a single node.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assert.Equal(t, 1, fetchTargetSize(), "CA should scale up a single node for the pending pod")

		// The Node has no CSINode, so it's treated as not ready and replaced in the snapshot by the template.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assert.Equal(t, 1, fetchTargetSize(), "CA shouldn't scale up while the node without CSINode is upcoming")

		// Past MaxNodeStartupTime the node is unready and no longer substituted, autoscaler goes for another scale up.
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, startupTimeout)
		assert.Equal(t, 2, fetchTargetSize(), "CA should scale up one more node once the Node without CSINode stops being upcoming")

		// The first node finally reports the CSI driver, autoscaler no longer overrides it to unready, no scale up triggered.
		nodes := fakes.K8s.Nodes().Items
		require.Greater(t, len(nodes), 0)
		csiNode := csiNodeWithDriver(nodes[0].Name, gcePersistentDiskCSIDriver)
		_, err = fakes.KubeClient.StorageV1().CSINodes().Create(ctx, csiNode, metav1.CreateOptions{})
		require.NoError(t, err)
		synctestutils.MustRunOnceAfter(ctx, t, autoscaler, startupTimeout*3)
		assert.Equal(t, 2, fetchTargetSize(), "CA shouldn't scale up once the node reports the CSI driver")
	})
}

// csiNodeWithDriver returns a CSINode for the given Node that reports the given CSI driver as installed.
func csiNodeWithDriver(nodeName, csiDriverName string) *storagev1.CSINode {
	return &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{
			{Name: csiDriverName, NodeID: nodeName},
		}},
	}
}

// addCSIDriverBlockingScheduling registers a CSIDriver which opts into PreventPodSchedulingIfMissing. Pods mounting
// volumes provisioned by this driver can then only be scheduled on Nodes whose CSINode lists the driver
func addCSIDriverBlockingScheduling(ctx context.Context, t *testing.T, client kubernetes.Interface, driverName string) {
	t.Helper()
	_, err := client.StorageV1().CSIDrivers().Create(ctx, &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec:       storagev1.CSIDriverSpec{PreventPodSchedulingIfMissing: ptr.To(true)},
	}, metav1.CreateOptions{})
	require.NoError(t, err, "failed to create CSIDriver %q", driverName)
}

// addLateBindingStorageClass creates a StorageClass provisioned by the given CSI driver with WaitForFirstConsumer
// binding mode. PVCs in this class resolve to the driver for volume limit purposes, while staying unbound
// doesn't make the Pods using them unschedulable before they're placed on a Node.
func addLateBindingStorageClass(ctx context.Context, t *testing.T, client kubernetes.Interface, name, csiDriverName string) {
	t.Helper()
	_, err := client.StorageV1().StorageClasses().Create(ctx, &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: name},
		Provisioner:       csiDriverName,
		VolumeBindingMode: ptr.To(storagev1.VolumeBindingWaitForFirstConsumer),
	}, metav1.CreateOptions{})
	require.NoError(t, err, "failed to create StorageClass %q", name)
}

// addUnboundPVC creates a PersistentVolumeClaim in the given StorageClass that is not bound to any volume yet,
// i.e. one that's waiting for a Pod to be scheduled before a volume gets provisioned for it.
func addUnboundPVC(ctx context.Context, t *testing.T, client kubernetes.Interface, namespace, name, storageClassName string) {
	t.Helper()
	_, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(namespace + "/" + name),
		},
		Spec: apiv1.PersistentVolumeClaimSpec{StorageClassName: ptr.To(storageClassName)},
	}, metav1.CreateOptions{})
	require.NoError(t, err, "failed to create PVC %s/%s", namespace, name)
}
