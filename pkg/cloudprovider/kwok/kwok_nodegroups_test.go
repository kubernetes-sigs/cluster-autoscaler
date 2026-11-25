/*
Copyright 2023 The Kubernetes Authors.

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

package kwok

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

func TestIncreaseSize(t *testing.T) {
	fakeClient := &fake.Clientset{}

	nodes := []*apiv1.Node{}

	fakeClient.Fake.AddReactor("create", "nodes",
		func(action core.Action) (bool, runtime.Object, error) {
			createAction := action.(core.CreateAction)
			if createAction == nil {
				return false, nil, nil
			}

			nodes = append(nodes, createAction.GetObject().(*apiv1.Node))

			return true, nil, nil
		})

	ng := NodeGroup{
		name:       "ng",
		kubeClient: fakeClient,
		lister:     kube_util.NewTestNodeLister(nil),
		nodeTemplate: &apiv1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "template-node-ng",
			},
		},
		minSize:    0,
		targetSize: 2,
		maxSize:    3,
	}

	// usual case
	err := ng.IncreaseSize(context.Background(), 1)
	assert.Nil(t, err)
	assert.Len(t, nodes, 1)
	assert.Equal(t, 3, ng.targetSize)
	for _, n := range nodes {
		assert.Contains(t, n.Spec.ProviderID, "kwok")
		assert.Contains(t, n.GetName(), ng.name)
		assert.Contains(t, n.Annotations["metrics.k8s.io/resource-metrics-path"], fmt.Sprintf("/metrics/nodes/%s/metrics/resource", n.GetName()))
	}

	// delta is negative
	nodes = []*apiv1.Node{}
	err = ng.IncreaseSize(context.Background(), -1)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), sizeIncreaseMustBePositiveErr)
	assert.Len(t, nodes, 0)

	// delta is greater than max size
	nodes = []*apiv1.Node{}
	err = ng.IncreaseSize(context.Background(), ng.maxSize+1)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), maxSizeReachedErr)
	assert.Len(t, nodes, 0)

}

func TestIncreaseSizeDoesNotMutateTemplate(t *testing.T) {
	fakeClient := &fake.Clientset{}

	nodes := []*apiv1.Node{}
	fakeClient.Fake.AddReactor("create", "nodes",
		func(action core.Action) (bool, runtime.Object, error) {
			nodes = append(nodes, action.(core.CreateAction).GetObject().(*apiv1.Node))
			return true, nil, nil
		})

	template := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "template-node-ng",
			Labels:      map[string]string{"kwok-nodegroup": "ng"},
			Annotations: map[string]string{NGNameAnnotation: "ng"},
		},
		Spec: apiv1.NodeSpec{
			ProviderID: getProviderID("template-node-ng"),
		},
	}
	templateCopy := template.DeepCopy()

	ng := NodeGroup{
		name:         "ng",
		kubeClient:   fakeClient,
		lister:       kube_util.NewTestNodeLister(nil),
		nodeTemplate: template,
		minSize:      0,
		targetSize:   0,
		maxSize:      10,
	}

	err := ng.IncreaseSize(context.Background(), 3)
	assert.NoError(t, err)
	assert.Equal(t, 3, ng.targetSize)
	assert.Equal(t, templateCopy, ng.nodeTemplate, "template must not be mutated by IncreaseSize")

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.GetName()] = true
		assert.Equal(t, n.GetName(), n.Labels["kubernetes.io/hostname"])
		assert.Equal(t, getProviderID(n.GetName()), n.Spec.ProviderID)
		assert.Equal(t, "ng", n.Labels["kwok-nodegroup"])
	}
	assert.Len(t, names, 3, "every created node must have a unique name")

	// Template returned by TemplateNodeInfo must be a copy as well.
	ti, err := ng.TemplateNodeInfo(context.Background())
	assert.NoError(t, err)
	ti.Node().Name = "mutated"
	ti.Node().Labels["mutated"] = "true"
	assert.Equal(t, templateCopy, ng.nodeTemplate, "template must not be mutated through TemplateNodeInfo")

	// nil template
	ng.nodeTemplate = nil
	err = ng.IncreaseSize(context.Background(), 1)
	assert.Error(t, err)
}

func TestDeleteNodes(t *testing.T) {
	fakeClient := &fake.Clientset{}

	deletedNodes := make(map[string]bool)
	fakeClient.Fake.AddReactor("delete", "nodes", func(action core.Action) (bool, runtime.Object, error) {
		deleteAction := action.(core.DeleteAction)

		if deleteAction == nil {
			return false, nil, nil
		}

		deletedNodes[deleteAction.GetName()] = true

		return true, nil, nil

	})

	ng := NodeGroup{
		name:       "ng",
		kubeClient: fakeClient,
		lister:     kube_util.NewTestNodeLister(nil),
		nodeTemplate: &apiv1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "template-node-ng",
			},
		},
		minSize:    0,
		targetSize: 1,
		maxSize:    3,
	}

	nodeToDelete1 := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-to-delete-1",
			Annotations: map[string]string{
				KwokManagedAnnotation: "fake",
			},
		},
	}

	nodeToDelete2 := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-to-delete-2",
			Annotations: map[string]string{
				KwokManagedAnnotation: "fake",
			},
		},
	}

	nodeWithoutKwokAnnotation := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node-to-delete-3",
			Annotations: map[string]string{},
		},
	}

	// usual case
	err := ng.DeleteNodes(context.Background(), []*apiv1.Node{nodeToDelete1})
	assert.Nil(t, err)
	assert.True(t, deletedNodes[nodeToDelete1.GetName()])

	// min size reached
	deletedNodes = make(map[string]bool)
	ng.targetSize = 0
	err = ng.DeleteNodes(context.Background(), []*apiv1.Node{nodeToDelete1})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), minSizeReachedErr)
	assert.False(t, deletedNodes[nodeToDelete1.GetName()])
	ng.targetSize = 1

	// too many nodes to delete - goes below ng's minSize
	deletedNodes = make(map[string]bool)
	err = ng.DeleteNodes(context.Background(), []*apiv1.Node{nodeToDelete1, nodeToDelete2})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), belowMinSizeErr)
	assert.False(t, deletedNodes[nodeToDelete1.GetName()])
	assert.False(t, deletedNodes[nodeToDelete2.GetName()])

	// kwok annotation is not present on the node to delete
	deletedNodes = make(map[string]bool)
	err = ng.DeleteNodes(context.Background(), []*apiv1.Node{nodeWithoutKwokAnnotation})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "not managed by kwok")
	assert.False(t, deletedNodes[nodeWithoutKwokAnnotation.GetName()])

}

func TestNodeGroupConcurrentSizeChanges(t *testing.T) {
	newFakeClient := func() *fake.Clientset {
		fakeClient := &fake.Clientset{}
		fakeClient.Fake.AddReactor("create", "nodes", func(action core.Action) (bool, runtime.Object, error) {
			return true, nil, nil
		})
		fakeClient.Fake.AddReactor("delete", "nodes", func(action core.Action) (bool, runtime.Object, error) {
			return true, nil, nil
		})
		return fakeClient
	}
	newNodeGroup := func(minSize, targetSize, maxSize int) *NodeGroup {
		return &NodeGroup{
			name:       "ng",
			kubeClient: newFakeClient(),
			lister:     kube_util.NewTestNodeLister(nil),
			nodeTemplate: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "template-node-ng",
					Labels:      map[string]string{"kwok-nodegroup": "ng"},
					Annotations: map[string]string{NGNameAnnotation: "ng", KwokManagedAnnotation: "fake"},
				},
			},
			minSize:    minSize,
			targetSize: targetSize,
			maxSize:    maxSize,
		}
	}
	nodeToDelete := func(i int) *apiv1.Node {
		return &apiv1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:        fmt.Sprintf("ng-%d", i),
				Annotations: map[string]string{KwokManagedAnnotation: "fake", NGNameAnnotation: "ng"},
			},
		}
	}

	t.Run("concurrent increases and deletes keep target size consistent", func(t *testing.T) {
		ng := newNodeGroup(0, 100, 1000)
		const ops = 50

		var wg sync.WaitGroup
		for i := 0; i < ops; i++ {
			wg.Add(3)
			go func(i int) {
				defer wg.Done()
				assert.NoError(t, ng.DeleteNodes(context.Background(), []*apiv1.Node{nodeToDelete(i)}))
			}(i)
			go func() {
				defer wg.Done()
				assert.NoError(t, ng.IncreaseSize(context.Background(), 1))
			}()
			go func() {
				defer wg.Done()
				_, err := ng.TargetSize(context.Background())
				assert.NoError(t, err)
			}()
		}
		wg.Wait()

		size, err := ng.TargetSize(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 100, size)
	})

	t.Run("concurrent deletes never go below min size", func(t *testing.T) {
		ng := newNodeGroup(5, 10, 100)
		const deletes = 20

		var wg sync.WaitGroup
		var mutex sync.Mutex
		succeeded := 0
		for i := 0; i < deletes; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if err := ng.DeleteNodes(context.Background(), []*apiv1.Node{nodeToDelete(i)}); err == nil {
					mutex.Lock()
					succeeded++
					mutex.Unlock()
				}
			}(i)
		}
		wg.Wait()

		size, err := ng.TargetSize(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 5, size)
		assert.Equal(t, 5, succeeded)
	})

	t.Run("concurrent increases never go above max size", func(t *testing.T) {
		ng := newNodeGroup(0, 0, 5)
		const increases = 20

		var wg sync.WaitGroup
		for i := 0; i < increases; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = ng.IncreaseSize(context.Background(), 1)
			}()
		}
		wg.Wait()

		size, err := ng.TargetSize(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 5, size)
	})

	t.Run("refresh runs concurrently with size changes", func(t *testing.T) {
		ng := newNodeGroup(0, 100, 1000)
		provider := &KwokCloudProvider{
			nodeGroups: []*NodeGroup{ng},
			config: &KwokProviderConfig{
				status: &GroupingConfig{groupNodesBy: groupNodesByLabel, key: "kwok-nodegroup"},
			},
			allNodesLister: newTestAllNodeLister(map[string]*apiv1.Node{
				"ng-0": {ObjectMeta: metav1.ObjectMeta{Name: "ng-0", Annotations: map[string]string{NGNameAnnotation: "ng"}}},
			}),
		}

		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(4)
			go func() {
				defer wg.Done()
				assert.NoError(t, provider.Refresh(context.Background()))
			}()
			go func(i int) {
				defer wg.Done()
				_ = ng.DeleteNodes(context.Background(), []*apiv1.Node{nodeToDelete(i)})
			}(i)
			go func() {
				defer wg.Done()
				_ = ng.IncreaseSize(context.Background(), 1)
			}()
			go func() {
				defer wg.Done()
				_ = ng.DecreaseTargetSize(context.Background(), -1)
			}()
		}
		wg.Wait()

		// Refresh sets the target size to the number of nodes seen in the cluster.
		assert.NoError(t, provider.Refresh(context.Background()))
		size, err := ng.TargetSize(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
	})
}

func TestDecreaseTargetSize(t *testing.T) {
	fakeClient := &fake.Clientset{}

	fakeNodes := []*apiv1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-2",
			},
		},
	}

	ng := NodeGroup{
		name:       "ng",
		kubeClient: fakeClient,
		lister:     kube_util.NewTestNodeLister(fakeNodes),
		nodeTemplate: &apiv1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "template-node-ng",
			},
		},
		minSize:    0,
		targetSize: 3,
		maxSize:    4,
	}

	// usual case
	err := ng.DecreaseTargetSize(context.Background(), -1)
	assert.Nil(t, err)
	assert.Equal(t, 2, ng.targetSize)

	// delta is positive
	ng.targetSize = 3
	err = ng.DecreaseTargetSize(context.Background(), 1)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), sizeDecreaseMustBeNegativeErr)
	assert.Equal(t, 3, ng.targetSize)

	// attempt to delete existing nodes
	err = ng.DecreaseTargetSize(context.Background(), -2)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), attemptToDeleteExistingNodesErr)
	assert.Equal(t, 3, ng.targetSize)

	// error from lister
	ng.lister = &ErroneousNodeLister{}
	err = ng.DecreaseTargetSize(context.Background(), -1)
	assert.NotNil(t, err)
	assert.Equal(t, cloudprovider.ErrNotImplemented.Error(), err.Error())
	assert.Equal(t, 3, ng.targetSize)
	ng.lister = kube_util.NewTestNodeLister(fakeNodes)
}

func TestNodes(t *testing.T) {
	fakeClient := &fake.Clientset{}

	fakeNodes := []*apiv1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-2",
			},
		},
	}

	ng := NodeGroup{
		name:       "ng",
		kubeClient: fakeClient,
		lister:     kube_util.NewTestNodeLister(fakeNodes),
		nodeTemplate: &apiv1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "template-node-ng",
			},
		},
		minSize:    0,
		targetSize: 2,
		maxSize:    3,
	}

	// usual case
	cpInstances, err := ng.Nodes(context.Background())
	assert.Nil(t, err)
	assert.Len(t, cpInstances, 2)
	for i := range cpInstances {
		assert.Contains(t, cpInstances[i].Id, fakeNodes[i].GetName())
		assert.Equal(t, &cloudprovider.InstanceStatus{
			State:     cloudprovider.InstanceRunning,
			ErrorInfo: nil,
		}, cpInstances[i].Status)
	}

	// error from lister
	ng.lister = &ErroneousNodeLister{}
	cpInstances, err = ng.Nodes(context.Background())
	assert.NotNil(t, err)
	assert.Len(t, cpInstances, 0)
	assert.Equal(t, cloudprovider.ErrNotImplemented.Error(), err.Error())

}

func TestTemplateNodeInfo(t *testing.T) {
	fakeClient := &fake.Clientset{}

	ng := NodeGroup{
		name:       "ng",
		kubeClient: fakeClient,
		lister:     kube_util.NewTestNodeLister(nil),
		nodeTemplate: &apiv1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "template-node-ng",
			},
		},
		minSize:    0,
		targetSize: 2,
		maxSize:    3,
	}

	// usual case
	ti, err := ng.TemplateNodeInfo(context.Background())
	assert.Nil(t, err)
	assert.NotNil(t, ti)
	assert.Len(t, ti.Pods(), 1)
	assert.Contains(t, ti.Pods()[0].Pod.Name, fmt.Sprintf("kube-proxy-%s", ng.name))
	assert.Equal(t, ng.nodeTemplate, ti.Node())
	assert.NotSame(t, ng.nodeTemplate, ti.Node())

}

func TestGetOptions(t *testing.T) {
	fakeClient := &fake.Clientset{}

	ng := NodeGroup{
		name:       "ng",
		kubeClient: fakeClient,
		lister:     kube_util.NewTestNodeLister(nil),
		nodeTemplate: &apiv1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "template-node-ng",
			},
		},
		minSize:    0,
		targetSize: 2,
		maxSize:    3,
	}

	// dummy values
	autoscalingOptions := config.NodeGroupAutoscalingOptions{
		ScaleDownUtilizationThreshold:    50.0,
		ScaleDownGpuUtilizationThreshold: 50.0,
		ScaleDownUnneededTime:            time.Minute * 5,
		ScaleDownUnreadyTime:             time.Minute * 5,
		MaxNodeProvisionTime:             time.Minute * 5,
		ZeroOrMaxNodeScaling:             true,
		IgnoreDaemonSetsUtilization:      true,
	}

	// usual case
	opts, err := ng.GetOptions(context.Background(), autoscalingOptions)
	assert.Nil(t, err)
	assert.Equal(t, autoscalingOptions, *opts)

}

// ErroneousNodeLister is used to check if the caller function throws an error
// if lister throws an error
type ErroneousNodeLister struct {
}

func (e *ErroneousNodeLister) List() ([]*apiv1.Node, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (e *ErroneousNodeLister) Get(name string) (*apiv1.Node, error) {
	return nil, cloudprovider.ErrNotImplemented
}
