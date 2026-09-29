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

package updater

import (
	"testing"

	ctesting "k8s.io/client-go/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/testutil"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	fakeclientset "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/clientset/versioned/fake"
	buffersinformers "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/informers/externalversions"
	bufferslisters "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/listers/autoscaling.x-k8s.io/v1beta1"
	cbclient "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/client"
)

func provisionedBuffer(name string, uid types.UID, replicas int32) *v1.CapacityBuffer {
	return testutil.NewBuffer(
		testutil.WithName(name),
		testutil.WithStatusReplicas(replicas),
		func(buffer *v1.CapacityBuffer) {
			buffer.UID = uid
		},
		func(buffer *v1.CapacityBuffer) {
			buffer.Status.Conditions = testutil.GetConditionReady()
		},
	)
}

func TestStatusUpdater(t *testing.T) {
	cachedBuffer := provisionedBuffer("buffer1", "uid1", 1)
	cachedUntouchedBuffer := provisionedBuffer("buffer3", "uid3", 1)

	changedBuffer := provisionedBuffer("buffer1", "uid1", 2)
	unchangedBuffer := cachedUntouchedBuffer.DeepCopy()
	nonExistingBuffer := provisionedBuffer("buffer2", "uid2", 1)

	tests := []struct {
		name               string
		buffers            []*v1.CapacityBuffer
		wantNumberOfCalls  int
		wantNumberOfErrors int
		wantUpdatedCount   int
	}{
		{
			name:               "updates a buffer whose status changed",
			buffers:            []*v1.CapacityBuffer{changedBuffer},
			wantNumberOfCalls:  1,
			wantNumberOfErrors: 0,
			wantUpdatedCount:   1,
		},
		{
			name:               "skips a buffer whose status is unchanged",
			buffers:            []*v1.CapacityBuffer{unchangedBuffer},
			wantNumberOfCalls:  0,
			wantNumberOfErrors: 0,
			wantUpdatedCount:   1,
		},
		{
			name:               "updates a buffer that does not exist",
			buffers:            []*v1.CapacityBuffer{nonExistingBuffer},
			wantNumberOfCalls:  1,
			wantNumberOfErrors: 1,
			wantUpdatedCount:   0,
		},
		{
			name:               "updates multiple buffers",
			buffers:            []*v1.CapacityBuffer{changedBuffer, unchangedBuffer, nonExistingBuffer},
			wantNumberOfCalls:  2,
			wantNumberOfErrors: 1,
			wantUpdatedCount:   2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fakeclientset.NewSimpleClientset(cachedBuffer, cachedUntouchedBuffer)
			fakeCapacityBuffersClient, err := cbclient.NewCapacityBufferClient(fakeClient, nil, newBuffersLister(t, fakeClient), nil, nil, nil, nil, nil, nil, nil, nil)
			assert.NoError(t, err)

			updateCallsCount := 0
			fakeClient.Fake.PrependReactor("update", "capacitybuffers",
				func(action ctesting.Action) (handled bool, ret runtime.Object, err error) {
					updateCallsCount++
					return false, nil, nil
				},
			)
			buffersUpdater := NewStatusUpdater(fakeCapacityBuffersClient)
			updatedBuffers, errors := buffersUpdater.Update(tc.buffers)
			assert.Equal(t, tc.wantNumberOfErrors, len(errors))
			assert.Equal(t, tc.wantNumberOfCalls, updateCallsCount)
			assert.Equal(t, tc.wantUpdatedCount, len(updatedBuffers))
		})
	}
}

func newBuffersLister(t *testing.T, client *fakeclientset.Clientset) bufferslisters.CapacityBufferLister {
	t.Helper()

	factory := buffersinformers.NewSharedInformerFactory(client, 0)
	lister := factory.Autoscaling().V1beta1().CapacityBuffers().Lister()

	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	factory.Start(stop)
	factory.WaitForCacheSync(stop)

	return lister
}
