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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/klog/v2"
	cbclient "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/client"
)

// StatusUpdater updates the buffer status bassed
type StatusUpdater struct {
	client *cbclient.CapacityBufferClient
}

// NewStatusUpdater creates an instance of StatusUpdater.
func NewStatusUpdater(client *cbclient.CapacityBufferClient) *StatusUpdater {
	return &StatusUpdater{
		client: client,
	}
}

// Update updates the buffer status with pod capacity
//
// Buffers whose status is already up to date are returned without being written, so that
// a steady state cluster does not generate a write per buffer per resync.
func (u *StatusUpdater) Update(buffers []*v1.CapacityBuffer) ([]*v1.CapacityBuffer, []error) {
	var errors []error
	var updatedBuffers []*v1.CapacityBuffer

	for _, buffer := range buffers {
		if !u.statusChanged(buffer) {
			// The buffer was still reconciled, so report it back to the caller to keep
			// the reconciliation timestamps it tracks accurate.
			updatedBuffers = append(updatedBuffers, buffer)
			continue
		}
		updatedBuffer, err := u.client.UpdateCapacityBuffer(buffer)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		if updatedBuffer != nil {
			updatedBuffers = append(updatedBuffers, updatedBuffer)
		}
	}
	return updatedBuffers, errors
}

// statusChanged reports whether writing the passed buffer would change the status the
// informer cache currently holds.
func (u *StatusUpdater) statusChanged(buffer *v1.CapacityBuffer) bool {
	observed, err := u.client.GetCapacityBuffer(buffer.Namespace, buffer.Name)
	if err != nil {
		klog.V(4).Infof("capacity buffer status updater: failed to read the cached buffer %s/%s, updating unconditionally: %v", buffer.Namespace, buffer.Name, err)
		return true
	}
	return !apiequality.Semantic.DeepEqual(observed.Status, buffer.Status)
}

// CleanUp cleans up the updater's internal structures.
func (u *StatusUpdater) CleanUp() {
}
