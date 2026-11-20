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

package core

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/deletiontracker"
)

func TestAsyncNodeGroupDecreaseOps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var ops asyncNodeGroupDecreaseOps
		started := make(chan string, 10)
		unblock := make(chan struct{})
		blockingOp := func(nodeGroupId string) func() {
			return func() {
				started <- nodeGroupId
				<-unblock
			}
		}

		// Operations on different node groups run concurrently, without blocking the caller.
		assert.True(t, ops.tryStart("ng1", 1, blockingOp("ng1")))
		assert.True(t, ops.tryStart("ng2", 2, blockingOp("ng2")))
		assert.ElementsMatch(t, []string{"ng1", "ng2"}, []string{<-started, <-started})

		// Only one operation per node group can be in flight at a time.
		assert.False(t, ops.tryStart("ng1", 3, blockingOp("ng1")))
		assert.False(t, ops.tryStart("ng2", 3, blockingOp("ng2")))
		assert.True(t, ops.tryStart("ng3", 3, blockingOp("ng3")))
		assert.Equal(t, "ng3", <-started)

		// The target size decreases of the operations in flight are pending.
		assert.Equal(t, map[string]int{"ng1": 1, "ng2": 2, "ng3": 3}, ops.pendingTargetSizeDecreases())

		close(unblock)
		synctest.Wait()
		assert.Empty(t, started)
		assert.Empty(t, ops.pendingTargetSizeDecreases())

		// Once an operation has finished, another one can be started for the same node group.
		done := make(chan string, 1)
		assert.True(t, ops.tryStart("ng1", 1, func() { done <- "ng1" }))
		synctest.Wait()
		assert.Equal(t, "ng1", <-done)
	})
}

func TestActuationStatusWithPendingTargetSizeDecreases(t *testing.T) {
	tracker := deletiontracker.NewNodeDeletionTracker(0)
	tracker.StartDeletion("ng1", "ng1-node-0")
	tracker.StartDeletion("ng2", "ng2-node-0")
	status := actuationStatusWithPendingTargetSizeDecreases{
		ActuationStatus:            tracker.Snapshot(),
		pendingTargetSizeDecreases: map[string]int{"ng2": 2, "ng3": 3},
	}

	// The pending target size decreases add up with the deletions in progress.
	assert.Equal(t, 1, status.DeletionsCount("ng1"))
	assert.Equal(t, 3, status.DeletionsCount("ng2"))
	assert.Equal(t, 3, status.DeletionsCount("ng3"))
	assert.Equal(t, 0, status.DeletionsCount("ng4"))
	// The rest of the status is unchanged.
	empty, drained := status.DeletionsInProgress()
	assert.ElementsMatch(t, []string{"ng1-node-0", "ng2-node-0"}, empty)
	assert.Empty(t, drained)
}
