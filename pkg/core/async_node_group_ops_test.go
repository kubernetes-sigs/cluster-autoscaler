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

	"github.com/stretchr/testify/assert"
)

func TestAsyncNodeGroupOps(t *testing.T) {
	var ops asyncNodeGroupOps
	started := make(chan string, 10)
	unblock := make(chan struct{})
	blockingOp := func(nodeGroupId string) func() {
		return func() {
			started <- nodeGroupId
			<-unblock
		}
	}

	// Operations on different node groups run concurrently, without blocking the caller.
	assert.True(t, ops.tryStart("ng1", blockingOp("ng1")))
	assert.True(t, ops.tryStart("ng2", blockingOp("ng2")))
	assert.ElementsMatch(t, []string{"ng1", "ng2"}, []string{<-started, <-started})

	// Only one operation per node group can be in flight at a time.
	assert.False(t, ops.tryStart("ng1", blockingOp("ng1")))
	assert.False(t, ops.tryStart("ng2", blockingOp("ng2")))
	assert.True(t, ops.tryStart("ng3", blockingOp("ng3")))
	assert.Equal(t, "ng3", <-started)

	close(unblock)
	ops.wait()
	assert.Empty(t, started)

	// Once an operation has finished, another one can be started for the same node group.
	done := make(chan string, 1)
	assert.True(t, ops.tryStart("ng1", func() { done <- "ng1" }))
	ops.wait()
	assert.Equal(t, "ng1", <-done)
}
