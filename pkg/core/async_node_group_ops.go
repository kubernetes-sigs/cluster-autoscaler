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
	"sync"
)

// asyncNodeGroupOps runs cloud provider operations on node groups in the background, allowing
// at most one operation per node group at a time. It is used for cluster reconciliation
// operations (removing long unregistered nodes, deleting nodes that failed to be created,
// fixing node group target sizes) that are not latency critical, so that a slow or hanging
// cloud provider call doesn't block the main loop, and so that the same operation isn't
// re-issued every iteration while the previous one is still in progress.
//
// The zero value is ready to use.
type asyncNodeGroupOps struct {
	mu       sync.Mutex
	wg       sync.WaitGroup
	inFlight map[string]bool
}

// tryStart runs op in a new goroutine and returns true, unless an operation for nodeGroupId is
// already in flight, in which case it does nothing and returns false.
func (a *asyncNodeGroupOps) tryStart(nodeGroupId string, op func()) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.inFlight[nodeGroupId] {
		return false
	}
	if a.inFlight == nil {
		a.inFlight = make(map[string]bool)
	}
	a.inFlight[nodeGroupId] = true
	a.wg.Add(1)
	go func() {
		defer a.finish(nodeGroupId)
		op()
	}()
	return true
}

func (a *asyncNodeGroupOps) finish(nodeGroupId string) {
	a.mu.Lock()
	delete(a.inFlight, nodeGroupId)
	a.mu.Unlock()
	a.wg.Done()
}

// wait blocks until all operations that are currently in flight have finished. It is meant
// for tests, which need deterministic results of the asynchronous operations.
func (a *asyncNodeGroupOps) wait() {
	a.wg.Wait()
}
