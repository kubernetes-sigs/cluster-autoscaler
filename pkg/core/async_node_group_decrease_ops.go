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
	"maps"
	"sync"

	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown"
)

// asyncNodeGroupDecreaseOps runs cloud provider operations that decrease node groups in the
// background, allowing at most one operation per node group at a time. It is used for the
// cluster reconciliation operations (removing long unregistered nodes, deleting nodes that failed
// to be created, fixing node group target sizes) that are not latency critical, so that a slow or
// hanging cloud provider call doesn't block the main loop, and so that the same operation isn't
// re-issued every iteration while the previous one is still in progress. The limit is per node
// group, not per kind of operation: e.g. unregistered nodes aren't removed from a node group
// while its target size is being decreased, and vice versa.
//
// All these operations decrease the target size of their node group once they complete. Until
// then, the target size reported by the cloud provider still counts the instances being
// removed, which must not be mistaken for room to remove other nodes from the node group: the
// pending decreases are exposed by pendingTargetSizeDecreases, for the min size checks of
// scale-down.
//
// The zero value is ready to use.
type asyncNodeGroupDecreaseOps struct {
	mu sync.Mutex
	// inFlight maps the ids of the node groups with an operation in flight to the decrease of
	// the node group's target size the operation is about to cause.
	inFlight map[string]int
}

// tryStart runs op in a new goroutine and returns true, unless an operation for nodeGroupId is
// already in flight, in which case it does nothing and returns false. targetSizeDecrease is the
// number of instances op removes from the node group, i.e. the decrease of the node group's
// target size once op completes.
func (a *asyncNodeGroupDecreaseOps) tryStart(nodeGroupId string, targetSizeDecrease int, op func()) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, inFlight := a.inFlight[nodeGroupId]; inFlight {
		return false
	}
	if a.inFlight == nil {
		a.inFlight = make(map[string]int)
	}
	a.inFlight[nodeGroupId] = targetSizeDecrease
	go a.run(nodeGroupId, op)
	return true
}

// run runs op and then marks the node group as free for the next operation.
func (a *asyncNodeGroupDecreaseOps) run(nodeGroupId string, op func()) {
	op()
	a.mu.Lock()
	delete(a.inFlight, nodeGroupId)
	a.mu.Unlock()
}

// pendingTargetSizeDecreases returns, by node group id, the decrease of the node group's target
// size that the operation in flight for it is about to cause.
func (a *asyncNodeGroupDecreaseOps) pendingTargetSizeDecreases() map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return maps.Clone(a.inFlight)
}

// actuationStatusWithPendingTargetSizeDecreases is a scaledown.ActuationStatus that counts the
// pending target size decreases of the background node group operations (see
// asyncNodeGroupDecreaseOps) as deletions in progress, so that the min size checks of scale-down
// see the node groups' target sizes as they'll be once the operations complete, like they already
// do for the scale-down deletions in progress.
type actuationStatusWithPendingTargetSizeDecreases struct {
	scaledown.ActuationStatus
	pendingTargetSizeDecreases map[string]int
}

// DeletionsCount returns the number of deletions in progress in the node group, including the
// pending target size decrease of the background operation in flight for it, if any.
func (s actuationStatusWithPendingTargetSizeDecreases) DeletionsCount(nodeGroupId string) int {
	return s.ActuationStatus.DeletionsCount(nodeGroupId) + s.pendingTargetSizeDecreases[nodeGroupId]
}
