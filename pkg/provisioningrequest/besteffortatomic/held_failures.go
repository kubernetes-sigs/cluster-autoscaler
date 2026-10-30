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

package besteffortatomic

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/observers/nodegroupchange"
)

// heldFailureObserver forwards node group change notifications from this class's private
// scale-up orchestrator to the shared observers. While a batch is being retried at smaller sizes
// it holds failed scale-ups back, so that a rejected resize doesn't put the node group into
// backoff before a smaller resize has been tried in the same iteration. Held failures that aren't
// forwarded still reach the metrics, so that every failed resize is counted once.
type heldFailureObserver struct {
	next    nodegroupchange.NodeGroupChangeObserver
	metrics nodegroupchange.NodeGroupChangeObserver

	mu       sync.Mutex
	holding  bool
	failures []heldFailure
	scaledUp map[string]bool
}

type heldFailure struct {
	nodeGroup   cloudprovider.NodeGroup
	delta       int
	errorInfo   cloudprovider.InstanceErrorInfo
	currentTime time.Time
}

// newHeldFailureObserver forwards notifications to next. Held failures that aren't forwarded are
// reported to metrics instead, which may be nil.
func newHeldFailureObserver(next, metrics nodegroupchange.NodeGroupChangeObserver) *heldFailureObserver {
	return &heldFailureObserver{next: next, metrics: metrics}
}

// hold starts holding failed scale-ups until release is called.
func (o *heldFailureObserver) hold() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.holding = true
	o.failures = nil
	o.scaledUp = map[string]bool{}
}

// release stops holding and reports the last held failure of every node group that wasn't
// scaled up afterwards. A node group that accepted a smaller resize is healthy and only lacked
// capacity for the larger one, so its failures don't back it off. Every held failure that isn't
// reported, including earlier failures of a reported node group, is only recorded in the metrics.
func (o *heldFailureObserver) release(ctx context.Context) {
	o.mu.Lock()
	failures, scaledUp := o.failures, o.scaledUp
	o.holding = false
	o.failures = nil
	o.scaledUp = nil
	o.mu.Unlock()

	last := map[string]int{}
	for i, failure := range failures {
		last[failure.nodeGroup.Id()] = i
	}
	var report []heldFailure
	for i, failure := range failures {
		id := failure.nodeGroup.Id()
		if last[id] == i {
			if !scaledUp[id] {
				report = append(report, failure)
				continue
			}
			klog.FromContext(ctx).V(2).Info("Not backing off node group because a smaller resize succeeded", "nodeGroup", id, "failedDelta", failure.delta)
		}
		if o.metrics != nil {
			o.metrics.RegisterFailedScaleUp(ctx, failure.nodeGroup, failure.delta, failure.errorInfo, failure.currentTime)
		}
	}

	slices.SortFunc(report, func(a, b heldFailure) int { return strings.Compare(a.nodeGroup.Id(), b.nodeGroup.Id()) })
	for _, failure := range report {
		o.next.RegisterFailedScaleUp(ctx, failure.nodeGroup, failure.delta, failure.errorInfo, failure.currentTime)
	}
}

// RegisterScaleUp forwards a successful scale-up and remembers it while failures are held.
func (o *heldFailureObserver) RegisterScaleUp(ctx context.Context, nodeGroup cloudprovider.NodeGroup, delta int, currentTime time.Time) {
	o.mu.Lock()
	if o.holding {
		o.scaledUp[nodeGroup.Id()] = true
	}
	o.mu.Unlock()
	o.next.RegisterScaleUp(ctx, nodeGroup, delta, currentTime)
}

// RegisterFailedScaleUp holds a failed scale-up while failures are held, and forwards it otherwise.
func (o *heldFailureObserver) RegisterFailedScaleUp(ctx context.Context, nodeGroup cloudprovider.NodeGroup, delta int, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) {
	o.mu.Lock()
	if o.holding {
		o.failures = append(o.failures, heldFailure{nodeGroup: nodeGroup, delta: delta, errorInfo: errorInfo, currentTime: currentTime})
		o.mu.Unlock()
		return
	}
	o.mu.Unlock()
	o.next.RegisterFailedScaleUp(ctx, nodeGroup, delta, errorInfo, currentTime)
}

// RegisterScaleDown forwards a scale-down.
func (o *heldFailureObserver) RegisterScaleDown(nodeGroup cloudprovider.NodeGroup, nodeName string, currentTime time.Time, expectedDeleteTime time.Time) {
	o.next.RegisterScaleDown(nodeGroup, nodeName, currentTime, expectedDeleteTime)
}

// RegisterFailedScaleDown forwards a failed scale-down.
func (o *heldFailureObserver) RegisterFailedScaleDown(nodeGroup cloudprovider.NodeGroup, reason string, currentTime time.Time) {
	o.next.RegisterFailedScaleDown(nodeGroup, reason, currentTime)
}
