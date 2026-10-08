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
	"sync"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupset"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
)

const (
	// maxPendingBatches bounds the memory used to follow batches until their nodes arrive. When
	// it's reached, the batch accepted longest ago is forgotten first.
	maxPendingBatches = 100
	// pendingBatchTTL stops following a batch whose nodes neither arrive nor fail. It doesn't follow
	// how long batch sizes are remembered, because it must outlast the maximum node provisioning
	// time, after which cluster state reports a scale-up that timed out.
	pendingBatchTTL = time.Hour
)

// upcomingNodes reports how many nodes node groups are still expected to add. It leaves out the
// upcoming nodes of backed-off node groups, so it also reports which node groups are backed off.
type upcomingNodes interface {
	GetUpcomingNodes(ctx context.Context) (upcomingCounts map[string]int, registeredNodeNames map[string][]string)
	BackoffStatusForNodeGroup(ctx context.Context, nodeGroup cloudprovider.NodeGroup, now time.Time) backoff.Status
}

// batchOutcomes follows best-effort-atomic batches whose resizes the cloud provider accepted until
// their nodes arrive. Cloud providers without atomic resizes accept a resize before they know
// whether they have the capacity, and only report a shortfall later, when the scale-up times out or
// instances fail to be created. When a node group resized for a batch reports such a failure, later
// iterations batch half as many pods of matching requests. For the same reason, a batch only lets
// the remembered batch size grow once its nodes have arrived.
//
// A failure can't be traced to the scale-up it belongs to, so it counts against every batch still
// waiting for nodes that the node group can no longer provide. Failures registered while the
// provisioning class provisions are its own, which it handles as they happen; they don't affect
// the batches waiting for nodes. Cluster state doesn't report the upcoming nodes of a backed-off
// node group, so batches waiting for its nodes are only confirmed once the backoff ends.
type batchOutcomes struct {
	limits   batchLimiter
	upcoming upcomingNodes
	clock    clock.PassiveClock

	mu      sync.Mutex
	busy    bool
	nextID  uint64
	pending []pendingBatch
}

type pendingBatch struct {
	id       uint64
	request  *provreqwrapper.ProvisioningRequest
	requests int
	pods     int
	resizes  []nodegroupset.ScaleUpInfo
	// grow reports whether the batch lets the remembered size grow once its nodes arrive.
	grow       bool
	acceptedAt time.Time
}

func newBatchOutcomes(limits batchLimiter, upcoming upcomingNodes, clock clock.PassiveClock) *batchOutcomes {
	return &batchOutcomes{limits: limits, upcoming: upcoming, clock: clock}
}

// track follows a batch of requests, represented by request, whose resizes the cloud provider
// accepted.
func (o *batchOutcomes) track(request *provreqwrapper.ProvisioningRequest, requests, pods int, resizes []nodegroupset.ScaleUpInfo, grow bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.pending) == maxPendingBatches {
		o.pending = slices.Delete(o.pending, 0, 1)
	}
	o.nextID++
	o.pending = append(o.pending, pendingBatch{
		id: o.nextID, request: request, requests: requests, pods: pods, resizes: resizes, grow: grow, acceptedAt: o.clock.Now(),
	})
}

// setBusy records whether the provisioning class is provisioning.
func (o *batchOutcomes) setBusy(busy bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.busy = busy
}

// settle stops following batches whose nodes arrived, letting those that may grow the remembered
// size do so, and forgets batches that were followed for too long.
func (o *batchOutcomes) settle(ctx context.Context) {
	o.mu.Lock()
	batches := slices.Clone(o.pending)
	o.mu.Unlock()
	if len(batches) == 0 {
		return
	}

	// Cluster state is queried without holding the lock, because it notifies observers, including
	// this one, while holding its own.
	upcoming, _ := o.upcoming.GetUpcomingNodes(ctx)
	now := o.clock.Now()
	// present holds how many nodes each node group has, or -1 if that isn't known.
	present := map[string]int{}
	arrived := func(resize nodegroupset.ScaleUpInfo) bool {
		id := resize.Group.Id()
		if _, found := present[id]; !found {
			present[id] = -1
			// A backed-off node group's upcoming nodes aren't reported, so they'd seem to exist.
			if !o.upcoming.BackoffStatusForNodeGroup(ctx, resize.Group, now).IsBackedOff {
				if targetSize, err := resize.Group.TargetSize(ctx); err == nil {
					present[id] = targetSize - upcoming[id]
				}
			}
		}
		// Nodes up to the size this batch requested exist, so its own nodes arrived.
		return present[id] >= resize.NewSize
	}
	finished := map[uint64]bool{}
	for _, batch := range batches {
		if !now.Before(batch.acceptedAt.Add(pendingBatchTTL)) {
			finished[batch.id] = false
		} else if !slices.ContainsFunc(batch.resizes, func(resize nodegroupset.ScaleUpInfo) bool { return !arrived(resize) }) {
			finished[batch.id] = true
		}
	}

	var grown []pendingBatch
	o.mu.Lock()
	// A batch that failed in the meantime is no longer pending, so it can't grow the size.
	o.pending = slices.DeleteFunc(o.pending, func(batch pendingBatch) bool {
		succeeded, settled := finished[batch.id]
		if succeeded && batch.grow {
			grown = append(grown, batch)
		}
		return settled
	})
	o.mu.Unlock()
	for _, batch := range grown {
		o.limits.RecordBestEffortAtomicBatchSuccess(ctx, batch.request, batch.requests, batch.pods)
	}
}

// RegisterFailedScaleUp reports that delta of the node group's nodes won't arrive. Batches that need
// the node group to grow beyond its target size minus delta can't receive all of their nodes any
// more: they're no longer followed, and those of more than one request halve the remembered size.
// Batches that need fewer nodes keep waiting for them. If the size the node group can reach isn't
// known, every batch waiting for its nodes counts as failed. The provisioning class handles the
// failures it registers itself as they happen, so they don't affect any batch.
func (o *batchOutcomes) RegisterFailedScaleUp(ctx context.Context, nodeGroup cloudprovider.NodeGroup, delta int, errorInfo cloudprovider.InstanceErrorInfo, _ time.Time) {
	o.mu.Lock()
	busy := o.busy
	o.mu.Unlock()
	if busy {
		return
	}
	reachable, known := 0, false
	if targetSize, err := nodeGroup.TargetSize(ctx); err == nil && delta > 0 {
		reachable, known = targetSize-delta, true
	}
	o.mu.Lock()
	var failed []pendingBatch
	o.pending = slices.DeleteFunc(o.pending, func(batch pendingBatch) bool {
		if !slices.ContainsFunc(batch.resizes, func(resize nodegroupset.ScaleUpInfo) bool {
			return resize.Group.Id() == nodeGroup.Id() && (!known || resize.NewSize > reachable)
		}) {
			return false
		}
		failed = append(failed, batch)
		return true
	})
	o.mu.Unlock()
	for _, batch := range failed {
		if batch.requests < 2 {
			continue
		}
		klog.FromContext(ctx).V(2).Info("A node group resized for a best-effort-atomic batch failed to add nodes after accepting the resize",
			"nodeGroup", nodeGroup.Id(), "provReq", klog.KObj(batch.request), "batchSize", batch.requests, "batchPods", batch.pods, "errorCode", errorInfo.ErrorCode)
		o.limits.RememberBestEffortAtomicBatchLimit(ctx, batch.request, (batch.pods+1)/2)
	}
}

// RegisterScaleUp is a no-op: batches are tracked when the provisioning class admits them.
func (o *batchOutcomes) RegisterScaleUp(context.Context, cloudprovider.NodeGroup, int, time.Time) {}

// RegisterScaleDown is a no-op.
func (o *batchOutcomes) RegisterScaleDown(cloudprovider.NodeGroup, string, time.Time, time.Time) {}

// RegisterFailedScaleDown is a no-op.
func (o *batchOutcomes) RegisterFailedScaleDown(cloudprovider.NodeGroup, string, time.Time) {}
