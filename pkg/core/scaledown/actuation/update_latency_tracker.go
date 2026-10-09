/*
Copyright 2022 The Kubernetes Authors.

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

package actuation

import (
	"context"
	"maps"
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

const sleepDurationWhenPolling = 50 * time.Millisecond
const waitForTaintingTimeoutDuration = 30 * time.Second

// UpdateLatencyTracker measures the time from CA starting to taint a node with ToBeDeletedTaint
// until the taint shows up in CA's node watch cache (nodeLister), as an estimate of how long other
// watchers (e.g. kube-scheduler) take to see the taint. The start time is recorded before the node
// is queued for tainting, so the measured latency also includes time spent waiting for a free
// tainting worker and in client-side rate limiting.
type UpdateLatencyTracker struct {
	startTimestamp                 map[string]time.Time
	finishTimestamp                map[string]time.Time
	nodeLister                     kubernetes.NodeLister
	sleepDurationWhenPolling       time.Duration
	waitForTaintingTimeoutDuration time.Duration
	// ExpectedNodeCountChan receives the exact count of successfully tainted nodes
	// to wait for before completing latency calculation. Closing ExpectedNodeCountChan
	// (or passing <= 0) aborts latency calculation and closes ResultChan without a value.
	ExpectedNodeCountChan chan int
	// Communicate back the measured latency
	ResultChan chan time.Duration
	// now and sleep are used only to make the testing easier
	now   func() time.Time
	sleep func(time.Duration)
}

// NewUpdateLatencyTracker returns a new UpdateLatencyTracker for the nodes in startTimes, which
// maps node names to the time CA started tainting them.
func NewUpdateLatencyTracker(nodeLister kubernetes.NodeLister, startTimes map[string]time.Time) *UpdateLatencyTracker {
	return &UpdateLatencyTracker{
		startTimestamp:                 maps.Clone(startTimes),
		finishTimestamp:                map[string]time.Time{},
		nodeLister:                     nodeLister,
		sleepDurationWhenPolling:       sleepDurationWhenPolling,
		waitForTaintingTimeoutDuration: waitForTaintingTimeoutDuration,
		ExpectedNodeCountChan:          make(chan int, 1),
		ResultChan:                     make(chan time.Duration),
		now:                            time.Now,
		sleep:                          time.Sleep,
	}
}

// Start polls the nodeLister and records when the taint first appears for each tracked node.
// It listens on ExpectedNodeCountChan to transition to awaiting the remaining nodes or to abort
// latency calculation.
func (u *UpdateLatencyTracker) Start(ctx context.Context) {
	defer close(u.ResultChan)
	for {
		select {
		case <-ctx.Done():
			return
		case expectedCount, ok := <-u.ExpectedNodeCountChan:
			if ok && expectedCount > 0 {
				u.updateFinishTime(ctx)
				u.await(ctx, expectedCount)
			}
			return
		default:
		}
		u.updateFinishTime(ctx)
		u.sleep(u.sleepDurationWhenPolling)
	}
}

// updateFinishTime checks the nodeLister for each tracked node that has not yet been observed
// with ToBeDeletedTaint, and records its finish timestamp when the taint first appears.
func (u *UpdateLatencyTracker) updateFinishTime(ctx context.Context) {
	logger := klog.FromContext(ctx)
	for nodeName := range u.startTimestamp {
		if _, ok := u.finishTimestamp[nodeName]; ok {
			continue
		}
		node, err := u.nodeLister.Get(nodeName)
		if err != nil {
			logger.Error(err, "Error getting node")
			continue
		}
		if taints.HasToBeDeletedTaint(node) {
			u.finishTimestamp[node.Name] = u.now()
		}
	}
}

// calculateLatency returns the maximum duration between startTimestamp and finishTimestamp
// across all nodes that have been observed with ToBeDeletedTaint.
func (u *UpdateLatencyTracker) calculateLatency() time.Duration {
	var maxLatency time.Duration = 0
	for node, startTime := range u.startTimestamp {
		endTime, ok := u.finishTimestamp[node]
		if !ok {
			continue
		}
		currentLatency := endTime.Sub(startTime)
		if currentLatency > maxLatency {
			maxLatency = currentLatency
		}
	}
	return maxLatency
}

// await polls until expectedNodeCount nodes have been observed with ToBeDeletedTaint (sending the
// measured latency to ResultChan), waitForTaintingTimeoutDuration elapses, or ctx is cancelled.
func (u *UpdateLatencyTracker) await(ctx context.Context, expectedNodeCount int) {
	logger := klog.FromContext(ctx)
	waitingForTaintingStartTime := u.now()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		switch {
		case len(u.finishTimestamp) >= expectedNodeCount:
			latency := u.calculateLatency()
			select {
			case u.ResultChan <- latency:
			case <-ctx.Done():
			}
			return
		case u.now().After(waitingForTaintingStartTime.Add(u.waitForTaintingTimeoutDuration)):
			logger.Error(nil, "Timeout before tainting all nodes, latency measurement will be stale")
			return
		default:
			u.sleep(u.sleepDurationWhenPolling)
			u.updateFinishTime(ctx)
		}
	}
}

// NewUpdateLatencyTrackerForTesting returns an UpdateLatencyTracker object with
// reduced sleepDurationWhenPolling and mock clock for testing.
func NewUpdateLatencyTrackerForTesting(nodeLister kubernetes.NodeLister, startTimes map[string]time.Time, now func() time.Time) *UpdateLatencyTracker {
	updateLatencyTracker := NewUpdateLatencyTracker(nodeLister, startTimes)
	updateLatencyTracker.now = now
	updateLatencyTracker.waitForTaintingTimeoutDuration = 200 * time.Millisecond
	updateLatencyTracker.sleepDurationWhenPolling = time.Millisecond
	return updateLatencyTracker
}

func maxLatency(latencies []interface{}) time.Duration {
	var currentMax time.Duration = 0
	for _, l := range latencies {
		latency := l.(time.Duration)
		if latency > currentMax {
			currentMax = latency
		}
	}
	return currentMax
}
