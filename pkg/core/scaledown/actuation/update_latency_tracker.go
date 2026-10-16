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
	"errors"
	"fmt"
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

const pollInterval = 50 * time.Millisecond
const waitForTaintingTimeoutDuration = 30 * time.Second

type nodeTaintStartTime struct {
	nodeName  string
	startTime time.Time
}

// UpdateLatencyTracker estimates the end-to-end propagation latency of ToBeDeletedTaint from CA
// to kube-scheduler via the API server. Because CA cannot observe kube-scheduler's informer cache
// directly, it uses its own node informer cache (nodeLister) as a proxy and measures the time from
// when tainting starts for a node (passed to RecordStartTime once the taint API call succeeds)
// until nodeLister returns the node with ToBeDeletedTaint; the API call duration alone would not
// cover watch delivery to informers.
//
// This prevents a race where kube-scheduler places new pods onto a node before learning about its
// taint. With DynamicNodeDeleteDelayAfterTaintEnabled, Actuator waits nodeDeleteDelayAfterTaint
// (3x the measured propagation latency in total, i.e. 2x after tainting completes) and re-checks
// that the node is still safe to delete, avoiding a large static delay that would reduce scale-down
// throughput.
type UpdateLatencyTracker struct {
	// Closing the stopChan will abort the latency tracking and maxLatency calculation.
	stopChan chan struct{}
	// Sends node tainting start timestamps to the tracker
	startTimeChan chan nodeTaintStartTime
	// nodesInProgress records when each node's taint request began so latency excludes worker queue
	// wait time and only successfully tainted nodes are polled.
	nodesInProgress map[string]time.Time
	// maxLatency is the maximum observed taint propagation latency. Is returned from WaitForLatency().
	maxLatency time.Duration
	// error returned from the WaitForLatency().
	latencyTrackingErr error
	// nodeLister provides access to CA's local node watch cache to observe taint propagation
	// as a proxy for other cluster watchers.
	nodeLister kubernetes.NodeLister
	// pollInterval controls how frequently the watch cache is checked, balancing measurement
	// precision against cache read overhead (and allowing faster polling in tests).
	pollInterval time.Duration
	// waitForTaintingTimeoutDuration bounds how long to wait for taints to appear after all
	// nodes have been tainted, so lost or delayed watch events do not block scale-down.
	waitForTaintingTimeoutDuration time.Duration
	// done is closed when Start() returns.
	done chan struct{}
}

// NewUpdateLatencyTracker creates a new UpdateLatencyTracker. expectedNodeCount must be at least the
// number of RecordStartTime calls that will be made: it sizes the channel the start times are sent
// on, so that RecordStartTime never blocks.
func NewUpdateLatencyTracker(nodeLister kubernetes.NodeLister, expectedNodeCount int) *UpdateLatencyTracker {
	return &UpdateLatencyTracker{
		nodesInProgress:                make(map[string]time.Time, expectedNodeCount),
		done:                           make(chan struct{}),
		startTimeChan:                  make(chan nodeTaintStartTime, expectedNodeCount),
		stopChan:                       make(chan struct{}),
		nodeLister:                     nodeLister,
		pollInterval:                   pollInterval,
		waitForTaintingTimeoutDuration: waitForTaintingTimeoutDuration,
	}
}

// Start runs concurrently with the tainting workers so fast taint propagations are captured as
// soon as they reach the watch cache, without waiting for the rest of the batch to finish tainting.
func (u *UpdateLatencyTracker) Start(ctx context.Context) {
	ticker := time.NewTicker(u.pollInterval)
	defer ticker.Stop()
	defer close(u.done)

	for {
		select {
		case record, ok := <-u.startTimeChan:
			if !ok {
				awaitCtx, awaitCancel := context.WithTimeout(ctx, u.waitForTaintingTimeoutDuration)
				u.latencyTrackingErr = u.await(awaitCtx, ticker)
				awaitCancel()
				return
			}
			u.nodesInProgress[record.nodeName] = record.startTime
		case <-ctx.Done():
			u.latencyTrackingErr = ctx.Err()
			return
		case <-u.stopChan:
			u.latencyTrackingErr = errors.New("latency tracking aborted")
			return
		case <-ticker.C:
			u.updateFinishTime(ctx)
		}
	}
}

// RecordStartTime starts tracking taint propagation latency for a node.
// Can be called with a nil receiver.
// Panics if called after WaitForLatency().
func (u *UpdateLatencyTracker) RecordStartTime(nodeName string, startTime time.Time) {
	if u == nil {
		return
	}
	u.startTimeChan <- nodeTaintStartTime{nodeName, startTime}
}

// Abort stops tracking all nodes.
// Can be called with a nil receiver.
// Panics if called more than once.
func (u *UpdateLatencyTracker) Abort() {
	if u == nil {
		return
	}
	close(u.stopChan)
}

// WaitForLatency blocks until all node taints are observed in the informer or a timeout elapses.
// Panics if called more than once. After WaitForLatency was called, RecordStartTime cannot be called anymore.
func (u *UpdateLatencyTracker) WaitForLatency() (time.Duration, error) {
	close(u.startTimeChan)
	<-u.done
	return u.maxLatency, u.latencyTrackingErr
}

func (u *UpdateLatencyTracker) await(ctx context.Context, ticker *time.Ticker) error {
	// This is required in case RecordStartTime + WaitForLatency() happens faster than a single u.pollInterval in Start() elapses.
	u.updateFinishTime(ctx)

	for {
		if len(u.nodesInProgress) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %d node taints to propagate: %w", len(u.nodesInProgress), ctx.Err())
		case <-ticker.C:
			u.updateFinishTime(ctx)
		}
	}
}

// updateFinishTime captures each node's propagation latency on the first poll where its taint is
// visible in the watch cache, removing it from nodesInProgress so subsequent ticks don't re-query it.
func (u *UpdateLatencyTracker) updateFinishTime(ctx context.Context) {
	logger := klog.FromContext(ctx)
	for nodeName, startTime := range u.nodesInProgress {
		node, err := u.nodeLister.Get(nodeName)
		if err != nil {
			logger.Error(err, "Error getting node from nodeLister", "nodeName", nodeName)
			continue
		}
		if !taints.HasToBeDeletedTaint(node) {
			continue
		}
		delete(u.nodesInProgress, nodeName)
		latency := time.Since(startTime)
		if latency > u.maxLatency {
			u.maxLatency = latency
		}
	}
}

// NewUpdateLatencyTrackerForTesting configures shorter polling and timeout intervals so unit tests
// can exercise polling and timeout paths quickly.
func NewUpdateLatencyTrackerForTesting(nodeLister kubernetes.NodeLister, expectedNodeCount int) *UpdateLatencyTracker {
	updateLatencyTracker := NewUpdateLatencyTracker(nodeLister, expectedNodeCount)
	updateLatencyTracker.waitForTaintingTimeoutDuration = 200 * time.Millisecond
	updateLatencyTracker.pollInterval = time.Millisecond
	return updateLatencyTracker
}

// maxLatency extracts the highest latency from an untyped expiring.List slice to determine the
// worst-case propagation delay over the lookback window.
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
