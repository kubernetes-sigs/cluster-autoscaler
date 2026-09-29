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
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

const pollInterval = 50 * time.Millisecond
const waitForTaintingTimeoutDuration = 30 * time.Second

// UpdateLatencyTracker measures the time until the taint appears in CA's own node
// watch cache. Actuator uses this latency to estimate how long it takes for ToBeDeletedTaint to propagate to other
// watchers (e.g. kube-scheduler) and delay node drain and deletion until the
// scheduler is aware of the taints.
// The latency is measured from the time when RecordStartTime is called for a taint to when the nodeLister returns the node with a ToBeDeleted taint.
type UpdateLatencyTracker struct {
	// mu synchronizes state transitions across concurrent tainting workers, the background
	// polling goroutine, and the caller awaiting the result.
	mu sync.Mutex
	// notStarted tracks nodes queued for tainting so the polling loop does not exit or start
	// the timeout clock before all taint workers have begun.
	notStarted map[string]struct{}
	// started records when each node's taint request began so latency excludes worker queue
	// wait time and only nodes being tainted are polled.
	started map[string]time.Time
	// finished retains each node's propagation latency once its taint is observed so it is no
	// longer polled while waiting for remaining nodes.
	finished map[string]time.Duration
	// done signals that the background polling loop has stopped and the recorded latencies are
	// final.
	done chan struct{}

	// nodeLister provides access to CA's local node watch cache to observe taint propagation
	// as a proxy for other cluster watchers.
	nodeLister kubernetes.NodeLister
	// pollInterval controls how frequently the watch cache is checked, balancing measurement
	// precision against cache read overhead (and allowing faster polling in tests).
	pollInterval time.Duration
	// waitForTaintingTimeoutDuration bounds how long to wait for taints to appear after all
	// nodes have started tainting, so lost or delayed watch events do not block scale-down.
	waitForTaintingTimeoutDuration time.Duration
}

// NewUpdateLatencyTracker registers the full batch of nodes upfront so the concurrent polling loop
// knows not to exit or start its timeout until every node in the batch has either begun tainting
// or been dropped.
func NewUpdateLatencyTracker(nodeLister kubernetes.NodeLister, nodeNames []string) *UpdateLatencyTracker {
	notStarted := make(map[string]struct{}, len(nodeNames))
	for _, name := range nodeNames {
		notStarted[name] = struct{}{}
	}
	return &UpdateLatencyTracker{
		notStarted:                     notStarted,
		started:                        make(map[string]time.Time, len(nodeNames)),
		finished:                       make(map[string]time.Duration, len(nodeNames)),
		done:                           make(chan struct{}),
		nodeLister:                     nodeLister,
		pollInterval:                   pollInterval,
		waitForTaintingTimeoutDuration: waitForTaintingTimeoutDuration,
	}
}

// RecordStartTime marks the start of tainting for a node and should be called right before the API request is sent.
func (u *UpdateLatencyTracker) RecordStartTime(nodeName string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, ok := u.notStarted[nodeName]; ok {
		delete(u.notStarted, nodeName)
		u.started[nodeName] = time.Now()
	}
}

// DropNodes removes nodes whose taints failed or were rolled back so the tracker does not wait
// until timeout for taints that will never appear, and does not include rolled-back nodes in the result.
func (u *UpdateLatencyTracker) DropNodes(nodeNames ...string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, name := range nodeNames {
		delete(u.notStarted, name)
		delete(u.started, name)
		delete(u.finished, name)
	}
}

// Start runs concurrently with the tainting workers so fast taint propagations are captured as
// soon as they reach the watch cache, without waiting for the rest of the batch to finish tainting.
func (u *UpdateLatencyTracker) Start(ctx context.Context) {
	defer close(u.done)
	ticker := time.NewTicker(u.pollInterval)
	defer ticker.Stop()
	var allStartedTime time.Time
	for {
		u.mu.Lock()
		u.updateFinishTime(ctx)
		allStarted := len(u.notStarted) == 0
		allFinished := allStarted && len(u.started) == 0
		u.mu.Unlock()

		if allFinished {
			return
		}
		// Start the timeout clock only after every node's taint request has begun, so
		// worker pool queueing delay for large batches does not consume the timeout budget.
		if allStarted {
			if allStartedTime.IsZero() {
				allStartedTime = time.Now()
			} else if time.Since(allStartedTime) > u.waitForTaintingTimeoutDuration {
				return
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// updateFinishTime captures each node's propagation latency on the first poll where its taint is
// visible in the watch cache, removing it from started so subsequent ticks don't re-query it.
// Must be called with mu held.
func (u *UpdateLatencyTracker) updateFinishTime(ctx context.Context) {
	logger := klog.FromContext(ctx)
	for nodeName, startTime := range u.started {
		node, err := u.nodeLister.Get(nodeName)
		if err != nil {
			logger.Error(err, "Error getting node")
			continue
		}
		if taints.HasToBeDeletedTaint(node) {
			delete(u.started, nodeName)
			u.finished[nodeName] = time.Since(startTime)
		}
	}
}

// WaitForLatency returns the slowest taint propagation latency across the batch so the caller can
// delay node deletion long enough for all taints to reach the scheduler. If any tracked node was
// not observed (or none remained to measure), it returns an error so the caller falls back to the
// static delay instead of acting on an incomplete measurement.
func (u *UpdateLatencyTracker) WaitForLatency() (time.Duration, error) {
	u.mu.Lock()
	if n := len(u.notStarted); n > 0 {
		// Unblock Start, which would otherwise wait indefinitely for these nodes to start
		// and leak its goroutine until context cancellation.
		clear(u.notStarted)
		clear(u.started)
		u.mu.Unlock()
		return 0, fmt.Errorf("%d nodes were neither started nor dropped", n)
	}
	u.mu.Unlock()

	<-u.done

	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.started) > 0 {
		return 0, fmt.Errorf("taint wasn't observed for %d nodes", len(u.started))
	}
	if len(u.finished) == 0 {
		return 0, fmt.Errorf("no nodes to measure taint latency for")
	}
	var maxLatency time.Duration
	for _, latency := range u.finished {
		maxLatency = max(maxLatency, latency)
	}
	return maxLatency, nil
}

// NewUpdateLatencyTrackerForTesting configures shorter polling and timeout intervals so unit tests
// can exercise polling and timeout paths quickly.
func NewUpdateLatencyTrackerForTesting(nodeLister kubernetes.NodeLister, nodeNames []string) *UpdateLatencyTracker {
	updateLatencyTracker := NewUpdateLatencyTracker(nodeLister, nodeNames)
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
