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
	"slices"
	"sync"
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/expiring"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

const (
	// pollInterval controls how frequently the watch cache is checked, balancing measurement
	// precision against cache read overhead.
	pollInterval = 50 * time.Millisecond
	// waitForTaintingTimeoutDuration bounds how long to wait for taints to appear after all
	// nodes have been tainted, so lost or delayed watch events do not block scale-down.
	waitForTaintingTimeoutDuration = 30 * time.Second
	pastLatencyExpireDuration      = time.Hour
)

type nodeTaintStartTime struct {
	nodeName  string
	startTime time.Time
}

// nodeDelayAfterTaintCalculator computes how much time CA should wait after tainting the nodes before attempting to drain and delete them.
// Waiting prevents a race where kube-scheduler places new pods onto a node before learning about its
// taint.
// If DynamicNodeDeleteDelayAfterTaintEnabled is true, nodeDelayAfterTaintCalculator keeps track of max taint propagation latency observed in the past hour.
type nodeDelayAfterTaintCalculator struct {
	staticNodeDeleteDelayAfterTaint time.Duration
	dynamicDelayEnabled             bool
	// pastLatenciesMu prevents a data race if two loops' background goroutines finish waitForLatency() around the same time.
	// Since ComputeDelay runs in a background go func(), a slow/timed-out ComputeDelay from loop N can still be in flight
	// when loop N + 1 starts another scale-down batch and calls ComputeDelay.
	pastLatenciesMu sync.Mutex
	pastLatencies   *expiring.List
}

// updateLatencyTracker estimates the end-to-end propagation latency of ToBeDeletedTaint from CA
// to kube-scheduler via the API server. Because CA cannot observe kube-scheduler's informer cache
// directly, it uses its own node informer cache (nodeLister) as a proxy and measures the time from
// when tainting starts for a node (passed to recordStartTime once the taint API call succeeds)
// until nodeLister returns the node with ToBeDeletedTaint; the API call duration alone would not
// cover watch delivery to informers.
// Used by `Actuator`.
type updateLatencyTracker struct {
	// Sends node tainting start timestamps to the tracker.
	startTimeChan      chan nodeTaintStartTime
	closeStartTimeChan sync.Once
	// droppedNodes holds nodes whose taints were rolled back and should not be awaited.
	droppedNodes []string
	// nodesInProgress records when each node's taint request began.
	nodesInProgress map[string]time.Time
	// maxLatency is the maximum observed taint propagation latency. Is returned from waitForLatency().
	maxLatency time.Duration
	// error returned from the waitForLatency().
	err error
	// nodeLister provides access to CA's local node watch cache to observe taint propagation
	// as a proxy for other cluster watchers.
	nodeLister kubernetes.NodeLister
	// done is closed when start() returns.
	done chan struct{}
}

// newNodeDelayAfterTaintCalculator initializes nodeDelayAfterTaintCalculator.
func newNodeDelayAfterTaintCalculator(staticNodeDeleteDelayAfterTaint time.Duration, dynamicNodeDeleteDelayAfterTaintEnabled bool) *nodeDelayAfterTaintCalculator {
	return &nodeDelayAfterTaintCalculator{
		staticNodeDeleteDelayAfterTaint: staticNodeDeleteDelayAfterTaint,
		dynamicDelayEnabled:             dynamicNodeDeleteDelayAfterTaintEnabled,
		pastLatencies:                   expiring.NewList(),
	}
}

// startLatencyTracker starts and returns an initialized latency tracker if dynamicNodeDeleteDelayAfterTaintEnabled is true.
// If dynamicNodeDeleteDelayAfterTaintEnabled is false, returns nil.
// Used by Actuator.
func (c *nodeDelayAfterTaintCalculator) startLatencyTracker(ctx context.Context, nodeLister kubernetes.NodeLister, expectedNodeCount int) *updateLatencyTracker {
	if !c.dynamicDelayEnabled {
		return nil
	}
	tracker := newUpdateLatencyTracker(nodeLister, expectedNodeCount)
	go tracker.start(ctx)
	return tracker
}

// computeDelay computes the time that Cluster Autoscaler needs to wait before draining and deleting tainted nodes.
//
// If DynamicNodeDeleteDelayAfterTaintEnabled is false, computeDelay returns the static NodeDeleteDelayAfterTaint.
// If DynamicNodeDeleteDelayAfterTaintEnabled is true, latencyTracker cannot be nil and computeDelay blocks until latency tracker observes taint
// propagation latency for all non-dropped nodes or times out, and returns 2 times the maximum latency observed during the last hour.
// If latency tracker returns zero as observed latency and pastLatencies are empty, returns static NodeDeleteDelayAfterTaint.
//
// After this method is called, latencyTracker.recordStartTime() cannot be called anymore.
// Used by Actuator.
func (c *nodeDelayAfterTaintCalculator) computeDelay(ctx context.Context, latencyTracker *updateLatencyTracker) time.Duration {
	if !c.dynamicDelayEnabled {
		return c.staticNodeDeleteDelayAfterTaint
	}
	latency, err := latencyTracker.waitForLatency()
	if err != nil {
		if latency == 0 {
			klog.FromContext(ctx).Error(err, "No node taint was observed by latency tracker")
		} else {
			klog.FromContext(ctx).Error(err, "Failed to measure taint latency, using max latency")
		}
	}
	c.pastLatenciesMu.Lock()
	defer c.pastLatenciesMu.Unlock()
	if latency > 0 {
		c.pastLatencies.RegisterElement(latency)
	}
	c.pastLatencies.DropNotNewerThan(time.Now().Add(-1 * pastLatencyExpireDuration))

	// CA is expected to wait 3 times the time from CA calling taintNode() to the time it takes for taints to propagate to the CA informer cache.
	// At this point, latency tracker has already observed taints for the nodes in the informer cache (or timed out).
	// Therefore, the nodeDeleteDelayAfterTaint is set 2 times the maximum latency observed during the last hour.
	nodeDeleteDelayAfterTaint := 2 * maxLatency(c.pastLatencies.ToSlice())
	if nodeDeleteDelayAfterTaint == 0 {
		klog.FromContext(ctx).Info("No observed node taint latency in the past hour, using static scaledown delay", "delay", c.staticNodeDeleteDelayAfterTaint)
		return c.staticNodeDeleteDelayAfterTaint
	}
	return nodeDeleteDelayAfterTaint
}

// newUpdateLatencyTracker creates a new updateLatencyTracker. expectedNodeCount is the upper bound on the
// number of recordStartTime calls that will be made: it sizes the channel the start times are sent
// on, so that recordStartTime never blocks.
func newUpdateLatencyTracker(nodeLister kubernetes.NodeLister, expectedNodeCount int) *updateLatencyTracker {
	return &updateLatencyTracker{
		nodesInProgress: make(map[string]time.Time, expectedNodeCount),
		done:            make(chan struct{}),
		startTimeChan:   make(chan nodeTaintStartTime, expectedNodeCount),
		nodeLister:      nodeLister,
	}
}

// start runs concurrently with the tainting workers so fast taint propagations are captured as
// soon as they reach the watch cache, without waiting for the rest of the batch to finish tainting.
func (u *updateLatencyTracker) start(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	defer close(u.done)

	for {
		select {
		case record, ok := <-u.startTimeChan:
			if !ok {
				awaitCtx, cancel := context.WithTimeout(ctx, waitForTaintingTimeoutDuration)
				defer cancel()
				u.err = u.await(awaitCtx, ticker)
				return
			}
			u.nodesInProgress[record.nodeName] = record.startTime
		case <-ctx.Done():
			u.err = ctx.Err()
			return
		case <-ticker.C:
			u.updateFinishTime(ctx)
		}
	}
}

// recordStartTime starts tracking taint propagation latency for a node.
// Can be called with a nil receiver.
// Used by Actuator.
func (u *updateLatencyTracker) recordStartTime(nodeName string, startTime time.Time) {
	if u == nil {
		return
	}
	u.startTimeChan <- nodeTaintStartTime{nodeName, startTime}
}

// stopWaitingForNodes stops accepting new nodes, removes the given nodes from tracking,
// and transitions the tracker to awaiting taint propagation for the remaining nodes.
// If no nodes remain in progress, the tracker finishes immediately.
// After stopWaitingForNodes is called, recordStartTime cannot be called anymore.
// Can be called with a nil receiver.
// Used by Actuator.
func (u *updateLatencyTracker) stopWaitingForNodes(nodeNames []string) {
	if u == nil {
		return
	}
	u.closeStartTimeChan.Do(func() {
		u.droppedNodes = slices.Clone(nodeNames)
		close(u.startTimeChan)
	})
}

// waitForLatency blocks until all non-dropped node taints are observed in the informer or a timeout elapses.
// After waitForLatency was called, recordStartTime cannot be called anymore.
// Returns maximum of the observed node taint propagation latency and error that prevented the tracker from processing all nodes.
// If a tracker times out before all node taints are propagated to the informer, the max latency observed before timeout is returned, together with a timeout error.
func (u *updateLatencyTracker) waitForLatency() (time.Duration, error) {
	u.stopWaitingForNodes(nil)
	<-u.done
	return u.maxLatency, u.err
}

func (u *updateLatencyTracker) await(ctx context.Context, ticker *time.Ticker) error {
	for _, nodeName := range u.droppedNodes {
		delete(u.nodesInProgress, nodeName)
	}
	// This is required in case recordStartTime + waitForLatency() happens faster than a single pollInterval in start() elapses.
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
func (u *updateLatencyTracker) updateFinishTime(ctx context.Context) {
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
