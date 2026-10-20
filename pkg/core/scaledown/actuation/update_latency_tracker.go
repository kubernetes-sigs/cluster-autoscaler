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
	"sync"
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/expiring"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

const (
	pollInterval                   = 50 * time.Millisecond
	waitForTaintingTimeoutDuration = 30 * time.Second
	pastLatencyExpireDuration      = time.Hour
)

type nodeTaintStartTime struct {
	nodeName  string
	startTime time.Time
}

// NodeDelayAfterTaintCalculator computes how much time CA should wait after tainting the nodes before attempting to drain and delete them.
// Waiting prevents a race where kube-scheduler places new pods onto a node before learning about its
// taint.
// If DynamicNodeDeleteDelayAfterTaintEnabled is true, NodeDelayAfterTaintCalculator keeps track of max taint propagation latency observed in the past hour.
type NodeDelayAfterTaintCalculator struct {
	staticNodeDeleteDelayAfterTaint time.Duration
	dynamicDelayEnabled             bool
	pastLatencies                   *expiring.List
}

// UpdateLatencyTracker estimates the end-to-end propagation latency of ToBeDeletedTaint from CA
// to kube-scheduler via the API server. Because CA cannot observe kube-scheduler's informer cache
// directly, it uses its own node informer cache (nodeLister) as a proxy and measures the time from
// when tainting starts for a node (passed to RecordStartTime once the taint API call succeeds)
// until nodeLister returns the node with ToBeDeletedTaint; the API call duration alone would not
// cover watch delivery to informers.
type UpdateLatencyTracker struct {
	// Closing the stopChan will abort the latency tracking and maxLatency calculation.
	stopChan      chan struct{}
	closeStopChan sync.Once
	// Sends node tainting start timestamps to the tracker
	startTimeChan      chan nodeTaintStartTime
	closeStartTimeChan sync.Once
	// nodesInProgress records when each node's taint request began
	nodesInProgress map[string]time.Time
	// maxLatency is the maximum observed taint propagation latency. Is returned from waitForLatency().
	maxLatency time.Duration
	// error returned from the waitForLatency().
	err error
	// nodeLister provides access to CA's local node watch cache to observe taint propagation
	// as a proxy for other cluster watchers.
	nodeLister kubernetes.NodeLister
	// pollInterval controls how frequently the watch cache is checked, balancing measurement
	// precision against cache read overhead (and allowing faster polling in tests).
	pollInterval time.Duration
	// waitForTaintingTimeoutDuration bounds how long to wait for taints to appear after all
	// nodes have been tainted, so lost or delayed watch events do not block scale-down.
	waitForTaintingTimeoutDuration time.Duration
	// done is closed when start() returns.
	done chan struct{}
}

// NewNodeDelayAfterTaintCalculator initializes NodeDelayAfterTaintCalculator.
func NewNodeDelayAfterTaintCalculator(staticNodeDeleteDelayAfterTaint time.Duration, dynamicNodeDeleteDelayAfterTaintEnabled bool) *NodeDelayAfterTaintCalculator {
	return &NodeDelayAfterTaintCalculator{
		staticNodeDeleteDelayAfterTaint: staticNodeDeleteDelayAfterTaint,
		dynamicDelayEnabled:             dynamicNodeDeleteDelayAfterTaintEnabled,
		pastLatencies:                   expiring.NewList(),
	}
}

// StartLatencyTracker starts and returns an initialized latency tracker if dynamicNodeDeleteDelayAfterTaintEnabled is true.
// If dynamicNodeDeleteDelayAfterTaintEnabled is false, returns nil.
func (c *NodeDelayAfterTaintCalculator) StartLatencyTracker(ctx context.Context, nodeLister kubernetes.NodeLister, expectedNodeCount int) *UpdateLatencyTracker {
	if !c.dynamicDelayEnabled {
		return nil
	}
	tracker := newUpdateLatencyTracker(nodeLister, expectedNodeCount)
	go tracker.start(ctx)
	return tracker
}

// ComputeDelay computes the time that Cluster Autoscaler needs to wait before draining and deleting tainted nodes.
//
// If DynamicNodeDeleteDelayAfterTaintEnabled is false, ComputeDelay returns the static NodeDeleteDelayAfterTaint.
// If DynamicNodeDeleteDelayAfterTaintEnabled is true, latencyTracker cannot be nil and ComputeDelay blocks until latency tracker observes taint
// propagation latency for all nodes or times out, and returns 2 times the maximum latency observed during the last hour.
// If latency tracker returns zero as observed latency and pastLatencies is empty, returns static NodeDeleteDelayAfterTaint.
//
// After this method is called, latencyTracker.RecordStartTime() cannot be called anymore.
// If latencyTracker.Abort() was called, ComputeDelay() shouldn't be called with this tracker as an argument.
func (c *NodeDelayAfterTaintCalculator) ComputeDelay(ctx context.Context, latencyTracker *UpdateLatencyTracker) time.Duration {
	if !c.dynamicDelayEnabled {
		return c.staticNodeDeleteDelayAfterTaint
	}
	latency, err := latencyTracker.waitForLatency()
	if err != nil {
		if latency == 0 {
			klog.FromContext(ctx).Error(err, "No node taint was observed by latency tracker")
		} else {
			klog.FromContext(ctx).Error(err, "Failed to measure taint latency for some nodes in the batch, using max latency from the available data")
		}
	}
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

// newUpdateLatencyTracker creates a new UpdateLatencyTracker. expectedNodeCount is the upper bound on the
// number of RecordStartTime calls that will be made: it sizes the channel the start times are sent
// on, so that RecordStartTime never blocks.
func newUpdateLatencyTracker(nodeLister kubernetes.NodeLister, expectedNodeCount int) *UpdateLatencyTracker {
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

// start runs concurrently with the tainting workers so fast taint propagations are captured as
// soon as they reach the watch cache, without waiting for the rest of the batch to finish tainting.
func (u *UpdateLatencyTracker) start(ctx context.Context) {
	ticker := time.NewTicker(u.pollInterval)
	defer ticker.Stop()
	defer close(u.done)

	for {
		select {
		case record, ok := <-u.startTimeChan:
			if !ok {
				awaitCtx, cancel := context.WithTimeout(ctx, u.waitForTaintingTimeoutDuration)
				defer cancel()
				u.err = u.await(awaitCtx, ticker)
				return
			}
			u.nodesInProgress[record.nodeName] = record.startTime
		case <-ctx.Done():
			u.err = ctx.Err()
			return
		case <-u.stopChan:
			u.err = errors.New("latency tracking aborted")
			return
		case <-ticker.C:
			u.updateFinishTime(ctx)
		}
	}
}

// RecordStartTime starts tracking taint propagation latency for a node.
// Can be called with a nil receiver.
func (u *UpdateLatencyTracker) RecordStartTime(nodeName string, startTime time.Time) {
	if u == nil {
		return
	}
	u.startTimeChan <- nodeTaintStartTime{nodeName, startTime}
}

// Abort is used for cancelling the tracker when ComputeDelay() will not be called.
// Can be called with a nil receiver.
func (u *UpdateLatencyTracker) Abort() {
	if u == nil {
		return
	}
	u.closeStopChan.Do(func() {
		close(u.stopChan)
	})
}

// waitForLatency blocks until all node taints are observed in the informer or a timeout elapses.
// After waitForLatency was called, RecordStartTime cannot be called anymore.
// Returns maximum of the observed node taint propagation latency and error that prevented the tracker from processing all nodes.
// If a tracker times out before all node taints are propagated to the informer, the max latency observed before timeout is returned, together with a timeout error.
func (u *UpdateLatencyTracker) waitForLatency() (time.Duration, error) {
	u.closeStartTimeChan.Do(func() {
		close(u.startTimeChan)
	})
	<-u.done
	return u.maxLatency, u.err
}

func (u *UpdateLatencyTracker) await(ctx context.Context, ticker *time.Ticker) error {
	// This is required in case RecordStartTime + waitForLatency() happens faster than a single u.pollInterval in start() elapses.
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
	updateLatencyTracker := newUpdateLatencyTracker(nodeLister, expectedNodeCount)
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
