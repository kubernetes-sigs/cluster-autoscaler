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
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

const sleepDurationWhenPolling = 50 * time.Millisecond

var waitForTaintingTimeoutDuration = 30 * time.Second

type nodeTaintStartTime struct {
	nodeName  string
	startTime time.Time
}

// UpdateLatencyTracker can be used to calculate round-trip time between CA and api-server
// when adding ToBeDeletedTaint to nodes
type UpdateLatencyTracker struct {
	startTimestamp     map[string]time.Time
	finishTimestamp    map[string]time.Time
	remainingNodeCount int
	nodeLister         kubernetes.NodeLister
	// Sends node tainting start timestamps to the tracker
	StartTimeChan            chan nodeTaintStartTime
	sleepDurationWhenPolling time.Duration
	// ExpectedNodeCountChan receives the capacity limit needed precisely to the count
	// of successfully tainted nodes. This instructs the tracker to discard start stamps
	// of any node that failed its API request to bypass the hang.
	ExpectedNodeCountChan chan int
	// Communicate back the measured latency
	ResultChan chan time.Duration
	// now is used only to make the testing easier
	now   func() time.Time
	sleep func(time.Duration)
}

// NewUpdateLatencyTracker returns a new NewUpdateLatencyTracker object
func NewUpdateLatencyTracker(nodeLister kubernetes.NodeLister, maxNodes int) *UpdateLatencyTracker {
	return &UpdateLatencyTracker{
		startTimestamp:           map[string]time.Time{},
		finishTimestamp:          map[string]time.Time{},
		remainingNodeCount:       0,
		nodeLister:               nodeLister,
		StartTimeChan:            make(chan nodeTaintStartTime, maxNodes),
		sleepDurationWhenPolling: sleepDurationWhenPolling,
		ExpectedNodeCountChan:    make(chan int, 1),
		ResultChan:               make(chan time.Duration),
		now:                      time.Now,
		sleep:                    time.Sleep,
	}
}

// Start starts listening for node tainting start timestamps and update the timestamps that
// the taint appears for the first time for a particular node. Listen ExpectedNodeCountChan for stop/await signals
func (u *UpdateLatencyTracker) Start(ctx context.Context) {
	defer close(u.ResultChan)
	for {
		select {
		case <-ctx.Done():
			return
		case expectedCount, ok := <-u.ExpectedNodeCountChan:
			if ok {
				u.drainStartTimeChan()
				u.remainingNodeCount = expectedCount - len(u.finishTimestamp)
				if u.remainingNodeCount < 0 {
					u.remainingNodeCount = 0
				}
				u.await(ctx)
			}
			return
		case ntst := <-u.StartTimeChan:
			u.startTimestamp[ntst.nodeName] = ntst.startTime
			u.remainingNodeCount += 1
			continue
		default:
			u.drainStartTimeChan()
		}
		u.updateFinishTime(ctx)
		u.sleep(u.sleepDurationWhenPolling)
	}
}

// drainStartTimeChan pulls all pending items out  of u.StartTimeChan.
// Returns immediately if u.StartTimeChan is empty.
func (u *UpdateLatencyTracker) drainStartTimeChan() {
	for {
		select {
		case ntst, ok := <-u.StartTimeChan:
			if !ok {
				return
			}
			u.startTimestamp[ntst.nodeName] = ntst.startTime
		default:
			// Channel is empty, safely exit without blocking.
			return
		}
	}
}

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
			u.remainingNodeCount -= 1
		}
	}
}

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

func (u *UpdateLatencyTracker) await(ctx context.Context) {
	logger := klog.FromContext(ctx)
	waitingForTaintingStartTime := u.now()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		switch {
		case u.remainingNodeCount <= 0:
			latency := u.calculateLatency()
			select {
			case u.ResultChan <- latency:
			case <-ctx.Done():
			}
			return
		case u.now().After(waitingForTaintingStartTime.Add(waitForTaintingTimeoutDuration)):
			logger.Error(nil, "Timeout before tainting all nodes, latency measurement will be stale")
			return
		default:
			u.sleep(u.sleepDurationWhenPolling)
			u.updateFinishTime(ctx)
		}
	}
}

// NewUpdateLatencyTrackerForTesting returns a UpdateLatencyTracker object with
// reduced sleepDurationWhenPolling and mock clock for testing
func NewUpdateLatencyTrackerForTesting(nodeLister kubernetes.NodeLister, maxNodes int, now func() time.Time) *UpdateLatencyTracker {
	updateLatencyTracker := NewUpdateLatencyTracker(nodeLister, maxNodes)
	updateLatencyTracker.now = now
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
