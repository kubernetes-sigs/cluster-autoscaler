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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type mockClock struct {
	startTime   time.Time
	currentTime time.Time
	mutex       sync.Mutex
}

func NewMockClock(startTime time.Time) *mockClock {
	return &mockClock{
		startTime:   startTime,
		currentTime: startTime,
	}
}

func (m *mockClock) Now() time.Time {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.currentTime
}

func (m *mockClock) SetTime(t time.Time) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.currentTime = t
}

type TestCustomNodeLister struct {
	nodes                  map[string]*apiv1.Node
	nodeTaintAfterDuration map[string]time.Duration
	clock                  *mockClock
	mutex                  sync.Mutex
}

// List returns all nodes in test lister.
func (l *TestCustomNodeLister) List() ([]*apiv1.Node, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var nodes []*apiv1.Node
	for _, node := range l.nodes {
		nodes = append(nodes, node.DeepCopy())
	}
	return nodes, nil
}

func (l *TestCustomNodeLister) Get(name string) (*apiv1.Node, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	for _, node := range l.nodes {
		if node.Name == name {
			if expectedDuration, ok := l.nodeTaintAfterDuration[node.Name]; ok {
				if l.clock.Now().Sub(l.clock.startTime) >= expectedDuration && !taints.HasToBeDeletedTaint(node) {
					toBeDeletedTaint := apiv1.Taint{Key: taints.ToBeDeletedTaint, Effect: apiv1.TaintEffectNoSchedule}
					node.Spec.Taints = append(node.Spec.Taints, toBeDeletedTaint)
				}
			}
			return node.DeepCopy(), nil
		}
	}
	return nil, fmt.Errorf("Node %s not found", name)
}

// Return new TestCustomNodeLister object
func NewTestCustomNodeLister(nodes map[string]*apiv1.Node, nodeTaintAfterDuration map[string]time.Duration, clock *mockClock) *TestCustomNodeLister {
	return &TestCustomNodeLister{
		nodes:                  nodes,
		nodeTaintAfterDuration: nodeTaintAfterDuration,
		clock:                  clock,
	}
}

func TestUpdateLatencyCalculation(t *testing.T) {
	testCases := []struct {
		description string
		startTime   time.Time
		nodes       []string
		// If an entry is not added for a node, that node will never get tainted
		nodeTaintAfterDuration map[string]time.Duration
		expectedCountOverride  *int
		closeExpectedCountChan bool
		wantLatency            time.Duration
		wantResultChanOpen     bool
	}{
		{
			description:            "latency when tainting a single node - node is tainted in the first call to the lister",
			startTime:              time.Now(),
			nodes:                  []string{"n1"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 0},
			wantLatency:            0,
			wantResultChanOpen:     true,
		},
		{
			description:            "latency when tainting a single node - node is not tainted in the first call to the lister",
			startTime:              time.Now(),
			nodes:                  []string{"n1"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 100 * time.Millisecond},
			wantLatency:            100 * time.Millisecond,
			wantResultChanOpen:     true,
		},
		{
			description:            "latency when tainting multiple nodes - nodes are tainted in the first calls to the lister",
			startTime:              time.Now(),
			nodes:                  []string{"n1", "n2"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 0, "n2": 0},
			wantLatency:            0,
			wantResultChanOpen:     true,
		},
		{
			description:            "latency when tainting multiple nodes - nodes are not tainted in the first calls to the lister",
			startTime:              time.Now(),
			nodes:                  []string{"n1", "n2"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 100 * time.Millisecond, "n2": 150 * time.Millisecond},
			wantLatency:            150 * time.Millisecond,
			wantResultChanOpen:     true,
		},
		{
			description:            "partial taint actuation: expected count is less than total started nodes",
			startTime:              time.Now(),
			nodes:                  []string{"n1", "n2", "n3"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 50 * time.Millisecond, "n2": 100 * time.Millisecond},
			expectedCountOverride:  func() *int { v := 2; return &v }(),
			wantLatency:            100 * time.Millisecond,
			wantResultChanOpen:     true,
		},
		{
			description:            "more nodes tainted than the expected count",
			startTime:              time.Now(),
			nodes:                  []string{"n1", "n2", "n3"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 0, "n2": 0, "n3": 0},
			expectedCountOverride:  func() *int { v := 2; return &v }(),
			wantLatency:            0,
			wantResultChanOpen:     true,
		},
		{
			description:            "Some nodes fails to taint before timeout",
			startTime:              time.Now(),
			nodes:                  []string{"n1", "n3"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 100 * time.Millisecond, "n3": 250 * time.Millisecond},
			wantResultChanOpen:     false,
		},
		{
			description:            "ExpectedNodeCountChan closed resulting in ResultChan closure",
			startTime:              time.Now(),
			nodes:                  []string{"n1"},
			nodeTaintAfterDuration: map[string]time.Duration{},
			closeExpectedCountChan: true,
			wantResultChanOpen:     false,
		},
		{
			description:            "Expected count is zero resulting in ResultChan closure",
			startTime:              time.Now(),
			nodes:                  []string{"n1"},
			nodeTaintAfterDuration: map[string]time.Duration{},
			expectedCountOverride:  func() *int { v := 0; return &v }(),
			wantResultChanOpen:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			ctx := t.Context()

			mc := NewMockClock(tc.startTime)
			nodes := map[string]*apiv1.Node{}
			startTimes := map[string]time.Time{}
			for _, name := range tc.nodes {
				node := test.BuildTestNode(name, 100, 100)
				nodes[name] = node
				startTimes[name] = tc.startTime
			}
			nodeLister := NewTestCustomNodeLister(nodes, tc.nodeTaintAfterDuration, mc)
			updateLatencyTracker := NewUpdateLatencyTrackerForTesting(nodeLister, startTimes, mc.Now)

			// Synthetically advance mock clock upon each sleep.
			updateLatencyTracker.sleep = func(d time.Duration) {
				mc.SetTime(mc.Now().Add(d))
			}

			// Send the count before starting the tracker, so that await() always starts at the
			// mock clock's start time and the timeout cases are deterministic.
			if tc.closeExpectedCountChan {
				close(updateLatencyTracker.ExpectedNodeCountChan)
			} else if tc.expectedCountOverride != nil {
				updateLatencyTracker.ExpectedNodeCountChan <- *tc.expectedCountOverride
			} else {
				updateLatencyTracker.ExpectedNodeCountChan <- len(tc.nodes)
			}
			go updateLatencyTracker.Start(ctx)

			latency, ok := <-updateLatencyTracker.ResultChan
			assert.Equal(t, tc.wantResultChanOpen, ok)
			if ok {
				assert.Equal(t, tc.wantLatency, latency)
			}
		})
	}
}

func TestUpdateLatencyTrackerContextCancellation(t *testing.T) {
	t.Run("context cancelled while Start is waiting for ExpectedNodeCountChan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		mc := NewMockClock(time.Now())
		nodeLister := NewTestCustomNodeLister(nil, nil, mc)
		tracker := NewUpdateLatencyTrackerForTesting(nodeLister, nil, mc.Now)

		go tracker.Start(ctx)

		// Cancel ctx without ever writing to or closing ExpectedNodeCountChan.
		// Start() must exit via `case <-ctx.Done():` and close ResultChan.
		cancel()

		_, ok := <-tracker.ResultChan
		assert.False(t, ok)
	})

	t.Run("context cancelled while await is waiting for nodes to be tainted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		mc := NewMockClock(time.Now())
		nodes := map[string]*apiv1.Node{
			"n1": test.BuildTestNode("n1", 100, 100),
		}
		// Empty nodeTaintAfterDuration means "n1" will never get tainted, and mc is never
		// advanced so waitForTaintingTimeoutDuration will never fire.
		nodeLister := NewTestCustomNodeLister(nodes, map[string]time.Duration{}, mc)
		tracker := NewUpdateLatencyTrackerForTesting(nodeLister, map[string]time.Time{"n1": mc.Now()}, mc.Now)

		tracker.ExpectedNodeCountChan <- 1

		go tracker.Start(ctx)

		// Wait until Start() has consumed ExpectedNodeCountChan and entered await(),
		// then cancel ctx so await() must exit via `case <-ctx.Done():`.
		for len(tracker.ExpectedNodeCountChan) > 0 {
			time.Sleep(time.Millisecond)
		}
		cancel()

		_, ok := <-tracker.ResultChan
		assert.False(t, ok)
	})
}

func TestUpdateLatencyTrackerNodesTaintedBeforeExpectedCount(t *testing.T) {
	// In production the tracker polls while CA is still tainting nodes, so most taints are seen
	// before the expected count arrives. The finish times recorded then must be kept.
	start := time.Now()
	mc := NewMockClock(start)
	nodes := map[string]*apiv1.Node{
		"n1": test.BuildTestNode("n1", 100, 100),
		"n2": test.BuildTestNode("n2", 100, 100),
	}
	nodeLister := NewTestCustomNodeLister(nodes, map[string]time.Duration{"n1": 10 * time.Millisecond, "n2": 30 * time.Millisecond}, mc)
	tracker := NewUpdateLatencyTrackerForTesting(nodeLister, map[string]time.Time{"n1": start, "n2": start}, mc.Now)

	// sleep runs on the tracker goroutine, so the count arrives at a fixed point of the polling
	// loop: at 100ms, well after both taints were seen at 10ms and 30ms.
	tracker.sleep = func(d time.Duration) {
		if mc.Now().Sub(start) == 100*time.Millisecond {
			tracker.ExpectedNodeCountChan <- 2
		}
		mc.SetTime(mc.Now().Add(d))
	}
	go tracker.Start(t.Context())

	latency, ok := <-tracker.ResultChan
	assert.True(t, ok)
	// n2's taint was first seen at 30ms. Recording the finish times only after the count
	// arrived would give more than 100ms.
	assert.Equal(t, 30*time.Millisecond, latency)
}
