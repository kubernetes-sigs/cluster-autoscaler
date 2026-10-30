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
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestCustomNodeLister adds ToBeDeletedTaint to a node once nodeTaintAfterDuration[node] has
// elapsed since the lister was created. The tests run inside synctest bubbles, so time is fake
// and deterministic.
type TestCustomNodeLister struct {
	nodes                  map[string]*apiv1.Node
	nodeTaintAfterDuration map[string]time.Duration
	startTime              time.Time
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

// Get returns the node from the test lister, adding ToBeDeletedTaint once
// nodeTaintAfterDuration[name] has elapsed since the lister was created.
func (l *TestCustomNodeLister) Get(name string) (*apiv1.Node, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	node, ok := l.nodes[name]
	if !ok {
		return nil, fmt.Errorf("Node %s not found", name)
	}
	if expectedDuration, ok := l.nodeTaintAfterDuration[name]; ok {
		if time.Since(l.startTime) >= expectedDuration && !taints.HasToBeDeletedTaint(node) {
			toBeDeletedTaint := apiv1.Taint{Key: taints.ToBeDeletedTaint, Effect: apiv1.TaintEffectNoSchedule}
			node.Spec.Taints = append(node.Spec.Taints, toBeDeletedTaint)
		}
	}
	return node.DeepCopy(), nil
}

// NewTestCustomNodeLister returns a new TestCustomNodeLister. If a node has no entry in
// nodeTaintAfterDuration, it never gets tainted.
func NewTestCustomNodeLister(nodeNames []string, nodeTaintAfterDuration map[string]time.Duration) *TestCustomNodeLister {
	nodes := make(map[string]*apiv1.Node, len(nodeNames))
	for _, name := range nodeNames {
		nodes[name] = test.BuildTestNode(name, 100, 100)
	}
	return &TestCustomNodeLister{
		nodes:                  nodes,
		nodeTaintAfterDuration: nodeTaintAfterDuration,
		startTime:              time.Now(),
	}
}

func TestUpdateLatencyCalculation(t *testing.T) {
	testCases := []struct {
		description string
		nodes       []string
		// If an entry is not added for a node, that node will never get tainted
		nodeTaintAfterDuration map[string]time.Duration
		wantLatency            time.Duration
		wantErr                bool
	}{
		{
			description:            "latency when tainting a single node - node is tainted in the first call to the lister",
			nodes:                  []string{"n1"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 10 * time.Millisecond},
			wantLatency:            10 * time.Millisecond,
		},
		{
			description:            "latency when tainting a single node - node is not tainted in the first call to the lister",
			nodes:                  []string{"n1"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 100 * time.Millisecond},
			wantLatency:            100 * time.Millisecond,
		},
		{
			description:            "latency when tainting multiple nodes - nodes are tainted in the first calls to the lister",
			nodes:                  []string{"n1", "n2"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 10 * time.Millisecond, "n2": 10 * time.Millisecond},
			wantLatency:            10 * time.Millisecond,
		},
		{
			description:            "latency when tainting multiple nodes - nodes are not tainted in the first calls to the lister",
			nodes:                  []string{"n1", "n2"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 100 * time.Millisecond, "n2": 150 * time.Millisecond},
			wantLatency:            150 * time.Millisecond,
		},
		{
			description:            "Some nodes fail to taint before timeout",
			nodes:                  []string{"n1", "n3"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 100 * time.Millisecond, "n3": waitForTaintingTimeoutDuration + time.Second},
			wantLatency:            100 * time.Millisecond,
			wantErr:                true,
		},
		{
			description:            "All nodes fail to taint before timeout",
			nodes:                  []string{"n1", "n2"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": waitForTaintingTimeoutDuration + time.Second, "n2": waitForTaintingTimeoutDuration + 2*time.Second},
			wantLatency:            0,
			wantErr:                true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				nodeLister := NewTestCustomNodeLister(tc.nodes, tc.nodeTaintAfterDuration)
				tracker := newUpdateLatencyTracker(nodeLister, len(tc.nodes))
				startTime := time.Now()
				go tracker.start(t.Context())
				// Simulate taintNode RPC duration before recordStartTime is called.
				time.Sleep(10 * time.Millisecond)
				for _, name := range tc.nodes {
					tracker.recordStartTime(name, startTime)
				}
				latency, err := tracker.waitForLatency()
				if tc.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, tc.wantLatency, latency)

				// Calling waitForLatency a second time should return the same result without panicking.
				latency2, err2 := tracker.waitForLatency()
				assert.Equal(t, err, err2)
				assert.Equal(t, latency, latency2)
			})
		})
	}
}

func TestUpdateLatencyTrackerContextCancellation(t *testing.T) {
	t.Run("context cancelled while waiting for nodes to start", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			nodeLister := NewTestCustomNodeLister(nil, nil)
			tracker := newUpdateLatencyTracker(nodeLister, 1)

			go tracker.start(ctx)

			// Cancel ctx without ever starting or dropping "n1". start() must return.
			cancel()
			select {
			case <-tracker.done:
			case <-time.After(time.Second):
				t.Fatal("tracker did not finish after context cancellation")
			}
		})
	})

	t.Run("context cancelled while waiting for nodes to be tainted", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			// "n1" never gets tainted.
			nodeLister := NewTestCustomNodeLister([]string{"n1"}, nil)
			tracker := newUpdateLatencyTracker(nodeLister, 1)
			tracker.recordStartTime("n1", time.Now())

			go tracker.start(ctx)
			go func() {
				time.Sleep(50 * time.Millisecond)
				cancel()
			}()

			_, err := tracker.waitForLatency()
			assert.ErrorIs(t, err, context.Canceled)
		})
	})
}

func TestUpdateLatencyTrackerStopWaitingForNodes(t *testing.T) {
	t.Run("dropping all started nodes finishes immediately without waiting for taints", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// "n1" never gets tainted.
			nodeLister := NewTestCustomNodeLister([]string{"n1"}, nil)
			tracker := newUpdateLatencyTracker(nodeLister, 1)
			tracker.recordStartTime("n1", time.Now())

			go tracker.start(t.Context())
			tracker.stopWaitingForNodes([]string{"n1"})
			// Calling stopWaitingForNodes a second time should not panic.
			tracker.stopWaitingForNodes([]string{"n1"})

			select {
			case <-tracker.done:
			case <-time.After(time.Second):
				t.Fatal("tracker did not finish after dropping all nodes")
			}
			latency, err := tracker.waitForLatency()
			assert.NoError(t, err)
			assert.Equal(t, time.Duration(0), latency)
		})
	})

	t.Run("dropping while polling keeps already observed latencies and does not wait for dropped nodes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			nodes := []string{"n1", "n2", "n3"}
			// n1 propagates in 10ms, n2 propagates in 50ms (then gets dropped), n3 never propagates (and gets dropped).
			nodeLister := NewTestCustomNodeLister(nodes, map[string]time.Duration{
				"n1": 10 * time.Millisecond,
				"n2": 50 * time.Millisecond,
			})
			tracker := newUpdateLatencyTracker(nodeLister, len(nodes))
			startTime := time.Now()
			for _, name := range nodes {
				tracker.recordStartTime(name, startTime)
			}
			go tracker.start(t.Context())

			// Let the tracker poll long enough to observe both n1 (10ms) and n2 (50ms) before dropping n2 and n3.
			time.Sleep(80 * time.Millisecond)
			tracker.stopWaitingForNodes([]string{"n2", "n3"})

			latency, err := tracker.waitForLatency()
			assert.NoError(t, err)
			assert.Equal(t, 50*time.Millisecond, latency)
		})
	})
}

func TestUpdateLatencyTrackerNilReceiver(t *testing.T) {
	var tracker *updateLatencyTracker
	assert.NotPanics(t, func() {
		tracker.recordStartTime("n1", time.Now())
		tracker.stopWaitingForNodes([]string{"n1"})
	})
}

func TestUpdateLatencyTrackerTimeoutStartsWhenAllNodesStarted(t *testing.T) {
	// n2 starts tainting after waitForTaintingTimeoutDuration has already elapsed since n1
	// started. The timeout must only start once n2 has started, and n2's latency must be
	// measured from its own start time.
	synctest.Test(t, func(t *testing.T) {
		nodes := []string{"n1", "n2"}
		n2Start := waitForTaintingTimeoutDuration + time.Second
		nodeLister := NewTestCustomNodeLister(nodes, map[string]time.Duration{"n1": 0, "n2": n2Start + 100*time.Millisecond})
		tracker := newUpdateLatencyTracker(nodeLister, len(nodes))
		tracker.recordStartTime("n1", time.Now())
		go tracker.start(t.Context())

		time.Sleep(n2Start)
		tracker.recordStartTime("n2", time.Now())

		latency, err := tracker.waitForLatency()
		assert.NoError(t, err)
		// n1: seen on the first 50ms tick. n2: taint seen 100ms after its own start.
		assert.Equal(t, 100*time.Millisecond, latency)
	})
}

func TestNodeDelayAfterTaintCalculator(t *testing.T) {
	const staticDelay = 5 * time.Second

	type loop struct {
		waitBefore time.Duration
		nodes      []string
		// if a node from `nodes` has no entry in `nodeTaintAfterDuration`, waiting for the node's taint will time out.
		nodeTaintAfterDuration map[string]time.Duration
		dropAllWithoutCompute  bool
		wantDelay              time.Duration
	}

	testCases := []struct {
		name                string
		dynamicDelayEnabled bool
		loops               []loop
	}{
		{
			name:                "dynamic delay disabled returns static delay",
			dynamicDelayEnabled: false,
			loops: []loop{
				{
					nodes:                  []string{"n1"},
					nodeTaintAfterDuration: map[string]time.Duration{"n1": 50 * time.Millisecond},
					wantDelay:              staticDelay,
				},
			},
		},
		{
			name:                "multiple loops within 1 hour keep the maximum latency",
			dynamicDelayEnabled: true,
			loops: []loop{
				{
					nodes:                  []string{"n1", "n2"},
					nodeTaintAfterDuration: map[string]time.Duration{"n1": 50 * time.Millisecond, "n2": 150 * time.Millisecond},
					wantDelay:              300 * time.Millisecond,
				},
				{
					waitBefore:             10 * time.Minute,
					nodes:                  []string{"n3"},
					nodeTaintAfterDuration: map[string]time.Duration{"n3": 100 * time.Millisecond},
					wantDelay:              300 * time.Millisecond,
				},
				{
					waitBefore:             10 * time.Minute,
					nodes:                  []string{"n4"},
					nodeTaintAfterDuration: map[string]time.Duration{"n4": 200 * time.Millisecond},
					wantDelay:              400 * time.Millisecond,
				},
			},
		},
		{
			name:                "loop 2 runs more than 1 hour after loop 1 so old sample expires",
			dynamicDelayEnabled: true,
			loops: []loop{
				{
					nodes:                  []string{"n1"},
					nodeTaintAfterDuration: map[string]time.Duration{"n1": 150 * time.Millisecond},
					wantDelay:              300 * time.Millisecond,
				},
				{
					waitBefore:             pastLatencyExpireDuration + time.Minute,
					nodes:                  []string{"n2"},
					nodeTaintAfterDuration: map[string]time.Duration{"n2": 50 * time.Millisecond},
					wantDelay:              100 * time.Millisecond,
				},
			},
		},
		{
			name:                "loop where all nodes are dropped without calling ComputeDelay does not affect subsequent loops",
			dynamicDelayEnabled: true,
			loops: []loop{
				{
					nodes:                  []string{"n1"},
					nodeTaintAfterDuration: map[string]time.Duration{"n1": 100 * time.Millisecond},
					wantDelay:              200 * time.Millisecond,
				},
				{
					// n2 would take 200ms (raising delay to 400ms), but is dropped without calling computeDelay.
					nodes:                  []string{"n2"},
					nodeTaintAfterDuration: map[string]time.Duration{"n2": 200 * time.Millisecond},
					dropAllWithoutCompute:  true,
				},
				{
					waitBefore:             time.Second,
					nodes:                  []string{"n3"},
					nodeTaintAfterDuration: map[string]time.Duration{"n3": 50 * time.Millisecond},
					wantDelay:              200 * time.Millisecond,
				},
			},
		},
		{
			name:                "partial timeout in a loop uses the available latency instead of static delay",
			dynamicDelayEnabled: true,
			loops: []loop{
				{
					// n1 propagates in 100ms, n2 never propagates and times out.
					nodes:                  []string{"n1", "n2"},
					nodeTaintAfterDuration: map[string]time.Duration{"n1": 100 * time.Millisecond},
					wantDelay:              200 * time.Millisecond,
				},
			},
		},
		{
			name:                "full timeout with no prior latency in the past hour falls back to static delay",
			dynamicDelayEnabled: true,
			loops: []loop{
				{
					nodes:     []string{"n1"}, // times out
					wantDelay: staticDelay,
				},
			},
		},
		{
			name:                "full timeout reuses prior latency within 1 hour then falls back to static delay after expiry",
			dynamicDelayEnabled: true,
			loops: []loop{
				{
					nodes:                  []string{"n1"},
					nodeTaintAfterDuration: map[string]time.Duration{"n1": 150 * time.Millisecond},
					wantDelay:              300 * time.Millisecond,
				},
				{
					waitBefore: 10 * time.Minute,
					nodes:      []string{"n2"}, // times out
					wantDelay:  300 * time.Millisecond,
				},
				{
					waitBefore: 51 * time.Minute,
					nodes:      []string{"n3"}, // times out
					wantDelay:  staticDelay,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				delayCalculator := newNodeDelayAfterTaintCalculator(staticDelay, tc.dynamicDelayEnabled)

				for i, l := range tc.loops {
					if l.waitBefore > 0 {
						time.Sleep(l.waitBefore)
					}
					nodeLister := NewTestCustomNodeLister(l.nodes, l.nodeTaintAfterDuration)
					tracker := delayCalculator.startLatencyTracker(t.Context(), nodeLister, len(l.nodes))
					startTime := time.Now()
					for _, name := range l.nodes {
						tracker.recordStartTime(name, startTime)
					}
					if l.dropAllWithoutCompute {
						tracker.stopWaitingForNodes(l.nodes)
						select {
						case <-tracker.done:
						case <-time.After(time.Second):
							t.Fatalf("loop %d: tracker did not finish after stopWaitingForNodes", i)
						}
						continue
					}
					gotDelay := delayCalculator.computeDelay(t.Context(), tracker)
					assert.Equal(t, l.wantDelay, gotDelay, "loop %d delay", i)
				}
			})
		})
	}
}
