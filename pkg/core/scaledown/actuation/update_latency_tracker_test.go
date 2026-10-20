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
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 0},
			wantLatency:            0,
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
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 0, "n2": 0},
			wantLatency:            0,
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
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 100 * time.Millisecond, "n3": 250 * time.Millisecond},
			wantLatency:            100 * time.Millisecond,
			wantErr:                true,
		},
		{
			description:            "All nodes fail to taint before timeout",
			nodes:                  []string{"n1", "n2"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 250 * time.Millisecond, "n2": 300 * time.Millisecond},
			wantLatency:            0,
			wantErr:                true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				nodeLister := NewTestCustomNodeLister(tc.nodes, tc.nodeTaintAfterDuration)
				tracker := NewUpdateLatencyTrackerForTesting(nodeLister, len(tc.nodes))
				startTime := time.Now()
				go tracker.start(t.Context())
				for _, name := range tc.nodes {
					tracker.RecordStartTime(name, startTime)
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
			tracker := NewUpdateLatencyTrackerForTesting(nodeLister, 1)

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
			tracker := NewUpdateLatencyTrackerForTesting(nodeLister, 1)
			tracker.RecordStartTime("n1", time.Now())

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

func TestUpdateLatencyTrackerAbort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// "n1" never gets tainted.
		nodeLister := NewTestCustomNodeLister([]string{"n1"}, nil)
		tracker := NewUpdateLatencyTrackerForTesting(nodeLister, 1)
		tracker.RecordStartTime("n1", time.Now())

		go tracker.start(t.Context())
		tracker.Abort()
		// Calling Abort a second time should not panic.
		tracker.Abort()

		select {
		case <-tracker.done:
		case <-time.After(time.Second):
			t.Fatal("tracker did not finish after Abort")
		}
		_, err := tracker.waitForLatency()
		assert.Error(t, err)
	})
}

func TestUpdateLatencyTrackerNilReceiver(t *testing.T) {
	var tracker *UpdateLatencyTracker
	assert.NotPanics(t, func() {
		tracker.RecordStartTime("n1", time.Now())
		tracker.Abort()
	})
}

func TestUpdateLatencyTrackerTimeoutStartsWhenAllNodesStarted(t *testing.T) {
	// n2 starts tainting 300ms after n1, which is longer than the 200ms timeout. The timeout must
	// only start once n2 has started, and n2's latency must be measured from its own start time.
	synctest.Test(t, func(t *testing.T) {
		nodes := []string{"n1", "n2"}
		nodeLister := NewTestCustomNodeLister(nodes, map[string]time.Duration{"n1": 0, "n2": 350 * time.Millisecond})
		tracker := NewUpdateLatencyTrackerForTesting(nodeLister, len(nodes))
		tracker.RecordStartTime("n1", time.Now())
		go tracker.start(t.Context())

		time.Sleep(300 * time.Millisecond)
		tracker.RecordStartTime("n2", time.Now())

		latency, err := tracker.waitForLatency()
		assert.NoError(t, err)
		// n1: 0ms. n2: started at 300ms, taint seen at 350ms.
		assert.Equal(t, 50*time.Millisecond, latency)
	})
}

func TestNodeDelayAfterTaintCalculator(t *testing.T) {
	const staticDelay = 5 * time.Second

	type loop struct {
		waitBefore time.Duration
		nodes      []string
		// if a node from `nodes` has no entry in `nodeTaintAfterDuration`, waiting for the node's taint will time out.
		nodeTaintAfterDuration map[string]time.Duration
		abort                  bool
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
			name:                "aborted loop does not affect subsequent loops",
			dynamicDelayEnabled: true,
			loops: []loop{
				{
					nodes:                  []string{"n1"},
					nodeTaintAfterDuration: map[string]time.Duration{"n1": 100 * time.Millisecond},
					wantDelay:              200 * time.Millisecond,
				},
				{
					// n2 would take 200ms (raising delay to 400ms), but the tracker is aborted without calling ComputeDelay.
					nodes:                  []string{"n2"},
					nodeTaintAfterDuration: map[string]time.Duration{"n2": 200 * time.Millisecond},
					abort:                  true,
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
				delayCalculator := NewNodeDelayAfterTaintCalculator(staticDelay, tc.dynamicDelayEnabled)

				for i, l := range tc.loops {
					if l.waitBefore > 0 {
						time.Sleep(l.waitBefore)
					}
					nodeLister := NewTestCustomNodeLister(l.nodes, l.nodeTaintAfterDuration)
					tracker := delayCalculator.StartLatencyTracker(t.Context(), nodeLister, len(l.nodes))
					startTime := time.Now()
					for _, name := range l.nodes {
						tracker.RecordStartTime(name, startTime)
					}
					if l.abort {
						tracker.Abort()
						select {
						case <-tracker.done:
						case <-time.After(time.Second):
							t.Fatalf("loop %d: tracker did not finish after Abort", i)
						}
						continue
					}
					gotDelay := delayCalculator.ComputeDelay(t.Context(), tracker)
					assert.Equal(t, l.wantDelay, gotDelay, "loop %d delay", i)
				}
			})
		})
	}
}
