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
		// Nodes dropped from the tracker before it starts.
		droppedNodes []string
		wantLatency  time.Duration
		wantErr      bool
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
			description:            "dropped node that never gets tainted is not awaited",
			nodes:                  []string{"n1", "n2", "n3"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 50 * time.Millisecond, "n2": 100 * time.Millisecond},
			droppedNodes:           []string{"n3"},
			wantLatency:            100 * time.Millisecond,
		},
		{
			description:            "dropped node's latency is not included in the result",
			nodes:                  []string{"n1", "n2", "n3"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 50 * time.Millisecond, "n2": 100 * time.Millisecond, "n3": 150 * time.Millisecond},
			droppedNodes:           []string{"n3"},
			wantLatency:            100 * time.Millisecond,
		},
		{
			description:            "Some nodes fails to taint before timeout",
			nodes:                  []string{"n1", "n3"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 100 * time.Millisecond, "n3": 250 * time.Millisecond},
			wantErr:                true,
		},
		{
			description:            "all nodes dropped",
			nodes:                  []string{"n1", "n2"},
			nodeTaintAfterDuration: map[string]time.Duration{"n1": 0, "n2": 0},
			droppedNodes:           []string{"n1", "n2"},
			wantErr:                true,
		},
		{
			description: "no nodes",
			wantErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				nodeLister := NewTestCustomNodeLister(tc.nodes, tc.nodeTaintAfterDuration)
				tracker := NewUpdateLatencyTrackerForTesting(nodeLister, tc.nodes)
				startTime := time.Now()
				for _, name := range tc.nodes {
					tracker.RecordStartTime(name, startTime)
				}
				tracker.DropNodes(tc.droppedNodes...)
				go tracker.Start(t.Context())

				latency, err := tracker.WaitForLatency()
				if tc.wantErr {
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err)
				assert.Equal(t, tc.wantLatency, latency)
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
			tracker := NewUpdateLatencyTrackerForTesting(nodeLister, []string{"n1"})

			go tracker.Start(ctx)

			// Cancel ctx without ever starting or dropping "n1". Start() must return.
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
			tracker := NewUpdateLatencyTrackerForTesting(nodeLister, []string{"n1"})
			tracker.RecordStartTime("n1", time.Now())

			go tracker.Start(ctx)
			cancel()

			_, err := tracker.WaitForLatency()
			assert.EqualError(t, err, "taint wasn't observed for 1 nodes")
		})
	})
}

func TestUpdateLatencyTrackerWaitForLatencyWithNotStartedNodes(t *testing.T) {
	t.Run("nodes neither started nor dropped return an error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			nodes := []string{"n1", "n2", "n3"}
			nodeLister := NewTestCustomNodeLister(nodes, map[string]time.Duration{"n1": 0, "n2": 0, "n3": 0})
			tracker := NewUpdateLatencyTrackerForTesting(nodeLister, nodes)
			tracker.RecordStartTime("n1", time.Now())
			go tracker.Start(t.Context())

			_, err := tracker.WaitForLatency()
			assert.EqualError(t, err, "2 nodes were neither started nor dropped")

			// Start must return, even though ctx is never cancelled.
			select {
			case <-tracker.done:
			case <-time.After(time.Second):
				t.Fatal("tracker did not finish after WaitForLatency returned")
			}
		})
	})

	t.Run("dropped nodes that never started are fine", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			nodeLister := NewTestCustomNodeLister([]string{"n1"}, map[string]time.Duration{"n1": 0})
			tracker := NewUpdateLatencyTrackerForTesting(nodeLister, []string{"n1", "n2"})
			tracker.RecordStartTime("n1", time.Now())
			tracker.DropNodes("n2")
			go tracker.Start(t.Context())

			latency, err := tracker.WaitForLatency()
			assert.NoError(t, err)
			assert.Equal(t, time.Duration(0), latency)
		})
	})
}

func TestUpdateLatencyTrackerTimeoutStartsWhenAllNodesStarted(t *testing.T) {
	// n2 starts tainting 300ms after n1, which is longer than the 200ms timeout. The timeout must
	// only start once n2 has started, and n2's latency must be measured from its own start time.
	synctest.Test(t, func(t *testing.T) {
		nodes := []string{"n1", "n2"}
		nodeLister := NewTestCustomNodeLister(nodes, map[string]time.Duration{"n1": 0, "n2": 350 * time.Millisecond})
		tracker := NewUpdateLatencyTrackerForTesting(nodeLister, nodes)
		tracker.RecordStartTime("n1", time.Now())
		go tracker.Start(t.Context())

		time.Sleep(300 * time.Millisecond)
		tracker.RecordStartTime("n2", time.Now())

		latency, err := tracker.WaitForLatency()
		assert.NoError(t, err)
		// n1: 0ms. n2: started at 300ms, taint seen at 350ms.
		assert.Equal(t, 50*time.Millisecond, latency)
	})
}

func TestUpdateLatencyTrackerDropNodesWhilePolling(t *testing.T) {
	// In production the tracker polls while CA is still tainting nodes, so most taints are seen
	// before failed nodes are dropped. The latencies recorded then must be kept.
	synctest.Test(t, func(t *testing.T) {
		nodes := []string{"n1", "n2"}
		// n2 never gets tainted.
		nodeLister := NewTestCustomNodeLister(nodes, map[string]time.Duration{"n1": 10 * time.Millisecond})
		tracker := NewUpdateLatencyTrackerForTesting(nodeLister, nodes)
		startTime := time.Now()
		tracker.RecordStartTime("n1", startTime)
		tracker.RecordStartTime("n2", startTime)
		go tracker.Start(t.Context())

		// Drop n2 well after n1's taint was seen at 10ms and before the 200ms timeout.
		time.Sleep(100 * time.Millisecond)
		tracker.DropNodes("n2")

		latency, err := tracker.WaitForLatency()
		assert.NoError(t, err)
		// n1's taint was first seen at 10ms. Recording latencies only after n2 was dropped
		// would give more than 100ms.
		assert.Equal(t, 10*time.Millisecond, latency)
	})
}
