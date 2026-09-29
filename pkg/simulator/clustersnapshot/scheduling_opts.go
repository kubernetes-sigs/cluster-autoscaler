/*
Copyright 2025 The Kubernetes Authors.

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

package clustersnapshot

import (
	"sort"

	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// NodeOrderMapping defines the order in which nodes are iterated during scheduling simulation.
type NodeOrderMapping interface {
	// Reset initializes or resets the mapping with the list of nodeInfos.
	// It will be called every time the scheduler tries to schedule a given pod on nodes.
	Reset(collection []*framework.NodeInfo)
	// At returns the index of the node at the given iteration step 'i'.
	// Returns -1 if no more nodes should be processed.
	// It starts from 0 every time a  pod is being scheduled.
	At(i int) int
	// MarkMatch marks the node at the given index as the last successful match.
	// This can be used by the mapping to adjust the order for subsequent pods.
	MarkMatch(index int)
	// PreferProcessingInOrder returns true if the passing node at the smallest step 'i' is preferred.
	// Otherwise, a passing node at a later step may be picked. Processing in order may cause a slight performance penalty.
	PreferProcessingInOrder() bool
}

type lastIndexOrderMapping struct {
	lastIndex  int
	offset     int
	collection []*framework.NodeInfo
}

// NewLastIndexOrderMapping returns a NodeOrderMapping that starts the iteration from the last match + some defined offset.
func NewLastIndexOrderMapping(offset int) NodeOrderMapping {
	return &lastIndexOrderMapping{offset: offset}
}

func (m *lastIndexOrderMapping) Reset(collection []*framework.NodeInfo) {
	m.collection = collection
}

func (m *lastIndexOrderMapping) At(i int) int {
	if len(m.collection) == 0 {
		return -1
	}
	return (i + m.offset + m.lastIndex) % len(m.collection)
}

func (m *lastIndexOrderMapping) MarkMatch(index int) {
	m.lastIndex = index
}

func (m *lastIndexOrderMapping) PreferProcessingInOrder() bool {
	// Starting from the last match is only a heuristic, so any passing node will do.
	return false
}

type priorityNodeOrderMapping struct {
	less       func(a, b *framework.NodeInfo) bool
	order      []int
	collection []*framework.NodeInfo
}

// NewPriorityNodeOrderMapping returns a NodeOrderMapping that iterates on nodes in order of the provided comparison function.
func NewPriorityNodeOrderMapping(less func(a, b *framework.NodeInfo) bool) NodeOrderMapping {
	return &priorityNodeOrderMapping{less: less}
}

func (m *priorityNodeOrderMapping) Reset(collection []*framework.NodeInfo) {
	m.collection = collection
	m.order = make([]int, len(collection))
	for i := range collection {
		m.order[i] = i
	}
	sort.SliceStable(m.order, func(i, j int) bool {
		return m.less(collection[m.order[i]], collection[m.order[j]])
	})
}

func (m *priorityNodeOrderMapping) At(i int) int {
	if i < 0 || i >= len(m.order) {
		return -1
	}
	return m.order[i]
}

func (m *priorityNodeOrderMapping) MarkMatch(index int) {
	// No work needed here bec we want to respect the ordering every time.
}

func (m *priorityNodeOrderMapping) PreferProcessingInOrder() bool {
	// The nodes are sorted by priority, so the passing node with the highest priority is preferred.
	return true
}

// SchedulingOptions contains options for the scheduling strategies and simulation.
type SchedulingOptions struct {
	NodeOrdering     NodeOrderMapping               // Defines the order in which nodes are iterated during scheduling simulation.
	IsNodeAcceptable func(*framework.NodeInfo) bool // Determines if a node is acceptable for scheduling.
}
