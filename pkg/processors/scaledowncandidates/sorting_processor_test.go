/*
Copyright 2023 The Kubernetes Authors.

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

package scaledowncandidates

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	apiv1 "k8s.io/api/core/v1"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type scoreProcessor struct {
	scores []int
}

func (p *scoreProcessor) ScaleDownEarlierThan(node1, node2 *apiv1.Node) bool {
	idx1, _ := strconv.Atoi(node1.Name[5:])
	idx2, _ := strconv.Atoi(node2.Name[5:])
	return p.scores[idx1] > p.scores[idx2]
}

func (p *scoreProcessor) ResetState() {}

func TestSort(t *testing.T) {
	testCases := []struct {
		name          string
		numNodes      int
		numProcessors int
		nodeScores    [][]int //2d array that represent the score of node for each processor
		sortedOrder   []int
	}{
		{
			name:        "No score, order the same",
			numNodes:    5,
			sortedOrder: []int{0, 1, 2, 3, 4},
		},
		{
			name:          "One processor, the order has changed",
			numNodes:      5,
			numProcessors: 1,
			nodeScores:    [][]int{{3}, {4}, {1}, {2}, {0}},
			sortedOrder:   []int{1, 0, 3, 2, 4},
		},
		{
			name:          "Two processors, second processor did not affect the order",
			numNodes:      5,
			numProcessors: 2,
			nodeScores:    [][]int{{3, 5}, {4, 0}, {1, 2}, {2, 4}, {0, 5}},
			sortedOrder:   []int{1, 0, 3, 2, 4},
		},
		{
			name:          "Two processors, the first processor has equal scores",
			numNodes:      5,
			numProcessors: 2,
			nodeScores:    [][]int{{4, 5}, {4, 0}, {1, 2}, {2, 4}, {0, 5}},
			sortedOrder:   []int{0, 1, 3, 2, 4},
		},
		{
			name:          "Three processors, all three processors affected the order",
			numNodes:      5,
			numProcessors: 3,
			nodeScores:    [][]int{{1, 1, 1}, {1, 1, 2}, {1, 1, 3}, {1, 2, 1}, {2, 1, 1}},
			sortedOrder:   []int{4, 3, 2, 1, 0},
		},
	}
	for _, test := range testCases {
		nodes := []*apiv1.Node{}
		for i := 0; i < test.numNodes; i++ {
			node := BuildTestNode(fmt.Sprintf("node-%d", i), 10, 100)
			nodes = append(nodes, node)
		}
		processors := []CandidatesComparer{}
		for i := 0; i < test.numProcessors; i++ {
			scores := []int{}
			for _, nodeScore := range test.nodeScores {
				scores = append(scores, nodeScore[i])
			}
			processors = append(processors, &scoreProcessor{scores: scores})
		}
		nd := NodeSorter{nodes: nodes, processors: processors}
		sorted := nd.Sort()
		got := []int{}
		for _, node := range sorted {
			idx, _ := strconv.Atoi(node.Name[5:])
			got = append(got, idx)
		}
		if diff := cmp.Diff(test.sortedOrder, got); diff != "" {
			t.Errorf("%s: NodeSorter.Sort() diff (-want +got):\n%s", test.name, diff)
		}
	}
}

func TestSortStable(t *testing.T) {
	t.Parallel()

	const nodeCount = 60
	nodes := make([]*apiv1.Node, nodeCount)
	scores := make([]int, nodeCount)
	for i := range nodeCount {
		nodes[i] = BuildTestNode(fmt.Sprintf("node-%d", i), 10, 100)
		scores[i] = i % 3
	}

	var want []int
	for tier := 2; tier >= 0; tier-- {
		for i := range nodeCount {
			if scores[i] == tier {
				want = append(want, i)
			}
		}
	}

	sorter := NodeSorter{
		nodes:      nodes,
		processors: []CandidatesComparer{&scoreProcessor{scores: scores}},
	}
	sorted := sorter.Sort()

	got := make([]int, len(sorted))
	for i, node := range sorted {
		suffix := strings.TrimPrefix(node.Name, "node-")
		idx, err := strconv.Atoi(suffix)
		if err != nil {
			t.Fatalf("strconv.Atoi(%q) failed: %v", suffix, err)
		}
		got[i] = idx
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("NodeSorter.Sort() stability diff (-want +got):\n%s", diff)
	}
}
