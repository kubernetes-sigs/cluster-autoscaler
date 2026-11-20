/*
Copyright The Kubernetes Authors.

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

package predicate

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func podInfoOf(t *testing.T, s *PredicateSnapshot, nodeName, podName string) *framework.PodInfo {
	t.Helper()
	nodeInfo, err := s.GetNodeInfo(nodeName)
	assert.NoError(t, err)
	for _, pi := range nodeInfo.Pods() {
		if pi.Pod.Name == podName {
			return pi
		}
	}
	t.Fatalf("pod %s not found on %s", podName, nodeName)
	return nil
}

func TestSetClusterStateReusesPodInfos(t *testing.T) {
	fwHandle, err := framework.NewTestFrameworkHandle()
	assert.NoError(t, err)
	s := NewPredicateSnapshot(store.NewDeltaSnapshotStore(), fwHandle, false, 4, false, 0)

	node := BuildTestNode("n1", 4000, 4096)
	stable := BuildTestPod("stable", 100, 100, WithNodeName("n1"))
	stable.ResourceVersion = "1"
	updated := BuildTestPod("updated", 100, 100, WithNodeName("n1"))
	updated.ResourceVersion = "1"
	noRV := BuildTestPod("no-rv", 100, 100, WithNodeName("n1"))

	assert.NoError(t, s.SetClusterState(context.Background(), []*apiv1.Node{node}, []*apiv1.Pod{stable, updated, noRV}, nil, nil))
	stableInfo := podInfoOf(t, s, "n1", "stable")
	updatedInfo := podInfoOf(t, s, "n1", "updated")
	noRVInfo := podInfoOf(t, s, "n1", "no-rv")

	// Informer hands out a new object when a pod changes.
	updatedV2 := updated.DeepCopy()
	updatedV2.ResourceVersion = "2"
	updatedV2.Spec.Containers[0].Resources.Requests = BuildTestPod("x", 300, 100).Spec.Containers[0].Resources.Requests

	assert.NoError(t, s.SetClusterState(context.Background(), []*apiv1.Node{node}, []*apiv1.Pod{stable, updatedV2, noRV}, nil, nil))
	assert.Same(t, stableInfo, podInfoOf(t, s, "n1", "stable"), "unchanged pod should reuse its PodInfo")
	assert.NotSame(t, updatedInfo, podInfoOf(t, s, "n1", "updated"), "updated pod must get a new PodInfo")
	assert.NotSame(t, noRVInfo, podInfoOf(t, s, "n1", "no-rv"), "pods without ResourceVersion are never cached")

	nodeInfo, err := s.GetNodeInfo("n1")
	assert.NoError(t, err)
	assert.Equal(t, int64(500), nodeInfo.Requested.MilliCPU, "requests must reflect the updated pod")

	// Pods that are gone are dropped from the cache.
	assert.NoError(t, s.SetClusterState(context.Background(), []*apiv1.Node{node}, []*apiv1.Pod{stable}, nil, nil))
	assert.Len(t, s.podInfoCache, 1)
}

func randomCachePod(r *rand.Rand, id int, nodeName string) *apiv1.Pod {
	pod := BuildTestPod(fmt.Sprintf("p-%d", id), int64(100*(1+r.Intn(5))), int64(1024*(1+r.Intn(5))), WithNodeName(nodeName))
	pod.UID = types.UID(fmt.Sprintf("uid-%d", id))
	pod.ResourceVersion = "1"
	app := fmt.Sprintf("app-%d", r.Intn(4))
	pod.Labels = map[string]string{"app": app}
	if r.Intn(2) == 0 {
		pod.Spec.Affinity = benchAntiAffinity(app)
	}
	return pod
}

// TestSetClusterStateCacheMatchesFreshSnapshot runs random loops (pods added, removed,
// updated by the informer, moved, simulated scheduling in forks) and checks that a snapshot
// reusing PodInfos answers exactly like a snapshot built from scratch every time.
func TestSetClusterStateCacheMatchesFreshSnapshot(t *testing.T) {
	for _, storeName := range []string{"basic", "delta"} {
		t.Run(storeName, func(t *testing.T) { testSetClusterStateCacheMatchesFreshSnapshot(t, storeName) })
	}
}

func testSetClusterStateCacheMatchesFreshSnapshot(t *testing.T, storeName string) {
	r := rand.New(rand.NewSource(3))
	newSnapshot := func() *PredicateSnapshot {
		fwHandle, err := framework.NewTestFrameworkHandle()
		assert.NoError(t, err)
		snapshotStore := clustersnapshot.ClusterSnapshotStore(store.NewDeltaSnapshotStore())
		if storeName == "basic" {
			snapshotStore = store.NewBasicSnapshotStore()
		}
		return NewPredicateSnapshot(snapshotStore, fwHandle, false, 4, false, 0)
	}
	cached := newSnapshot()

	var nodes []*apiv1.Node
	for i := 0; i < 8; i++ {
		nodes = append(nodes, BuildTestNode(fmt.Sprintf("n%d", i), 4000, 64*1024))
	}
	pods := map[int]*apiv1.Pod{}
	nextID := 0
	for i := 0; i < 20; i++ {
		pods[nextID] = randomCachePod(r, nextID, nodes[r.Intn(len(nodes))].Name)
		nextID++
	}

	for loop := 0; loop < 50; loop++ {
		// Informer churn: new objects for updated pods, never in-place mutations.
		for id, pod := range pods {
			switch r.Intn(10) {
			case 0:
				delete(pods, id)
			case 1:
				updated := pod.DeepCopy()
				updated.ResourceVersion = fmt.Sprint(loop + 2)
				updated.Spec.Containers[0].Resources.Requests = BuildTestPod("x", int64(100*(1+r.Intn(5))), 1024).Spec.Containers[0].Resources.Requests
				pods[id] = updated
			case 2:
				moved := pod.DeepCopy()
				moved.ResourceVersion = fmt.Sprint(loop + 2)
				moved.Spec.NodeName = nodes[r.Intn(len(nodes))].Name
				pods[id] = moved
			}
		}
		for i := r.Intn(4); i > 0; i-- {
			pods[nextID] = randomCachePod(r, nextID, nodes[r.Intn(len(nodes))].Name)
			nextID++
		}
		var podList []*apiv1.Pod
		for _, pod := range pods {
			podList = append(podList, pod)
		}

		assert.NoError(t, cached.SetClusterState(context.Background(), nodes, podList, nil, nil))
		fresh := newSnapshot()
		assert.NoError(t, fresh.SetClusterState(context.Background(), nodes, podList, nil, nil))

		for _, node := range nodes {
			got, err := cached.GetNodeInfo(node.Name)
			assert.NoError(t, err)
			want, err := fresh.GetNodeInfo(node.Name)
			assert.NoError(t, err)
			assert.Equal(t, want.Requested, got.Requested, "loop %d node %s", loop, node.Name)
			assert.Equal(t, want.NonZeroRequested, got.NonZeroRequested, "loop %d node %s", loop, node.Name)
			assert.Equal(t, len(want.PodsWithRequiredAntiAffinity), len(got.PodsWithRequiredAntiAffinity), "loop %d node %s", loop, node.Name)
			assert.ElementsMatch(t, podNames(want), podNames(got), "loop %d node %s", loop, node.Name)
		}

		// Scheduling answers must match too.
		probe := randomCachePod(r, 1_000_000+loop, "")
		probe.Spec.NodeName = ""
		for _, node := range nodes {
			gotErr := cached.CheckPredicates(probe, node.Name)
			wantErr := fresh.CheckPredicates(probe, node.Name)
			if assert.Equal(t, wantErr == nil, gotErr == nil, "loop %d node %s", loop, node.Name) && wantErr != nil {
				assert.Equal(t, wantErr.Error(), gotErr.Error(), "loop %d node %s", loop, node.Name)
			}
		}

		// Simulations fork, move pods around and revert: the cached PodInfos must survive.
		cached.Fork()
		for _, pod := range podList[:min(3, len(podList))] {
			assert.NoError(t, cached.UnschedulePod(pod.Namespace, pod.Name, pod.Spec.NodeName))
			_ = cached.SchedulePod(pod, nodes[r.Intn(len(nodes))].Name)
		}
		cached.Revert()
	}
}

func podNames(ni *framework.NodeInfo) []string {
	var names []string
	for _, pi := range ni.Pods() {
		names = append(names, pi.Pod.Name+"@"+pi.Pod.ResourceVersion)
	}
	return names
}

func TestSetClusterStateDoesNotCachePodsWithClaims(t *testing.T) {
	fwHandle, err := framework.NewTestFrameworkHandle()
	assert.NoError(t, err)
	s := NewPredicateSnapshot(store.NewDeltaSnapshotStore(), fwHandle, false, 4, false, 0)
	pod := BuildTestPod("claims", 100, 100, WithNodeName("n1"))
	pod.ResourceVersion = "1"
	pod.Spec.ResourceClaims = []apiv1.PodResourceClaim{{Name: "gpu"}}
	node := BuildTestNode("n1", 4000, 4096)
	assert.NoError(t, s.SetClusterState(context.Background(), []*apiv1.Node{node}, []*apiv1.Pod{pod}, nil, nil))
	first := podInfoOf(t, s, "n1", "claims")
	assert.NoError(t, s.SetClusterState(context.Background(), []*apiv1.Node{node}, []*apiv1.Pod{pod}, nil, nil))
	assert.NotSame(t, first, podInfoOf(t, s, "n1", "claims"))
	assert.Empty(t, s.podInfoCache)
}
