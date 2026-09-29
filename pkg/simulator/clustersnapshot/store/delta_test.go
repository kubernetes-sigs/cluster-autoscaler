/*
Copyright 2024 The Kubernetes Authors.

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

package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuretesting "k8s.io/component-base/featuregate/testing"
	schedulerinterface "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	hostnameTopologyKey = "kubernetes.io/hostname"
	zoneTopologyKey     = "topology.kubernetes.io/zone"
)

var (
	affinityLabels     = map[string]string{"app": "affinity-target"}
	antiAffinityLabels = map[string]string{"app": "anti-affinity-target"}
)

func buildTestNodeInfo(name string) *framework.NodeInfo {
	return framework.NewNodeInfo(test.BuildTestNode(name, 4000, 8*1024*1024*1024), nil)
}

// appendInOrder is a randIntN for newDeltaSnapshotStore that always picks the last position, so
// added nodes keep their insertion order.
func appendInOrder(n int) int { return n - 1 }

func extractNodeInfoSummary(list []schedulerinterface.NodeInfo) map[string][]string {
	res := make(map[string][]string, len(list))
	for _, ni := range list {
		pods := make([]string, 0, len(ni.GetPods()))
		for _, p := range ni.GetPods() {
			pods = append(pods, fmt.Sprintf("%s/%s:%d:%d",
				p.GetPod().Namespace,
				p.GetPod().Name,
				ni.GetRequested().GetMilliCPU(),
				ni.GetRequested().GetMemory()))
		}
		sort.Strings(pods)
		res[ni.Node().Name] = pods
	}
	return res
}

func nodeNamesInOrder(list []schedulerinterface.NodeInfo) []string {
	names := make([]string, 0, len(list))
	for _, ni := range list {
		names = append(names, ni.Node().Name)
	}
	return names
}

func extractNodeNames(list []schedulerinterface.NodeInfo) []string {
	names := nodeNamesInOrder(list)
	sort.Strings(names)
	return names
}

// assertLayerConsistent checks the invariants of a single snapshot layer's internal state.
func assertLayerConsistent(t *testing.T, step string, data *internalDeltaSnapshotData) {
	t.Helper()
	assert.Equal(t, len(data.addedNodeInfos), len(data.addedNodeIndex), "%s: addedNodeIndex size mismatch", step)
	for i, ni := range data.addedNodeInfos {
		idx, found := data.addedNodeIndex[ni.Node().Name]
		assert.True(t, found, "%s: added node %s missing from addedNodeIndex", step, ni.Node().Name)
		assert.Equal(t, i, idx, "%s: wrong addedNodeIndex for %s", step, ni.Node().Name)
	}
	// Added, modified and deleted nodes are disjoint: added nodes aren't visible in the base, while
	// modified and deleted ones are.
	for name := range data.addedNodeIndex {
		_, modified := data.modifiedNodeInfoMap[name]
		_, inBase := data.baseData.getNodeInfo(name)
		assert.False(t, modified || data.deletedNodeInfos[name] || inBase, "%s: added node %s is also modified, deleted or in the base", step, name)
	}
	for name := range data.modifiedNodeInfoMap {
		_, inBase := data.baseData.getNodeInfo(name)
		assert.True(t, inBase && !data.deletedNodeInfos[name], "%s: modified node %s must be in the base and not deleted", step, name)
	}
	for name := range data.deletedNodeInfos {
		_, inBase := data.baseData.getNodeInfo(name)
		assert.True(t, inBase, "%s: deleted node %s must be in the base", step, name)
	}
	if len(data.deletedNodeInfos) == 0 {
		assert.Nil(t, data.compactedNodeIndex, "%s: a layer without deletions must not build an index", step)
	}
	if data.baseData == nil {
		assert.Nil(t, data.nodeInfoList, "%s: the root layer must list addedNodeInfos directly", step)
		assert.Empty(t, data.modifiedNodeInfoMap, "%s: the root layer can't have modifications", step)
		assert.Empty(t, data.deletedNodeInfos, "%s: the root layer can't have deletions", step)
	}
	if data.nodeInfoList != nil {
		assert.Equal(t, len(data.nodeInfoList), data.nodeCount(), "%s: nodeCount mismatch", step)
		if assert.GreaterOrEqual(t, len(data.nodeInfoList), len(data.addedNodeInfos), "%s: list shorter than addedNodeInfos", step) {
			suffix := data.nodeInfoList[len(data.nodeInfoList)-len(data.addedNodeInfos):]
			for i, ni := range data.addedNodeInfos {
				assert.Same(t, ni, suffix[i], "%s: list suffix must be addedNodeInfos (at %d)", step, i)
			}
		}
	}
	if data.compactedNodeIndex != nil {
		if assert.NotNil(t, data.nodeInfoList, "%s: index must only exist alongside a built list", step) {
			baseNodes := data.nodeInfoList[:len(data.nodeInfoList)-len(data.addedNodeInfos)]
			assert.Equal(t, len(baseNodes), len(data.compactedNodeIndex), "%s: index size mismatch", step)
			for i, ni := range baseNodes {
				idx, found := data.compactedNodeIndex[ni.Node().Name]
				assert.True(t, found, "%s: missing %s in index", step, ni.Node().Name)
				assert.Equal(t, i, idx, "%s: wrong index for %s", step, ni.Node().Name)
			}
		}
	}
}

// assertDeltaInternalsConsistent checks that the cached list matches a fresh rebuild exactly
// (same order, same NodeInfo pointers), that getNodeIndex agrees with it, and that every layer
// satisfies its invariants.
func assertDeltaInternalsConsistent(t *testing.T, step string, delta *DeltaSnapshotStore) {
	t.Helper()
	deltaList, err := delta.NodeInfos().List()
	assert.NoError(t, err, step)

	freshBuilt := delta.data.buildNodeInfoList()
	assert.Equal(t, nodeNamesInOrder(freshBuilt), nodeNamesInOrder(deltaList), "%s: cached list order differs from a fresh rebuild", step)
	for i, ni := range deltaList {
		if i < len(freshBuilt) {
			assert.Same(t, freshBuilt[i], ni, "%s: pointer mismatch at %d (%s)", step, i, ni.Node().Name)
		}
		idx, found := delta.data.getNodeIndex(ni.Node().Name)
		assert.True(t, found, "%s: getNodeIndex(%s) returned false", step, ni.Node().Name)
		assert.Equal(t, i, idx, "%s: getNodeIndex(%s) mismatch", step, ni.Node().Name)
	}

	// A layer without deletions must lay out its list as its base's list followed by added nodes.
	if delta.data.baseData != nil && len(delta.data.deletedNodeInfos) == 0 {
		baseList := delta.data.baseData.getNodeInfoList()
		assert.GreaterOrEqual(t, len(deltaList), len(baseList), "%s: fork list shorter than baseList", step)
		for i, bni := range baseList {
			assert.Equal(t, bni.Node().Name, deltaList[i].Node().Name, "%s: base index order mismatch at %d", step, i)
		}
	}

	for data := delta.data; data != nil; data = data.baseData {
		assertLayerConsistent(t, step, data)
	}
}

func assertDeltaMatchesBasic(t *testing.T, step string, delta *DeltaSnapshotStore, basic *BasicSnapshotStore, pvcKeys []string) {
	t.Helper()

	deltaList, err := delta.NodeInfos().List()
	assert.NoError(t, err, step)
	basicList, err := basic.NodeInfos().List()
	assert.NoError(t, err, step)

	assert.ElementsMatch(t, extractNodeNames(basicList), extractNodeNames(deltaList), "NodeInfos().List() node names mismatch at %s", step)
	assert.Equal(t, extractNodeInfoSummary(basicList), extractNodeInfoSummary(deltaList), "NodeInfos().List() summary mismatch at %s", step)

	// Verify each node via Get(nodeName).
	for _, bni := range basicList {
		dni, err := delta.NodeInfos().Get(bni.Node().Name)
		assert.NoError(t, err, "%s: Get(%s)", step, bni.Node().Name)
		assert.Equal(t, bni.GetRequested().GetMilliCPU(), dni.GetRequested().GetMilliCPU(), "%s: MilliCPU for %s", step, bni.Node().Name)
		assert.Equal(t, bni.GetRequested().GetMemory(), dni.GetRequested().GetMemory(), "%s: Memory for %s", step, bni.Node().Name)
	}

	// Verify the pod-derived node lists.
	derivedLists := []struct {
		name string
		list func(schedulerinterface.NodeInfoLister) ([]schedulerinterface.NodeInfo, error)
	}{
		{"HavePodsWithAffinityList", schedulerinterface.NodeInfoLister.HavePodsWithAffinityList},
		{"HavePodsWithRequiredAntiAffinityList", schedulerinterface.NodeInfoLister.HavePodsWithRequiredAntiAffinityList},
		{"HavePodsWithRequiredNonHostScopedAntiAffinityList", schedulerinterface.NodeInfoLister.HavePodsWithRequiredNonHostScopedAntiAffinityList},
	}
	for _, dl := range derivedLists {
		deltaDerived, err := dl.list(delta.NodeInfos())
		assert.NoError(t, err, step)
		basicDerived, err := dl.list(basic.NodeInfos())
		assert.NoError(t, err, step)
		assert.Equal(t, extractNodeNames(basicDerived), extractNodeNames(deltaDerived), "%s: %s mismatch", step, dl.name)
	}

	// Verify IsPVCUsedByPods.
	for _, key := range pvcKeys {
		assert.Equal(t, basic.IsPVCUsedByPods(key), delta.IsPVCUsedByPods(key), "%s: IsPVCUsedByPods(%s) mismatch", step, key)
	}
}

// differentialStep is a single operation applied to both a DeltaSnapshotStore and a BasicSnapshotStore.
type differentialStep struct {
	name string
	op   func(s clustersnapshot.ClusterSnapshotStore) error
}

func forkStep(name string) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error { s.Fork(); return nil }}
}

func revertStep(name string) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error { s.Revert(); return nil }}
}

func commitStep(name string) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error { return s.Commit() }}
}

func addNodeStep(name string, nodeInfo func() *framework.NodeInfo) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error { return s.StoreNodeInfo(nodeInfo()) }}
}

func removeNodeStep(name string, nodeName string) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error {
		return s.RemoveNodeInfo(context.Background(), nodeName)
	}}
}

func addPodStep(name string, pod *apiv1.Pod, nodeName string) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error {
		return s.StorePodInfo(framework.NewPodInfo(pod, nil), nodeName)
	}}
}

func TestDeltaSnapshotStoreDifferentialMultiDepth(t *testing.T) {
	// Populates NodeInfo.PodsWithRequiredNonHostScopedAntiAffinity, so that
	// HavePodsWithRequiredNonHostScopedAntiAffinityList is exercised.
	featuretesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InterPodAffinityHostnameFastPath, true)
	pvcKeys := []string{"default/pvc-0", "default/pvc-1", "default/pvc-2", "default/pvc-3"}

	var steps []differentialStep

	// Depth 0: add 10 base nodes and schedule pods onto some of them.
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		steps = append(steps, addNodeStep("depth0-add-"+nodeName, func() *framework.NodeInfo { return buildTestNodeInfo(nodeName) }))
	}
	for i := 0; i < 5; i++ {
		opts := []func(*apiv1.Pod){test.WithPVC(fmt.Sprintf("pvc-%d", i%2))}
		if i%2 == 0 {
			opts = append(opts, test.WithPodAffinity(affinityLabels, hostnameTopologyKey))
		}
		if i%3 == 0 {
			opts = append(opts, test.WithPodAntiAffinity(antiAffinityLabels, hostnameTopologyKey))
		}
		pod := test.BuildTestPod(fmt.Sprintf("pod-d0-%d", i), 500, 1024, opts...)
		steps = append(steps, addPodStep(fmt.Sprintf("depth0-addpod-%d", i), pod, fmt.Sprintf("node-%d", i)))
	}

	// Depth 1: modify base nodes, add several nodes and remove one of them, without deleting base nodes.
	steps = append(steps, forkStep("fork-to-depth1"))
	for i := 5; i < 9; i++ {
		opts := []func(*apiv1.Pod){test.WithPVC("pvc-2")}
		if i == 5 {
			opts = append(opts, test.WithPodAffinity(affinityLabels, hostnameTopologyKey))
		}
		if i == 6 {
			opts = append(opts, test.WithPodAntiAffinity(antiAffinityLabels, hostnameTopologyKey))
		}
		pod := test.BuildTestPod(fmt.Sprintf("pod-d1-%d", i), 300, 512, opts...)
		steps = append(steps, addPodStep(fmt.Sprintf("depth1-addpod-%d", i), pod, fmt.Sprintf("node-%d", i)))
	}
	steps = append(steps, addPodStep("depth1-addpod-zone-anti-affinity-9",
		test.BuildTestPod("pod-d1-zone", 200, 256, test.WithPodAntiAffinity(antiAffinityLabels, zoneTopologyKey)), "node-9"))
	for i := 10; i < 14; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		steps = append(steps, addNodeStep("depth1-add-"+nodeName, func() *framework.NodeInfo { return buildTestNodeInfo(nodeName) }))
	}
	steps = append(steps,
		// Removes a node added in the same layer.
		removeNodeStep("depth1-remove-added-node-11", "node-11"),
		differentialStep{name: "depth1-removepod-0", op: func(s clustersnapshot.ClusterSnapshotStore) error {
			return s.RemovePodInfo("default", "pod-d0-0", "node-0")
		}},
	)

	// Depth 2: modify a node added in the parent, then delete and re-add nodes.
	steps = append(steps,
		forkStep("fork-to-depth2"),
		addPodStep("depth2-addpod-parent-added-node-13",
			test.BuildTestPod("pod-d2-13", 400, 512, test.WithPodAntiAffinity(antiAffinityLabels, zoneTopologyKey), test.WithPVC("pvc-0")), "node-13"),
		removeNodeStep("depth2-remove-node-2", "node-2"),
		// Re-adds a node deleted in the same layer.
		addNodeStep("depth2-readd-node-2", func() *framework.NodeInfo {
			node := test.BuildTestNode("node-2", 8000, 16*1024*1024*1024)
			pod := test.BuildTestPod("pod-readded-2", 1000, 2048,
				test.WithPodAffinity(affinityLabels, hostnameTopologyKey),
				test.WithPodAntiAffinity(antiAffinityLabels, hostnameTopologyKey),
				test.WithPVC("pvc-3"))
			return framework.NewNodeInfo(node, nil, framework.NewPodInfo(pod, nil))
		}),
		removeNodeStep("depth2-remove-node-3", "node-3"),
		// Removes a node added in the parent layer.
		removeNodeStep("depth2-remove-parent-added-node-12", "node-12"),
	)

	// Depth 3: make changes and revert them.
	steps = append(steps,
		forkStep("fork-to-depth3"),
		addPodStep("depth3-addpod-node-10", test.BuildTestPod("pod-d3-temp", 700, 1024, test.WithPodAntiAffinity(antiAffinityLabels, hostnameTopologyKey), test.WithPVC("pvc-1")), "node-10"),
		revertStep("revert-depth3-to-depth2"),
	)

	// Depth 3 again: re-add a node deleted in the parent layer and commit it.
	steps = append(steps,
		forkStep("fork-to-depth3-again"),
		addNodeStep("depth3-readd-parent-deleted-node-3", func() *framework.NodeInfo { return buildTestNodeInfo("node-3") }),
		addPodStep("depth3-addpod-node-3", test.BuildTestPod("pod-d3-3", 100, 128), "node-3"),
		commitStep("commit-depth3-to-depth2"),
	)

	// Commit all the way down.
	steps = append(steps,
		commitStep("commit-depth2-to-depth1"),
		commitStep("commit-depth1-to-depth0"),
	)

	// Besides the default store, run the scenario with seeded node orders, so that random insertions
	// are exercised at every depth in a reproducible way.
	newDeltas := map[string]func() *DeltaSnapshotStore{"default": NewDeltaSnapshotStore}
	for seed := uint64(1); seed <= 5; seed++ {
		newDeltas[fmt.Sprintf("seed-%d", seed)] = func() *DeltaSnapshotStore {
			return newDeltaSnapshotStore(rand.New(rand.NewPCG(seed, seed)).IntN)
		}
	}
	for name, newDelta := range newDeltas {
		t.Run(name, func(t *testing.T) {
			delta := newDelta()
			basic := NewBasicSnapshotStore()
			sawNonHostScopedAntiAffinity := false
			for _, step := range steps {
				errDelta := step.op(delta)
				errBasic := step.op(basic)
				assert.Equal(t, errBasic != nil, errDelta != nil, "%s: error presence mismatch (delta=%v, basic=%v)", step.name, errDelta, errBasic)
				assertDeltaMatchesBasic(t, step.name, delta, basic, pvcKeys)
				assertDeltaInternalsConsistent(t, step.name, delta)
				if list, _ := basic.NodeInfos().HavePodsWithRequiredNonHostScopedAntiAffinityList(); len(list) > 0 {
					sawNonHostScopedAntiAffinity = true
				}
			}
			assert.True(t, sawNonHostScopedAntiAffinity, "the scenario should exercise non-host-scoped anti-affinity")
		})
	}
}

func TestDeltaSnapshotStoreNodeInfoListCacheInvariants(t *testing.T) {
	delta := NewDeltaSnapshotStore()
	for i := 0; i < 4; i++ {
		assert.NoError(t, delta.StoreNodeInfo(buildTestNodeInfo(fmt.Sprintf("node-%d", i))))
	}

	// Warm the list at depth 0.
	baseListBefore, err := delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Len(t, baseListBefore, 4)

	// Scheduling a pod at depth 0 modifies the NodeInfo in place; the root lists addedNodeInfos directly.
	pod0 := test.BuildTestPod("pod-0", 100, 1024)
	assert.NoError(t, delta.StorePodInfo(framework.NewPodInfo(pod0, nil), "node-1"))

	baseListAfter, err := delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Same(t, &delta.data.addedNodeInfos[0], &baseListAfter[0], "the root should list addedNodeInfos directly")
	assert.Same(t, &baseListBefore[0], &baseListAfter[0], "depth 0 list backing array should be preserved across StorePodInfo")
	idx1, ok := delta.data.getNodeIndex("node-1")
	assert.True(t, ok)
	assert.Len(t, baseListAfter[idx1].GetPods(), 1)

	// Fork to depth 1, warm fork's list, and modify a base node via StorePodInfo.
	delta.Fork()
	forkListBefore, err := delta.NodeInfos().List()
	assert.NoError(t, err)

	idx2, ok := delta.data.getNodeIndex("node-2")
	assert.True(t, ok)
	origNode2Ptr := forkListBefore[idx2]

	pod1 := test.BuildTestPod("pod-1", 200, 2048)
	assert.NoError(t, delta.StorePodInfo(framework.NewPodInfo(pod1, nil), "node-2"))
	assert.NotNil(t, delta.data.nodeInfoList, "StorePodInfo in fork should patch the list in place rather than invalidating it")
	assert.Nil(t, delta.data.compactedNodeIndex, "fork without deletions should resolve positions without building an index")

	forkListAfter, err := delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Same(t, &forkListBefore[0], &forkListAfter[0], "fork list backing array should be preserved across StorePodInfo")
	assert.NotSame(t, origNode2Ptr, forkListAfter[idx2], "fork list entry should be replaced in place with the cloned NodeInfo")
	assert.Same(t, origNode2Ptr, baseListAfter[idx2], "base's list must not be affected by modifications made in the fork (Revert relies on it)")
	assert.Len(t, origNode2Ptr.GetPods(), 0, "base NodeInfo must remain unmodified")
	assert.Len(t, forkListAfter[idx2].GetPods(), 1, "fork NodeInfo must contain the newly scheduled pod")
}

// TestDeltaSnapshotStoreRemoveAddedNodeKeepsList verifies that removing a node added in the current
// layer patches the cached list in O(1) (moving the last added node into its slot) instead of
// invalidating it, both in the root and in a fork with deletions, and that pod-derived caches are
// only cleared when the removed node had pods. Nodes are added in order (appendInOrder), so that
// exact positions can be checked.
func TestDeltaSnapshotStoreRemoveAddedNodeKeepsList(t *testing.T) {
	ctx := context.Background()
	delta := newDeltaSnapshotStore(appendInOrder)
	for i := 0; i < 4; i++ {
		assert.NoError(t, delta.StoreNodeInfo(buildTestNodeInfo(fmt.Sprintf("node-%d", i))))
	}

	// Root: node-3 moves into node-1's slot.
	assert.NoError(t, delta.RemoveNodeInfo(ctx, "node-1"))
	list, err := delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Equal(t, []string{"node-0", "node-3", "node-2"}, nodeNamesInOrder(list))
	assertDeltaInternalsConsistent(t, "root", delta)

	// Fork with a deletion (so compactedNodeIndex is in use) and three added nodes.
	delta.Fork()
	assert.NoError(t, delta.RemoveNodeInfo(ctx, "node-0"))
	for i := 10; i < 13; i++ {
		assert.NoError(t, delta.StoreNodeInfo(buildTestNodeInfo(fmt.Sprintf("node-%d", i))))
	}
	withPods := buildTestNodeInfo("node-13")
	withPods.AddPod(framework.NewPodInfo(test.BuildTestPod("pod-13", 100, 128, test.WithPodAffinity(affinityLabels, hostnameTopologyKey)), nil))
	assert.NoError(t, delta.StoreNodeInfo(withPods))
	_, err = delta.NodeInfos().List()
	assert.NoError(t, err)
	_, found := delta.data.getNodeIndex("node-2")
	assert.True(t, found)
	assert.NotNil(t, delta.data.compactedNodeIndex)
	affinityList, err := delta.NodeInfos().HavePodsWithAffinityList()
	assert.NoError(t, err)
	assert.Equal(t, []string{"node-13"}, extractNodeNames(affinityList))

	// Removing a pod-less added node keeps the list, the index and the pod-derived caches.
	assert.NoError(t, delta.RemoveNodeInfo(ctx, "node-10"))
	assert.NotNil(t, delta.data.nodeInfoList, "removing an added node should patch the list")
	assert.NotNil(t, delta.data.compactedNodeIndex, "removing an added node should keep the base node index")
	assert.NotNil(t, delta.data.havePodsWithAffinity, "removing a pod-less node should keep pod-derived caches")
	list, err = delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Equal(t, []string{"node-3", "node-2", "node-13", "node-11", "node-12"}, nodeNamesInOrder(list))
	assertDeltaInternalsConsistent(t, "fork, pod-less node removed", delta)

	// Removing an added node with pods clears pod-derived caches, but still keeps the list.
	assert.NoError(t, delta.RemoveNodeInfo(ctx, "node-13"))
	assert.NotNil(t, delta.data.nodeInfoList)
	assert.Nil(t, delta.data.havePodsWithAffinity, "removing a node with pods should clear pod-derived caches")
	affinityList, err = delta.NodeInfos().HavePodsWithAffinityList()
	assert.NoError(t, err)
	assert.Empty(t, affinityList)
	list, err = delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Equal(t, []string{"node-3", "node-2", "node-12", "node-11"}, nodeNamesInOrder(list))
	assertDeltaInternalsConsistent(t, "fork, node with pods removed", delta)
}

// TestDeltaSnapshotStoreCommitPreservesNodeOrder verifies that committing modified nodes keeps their
// positions, added nodes are appended in insertion order (with appendInOrder), and a cold rebuild
// yields the same order as the incrementally maintained list. It commits into a non-root layer,
// which (unlike the root) maintains its own cached list.
func TestDeltaSnapshotStoreCommitPreservesNodeOrder(t *testing.T) {
	delta := newDeltaSnapshotStore(appendInOrder)
	for i := 0; i < 5; i++ {
		assert.NoError(t, delta.StoreNodeInfo(buildTestNodeInfo(fmt.Sprintf("node-%d", i))))
	}
	delta.Fork()
	baseList, err := delta.NodeInfos().List()
	assert.NoError(t, err)
	orderBefore := nodeNamesInOrder(baseList)
	assert.Equal(t, []string{"node-0", "node-1", "node-2", "node-3", "node-4"}, orderBefore, "base list should follow insertion order")

	delta.Fork()
	_, err = delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.NoError(t, delta.StorePodInfo(framework.NewPodInfo(test.BuildTestPod("pod-a", 100, 1024), nil), "node-3"))
	assert.NoError(t, delta.StorePodInfo(framework.NewPodInfo(test.BuildTestPod("pod-b", 100, 1024), nil), "node-0"))
	newNodes := []string{"node-new-1", "node-new-2", "node-new-3"}
	for _, name := range newNodes {
		assert.NoError(t, delta.StoreNodeInfo(buildTestNodeInfo(name)))
	}
	expectedOrder := append(append([]string{}, orderBefore...), newNodes...)
	forkList, err := delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Equal(t, expectedOrder, nodeNamesInOrder(forkList))

	assert.NoError(t, delta.Commit())
	assert.NotNil(t, delta.data.nodeInfoList, "committing modifications and additions should not invalidate the base list")
	assertLayerConsistent(t, "after commit", delta.data)

	listAfter, err := delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Equal(t, expectedOrder, nodeNamesInOrder(listAfter))
	for _, name := range []string{"node-0", "node-3"} {
		current, err := delta.NodeInfos().Get(name)
		assert.NoError(t, err)
		idx, found := delta.data.getNodeIndex(name)
		assert.True(t, found)
		assert.Same(t, current, listAfter[idx], "committed NodeInfo for %s should be in the list", name)
		assert.Len(t, current.GetPods(), 1)
	}

	delta.data.invalidateNodeInfoList()
	rebuilt, err := delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Equal(t, expectedOrder, nodeNamesInOrder(rebuilt), "a cold rebuild should yield the same order")
}

// TestDeltaSnapshotStoreReAddRestoresPosition verifies that re-adding a node deleted in the same
// layer turns it into a modification at the node's base position, invalidating the list only if it
// was built, that removing it again keeps hiding the base's copy, and that committing keeps its
// position. A node deleted in a parent and re-added in a child becomes a modification on commit.
func TestDeltaSnapshotStoreReAddRestoresPosition(t *testing.T) {
	ctx := context.Background()
	delta := newDeltaSnapshotStore(appendInOrder)
	for i := 0; i < 4; i++ {
		assert.NoError(t, delta.StoreNodeInfo(buildTestNodeInfo(fmt.Sprintf("node-%d", i))))
	}
	delta.Fork()
	assert.NoError(t, delta.RemoveNodeInfo(ctx, "node-1"))
	list, err := delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Equal(t, []string{"node-0", "node-2", "node-3"}, nodeNamesInOrder(list))
	_, found := delta.data.getNodeIndex("node-2")
	assert.True(t, found)
	assert.NotNil(t, delta.data.compactedNodeIndex)

	readded := buildTestNodeInfo("node-1")
	assert.NoError(t, delta.StoreNodeInfo(readded))
	assert.Empty(t, delta.data.deletedNodeInfos, "a re-added node should no longer be deleted")
	assert.Empty(t, delta.data.addedNodeInfos, "a re-added node should not be an added node")
	assert.Same(t, readded, delta.data.modifiedNodeInfoMap["node-1"], "a re-added node should be a modification")
	assert.Nil(t, delta.data.nodeInfoList, "re-adding a node should invalidate the built list")
	assert.Nil(t, delta.data.compactedNodeIndex, "re-adding a node should drop the index")
	list, err = delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Equal(t, []string{"node-0", "node-1", "node-2", "node-3"}, nodeNamesInOrder(list))
	current, err := delta.NodeInfos().Get("node-1")
	assert.NoError(t, err)
	assert.Same(t, readded, current)
	assertDeltaInternalsConsistent(t, "after re-add", delta)

	// Removing the re-added node must not resurrect the base's copy.
	assert.NoError(t, delta.RemoveNodeInfo(ctx, "node-1"))
	assert.True(t, delta.data.deletedNodeInfos["node-1"])
	assert.Empty(t, delta.data.modifiedNodeInfoMap)
	_, err = delta.NodeInfos().Get("node-1")
	assert.ErrorIs(t, err, clustersnapshot.ErrNodeNotFound)
	assertDeltaInternalsConsistent(t, "after removing the re-added node", delta)

	// With the list not built, a re-add leaves it unbuilt.
	delta.data.invalidateNodeInfoList()
	assert.NoError(t, delta.StoreNodeInfo(readded))
	assert.Nil(t, delta.data.nodeInfoList)
	assert.NoError(t, delta.Commit())
	list, err = delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Equal(t, []string{"node-0", "node-1", "node-2", "node-3"}, nodeNamesInOrder(list), "commit should keep the re-added node's position")
	current, err = delta.NodeInfos().Get("node-1")
	assert.NoError(t, err)
	assert.Same(t, readded, current)
	assertDeltaInternalsConsistent(t, "after commit into root", delta)

	// A node deleted in the parent is added in the child (appended there), and becomes a
	// modification at its original position when committed into the parent.
	delta.Fork()
	assert.NoError(t, delta.RemoveNodeInfo(ctx, "node-2"))
	delta.Fork()
	readded2 := buildTestNodeInfo("node-2")
	assert.NoError(t, delta.StoreNodeInfo(readded2))
	list, err = delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Equal(t, []string{"node-0", "node-1", "node-3", "node-2"}, nodeNamesInOrder(list))
	assertDeltaInternalsConsistent(t, "child re-adds a node deleted in the parent", delta)
	assert.NoError(t, delta.Commit())
	assert.Empty(t, delta.data.deletedNodeInfos)
	assert.Empty(t, delta.data.addedNodeInfos)
	assert.Same(t, readded2, delta.data.modifiedNodeInfoMap["node-2"])
	list, err = delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Equal(t, []string{"node-0", "node-1", "node-2", "node-3"}, nodeNamesInOrder(list))
	assertDeltaInternalsConsistent(t, "after commit into parent", delta)
}

// TestDeltaSnapshotStoreRemovePodIsAtomicOnError verifies that a failed RemovePodInfo leaves the snapshot untouched.
func TestDeltaSnapshotStoreRemovePodIsAtomicOnError(t *testing.T) {
	delta := NewDeltaSnapshotStore()
	pod := test.BuildTestPod("pod", 100, 1024)
	assert.NoError(t, delta.StoreNodeInfo(framework.NewNodeInfo(test.BuildTestNode("node-0", 4000, 8*1024*1024*1024), nil, framework.NewPodInfo(pod, nil))))
	baseNode, err := delta.NodeInfos().Get("node-0")
	assert.NoError(t, err)

	delta.Fork()
	list, err := delta.NodeInfos().List()
	assert.NoError(t, err)

	assert.Error(t, delta.RemovePodInfo(pod.Namespace, "missing-pod", "node-0"))
	assert.ErrorIs(t, delta.RemovePodInfo(pod.Namespace, pod.Name, "missing-node"), clustersnapshot.ErrNodeNotFound)
	assert.Empty(t, delta.data.modifiedNodeInfoMap, "a failed RemovePodInfo must not clone the node")
	assert.Same(t, baseNode, list[0], "a failed RemovePodInfo must not patch the cached list")

	assert.NoError(t, delta.RemovePodInfo(pod.Namespace, pod.Name, "node-0"))
	current, err := delta.NodeInfos().Get("node-0")
	assert.NoError(t, err)
	assert.Empty(t, current.GetPods())
	assert.Len(t, baseNode.GetPods(), 1, "base NodeInfo must remain unmodified")
}

// scriptedRand is a randIntN source returning scripted positions, for exact checks of node order.
type scriptedRand struct {
	t    *testing.T
	next []int
}

func (r *scriptedRand) intN(n int) int {
	r.t.Helper()
	if len(r.next) == 0 {
		r.t.Fatalf("unexpected randIntN(%d) call", n)
	}
	pos := r.next[0]
	r.next = r.next[1:]
	if pos < 0 || pos >= n {
		r.t.Fatalf("scripted position %d out of range for randIntN(%d)", pos, n)
	}
	return pos
}

// TestDeltaSnapshotStoreInsertsAddedNodesAtRandomPositions verifies that added nodes are inserted at
// the position picked by randIntN, moving the node there to the end, in the root and in forks with a
// built list and deletions, and that commits insert the committed nodes the same way.
func TestDeltaSnapshotStoreInsertsAddedNodesAtRandomPositions(t *testing.T) {
	ctx := context.Background()
	r := &scriptedRand{t: t}
	delta := newDeltaSnapshotStore(r.intN)
	nodeInfos := map[string]*framework.NodeInfo{}
	add := func(name string) func() error {
		return func() error {
			nodeInfos[name] = buildTestNodeInfo(name)
			return delta.StoreNodeInfo(nodeInfos[name])
		}
	}
	remove := func(name string) func() error {
		return func() error { return delta.RemoveNodeInfo(ctx, name) }
	}
	step := func(name string, positions []int, op func() error, want []string) {
		t.Helper()
		r.next = positions
		assert.NoError(t, op(), name)
		assert.Empty(t, r.next, "%s: unused scripted positions", name)
		list, err := delta.NodeInfos().List()
		assert.NoError(t, err, name)
		assert.Equal(t, want, nodeNamesInOrder(list), name)
		// Also checks that getNodeIndex matches the list position of every node.
		assertDeltaInternalsConsistent(t, name, delta)
	}

	// Root.
	step("root add a", []int{0}, add("a"), []string{"a"})
	step("root add b at 0", []int{0}, add("b"), []string{"b", "a"})
	step("root add c at 1", []int{1}, add("c"), []string{"b", "c", "a"})
	step("root add d at the end", []int{3}, add("d"), []string{"b", "c", "a", "d"})
	step("root add e at 0", []int{0}, add("e"), []string{"e", "c", "a", "d", "b"})
	step("root remove c", nil, remove("c"), []string{"e", "b", "a", "d"})

	// Depth 1, with deletions, so that compactedNodeIndex is in use.
	delta.Fork()
	step("depth1 remove base a", nil, remove("a"), []string{"e", "b", "d"})
	assert.NotNil(t, delta.data.compactedNodeIndex)
	step("depth1 remove base d", nil, remove("d"), []string{"e", "b"})
	step("depth1 add f", []int{0}, add("f"), []string{"e", "b", "f"})
	step("depth1 add g at 0", []int{0}, add("g"), []string{"e", "b", "g", "f"})
	step("depth1 add h at 1", []int{1}, add("h"), []string{"e", "b", "g", "h", "f"})
	// Re-adding d draws no position: d becomes a modification at its base position.
	readdD := func() error {
		err := add("d")()
		assert.Nil(t, delta.data.nodeInfoList, "re-adding a deleted node should invalidate the list")
		return err
	}
	step("depth1 re-add d", nil, readdD, []string{"e", "b", "d", "g", "h", "f"})
	step("depth1 remove h", nil, remove("h"), []string{"e", "b", "d", "g", "f"})
	assert.NotNil(t, delta.data.nodeInfoList, "adding and removing added nodes should patch the list")
	assert.NotNil(t, delta.data.compactedNodeIndex, "adding and removing added nodes should keep the index")

	// Depth 2, committed into depth 1, whose list is built. Commit inserts depth 2's added nodes in
	// their order (j, i): j goes to 1 and i to 0 among g, f.
	delta.Fork()
	step("depth2 add i", []int{0}, add("i"), []string{"e", "b", "d", "g", "f", "i"})
	step("depth2 add j at 0", []int{0}, add("j"), []string{"e", "b", "d", "g", "f", "j", "i"})
	step("commit depth2 into depth1", []int{1, 0}, delta.Commit, []string{"e", "b", "d", "i", "j", "f", "g"})
	assert.NotNil(t, delta.data.nodeInfoList, "committing additions should patch the list")

	// Commit into the root: a is removed from the root (swapping d into its slot), d is replaced in
	// place, then i, j, f and g are inserted at 0, 4 (the end), 2 and 6 (the end).
	step("commit depth1 into root", []int{0, 4, 2, 6}, delta.Commit, []string{"i", "b", "f", "e", "j", "d", "g"})
	for name, nodeInfo := range nodeInfos {
		if name == "a" || name == "c" || name == "h" {
			continue
		}
		current, err := delta.NodeInfos().Get(name)
		assert.NoError(t, err)
		assert.Same(t, nodeInfo, current, "root should hold the latest NodeInfo of %s", name)
	}
}

// TestDeltaSnapshotStoreGhostNodeKeepsLayerDeletionFree pins the scale-down simulation pattern
// (RemovalSimulator.replaceWithTaintedGhostNode): replacing a node with a same-named ghost leaves the
// layer without deletions, so the ghost keeps the node's position, pods can be rescheduled with the
// list patched in place, and no compactedNodeIndex is built.
func TestDeltaSnapshotStoreGhostNodeKeepsLayerDeletionFree(t *testing.T) {
	ctx := context.Background()
	delta := NewDeltaSnapshotStore()
	for i := 0; i < 10; i++ {
		assert.NoError(t, delta.StoreNodeInfo(buildTestNodeInfo(fmt.Sprintf("node-%d", i))))
	}
	delta.Fork()
	list, err := delta.NodeInfos().List()
	assert.NoError(t, err)
	order := nodeNamesInOrder(list)
	candidate := order[3]

	delta.Fork()
	assert.NoError(t, delta.RemoveNodeInfo(ctx, candidate))
	ghost := buildTestNodeInfo(candidate)
	assert.NoError(t, delta.StoreNodeInfo(ghost))
	assert.Empty(t, delta.data.deletedNodeInfos, "replacing a node with a ghost should leave no deletions")
	assert.Same(t, ghost, delta.data.modifiedNodeInfoMap[candidate])
	for j, target := range order[4:7] {
		assert.NoError(t, delta.StorePodInfo(framework.NewPodInfo(test.BuildTestPod(fmt.Sprintf("pod-%d", j), 100, 1024), nil), target))
		list, err = delta.NodeInfos().List()
		assert.NoError(t, err)
		assert.Equal(t, order, nodeNamesInOrder(list), "the ghost should keep the node's position")
		assert.Nil(t, delta.data.compactedNodeIndex, "a layer without deletions should not build an index")
		assertDeltaInternalsConsistent(t, fmt.Sprintf("after rescheduling pod %d", j), delta)
	}
	idx, found := delta.data.getNodeIndex(candidate)
	assert.True(t, found)
	assert.Equal(t, 3, idx)
	assert.Same(t, ghost, list[idx])

	// Removing the ghost hides the base node, and the commit carries the deletion into the outer layer.
	assert.NoError(t, delta.RemoveNodeInfo(ctx, candidate))
	assertDeltaInternalsConsistent(t, "after removing the ghost", delta)
	assert.NoError(t, delta.Commit())
	assert.True(t, delta.data.deletedNodeInfos[candidate])
	list, err = delta.NodeInfos().List()
	assert.NoError(t, err)
	expected := append(append([]string{}, order[:3]...), order[4:]...)
	assert.Equal(t, expected, nodeNamesInOrder(list))
	for _, target := range order[4:7] {
		current, err := delta.NodeInfos().Get(target)
		assert.NoError(t, err)
		assert.Len(t, current.GetPods(), 1, "rescheduled pods should be committed onto %s", target)
	}
	assertDeltaInternalsConsistent(t, "after commit", delta)
}

// countGroups counts nodes named "<group>-<node>" per group.
func countGroups(list []schedulerinterface.NodeInfo) map[string]int {
	counts := map[string]int{}
	for _, ni := range list {
		group, _, _ := strings.Cut(ni.Node().Name, "-")
		counts[group]++
	}
	return counts
}

// TestDeltaSnapshotStoreSpreadsGroupedNodes verifies that nodes added grouped (as SetClusterState
// callers often do, e.g. by node group or zone) aren't listed grouped. It uses a seeded source, so
// the result is deterministic.
func TestDeltaSnapshotStoreSpreadsGroupedNodes(t *testing.T) {
	const groups, nodesPerGroup = 10, 100
	delta := newDeltaSnapshotStore(rand.New(rand.NewPCG(1, 2)).IntN)
	for g := range groups {
		for i := range nodesPerGroup {
			assert.NoError(t, delta.StoreNodeInfo(buildTestNodeInfo(fmt.Sprintf("group%d-node%d", g, i))))
		}
	}
	list, err := delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Len(t, list, groups*nodesPerGroup)
	firstGroups := countGroups(list[:nodesPerGroup])
	assert.Greater(t, len(firstGroups), groups/2, "the first %d listed nodes should come from many groups, got %v", nodesPerGroup, firstGroups)
	assertDeltaInternalsConsistent(t, "root", delta)

	// Nodes added grouped in a fork follow the base nodes in random order, and are spread over the
	// base list when committed.
	delta.Fork()
	for g := range 2 {
		for i := range nodesPerGroup {
			assert.NoError(t, delta.StoreNodeInfo(buildTestNodeInfo(fmt.Sprintf("batch%d-node%d", g, i))))
		}
	}
	list, err = delta.NodeInfos().List()
	assert.NoError(t, err)
	added := list[groups*nodesPerGroup:]
	assert.Len(t, added, 2*nodesPerGroup)
	assert.Len(t, countGroups(added[:nodesPerGroup]), 2, "the first %d nodes added in the fork should come from both batches", nodesPerGroup)
	assertDeltaInternalsConsistent(t, "fork", delta)

	assert.NoError(t, delta.Commit())
	list, err = delta.NodeInfos().List()
	assert.NoError(t, err)
	assert.Len(t, list, (groups+2)*nodesPerGroup)
	committedInFirstHalf := 0
	for _, ni := range list[:len(list)/2] {
		if strings.HasPrefix(ni.Node().Name, "batch") {
			committedInFirstHalf++
		}
	}
	// About nodesPerGroup expected (half of the 2*nodesPerGroup committed nodes); 0 if appended.
	assert.Greater(t, committedInFirstHalf, nodesPerGroup/2, "committed nodes should be spread over the root list")
	assertDeltaInternalsConsistent(t, "after commit", delta)
}
