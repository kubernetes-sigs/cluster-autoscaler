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
	"errors"
	"fmt"
	"math/rand/v2"

	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	"k8s.io/klog/v2"
	schedulerinterface "k8s.io/kube-scheduler/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

var (
	errorGettingPodGroupState          = errors.New("PodGroupState is not integrated with CA simulator")
	errorGettingPodGroup               = errors.New("PodGroup is not integrated with CA simulator")
	errorGettingCompositePodGroupState = errors.New("CompositePodGroupState is not integrated with CA simulator")
	errorGettingCompositePodGroup      = errors.New("CompositePodGroup is not integrated with CA simulator")
)

// DeltaSnapshotStore is an implementation of ClusterSnapshotStore optimized for typical Cluster Autoscaler usage - (fork, add stuff, revert), repeated many times per loop.
//
// Complexity of some notable operations:
//
//	fork - O(1)
//	revert - O(1)
//	commit - O(n)
//	list all pods (no filtering) - O(n), cached
//	list all pods (with filtering) - O(n)
//	list node infos - O(n), cached; node additions, removals of nodes added
//	in the current delta and first modification of a node within a delta
//	patch the cached list in O(1); the root snapshot lists its nodes directly
//	remove node added in the current delta - O(1)
//
// Node order is pseudo-random, similar to map iteration order: nodes added to a layer are inserted
// at random positions among the nodes added to that layer, and follow the nodes of its base.
// Modifications (including committed ones) keep node positions, and re-adding a node deleted in
// the same layer turns it into a modification at its original position. Callers must not rely on
// any particular order.
//
// Watch out for:
//
// * Deletions of nodes that exist in the base, and re-adding them - invalidate the node info list
// cache of the current snapshot (when forked affects delta, but not base.) Deltas with such
// deletions also need an O(n) name->position index of their base nodes, built lazily; other
// deltas don't.
//
// * Pod additions & deletions - invalidate pod-derived caches (affinity lists, PVC usage)
// of the current snapshot, but not the node info list.
//
// * The slices returned by NodeInfos().List() and the HavePodsWith*List() methods are read-only
// and valid only until the next change to the snapshot. They share memory with internal caches,
// so callers must not modify them, and must not rely on their contents after a change
// (entries may or may not be updated in place). Copy the slice if it needs to outlive a change.
//
// * Pod affinity - causes scheduler framework to list pods with non-empty selector,
// so basic caching doesn't help.
//
// * DRA objects are tracked in the separate snapshot and while they don't exactly share
// memory and time complexities of DeltaSnapshotStore - they are optimized for
// cluster autoscaler operations
type DeltaSnapshotStore struct {
	data     *internalDeltaSnapshotData
	randIntN func(n int) int // see internalDeltaSnapshotData.randIntN
}

type deltaSnapshotStoreNodeLister DeltaSnapshotStore
type deltaSnapshotStoreStorageLister DeltaSnapshotStore
type deltaSnapshotPodGroupStateLister DeltaSnapshotStore
type deltaSnapshotPodGroupLister DeltaSnapshotStore
type deltaSnapshotCompositePodGroupStateLister DeltaSnapshotStore
type deltaSnapshotCompositePodGroupLister DeltaSnapshotStore

type internalDeltaSnapshotData struct {
	baseData *internalDeltaSnapshotData

	// addedNodeInfos holds nodes added in this layer, in pseudo-random order (see
	// insertAddedNodeInfo); addedNodeIndex maps their names to positions in addedNodeInfos.
	// Added, modified and deleted nodes are disjoint sets: added nodes aren't visible in the base,
	// while modified and deleted ones are.
	addedNodeInfos      []schedulerinterface.NodeInfo
	addedNodeIndex      map[string]int
	modifiedNodeInfoMap map[string]schedulerinterface.NodeInfo
	deletedNodeInfos    map[string]bool

	// nodeInfoList caches the NodeInfos visible in this layer: the base list with modified entries
	// patched in place and deleted entries dropped, followed by addedNodeInfos. Nil when not built.
	// Always nil in the root layer, which has no modifications or deletions (only nodes visible in
	// a base can be modified or deleted), so it lists addedNodeInfos directly.
	nodeInfoList []schedulerinterface.NodeInfo
	// compactedNodeIndex maps the names of surviving base nodes to their positions in nodeInfoList.
	// Only layers with deletions use it, since only their lists stop preserving base positions;
	// added nodes are always resolved via addedNodeIndex (see getNodeIndex).
	// Invariant: non-nil only when nodeInfoList is non-nil, and then it indexes exactly
	// nodeInfoList[:len(nodeInfoList)-len(addedNodeInfos)].
	compactedNodeIndex map[string]int

	havePodsWithAffinity                          []schedulerinterface.NodeInfo
	havePodsWithRequiredAntiAffinity              []schedulerinterface.NodeInfo
	havePodsWithRequiredNonHostScopedAntiAffinity []schedulerinterface.NodeInfo
	pvcNamespaceMap                               map[string]int

	// randIntN returns a pseudo-random int in [0, n). It picks the positions of added nodes
	// (see insertAddedNodeInfo). Shared by all layers of a store.
	randIntN func(n int) int
}

// nodeLocation describes where a node is stored, as seen from a single snapshot layer.
type nodeLocation int

const (
	// nodeAbsent - the node isn't visible in this layer: it doesn't exist in this layer or any of its bases.
	nodeAbsent nodeLocation = iota
	// nodeDeleted - the node exists in the base chain, but was deleted in this layer.
	nodeDeleted
	// nodeAdded - the node was added in this layer and isn't visible in its base. (Re-adding a node
	// deleted in this layer makes it nodeModified.)
	nodeAdded
	// nodeModified - the node exists in the base chain and this layer holds its modified copy.
	nodeModified
	// nodeInBase - the node exists in the base chain and wasn't touched by this layer.
	nodeInBase
)

// locateNode resolves where nodeName is stored from the perspective of this layer. The returned
// NodeInfo is non-nil for nodeAdded, nodeModified (the layer's copy) and nodeInBase (the base's copy).
func (data *internalDeltaSnapshotData) locateNode(nodeName string) (schedulerinterface.NodeInfo, nodeLocation) {
	if idx, found := data.addedNodeIndex[nodeName]; found {
		return data.addedNodeInfos[idx], nodeAdded
	}
	if nodeInfo, found := data.modifiedNodeInfoMap[nodeName]; found {
		return nodeInfo, nodeModified
	}
	if data.deletedNodeInfos[nodeName] {
		return nil, nodeDeleted
	}
	if nodeInfo, found := data.baseData.getNodeInfo(nodeName); found {
		return nodeInfo, nodeInBase
	}
	return nil, nodeAbsent
}

func newInternalDeltaSnapshotData(randIntN func(n int) int) *internalDeltaSnapshotData {
	return &internalDeltaSnapshotData{
		addedNodeIndex:      make(map[string]int),
		modifiedNodeInfoMap: make(map[string]schedulerinterface.NodeInfo),
		deletedNodeInfos:    make(map[string]bool),
		randIntN:            randIntN,
	}
}

func (data *internalDeltaSnapshotData) getNodeInfo(name string) (schedulerinterface.NodeInfo, bool) {
	if data == nil {
		return nil, false
	}
	nodeInfo, _ := data.locateNode(name)
	return nodeInfo, nodeInfo != nil
}

func (data *internalDeltaSnapshotData) getNodeInfoList() []schedulerinterface.NodeInfo {
	if data == nil {
		return nil
	}
	if data.baseData == nil {
		// The root layer has no modifications or deletions, so its list is exactly addedNodeInfos.
		return data.addedNodeInfos
	}
	if data.nodeInfoList == nil {
		data.nodeInfoList = data.buildNodeInfoList()
	}
	return data.nodeInfoList
}

// nodeCount returns the number of nodes visible in this layer, i.e. len(getNodeInfoList()),
// without building any lists.
func (data *internalDeltaSnapshotData) nodeCount() int {
	if data == nil {
		return 0
	}
	// Every deleted node exists in the base chain, so each deletion hides exactly one base node.
	return data.baseData.nodeCount() - len(data.deletedNodeInfos) + len(data.addedNodeInfos)
}

// getNodeIndex returns the position of nodeName in getNodeInfoList().
//
// Every layer lays out its list as the surviving base nodes (base.nodeCount() - len(deleted) of
// them) followed by addedNodeInfos, so added nodes are resolved arithmetically. A layer without
// deletions keeps its base's positions, so base nodes are resolved by the base. Only layers with
// deletions build compactedNodeIndex, covering their surviving base nodes. This relies on the base
// list not changing while this layer exists, which holds because a base is only mutated when this
// layer is committed into it (after which this layer is discarded).
func (data *internalDeltaSnapshotData) getNodeIndex(nodeName string) (int, bool) {
	if data == nil {
		return 0, false
	}
	// Added nodes aren't visible in the base, so they're resolved here.
	if idx, found := data.addedNodeIndex[nodeName]; found {
		return data.baseData.nodeCount() - len(data.deletedNodeInfos) + idx, true
	}
	if len(data.deletedNodeInfos) == 0 {
		return data.baseData.getNodeIndex(nodeName)
	}
	if data.compactedNodeIndex == nil {
		list := data.getNodeInfoList()
		baseNodes := list[:len(list)-len(data.addedNodeInfos)]
		data.compactedNodeIndex = make(map[string]int, len(baseNodes))
		for i, ni := range baseNodes {
			data.compactedNodeIndex[ni.Node().Name] = i
		}
	}
	// Deleted nodes aren't in the index, so they're reported as not found.
	idx, found := data.compactedNodeIndex[nodeName]
	return idx, found
}

// Contains costly copying throughout the struct chain. Use wisely.
//
// Every node in modifiedNodeInfoMap exists in the base chain (and so in the base list): nodes are
// only put there by nodeInfoToModify, commitModifiedNodeInfo and addNodeInfo (re-adding a node
// deleted in this layer), all of which require it.
func (data *internalDeltaSnapshotData) buildNodeInfoList() []schedulerinterface.NodeInfo {
	baseList := data.baseData.getNodeInfoList()
	totalLen := len(baseList) + len(data.addedNodeInfos) - len(data.deletedNodeInfos)
	var nodeInfoList []schedulerinterface.NodeInfo

	if len(data.deletedNodeInfos) > 0 {
		nodeInfoList = make([]schedulerinterface.NodeInfo, 0, totalLen)
		for _, bni := range baseList {
			if data.deletedNodeInfos[bni.Node().Name] {
				continue
			}
			if mni, found := data.modifiedNodeInfoMap[bni.Node().Name]; found {
				nodeInfoList = append(nodeInfoList, mni)
				continue
			}
			nodeInfoList = append(nodeInfoList, bni)
		}
	} else {
		nodeInfoList = make([]schedulerinterface.NodeInfo, len(baseList), totalLen)
		copy(nodeInfoList, baseList)
		for name, mni := range data.modifiedNodeInfoMap {
			if idx, found := data.baseData.getNodeIndex(name); found {
				nodeInfoList[idx] = mni
			}
		}
	}

	return append(nodeInfoList, data.addedNodeInfos...)
}

// appendToNodeInfoList appends nodeInfo to the cached list, if it's built. compactedNodeIndex
// doesn't cover added nodes, so it needs no update.
func (data *internalDeltaSnapshotData) appendToNodeInfoList(nodeInfo schedulerinterface.NodeInfo) {
	if data.nodeInfoList == nil {
		return
	}
	data.nodeInfoList = append(data.nodeInfoList, nodeInfo)
}

// replaceInNodeInfoList swaps the cached list entry for nodeInfo's node, keeping its position.
// No-op if the list isn't built.
func (data *internalDeltaSnapshotData) replaceInNodeInfoList(nodeInfo schedulerinterface.NodeInfo) {
	if data.nodeInfoList == nil {
		return
	}
	idx, found := data.getNodeIndex(nodeInfo.Node().Name)
	if !found {
		// Unreachable as long as callers only replace nodes present in this layer. Fall back to
		// rebuilding rather than serving a list that is missing the node.
		data.invalidateNodeInfoList()
		return
	}
	data.nodeInfoList[idx] = nodeInfo
}

func (data *internalDeltaSnapshotData) invalidateNodeInfoList() {
	data.nodeInfoList = nil
	data.compactedNodeIndex = nil
}

func (data *internalDeltaSnapshotData) addNodeInfo(nodeInfo schedulerinterface.NodeInfo) error {
	nodeName := nodeInfo.Node().Name
	switch _, location := data.locateNode(nodeName); location {
	case nodeAbsent:
		data.insertAddedNodeInfo(nodeInfo)
	case nodeDeleted:
		// Re-adding a node deleted in this layer turns it back into a modification of the base's
		// node, at its original position. Scale-down simulations do this when they replace a node
		// with a ghost copy; this keeps their layer free of deletions, so that its list keeps the
		// base's positions and needs no compactedNodeIndex. Un-hiding the base node shifts the
		// positions of the base nodes after it, so the list is rebuilt.
		delete(data.deletedNodeInfos, nodeName)
		data.modifiedNodeInfoMap[nodeName] = nodeInfo
		data.invalidateNodeInfoList()
	default:
		return fmt.Errorf("node %s already in snapshot", nodeName)
	}

	if len(nodeInfo.GetPods()) > 0 {
		data.clearPodCaches()
	}

	return nil
}

// swapAddedNodeInfos swaps the nodes added in this layer at positions i and j, in addedNodeInfos,
// addedNodeIndex and the suffix of the cached list, which mirrors addedNodeInfos.
func (data *internalDeltaSnapshotData) swapAddedNodeInfos(i, j int) {
	if i == j {
		return
	}
	added := data.addedNodeInfos
	added[i], added[j] = added[j], added[i]
	data.addedNodeIndex[added[i].Node().Name] = i
	data.addedNodeIndex[added[j].Node().Name] = j
	if data.nodeInfoList != nil {
		offset := len(data.nodeInfoList) - len(added)
		data.nodeInfoList[offset+i], data.nodeInfoList[offset+j] = added[i], added[j]
	}
}

// insertAddedNodeInfo adds nodeInfo at a uniformly random position among the nodes added in this
// layer, moving the node previously there to the end ("inside-out" Fisher-Yates), in O(1).
// Callers rely on nodes being listed in a pseudo-random order: nodes are often added grouped
// (e.g. by node group or zone), and listing them grouped makes scheduling simulations go through
// whole groups before finding a matching node.
func (data *internalDeltaSnapshotData) insertAddedNodeInfo(nodeInfo schedulerinterface.NodeInfo) {
	pos := data.randIntN(len(data.addedNodeInfos) + 1)
	data.addedNodeIndex[nodeInfo.Node().Name] = len(data.addedNodeInfos)
	data.addedNodeInfos = append(data.addedNodeInfos, nodeInfo)
	data.appendToNodeInfoList(nodeInfo)
	data.swapAddedNodeInfos(pos, len(data.addedNodeInfos)-1)
}

// removeAddedNodeInfo removes nodeName from addedNodeInfos in O(1) by swapping it with the last
// added node and truncating, and applies the same move to the cached list, whose suffix is
// addedNodeInfos. Freed slots aren't cleared, since previously returned slices may share them
// (in the root layer, List() returns addedNodeInfos itself).
func (data *internalDeltaSnapshotData) removeAddedNodeInfo(nodeName string) {
	last := len(data.addedNodeInfos) - 1
	data.swapAddedNodeInfos(data.addedNodeIndex[nodeName], last)
	data.addedNodeInfos = data.addedNodeInfos[:last]
	if data.nodeInfoList != nil {
		data.nodeInfoList = data.nodeInfoList[:len(data.nodeInfoList)-1]
	}
	delete(data.addedNodeIndex, nodeName)
}

func (data *internalDeltaSnapshotData) clearCaches() {
	data.invalidateNodeInfoList()
	data.clearPodCaches()
}

func (data *internalDeltaSnapshotData) clearPodCaches() {
	data.havePodsWithAffinity = nil
	data.havePodsWithRequiredAntiAffinity = nil
	data.havePodsWithRequiredNonHostScopedAntiAffinity = nil
	// TODO: update the cache when adding/removing pods instead of invalidating the whole cache
	data.pvcNamespaceMap = nil
}

func (data *internalDeltaSnapshotData) removeNodeInfo(nodeName string) error {
	nodeInfo, location := data.locateNode(nodeName)
	switch location {
	case nodeAbsent, nodeDeleted:
		return clustersnapshot.ErrNodeNotFound
	case nodeAdded:
		// Drop the change. Added nodes aren't visible in the base, so there's nothing to hide.
		data.removeAddedNodeInfo(nodeName)
		// Pod-derived caches can only refer to the node if it has pods.
		if len(nodeInfo.GetPods()) > 0 {
			data.clearPodCaches()
		}
		return nil
	case nodeModified:
		// Drop the local copy and hide the base node.
		delete(data.modifiedNodeInfoMap, nodeName)
		data.deletedNodeInfos[nodeName] = true
	case nodeInBase:
		data.deletedNodeInfos[nodeName] = true
	}

	// Hiding a base node shifts the positions of the base nodes after it, so the list is rebuilt.
	data.clearCaches()
	return nil
}

// nodeInfoToModify returns this layer's own copy of nodeName, cloning it from the base if needed.
func (data *internalDeltaSnapshotData) nodeInfoToModify(nodeName string) (schedulerinterface.NodeInfo, bool) {
	nodeInfo, location := data.locateNode(nodeName)
	switch location {
	case nodeAdded, nodeModified:
		return nodeInfo, true
	case nodeInBase:
		dni := nodeInfo.Snapshot()
		data.modifiedNodeInfoMap[nodeName] = dni
		data.replaceInNodeInfoList(dni)
		data.clearPodCaches()
		return dni, true
	default:
		return nil, false
	}
}

func (data *internalDeltaSnapshotData) addPodInfo(podInfo schedulerinterface.PodInfo, nodeName string) error {
	ni, found := data.nodeInfoToModify(nodeName)
	if !found {
		return clustersnapshot.ErrNodeNotFound
	}

	ni.AddPodInfo(podInfo)

	data.clearPodCaches()
	return nil
}

func findPodInfo(nodeInfo schedulerinterface.NodeInfo, namespace, name string) schedulerinterface.PodInfo {
	for _, podInfo := range nodeInfo.GetPods() {
		if podInfo.GetPod().Namespace == namespace && podInfo.GetPod().Name == name {
			return podInfo
		}
	}
	return nil
}

func (data *internalDeltaSnapshotData) removePod(namespace, name, nodeName string) error {
	// Validate against the current copy first, so that a failed removal doesn't clone the node.
	current, found := data.getNodeInfo(nodeName)
	if !found {
		return clustersnapshot.ErrNodeNotFound
	}
	podInfo := findPodInfo(current, namespace, name)
	if podInfo == nil {
		return fmt.Errorf("pod %s/%s not in snapshot", namespace, name)
	}

	// The clone shares PodInfos with current, and RemovePod matches pods by UID anyway.
	// The node is guaranteed to exist because it was checked via getNodeInfo.
	ni, _ := data.nodeInfoToModify(nodeName)
	if err := ni.RemovePod(klog.Background(), podInfo.GetPod()); err != nil {
		return fmt.Errorf("cannot remove pod; %v", err)
	}

	data.clearPodCaches()
	return nil
}

func (data *internalDeltaSnapshotData) isPVCUsedByPods(key string) bool {
	if data.pvcNamespaceMap != nil {
		return data.pvcNamespaceMap[key] > 0
	}
	nodeInfos := data.getNodeInfoList()
	pvcNamespaceMap := make(map[string]int)
	for _, v := range nodeInfos {
		for k, i := range v.GetPVCRefCounts() {
			pvcNamespaceMap[k] += i
		}
	}
	data.pvcNamespaceMap = pvcNamespaceMap
	return data.pvcNamespaceMap[key] > 0
}

func (data *internalDeltaSnapshotData) fork() *internalDeltaSnapshotData {
	forkedData := newInternalDeltaSnapshotData(data.randIntN)
	forkedData.baseData = data
	return forkedData
}

// commitModifiedNodeInfo stores nodeInfo, a modified copy of a node visible in this layer,
// keeping the node's position in the cached list.
func (data *internalDeltaSnapshotData) commitModifiedNodeInfo(nodeInfo schedulerinterface.NodeInfo) error {
	nodeName := nodeInfo.Node().Name
	switch _, location := data.locateNode(nodeName); location {
	case nodeAbsent, nodeDeleted:
		return clustersnapshot.ErrNodeNotFound
	case nodeAdded:
		data.addedNodeInfos[data.addedNodeIndex[nodeName]] = nodeInfo
	case nodeModified, nodeInBase:
		data.modifiedNodeInfoMap[nodeName] = nodeInfo
	}

	data.replaceInNodeInfoList(nodeInfo)
	data.clearPodCaches()
	return nil
}

func (data *internalDeltaSnapshotData) commit() (*internalDeltaSnapshotData, error) {
	if data.baseData == nil {
		// do nothing as in basic snapshot.
		return data, nil
	}
	// Removing a node added in the base swaps the base's last added node into the freed slot,
	// which keeps the base's added nodes in pseudo-random order.
	for node := range data.deletedNodeInfos {
		if err := data.baseData.removeNodeInfo(node); err != nil {
			return nil, err
		}
	}
	for _, node := range data.modifiedNodeInfoMap {
		if err := data.baseData.commitModifiedNodeInfo(node); err != nil {
			return nil, err
		}
	}
	// Nodes added here that the base deleted become modifications in the base, at their original
	// positions (see addNodeInfo).
	for _, node := range data.addedNodeInfos {
		if err := data.baseData.addNodeInfo(node); err != nil {
			return nil, err
		}
	}

	return data.baseData, nil
}

// List returns list of all node infos.
func (snapshot *deltaSnapshotStoreNodeLister) List() ([]schedulerinterface.NodeInfo, error) {
	return snapshot.data.getNodeInfoList(), nil
}

// HavePodsWithAffinityList returns list of all node infos with pods that have affinity constrints.
func (snapshot *deltaSnapshotStoreNodeLister) HavePodsWithAffinityList() ([]schedulerinterface.NodeInfo, error) {
	data := snapshot.data
	if data.havePodsWithAffinity != nil {
		return data.havePodsWithAffinity, nil
	}

	nodeInfoList := snapshot.data.getNodeInfoList()
	havePodsWithAffinityList := make([]schedulerinterface.NodeInfo, 0, len(nodeInfoList))
	for _, node := range nodeInfoList {
		if len(node.GetPodsWithAffinity()) > 0 {
			havePodsWithAffinityList = append(havePodsWithAffinityList, node)
		}
	}
	data.havePodsWithAffinity = havePodsWithAffinityList
	return data.havePodsWithAffinity, nil
}

// HavePodsWithRequiredAntiAffinityList returns the list of NodeInfos of nodes with pods with required anti-affinity terms.
func (snapshot *deltaSnapshotStoreNodeLister) HavePodsWithRequiredAntiAffinityList() ([]schedulerinterface.NodeInfo, error) {
	data := snapshot.data
	if data.havePodsWithRequiredAntiAffinity != nil {
		return data.havePodsWithRequiredAntiAffinity, nil
	}

	nodeInfoList := snapshot.data.getNodeInfoList()
	havePodsWithRequiredAntiAffinityList := make([]schedulerinterface.NodeInfo, 0, len(nodeInfoList))
	for _, node := range nodeInfoList {
		if len(node.GetPodsWithRequiredAntiAffinity()) > 0 {
			havePodsWithRequiredAntiAffinityList = append(havePodsWithRequiredAntiAffinityList, node)
		}
	}
	data.havePodsWithRequiredAntiAffinity = havePodsWithRequiredAntiAffinityList
	return data.havePodsWithRequiredAntiAffinity, nil
}

// HavePodsWithRequiredNonHostScopedAntiAffinityList returns nodes containing pods that require a wider topology scan (topologyKey other than hostname).
func (snapshot *deltaSnapshotStoreNodeLister) HavePodsWithRequiredNonHostScopedAntiAffinityList() ([]schedulerinterface.NodeInfo, error) {
	data := snapshot.data
	if data.havePodsWithRequiredNonHostScopedAntiAffinity != nil {
		return data.havePodsWithRequiredNonHostScopedAntiAffinity, nil
	}

	nodeInfoList := snapshot.data.getNodeInfoList()
	havePodsWithRequiredNonHostScopedAntiAffinityList := make([]schedulerinterface.NodeInfo, 0, len(nodeInfoList))
	for _, node := range nodeInfoList {
		if len(node.GetPodsWithRequiredNonHostScopedAntiAffinity()) > 0 {
			havePodsWithRequiredNonHostScopedAntiAffinityList = append(havePodsWithRequiredNonHostScopedAntiAffinityList, node)
		}
	}
	data.havePodsWithRequiredNonHostScopedAntiAffinity = havePodsWithRequiredNonHostScopedAntiAffinityList
	return data.havePodsWithRequiredNonHostScopedAntiAffinity, nil
}

// Get returns node info by node name.
func (snapshot *deltaSnapshotStoreNodeLister) Get(nodeName string) (schedulerinterface.NodeInfo, error) {
	return (*DeltaSnapshotStore)(snapshot).getNodeInfo(nodeName)
}

// IsPVCUsedByPods returns if PVC is used by pods
func (snapshot *deltaSnapshotStoreStorageLister) IsPVCUsedByPods(key string) bool {
	return (*DeltaSnapshotStore)(snapshot).IsPVCUsedByPods(key)
}

// Get returns pod group state by namespace and pod group name.
//
// This method is never supposed to be called in the cluster autoscaler simulations
// until PodGroups are integrated with cluster autoscaler.
func (snapshot *deltaSnapshotPodGroupStateLister) Get(namespace string, podGroupName string) (schedulerinterface.PodGroupState, error) {
	return nil, errorGettingPodGroupState
}

// This method is never supposed to be called in the cluster autoscaler simulations
// until PodGroups are integrated with cluster autoscaler.
func (snapshot *deltaSnapshotPodGroupLister) Get(namespace string, podGroupName string) (*schedulingv1beta1.PodGroup, error) {
	return nil, errorGettingPodGroup
}

// This method is never supposed to be called in the cluster autoscaler simulations
// until CompositePodGroups are integrated with cluster autoscaler.
func (snapshot *deltaSnapshotCompositePodGroupStateLister) Get(namespace string, name string) (schedulerinterface.CompositePodGroupState, error) {
	return nil, errorGettingCompositePodGroupState
}

// This method is never supposed to be called in the cluster autoscaler simulations
// until CompositePodGroups are integrated with cluster autoscaler.
func (snapshot *deltaSnapshotCompositePodGroupLister) Get(namespace string, name string) (*schedulingv1alpha3.CompositePodGroup, error) {
	return nil, errorGettingCompositePodGroup
}

func (snapshot *DeltaSnapshotStore) getNodeInfo(nodeName string) (schedulerinterface.NodeInfo, error) {
	data := snapshot.data
	node, found := data.getNodeInfo(nodeName)
	if !found {
		return nil, clustersnapshot.ErrNodeNotFound
	}
	return node, nil
}

// NodeInfos returns node lister.
func (snapshot *DeltaSnapshotStore) NodeInfos() schedulerinterface.NodeInfoLister {
	return (*deltaSnapshotStoreNodeLister)(snapshot)
}

// StorageInfos returns storage lister
func (snapshot *DeltaSnapshotStore) StorageInfos() schedulerinterface.StorageInfoLister {
	return (*deltaSnapshotStoreStorageLister)(snapshot)
}

// PodGroupStates returns pod group state lister.
func (snapshot *DeltaSnapshotStore) PodGroupStates() schedulerinterface.PodGroupStateLister {
	return (*deltaSnapshotPodGroupStateLister)(snapshot)
}

// PodGroups returns pod group lister.
func (snapshot *DeltaSnapshotStore) PodGroups() schedulerinterface.PodGroupLister {
	return (*deltaSnapshotPodGroupLister)(snapshot)
}

// CompositePodGroupStates returns composite pod group state lister.
func (snapshot *DeltaSnapshotStore) CompositePodGroupStates() schedulerinterface.CompositePodGroupStateLister {
	return (*deltaSnapshotCompositePodGroupStateLister)(snapshot)
}

// CompositePodGroups returns composite pod group lister.
func (snapshot *DeltaSnapshotStore) CompositePodGroups() schedulerinterface.CompositePodGroupLister {
	return (*deltaSnapshotCompositePodGroupLister)(snapshot)
}

// NewDeltaSnapshotStore creates instances of DeltaSnapshotStore.
func NewDeltaSnapshotStore() *DeltaSnapshotStore {
	return newDeltaSnapshotStore(rand.IntN)
}

// newDeltaSnapshotStore creates a DeltaSnapshotStore that uses randIntN to pick the positions of
// added nodes, which allows tests to make node order deterministic.
func newDeltaSnapshotStore(randIntN func(n int) int) *DeltaSnapshotStore {
	snapshot := &DeltaSnapshotStore{randIntN: randIntN}
	snapshot.Clear()
	return snapshot
}

// RemoveNodeInfo removes nodes (and pods scheduled to it) from the snapshot.
func (snapshot *DeltaSnapshotStore) RemoveNodeInfo(ctx context.Context, nodeName string) error {
	return snapshot.data.removeNodeInfo(nodeName)
}

// StoreNodeInfo adds the given *framework.NodeInfo to the snapshot without checking scheduler predicates.
func (snapshot *DeltaSnapshotStore) StoreNodeInfo(nodeInfo *framework.NodeInfo) error {
	return snapshot.data.addNodeInfo(nodeInfo)
}

// StorePodInfo adds pod to the snapshot and schedules it to given node.
func (snapshot *DeltaSnapshotStore) StorePodInfo(podInfo *framework.PodInfo, nodeName string) error {
	return snapshot.data.addPodInfo(podInfo, nodeName)
}

// RemovePodInfo removes pod from the snapshot.
func (snapshot *DeltaSnapshotStore) RemovePodInfo(namespace, podName, nodeName string) error {
	return snapshot.data.removePod(namespace, podName, nodeName)
}

// IsPVCUsedByPods returns if the pvc is used by any pod
func (snapshot *DeltaSnapshotStore) IsPVCUsedByPods(key string) bool {
	return snapshot.data.isPVCUsedByPods(key)
}

// Fork creates a fork of snapshot state. All modifications can later be reverted to moment of forking via Revert()
// Time: O(1)
func (snapshot *DeltaSnapshotStore) Fork() {
	snapshot.data = snapshot.data.fork()
}

// Revert reverts snapshot state to moment of forking.
// Time: O(1)
func (snapshot *DeltaSnapshotStore) Revert() {
	if snapshot.data.baseData != nil {
		snapshot.data = snapshot.data.baseData
	}
}

// Commit commits changes done after forking.
// Time: O(n), where n = size of delta (number of nodes added, modified or deleted since forking)
func (snapshot *DeltaSnapshotStore) Commit() error {
	newData, err := snapshot.data.commit()
	if err != nil {
		return err
	}
	snapshot.data = newData
	return nil
}

// Clear reset cluster snapshot to empty, unforked state
// Time: O(1)
func (snapshot *DeltaSnapshotStore) Clear() {
	snapshot.data = newInternalDeltaSnapshotData(snapshot.randIntN)
}
