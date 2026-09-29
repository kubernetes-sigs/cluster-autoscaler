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

package occupancy_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	config "k8s.io/kubernetes/pkg/scheduler/apis/config"
	latest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/extender/occupancy"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func pod(name string) *v1.Pod {
	p := testutils.BuildTestPod(name, 100, 100)
	p.UID = types.UID(name)
	p.Spec.Containers[0].Resources.Requests["example.com/slots"] = resource.MustParse("1")
	return p
}

func TestOpaqueMetadataDoesNotRequireVendorAllocation(t *testing.T) {
	requests := make(chan occupancy.Request, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var request occupancy.Request
		require.NoError(t, json.Unmarshal(body, &request))
		require.NoError(t, occupancy.Validate(request))
		requests <- request
		result := occupancy.Result{ExtenderFilterResult: extenderv1.ExtenderFilterResult{Nodes: &v1.NodeList{}, FailedNodes: extenderv1.FailedNodesMap{}}, Simulation: &occupancy.Ack{Version: occupancy.Version, Digest: fmt.Sprintf("%x", sha256.Sum256(body))}}
		for _, node := range request.Nodes.Items {
			// This independent evaluator owns its opaque assignment semantics.
			// One occupied slot plus the candidate exceeds this fixture's inventory.
			require.Equal(t, "opaque-inventory", node.Annotations["example.com/inventory"])
			for _, resident := range request.Simulation.Nodes[node.Name] {
				require.Equal(t, "opaque-assignment", resident.Pod.Annotations["example.com/assignment"])
			}
			if len(request.Simulation.Nodes[node.Name]) == 0 {
				result.Nodes.Items = append(result.Nodes.Items, node)
			} else {
				result.FailedNodes[node.Name] = "occupied slot"
			}
		}
		require.NoError(t, json.NewEncoder(w).Encode(result))
	}))
	t.Cleanup(srv.Close)
	cfg, err := latest.Default()
	require.NoError(t, err)
	cfg.Extenders = []config.Extender{{URLPrefix: srv.URL, FilterVerb: "filter", ManagedResources: []config.ExtenderManagedResource{{Name: "example.com/slots", IgnoredByScheduler: true}}}}
	handle, err := framework.NewHandle(context.Background(), informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cfg, false, false)
	require.NoError(t, err)
	require.NoError(t, handle.ConfigureExtenderOccupancy(cfg, []string{srv.URL}))
	snapshot := predicate.NewPredicateSnapshot(store.NewBasicSnapshotStore(), handle, false, 1, false, 0)
	node := testutils.BuildTestNode("node", 10000, 1000000)
	node.Annotations = map[string]string{"example.com/inventory": "opaque-inventory"}
	require.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(node)))
	resident := testutils.BuildTestPod("resident", 100, 100)
	resident.UID = "resident"
	resident.Spec.NodeName = node.Name
	resident.Spec.Containers[0].Resources.Requests["example.com/slots"] = resource.MustParse("1")
	resident.Annotations = map[string]string{"example.com/assignment": "opaque-assignment"}
	require.NoError(t, snapshot.ForceAddPod(resident, node.Name))
	candidate := resident.DeepCopy()
	candidate.Name, candidate.UID, candidate.Spec.NodeName = "candidate", "candidate", ""
	require.Error(t, snapshot.CheckPredicates(candidate, node.Name))
	request := <-requests
	require.True(t, equality.Semantic.DeepEqual(resident, request.Simulation.Nodes[node.Name][0].Pod))
	require.Equal(t, resident.Annotations, request.Simulation.Nodes[node.Name][0].Pod.Annotations)
	require.Equal(t, occupancy.Preserve, request.Simulation.Nodes[node.Name][0].Allocation.Mode)
	require.Equal(t, node.Annotations, request.Nodes.Items[0].Annotations)
	require.NoError(t, snapshot.ForceRemovePod(resident.Namespace, resident.Name, node.Name))
	require.NoError(t, snapshot.CheckPredicates(candidate, node.Name))
	require.Empty(t, (<-requests).Simulation.Nodes[node.Name])
}
func newSnapshot(t *testing.T, factory func() clustersnapshot.ClusterSnapshotStore, url string) (clustersnapshot.ClusterSnapshot, *framework.Handle) {
	cfg, err := latest.Default()
	require.NoError(t, err)
	cfg.Extenders = []config.Extender{{URLPrefix: url, FilterVerb: "filter", ManagedResources: []config.ExtenderManagedResource{{Name: "example.com/slots", IgnoredByScheduler: true}}}}
	handle, err := framework.NewHandle(context.Background(), informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cfg, false, false)
	require.NoError(t, err)
	require.NoError(t, handle.ConfigureExtenderOccupancy(cfg, []string{url}))
	s := predicate.NewPredicateSnapshot(factory(), handle, false, 1, false, 0)
	require.NoError(t, s.AddNodeInfo(framework.NewTestNodeInfo(testutils.BuildTestNode("node", 10000, 1000000))))
	return s, handle
}
func server(t *testing.T, alter func(*occupancy.Result)) (*httptest.Server, chan occupancy.Request) {
	requests := make(chan occupancy.Request, 32)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var request occupancy.Request
		if err := json.Unmarshal(body, &request); err != nil {
			t.Error(err)
			http.Error(w, "invalid request", 400)
			return
		}
		requests <- request
		result := occupancy.Result{ExtenderFilterResult: extenderv1.ExtenderFilterResult{Nodes: request.Nodes}, Simulation: &occupancy.Ack{Version: occupancy.Version, Digest: fmt.Sprintf("%x", sha256.Sum256(body))}}
		if alter != nil {
			alter(&result)
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	t.Cleanup(srv.Close)
	return srv, requests
}
func TestPlacementFollowsSnapshotLifecycle(t *testing.T) {
	for name, factory := range map[string]func() clustersnapshot.ClusterSnapshotStore{"basic": func() clustersnapshot.ClusterSnapshotStore { return store.NewBasicSnapshotStore() }, "delta": func() clustersnapshot.ClusterSnapshotStore { return store.NewDeltaSnapshotStore() }} {
		t.Run(name, func(t *testing.T) {
			srv, requests := server(t, nil)
			s, _ := newSnapshot(t, factory, srv.URL)
			resident := pod("resident")
			resident.Spec.NodeName = "node"
			resident.Annotations = map[string]string{"example.com/assignment": "opaque-assignment"}
			require.NoError(t, s.ForceAddPod(resident, "node"))
			require.NoError(t, s.CheckPredicates(pod("probe"), "node"))
			require.Equal(t, occupancy.Preserve, (<-requests).Simulation.Nodes["node"][0].Allocation.Mode)
			s.Fork()
			require.NoError(t, s.UnschedulePod(resident.Namespace, resident.Name, "node"))
			moved := resident.DeepCopy()
			moved.Spec.NodeName = ""
			require.NoError(t, s.SchedulePod(moved, "node"))
			<-requests
			require.NoError(t, s.CheckPredicates(pod("probe"), "node"))
			require.Equal(t, occupancy.Allocate, (<-requests).Simulation.Nodes["node"][0].Allocation.Mode)
			copy, err := s.GetNodeInfo("node")
			require.NoError(t, err)
			require.True(t, copy.DeepCopy().Pods()[0].SimulatedPlacement)
			s.Revert()
			require.NoError(t, s.CheckPredicates(pod("probe"), "node"))
			require.Equal(t, occupancy.Preserve, (<-requests).Simulation.Nodes["node"][0].Allocation.Mode)
			s.Fork()
			require.NoError(t, s.ForceRemovePod(resident.Namespace, resident.Name, "node"))
			require.NoError(t, s.Commit())
			require.NoError(t, s.CheckPredicates(pod("probe"), "node"))
			require.Empty(t, (<-requests).Simulation.Nodes["node"])
			require.Equal(t, "node", resident.Spec.NodeName)
		})
	}
}
func TestInvalidOrLegacyResponseCannotPassIgnoredResources(t *testing.T) {
	cases := map[string]func(*occupancy.Result){
		"legacy":        func(r *occupancy.Result) { r.Simulation = nil },
		"version":       func(r *occupancy.Result) { r.Simulation.Version = "unknown" },
		"digest":        func(r *occupancy.Result) { r.Simulation.Digest = "wrong" },
		"missing node":  func(r *occupancy.Result) { r.Nodes.Items = nil },
		"unknown node":  func(r *occupancy.Result) { r.Nodes.Items[0].Name = "other" },
		"contradictory": func(r *occupancy.Result) { r.FailedNodes = extenderv1.FailedNodesMap{"node": "failed"} },
	}
	for name, alter := range cases {
		t.Run(name, func(t *testing.T) {
			srv, _ := server(t, alter)
			s, _ := newSnapshot(t, func() clustersnapshot.ClusterSnapshotStore { return store.NewBasicSnapshotStore() }, srv.URL)
			require.Error(t, s.SchedulePod(pod("probe"), "node"))
			n, err := s.GetNodeInfo("node")
			require.NoError(t, err)
			require.Empty(t, n.Pods())
		})
	}
}
func TestUnknownPlacementFailsBeforeHTTP(t *testing.T) {
	srv, requests := server(t, nil)
	s, _ := newSnapshot(t, func() clustersnapshot.ClusterSnapshotStore { return store.NewBasicSnapshotStore() }, srv.URL)
	require.NoError(t, s.ForceAddPod(pod("unknown"), "node"))
	require.Error(t, s.CheckPredicates(pod("probe"), "node"))
	require.Empty(t, requests)
}

func TestNominatedPodIsReallocated(t *testing.T) {
	srv, requests := server(t, nil)
	s, _ := newSnapshot(t, func() clustersnapshot.ClusterSnapshotStore { return store.NewDeltaSnapshotStore() }, srv.URL)
	nominated := pod("nominated")
	nominated.Status.NominatedNodeName = "node"
	node := testutils.BuildTestNode("node", 10000, 1000000)
	require.NoError(t, s.SetClusterState(context.Background(), []*v1.Node{node}, []*v1.Pod{nominated}, nil, nil))
	require.NoError(t, s.CheckPredicates(pod("probe"), "node"))
	require.Equal(t, occupancy.Allocate, (<-requests).Simulation.Nodes["node"][0].Allocation.Mode)
}

func TestOccupancyIsExplicitlyOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			srv, requests := server(t, nil)
			cfg, err := latest.Default()
			require.NoError(t, err)
			cfg.Extenders = []config.Extender{{URLPrefix: srv.URL, FilterVerb: "filter", ManagedResources: []config.ExtenderManagedResource{{Name: "example.com/slots", IgnoredByScheduler: true}}}}
			handle, err := framework.NewHandle(context.Background(), informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cfg, false, false)
			require.NoError(t, err)
			var urls []string
			if enabled {
				urls = []string{srv.URL}
			}
			require.NoError(t, handle.ConfigureExtenderOccupancy(cfg, urls))
			snapshot := predicate.NewPredicateSnapshot(store.NewBasicSnapshotStore(), handle, false, 1, false, 0)
			require.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(testutils.BuildTestNode("node", 10000, 1000000))))
			require.NoError(t, snapshot.CheckPredicates(pod("probe"), "node"))
			request := <-requests
			if enabled {
				require.NotNil(t, request.Simulation)
			} else {
				require.Nil(t, request.Simulation)
			}
		})
	}
}

func TestOccupancyConfigurationRejectsUnsafeSelection(t *testing.T) {
	for _, mode := range []string{"unknown URL", "ignorable", "no filter", "no resources"} {
		t.Run(mode, func(t *testing.T) {
			cfg, err := latest.Default()
			require.NoError(t, err)
			cfg.Extenders = []config.Extender{{URLPrefix: "http://extender.invalid", FilterVerb: "filter", ManagedResources: []config.ExtenderManagedResource{{Name: "example.com/slots", IgnoredByScheduler: true}}}}
			url := cfg.Extenders[0].URLPrefix
			switch mode {
			case "unknown URL":
				url = "http://other.invalid"
			case "ignorable":
				cfg.Extenders[0].Ignorable = true
			case "no filter":
				cfg.Extenders[0].FilterVerb = ""
			case "no resources":
				cfg.Extenders[0].ManagedResources = nil
			}
			handle, err := framework.NewHandle(context.Background(), informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cfg, false, false)
			require.NoError(t, err)
			require.Error(t, handle.ConfigureExtenderOccupancy(cfg, []string{url}))
		})
	}
}

func TestTemplateCopyDoesNotPreserveRealAllocations(t *testing.T) {
	srv, requests := server(t, nil)
	s, _ := newSnapshot(t, func() clustersnapshot.ClusterSnapshotStore { return store.NewDeltaSnapshotStore() }, srv.URL)
	resident := pod("daemon")
	resident.Spec.NodeName = "node"
	resident.Annotations = map[string]string{"example.com/assignment": "opaque-source-assignment"}
	require.NoError(t, s.ForceAddPod(resident, "node"))
	original, err := s.GetNodeInfo("node")
	require.NoError(t, err)
	template, err := simulator.SanitizedNodeInfo(context.Background(), original, "copy")
	require.NoError(t, err)
	require.NoError(t, s.AddNodeInfo(template))
	require.NoError(t, s.CheckPredicates(pod("probe"), template.Node().Name))
	request := <-requests
	require.Equal(t, occupancy.Allocate, request.Simulation.Nodes[template.Node().Name][0].Allocation.Mode)
	require.Equal(t, "opaque-source-assignment", resident.Annotations["example.com/assignment"])
}
