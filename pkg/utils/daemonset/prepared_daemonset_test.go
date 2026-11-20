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

package daemonset

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/daemon"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

var (
	fuzzKeys    = []string{"example.com/arch", "example.com/pool", "topology.kubernetes.io/zone"}
	fuzzValues  = []string{"a", "b", "c"}
	fuzzEffects = []apiv1.TaintEffect{apiv1.TaintEffectNoSchedule, apiv1.TaintEffectNoExecute, apiv1.TaintEffectPreferNoSchedule}
)

func pick[T any](r *rand.Rand, xs []T) T { return xs[r.Intn(len(xs))] }

func randomDaemonSetNode(r *rand.Rand, name string) *apiv1.Node {
	node := BuildTestNode(name, 1000, 1000)
	node.Labels = map[string]string{apiv1.LabelHostname: name}
	for _, k := range fuzzKeys {
		if r.Intn(3) > 0 {
			node.Labels[k] = pick(r, fuzzValues)
		}
	}
	for i := r.Intn(3); i > 0; i-- {
		node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{Key: pick(r, fuzzKeys), Value: pick(r, fuzzValues), Effect: pick(r, fuzzEffects)})
	}
	if r.Intn(10) == 0 {
		node.Spec.Unschedulable = true
		node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{Key: apiv1.TaintNodeUnschedulable, Effect: apiv1.TaintEffectNoSchedule})
	}
	return node
}

func randomDaemonSet(r *rand.Rand, i int, nodeNames []string) *appsv1.DaemonSet {
	ds := newDaemonSet(fmt.Sprintf("ds-%d", i), "0.1", "100M", nil)
	ds.UID = types.UID(fmt.Sprintf("uid-%d", i))
	ds.Generation = 1
	spec := &ds.Spec.Template.Spec
	if r.Intn(3) == 0 {
		spec.NodeSelector = map[string]string{pick(r, fuzzKeys): pick(r, fuzzValues)}
	}
	if r.Intn(2) == 0 {
		ops := []apiv1.NodeSelectorOperator{apiv1.NodeSelectorOpIn, apiv1.NodeSelectorOpNotIn, apiv1.NodeSelectorOpExists, apiv1.NodeSelectorOpDoesNotExist}
		var terms []apiv1.NodeSelectorTerm
		for t := 1 + r.Intn(2); t > 0; t-- {
			op := pick(r, ops)
			req := apiv1.NodeSelectorRequirement{Key: pick(r, fuzzKeys), Operator: op}
			if op == apiv1.NodeSelectorOpIn || op == apiv1.NodeSelectorOpNotIn {
				req.Values = []string{pick(r, fuzzValues)}
			}
			term := apiv1.NodeSelectorTerm{MatchExpressions: []apiv1.NodeSelectorRequirement{req}}
			if r.Intn(8) == 0 {
				term.MatchFields = []apiv1.NodeSelectorRequirement{{Key: "metadata.name", Operator: apiv1.NodeSelectorOpIn, Values: []string{pick(r, nodeNames)}}}
			}
			terms = append(terms, term)
		}
		spec.Affinity = &apiv1.Affinity{NodeAffinity: &apiv1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{NodeSelectorTerms: terms}}}
	}
	for t := r.Intn(3); t > 0; t-- {
		tol := apiv1.Toleration{Key: pick(r, fuzzKeys), Effect: pick(r, append([]apiv1.TaintEffect{""}, fuzzEffects...))}
		if r.Intn(2) == 0 {
			tol.Operator = apiv1.TolerationOpExists
		} else {
			tol.Operator, tol.Value = apiv1.TolerationOpEqual, pick(r, fuzzValues)
		}
		spec.Tolerations = append(spec.Tolerations, tol)
	}
	if r.Intn(15) == 0 {
		spec.NodeName = pick(r, nodeNames)
	}
	return ds
}

// TestNodeShouldRunDaemonPodMatchesController checks that the prepared check agrees with the
// DaemonSet controller's NodeShouldRunDaemonPod on random DaemonSets and nodes, including when
// DaemonSets change between calls.
func TestNodeShouldRunDaemonPodMatchesController(t *testing.T) {
	logger := klog.FromContext(context.Background())
	r := rand.New(rand.NewSource(1))
	var nodes []*apiv1.Node
	var names []string
	for i := 0; i < 50; i++ {
		node := randomDaemonSetNode(r, fmt.Sprintf("node-%d", i))
		nodes = append(nodes, node)
		names = append(names, node.Name)
	}
	var daemonSets []*appsv1.DaemonSet
	for i := 0; i < 60; i++ {
		daemonSets = append(daemonSets, randomDaemonSet(r, i, names))
	}
	cache := NewPodsForNodeCache()
	matches := 0
	for round := 0; round < 3; round++ {
		for _, ds := range daemonSets {
			for _, node := range nodes {
				want, _ := daemon.NodeShouldRunDaemonPod(logger, node, ds)
				prepared := cache.get(ds)
				got := nodeShouldRunDaemonPod(logger, node, ds, prepared)
				assert.Equal(t, want, got, "round %d ds %s node %s\nds=%+v\nnode=%+v", round, ds.Name, node.Name, ds.Spec.Template.Spec, node.Spec)
				assert.Equal(t, daemon.NewPod(ds, node.Name), prepared.newPod(ds, node.Name), "round %d ds %s node %s", round, ds.Name, node.Name)
				if want {
					matches++
				}
			}
		}
		// Change every other DaemonSet's spec, bumping its generation like the API server.
		for i, ds := range daemonSets {
			if i%2 == 0 {
				updated := randomDaemonSet(r, i, names)
				updated.Generation = ds.Generation + 1
				daemonSets[i] = updated
			}
		}
	}
	assert.Greater(t, matches, 1000)
	assert.Less(t, matches, 3*60*50-1000)
}

func TestPodsForNodeCache(t *testing.T) {
	cache := NewPodsForNodeCache()
	ds := newDaemonSet("ds", "0.1", "100M", map[string]string{"pool": "a"})
	ds.UID, ds.Generation = "uid", 1
	first := cache.get(ds)
	assert.Same(t, first, cache.get(ds), "same generation is reused")

	updated := ds.DeepCopy()
	updated.Generation = 2
	updated.Spec.Template.Spec.NodeSelector = map[string]string{"pool": "b"}
	second := cache.get(updated)
	assert.NotSame(t, first, second, "a new generation is prepared again")
	node := BuildTestNode("n", 1000, 1000)
	node.Labels = map[string]string{"pool": "b"}
	assert.Len(t, DaemonSetPodsForNode(context.Background(), node, []*appsv1.DaemonSet{updated}, cache), 1)

	noGeneration := ds.DeepCopy()
	noGeneration.Generation = 0
	assert.NotSame(t, cache.get(noGeneration), cache.get(noGeneration), "no generation: never cached")

	var disabled *PodsForNodeCache
	assert.NotSame(t, disabled.get(ds), disabled.get(ds), "nil cache: never cached")
	assert.Len(t, DaemonSetPodsForNode(context.Background(), node, []*appsv1.DaemonSet{updated}, nil), 1)
}

func TestDaemonSetPodsForNodeConcurrent(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	var daemonSets []*appsv1.DaemonSet
	for i := 0; i < 20; i++ {
		daemonSets = append(daemonSets, randomDaemonSet(r, 1000+i, []string{"n"}))
	}
	node := randomDaemonSetNode(r, "n")
	cache := NewPodsForNodeCache()
	want := len(DaemonSetPodsForNode(context.Background(), node, daemonSets, nil))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				assert.Len(t, DaemonSetPodsForNode(context.Background(), node, daemonSets, cache), want)
			}
		}()
	}
	wg.Wait()
}

// BenchmarkDaemonSetPodsForNode mirrors one loop of template building in a large cluster:
// every DaemonSet evaluated against every node group template.
func BenchmarkDaemonSetPodsForNode(b *testing.B) {
	r := rand.New(rand.NewSource(3))
	const nodeGroups, daemonSetCount = 220, 35
	var nodes []*apiv1.Node
	var names []string
	for i := 0; i < nodeGroups; i++ {
		node := randomDaemonSetNode(r, fmt.Sprintf("template-%d", i))
		nodes = append(nodes, node)
		names = append(names, node.Name)
	}
	var daemonSets []*appsv1.DaemonSet
	for i := 0; i < daemonSetCount; i++ {
		ds := randomDaemonSet(r, i, names)
		ds.Spec.Template.Spec.NodeName = ""
		daemonSets = append(daemonSets, ds)
	}
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		cache *PodsForNodeCache
	}{{name: "uncached"}, {name: "cached", cache: NewPodsForNodeCache()}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, node := range nodes {
					DaemonSetPodsForNode(ctx, node, daemonSets, tc.cache)
				}
			}
		})
	}
}
