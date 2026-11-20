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

package orchestrator

import (
	stdcontext "context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/equivalence"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// Small value spaces so that random pods and nodes match each other often enough.
var (
	fuzzLabelValues = map[string][]string{
		benchInstanceTypeLabel:        {"type-a.large", "type-b.large", "type-c.large", "type-c.xlarge"},
		"example.com/cpu-arch":        {"amd64", "arm64"},
		"example.com/pool":            {"pool-a", "pool-b"},
		"topology.kubernetes.io/zone": {"a", "b"},
		"example.com/size":            {"1", "10", "100"},
	}
	fuzzTaintKeys    = []string{"node", "flavor", "dedicated"}
	fuzzTaintValues  = []string{"pool-a", "pool-b", ""}
	fuzzTaintEffects = []apiv1.TaintEffect{apiv1.TaintEffectNoSchedule, apiv1.TaintEffectNoExecute, apiv1.TaintEffectPreferNoSchedule}
)

func pick[T any](r *rand.Rand, xs []T) T { return xs[r.Intn(len(xs))] }

func fuzzLabelKeys() []string {
	keys := make([]string, 0, len(fuzzLabelValues))
	for k := range fuzzLabelValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func randomNode(r *rand.Rand, name string) *apiv1.Node {
	node := benchNode(name, pick(r, fuzzLabelValues[benchInstanceTypeLabel]))
	for _, key := range fuzzLabelKeys() {
		if key != benchInstanceTypeLabel && r.Intn(4) > 0 {
			node.Labels[key] = pick(r, fuzzLabelValues[key])
		}
	}
	for i := r.Intn(3); i > 0; i-- {
		node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{Key: pick(r, fuzzTaintKeys), Value: pick(r, fuzzTaintValues), Effect: pick(r, fuzzTaintEffects)})
	}
	if r.Intn(4) == 0 {
		node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{Key: "example.com/priority", Value: pick(r, []string{"1", "10", "100"}), Effect: pick(r, fuzzTaintEffects)})
	}
	node.Spec.Unschedulable = r.Intn(10) == 0
	return node
}

func randomRequirement(r *rand.Rand) apiv1.NodeSelectorRequirement {
	key := pick(r, fuzzLabelKeys())
	values := fuzzLabelValues[key]
	switch r.Intn(6) {
	case 0:
		return apiv1.NodeSelectorRequirement{Key: key, Operator: apiv1.NodeSelectorOpIn, Values: []string{pick(r, values)}}
	case 1:
		return apiv1.NodeSelectorRequirement{Key: key, Operator: apiv1.NodeSelectorOpNotIn, Values: []string{pick(r, values), pick(r, values)}}
	case 2:
		return apiv1.NodeSelectorRequirement{Key: key, Operator: apiv1.NodeSelectorOpExists}
	case 3:
		return apiv1.NodeSelectorRequirement{Key: key, Operator: apiv1.NodeSelectorOpDoesNotExist}
	case 4:
		op := pick(r, []apiv1.NodeSelectorOperator{apiv1.NodeSelectorOpGt, apiv1.NodeSelectorOpLt})
		return apiv1.NodeSelectorRequirement{Key: "example.com/size", Operator: op, Values: []string{pick(r, []string{"5", "50"})}}
	default:
		// Invalid requirement (In without values): parsing errors must be left to the framework.
		return apiv1.NodeSelectorRequirement{Key: key, Operator: apiv1.NodeSelectorOpIn}
	}
}

func randomPod(r *rand.Rand, name string, nodeNames []string) *apiv1.Pod {
	pod := BuildTestPod(name, 100, 1024)
	if r.Intn(20) == 0 {
		pod.Spec.NodeName = pick(r, nodeNames)
	}
	if r.Intn(3) == 0 {
		key := pick(r, fuzzLabelKeys())
		pod.Spec.NodeSelector = map[string]string{key: pick(r, fuzzLabelValues[key])}
	}
	if r.Intn(2) == 0 {
		var terms []apiv1.NodeSelectorTerm
		for t := 1 + r.Intn(2); t > 0; t-- {
			var term apiv1.NodeSelectorTerm
			for e := 1 + r.Intn(2); e > 0; e-- {
				term.MatchExpressions = append(term.MatchExpressions, randomRequirement(r))
			}
			if r.Intn(10) == 0 {
				term.MatchFields = []apiv1.NodeSelectorRequirement{{Key: "metadata.name", Operator: apiv1.NodeSelectorOpIn, Values: []string{pick(r, nodeNames)}}}
			}
			terms = append(terms, term)
		}
		pod.Spec.Affinity = &apiv1.Affinity{NodeAffinity: &apiv1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{NodeSelectorTerms: terms},
		}}
	}
	for i := r.Intn(3); i > 0; i-- {
		tol := apiv1.Toleration{Key: pick(r, fuzzTaintKeys), Effect: pick(r, append([]apiv1.TaintEffect{""}, fuzzTaintEffects...))}
		if r.Intn(5) == 0 {
			// Numeric comparison (TaintTolerationComparisonOperators gate).
			tol.Key = "example.com/priority"
			tol.Operator = pick(r, []apiv1.TolerationOperator{apiv1.TolerationOpGt, apiv1.TolerationOpLt})
			tol.Value = pick(r, []string{"5", "50"})
			pod.Spec.Tolerations = append(pod.Spec.Tolerations, tol)
			continue
		}
		if r.Intn(2) == 0 {
			tol.Operator = apiv1.TolerationOpExists
			if r.Intn(4) == 0 {
				tol.Key = "" // tolerates everything
			}
		} else {
			tol.Operator = apiv1.TolerationOpEqual
			tol.Value = pick(r, fuzzTaintValues)
		}
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, tol)
	}
	return pod
}

func isStaticPredicate(name string) bool {
	return name == "NodeAffinity" || name == "TaintToleration"
}

// TestStaticPredicatesErrorMatchesFramework checks, on random pods and nodes, that the
// pre-check rejects exactly what the framework rejects with TaintToleration / NodeAffinity,
// with the same error, with the taint comparison operators gate on and off.
func TestStaticPredicatesErrorMatchesFramework(t *testing.T) {
	for _, comparisonOperators := range []bool{false, true} {
		t.Run(fmt.Sprintf("TaintTolerationComparisonOperators=%v", comparisonOperators), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.TaintTolerationComparisonOperators, comparisonOperators)
			// The framework reads feature gates when it's built: build it after setting them.
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			logger := klog.Background()
			r := rand.New(rand.NewSource(42))
			rejected := 0
			for i := 0; i < 3000; i++ {
				node := randomNode(r, fmt.Sprintf("node-%d", i))
				pod := randomPod(r, fmt.Sprintf("pod-%d", i), []string{node.Name, "other-node"})
				require.NoError(t, snapshot.SetClusterState(stdcontext.Background(), []*apiv1.Node{node}, nil, nil, nil))

				got := staticPredicatesError(logger, pod, node)
				want := snapshot.CheckPredicates(pod, node.Name)

				if got != nil {
					rejected++
					if !assert.NotNil(t, want, "case %d: pre-check rejected a pod the framework accepts\npod=%+v\nnode=%+v", i, pod.Spec, node) {
						continue
					}
					assert.Equal(t, want.Error(), got.Error(), "case %d", i)
					continue
				}
				if want != nil && isStaticPredicate(want.FailingPredicateName()) && !hasMatchFields(pod) && !node.Spec.Unschedulable && pod.Spec.NodeName == "" {
					assert.True(t, hasInvalidNodeAffinity(pod), "case %d: framework rejected with %s (%v) but pre-check accepted\npod=%+v\nnode=%+v",
						i, want.FailingPredicateName(), want.FailingPredicateReasons(), pod.Spec, node)
				}
			}
			// Make sure the random inputs actually exercise both outcomes.
			assert.Greater(t, rejected, 500)
			assert.Less(t, rejected, 2500)
		})
	}
}

// hasInvalidNodeAffinity reports whether the pod's required node affinity can't be parsed.
func hasInvalidNodeAffinity(pod *apiv1.Pod) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil || pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, req := range term.MatchExpressions {
			if (req.Operator == apiv1.NodeSelectorOpIn || req.Operator == apiv1.NodeSelectorOpNotIn) && len(req.Values) == 0 {
				return true
			}
		}
	}
	return false
}

// oracleSchedulablePodGroups is SchedulablePodGroups without the pre-check: the behavior
// before it was added.
func oracleSchedulablePodGroups(t *testing.T, snapshot clustersnapshot.ClusterSnapshot, groups []*equivalence.PodGroup, nodeGroupID string, nodeInfo *framework.NodeInfo) []int {
	snapshot.Fork()
	defer snapshot.Revert()
	require.NoError(t, snapshot.AddNodeInfo(nodeInfo))
	var schedulable []int
	for i, eg := range groups {
		if err := snapshot.CheckPredicates(eg.Pods[0], nodeInfo.Node().Name); err == nil {
			schedulable = append(schedulable, i)
		} else {
			eg.SchedulingErrors[nodeGroupID] = err
		}
	}
	return schedulable
}

// TestSchedulablePodGroupsMatchesFramework compares SchedulablePodGroups with the
// pre-check-free implementation on random clusters, pod groups and templates: same
// schedulable groups, same failing predicates and reasons, snapshot left unchanged.
func TestSchedulablePodGroupsMatchesFramework(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for iter := 0; iter < 40; iter++ {
		snapshot, handle, err := testsnapshot.NewTestSnapshotAndHandle()
		require.NoError(t, err)
		var nodes []*apiv1.Node
		var pods []*apiv1.Pod
		for i := 0; i < 20; i++ {
			node := randomNode(r, fmt.Sprintf("node-%d", i))
			node.Spec.Taints = nil
			nodes = append(nodes, node)
			for j := 0; j < 3; j++ {
				pods = append(pods, BuildTestPod(fmt.Sprintf("p-%d-%d", i, j), 100, 1024, WithNodeName(node.Name), withHostnameAntiAffinity(fmt.Sprintf("app-%d", j))))
			}
		}
		require.NoError(t, snapshot.SetClusterState(stdcontext.Background(), nodes, pods, nil, nil))
		o := &ScaleUpOrchestrator{autoscalingCtx: &ca_context.AutoscalingContext{ClusterSnapshot: snapshot, FrameworkHandle: handle}}

		for tmpl := 0; tmpl < 5; tmpl++ {
			templateNode := randomNode(r, fmt.Sprintf("template-%d-%d", iter, tmpl))
			nodeInfo := framework.NewNodeInfo(templateNode, nil)
			nodeGroup := testprovider.NewTestNodeGroup("ng", 10, 0, 0, true, false, "", nil, nil)

			var newGroups, oracleGroups []*equivalence.PodGroup
			for g := 0; g < 6; g++ {
				pod := randomPod(r, fmt.Sprintf("pending-%d", g), []string{templateNode.Name})
				if r.Intn(2) == 0 {
					withHostnameAntiAffinity(fmt.Sprintf("app-%d", r.Intn(4)))(pod)
				}
				if r.Intn(4) == 0 {
					pod.Spec.Containers[0].Resources.Requests = BuildTestPod("big", 1_000_000, 1024).Spec.Containers[0].Resources.Requests
				}
				newGroups = append(newGroups, &equivalence.PodGroup{Pods: []*apiv1.Pod{pod}, SchedulingErrors: map[string]status.Reasons{}})
				oracleGroups = append(oracleGroups, &equivalence.PodGroup{Pods: []*apiv1.Pod{pod}, SchedulingErrors: map[string]status.Reasons{}})
			}

			before, err := snapshot.ListNodeInfos()
			require.NoError(t, err)
			got := o.SchedulablePodGroups(stdcontext.Background(), newGroups, nodeGroup, nodeInfo)
			after, err := snapshot.ListNodeInfos()
			require.NoError(t, err)
			assert.Equal(t, len(before), len(after), "snapshot must be left unchanged")

			want := oracleSchedulablePodGroups(t, snapshot, oracleGroups, nodeGroup.Id(), nodeInfo)
			require.Len(t, got, len(want), "iter %d template %d", iter, tmpl)
			for k, idx := range want {
				assert.Equal(t, newGroups[idx].Pods, got[k].Pods)
				assert.True(t, newGroups[idx].Schedulable)
				assert.Equal(t, []string{nodeGroup.Id()}, newGroups[idx].SchedulableGroups)
			}
			for g := range newGroups {
				gotErr, gotFound := newGroups[g].SchedulingErrors[nodeGroup.Id()]
				wantErr, wantFound := oracleGroups[g].SchedulingErrors[nodeGroup.Id()]
				if !assert.Equal(t, wantFound, gotFound, "iter %d template %d group %d", iter, tmpl, g) || !wantFound {
					continue
				}
				assert.Equal(t, wantErr.Reasons(), gotErr.Reasons(), "iter %d template %d group %d", iter, tmpl, g)
				assert.Equal(t, wantErr.(clustersnapshot.SchedulingError).FailingPredicateName(), gotErr.(clustersnapshot.SchedulingError).FailingPredicateName())
			}
		}
	}
}
