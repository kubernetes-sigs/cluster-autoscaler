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

package besteffortatomic

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupset"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/provreq"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
)

func TestBatchOutcomes(t *testing.T) {
	failure := cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OutOfResourcesErrorClass, ErrorCode: "STOCKOUT"}
	for _, tc := range []struct {
		name string
		run  func(env *outcomesTestEnv)
		want []limiterCall
	}{
		{
			name: "growth waits for the batch's nodes",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 8, 8, env.resizes(env.resize(env.a, 8)), true)
				env.outcomes.settle(env.ctx)
				env.arrive(env.a, 7)
				env.outcomes.settle(env.ctx)
				env.assertCalls(nil, "nodes are still upcoming")
				env.arrive(env.a, 1)
				env.outcomes.settle(env.ctx)
				env.outcomes.settle(env.ctx)
			},
			want: []limiterCall{{"success", 8}},
		},
		{
			name: "batches are confirmed one by one while the node group keeps scaling up",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 4, 4, env.resizes(env.resize(env.a, 4)), true)
				env.outcomes.track(env.request, 2, 2, env.resizes(env.resize(env.a, 2)), true)
				env.arrive(env.a, 4)
				env.outcomes.settle(env.ctx)
				env.assertCalls([]limiterCall{{"success", 4}}, "the first batch's nodes arrived")
				env.arrive(env.a, 2)
				env.outcomes.settle(env.ctx)
			},
			want: []limiterCall{{"success", 4}, {"success", 2}},
		},
		{
			name: "every resized node group must deliver",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 8, 8, env.resizes(env.resize(env.a, 4), env.resize(env.b, 4)), true)
				env.arrive(env.a, 4)
				env.outcomes.settle(env.ctx)
				env.assertCalls(nil, "node group b's nodes are still upcoming")
				env.arrive(env.b, 4)
				env.outcomes.settle(env.ctx)
			},
			want: []limiterCall{{"success", 8}},
		},
		{
			name: "a delayed failure halves the batch",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 8, 8, env.resizes(env.resize(env.a, 8)), true)
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.a, 8, failure, env.clock.Now())
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.a, 8, failure, env.clock.Now())
				env.arrive(env.a, 8)
				env.outcomes.settle(env.ctx)
			},
			want: []limiterCall{{"remember", 4}},
		},
		{
			name: "a batch that may not grow still halves on a delayed failure",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 5, 5, env.resizes(env.resize(env.a, 5)), false)
				env.outcomes.track(env.request, 6, 6, env.resizes(env.resize(env.b, 6)), false)
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.a, 5, failure, env.clock.Now())
				env.arrive(env.b, 6)
				env.outcomes.settle(env.ctx)
			},
			want: []limiterCall{{"remember", 3}},
		},
		{
			name: "the class's own failures don't stop following the node group",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 8, 8, env.resizes(env.resize(env.a, 8)), true)
				env.outcomes.track(env.request, 4, 4, env.resizes(env.resize(env.b, 4)), true)
				env.outcomes.setBusy(true)
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.a, 8, failure, env.clock.Now())
				env.outcomes.setBusy(false)
				// Backing node group a off hides its upcoming nodes, as if they had arrived.
				env.upcoming.backedOff[env.a.Id()] = true
				env.arrive(env.b, 4)
				env.outcomes.settle(env.ctx)
				env.assertCalls([]limiterCall{{"success", 4}}, "node group a's nodes can't be confirmed while it's backed off")
				env.upcoming.backedOff[env.a.Id()] = false
				env.outcomes.settle(env.ctx)
				env.assertCalls([]limiterCall{{"success", 4}}, "node group a's nodes are still upcoming")
				env.arrive(env.a, 8)
				env.outcomes.settle(env.ctx)
			},
			want: []limiterCall{{"success", 4}, {"success", 8}},
		},
		{
			name: "a failure only counts against batches the node group can no longer provide nodes for",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 4, 4, env.resizes(env.resize(env.a, 4)), true)
				env.outcomes.track(env.request, 6, 6, env.resizes(env.resize(env.a, 6)), true)
				// Six of node group a's eleven nodes fail to be created, so it can still reach five.
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.a, 6, failure, env.clock.Now())
				env.assertCalls([]limiterCall{{"remember", 3}}, "only the later batch can't receive all of its nodes")
				env.upcoming.backedOff[env.a.Id()] = true
				env.arrive(env.a, 4)
				env.outcomes.settle(env.ctx)
				env.assertCalls([]limiterCall{{"remember", 3}}, "nothing is confirmed while the node group is backed off")
				env.upcoming.backedOff[env.a.Id()] = false
				env.outcomes.settle(env.ctx)
			},
			want: []limiterCall{{"remember", 3}, {"success", 4}},
		},
		{
			name: "a failure counts against every batch waiting for the node group if its size is unknown",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 4, 4, env.resizes(env.resize(env.a, 4)), true)
				env.outcomes.track(env.request, 6, 6, env.resizes(env.resize(env.a, 6)), true)
				env.outcomes.RegisterFailedScaleUp(env.ctx, unknownSizeNodeGroup{env.a}, 6, failure, env.clock.Now())
			},
			want: []limiterCall{{"remember", 2}, {"remember", 3}},
		},
		{
			name: "a failure of an unknown number of nodes counts against every batch waiting for the node group",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 4, 4, env.resizes(env.resize(env.a, 4)), true)
				env.outcomes.track(env.request, 6, 6, env.resizes(env.resize(env.a, 6)), true)
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.a, 0, failure, env.clock.Now())
			},
			want: []limiterCall{{"remember", 2}, {"remember", 3}},
		},
		{
			name: "failures of other node groups are ignored",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 8, 8, env.resizes(env.resize(env.a, 8)), true)
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.b, 8, failure, env.clock.Now())
			},
		},
		{
			name: "a single request doesn't shrink the remembered size",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 1, 1, env.resizes(env.resize(env.a, 1)), true)
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.a, 1, failure, env.clock.Now())
			},
		},
		{
			name: "a single request with many pods doesn't shrink the remembered size",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 1, 50, env.resizes(env.resize(env.a, 50)), true)
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.a, 50, failure, env.clock.Now())
			},
		},
		{
			name: "a delayed failure halves the batch's pods",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 2, 9, env.resizes(env.resize(env.a, 9)), true)
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.a, 9, failure, env.clock.Now())
			},
			want: []limiterCall{{"remember", 5}},
		},
		{
			name: "growth counts the batch's pods",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 2, 9, env.resizes(env.resize(env.a, 9)), true)
				env.arrive(env.a, 9)
				env.outcomes.settle(env.ctx)
			},
			want: []limiterCall{{"success", 9}},
		},
		{
			name: "a batch followed for too long is forgotten",
			run: func(env *outcomesTestEnv) {
				env.outcomes.track(env.request, 8, 8, env.resizes(env.resize(env.a, 8)), true)
				env.clock.Step(pendingBatchTTL)
				env.outcomes.settle(env.ctx)
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.a, 8, failure, env.clock.Now())
				env.arrive(env.a, 8)
				env.outcomes.settle(env.ctx)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newOutcomesTestEnv(t)
			tc.run(env)
			env.assertCalls(tc.want, "")
		})
	}
}

func TestBatchOutcomesAreBounded(t *testing.T) {
	env := newOutcomesTestEnv(t)
	for i := 0; i < maxPendingBatches+1; i++ {
		env.outcomes.track(env.request, 2, 2, env.resizes(env.resize(env.a, 1)), true)
	}
	assert.Len(t, env.outcomes.pending, maxPendingBatches)
	assert.Equal(t, uint64(2), env.outcomes.pending[0].id, "the oldest batch is dropped")
}

// TestBatchOutcomesConcurrentFailures checks that failures reported from other goroutines, such as
// parallel scale-ups or node group initialization, are safe while the main loop settles batches.
func TestBatchOutcomesConcurrentFailures(t *testing.T) {
	env := newOutcomesTestEnv(t)
	injector := provreq.NewProvisioningRequestPodsInjectorWithOptions(nil, provreq.PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             100,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    100,
		KubeClientBurst:                 1,
	})
	env.outcomes.limits = injector
	for i := 0; i < 50; i++ {
		env.outcomes.track(env.request, 64, 64, []nodegroupset.ScaleUpInfo{{Group: env.a, CurrentSize: 1, NewSize: 2}}, true)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				env.outcomes.RegisterFailedScaleUp(env.ctx, env.a, 1, cloudprovider.InstanceErrorInfo{}, env.clock.Now())
				env.outcomes.track(env.request, 8, 8, []nodegroupset.ScaleUpInfo{{Group: env.a, CurrentSize: 1, NewSize: 2}}, true)
			}
		}()
	}
	for i := 0; i < 20; i++ {
		env.outcomes.settle(env.ctx)
		injector.RecordBestEffortAtomicBatchSuccess(env.ctx, env.request, 1, 1)
	}
	wg.Wait()
	assert.LessOrEqual(t, len(env.outcomes.pending), maxPendingBatches)
}

type outcomesTestEnv struct {
	t        *testing.T
	ctx      context.Context
	outcomes *batchOutcomes
	limiter  *recordingLimiter
	upcoming *fakeUpcomingNodes
	clock    *clocktesting.FakeClock
	a, b     *testprovider.TestNodeGroup
	request  *provreqwrapper.ProvisioningRequest
}

func newOutcomesTestEnv(t *testing.T) *outcomesTestEnv {
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup("a", 0, 1000, 1)
	provider.AddNodeGroup("b", 0, 1000, 1)
	env := &outcomesTestEnv{
		t:        t,
		ctx:      t.Context(),
		limiter:  &recordingLimiter{},
		upcoming: newFakeUpcomingNodes(),
		clock:    clocktesting.NewFakeClock(time.Now()),
		a:        provider.GetNodeGroup("a").(*testprovider.TestNodeGroup),
		b:        provider.GetNodeGroup("b").(*testprovider.TestNodeGroup),
		request: provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
			Name: "request", CPU: "100m", Memory: "1", PodCount: 1, Class: v1.ProvisioningClassBestEffortAtomicScaleUp,
		}),
	}
	env.outcomes = newBatchOutcomes(env.limiter, env.upcoming, env.clock)
	return env
}

// resize simulates a cloud provider accepting an increase of the node group by delta: its target
// size grows right away, and the new nodes are upcoming until they arrive.
func (env *outcomesTestEnv) resize(group *testprovider.TestNodeGroup, delta int) nodegroupset.ScaleUpInfo {
	size, err := group.TargetSize(env.ctx)
	assert.NoError(env.t, err)
	group.SetTargetSize(size + delta)
	env.upcoming.counts[group.Id()] += delta
	return nodegroupset.ScaleUpInfo{Group: group, CurrentSize: size, NewSize: size + delta}
}

func (env *outcomesTestEnv) resizes(resizes ...nodegroupset.ScaleUpInfo) []nodegroupset.ScaleUpInfo {
	return resizes
}

// arrive simulates count upcoming nodes of the node group coming up.
func (env *outcomesTestEnv) arrive(group *testprovider.TestNodeGroup, count int) {
	env.upcoming.counts[group.Id()] -= count
}

func (env *outcomesTestEnv) assertCalls(want []limiterCall, msg string) {
	env.t.Helper()
	assert.Equal(env.t, want, env.limiter.calls, msg)
}

// fakeUpcomingNodes reports upcoming nodes like cluster state, which omits node groups without any
// and backed-off node groups.
type fakeUpcomingNodes struct {
	counts    map[string]int
	backedOff map[string]bool
}

func newFakeUpcomingNodes() *fakeUpcomingNodes {
	return &fakeUpcomingNodes{counts: map[string]int{}, backedOff: map[string]bool{}}
}

func (f *fakeUpcomingNodes) GetUpcomingNodes(context.Context) (map[string]int, map[string][]string) {
	upcoming := maps.Clone(f.counts)
	maps.DeleteFunc(upcoming, func(id string, count int) bool { return count <= 0 || f.backedOff[id] })
	return upcoming, nil
}

func (f *fakeUpcomingNodes) BackoffStatusForNodeGroup(_ context.Context, nodeGroup cloudprovider.NodeGroup, _ time.Time) backoff.Status {
	return backoff.Status{IsBackedOff: f.backedOff[nodeGroup.Id()]}
}

// unknownSizeNodeGroup is a node group whose target size can't be read.
type unknownSizeNodeGroup struct {
	cloudprovider.NodeGroup
}

func (unknownSizeNodeGroup) TargetSize(context.Context) (int, error) {
	return 0, fmt.Errorf("target size unavailable")
}
