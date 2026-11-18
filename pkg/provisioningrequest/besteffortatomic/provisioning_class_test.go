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
	goerrors "errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	provreqfake "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/clientset/versioned/fake"
	clienttesting "k8s.io/client-go/testing"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/orchestrator"
	"sigs.k8s.io/cluster-autoscaler/pkg/observers/nodegroupchange"
	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroups"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupset"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/provreq"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type attemptOutcome int

const (
	planRejection attemptOutcome = iota
	resizeRejection
	resizeWithUnknownOutcome
	partialSuccess
	scaleUpSuccess
	// resizeAccepted is a scale-up whose resize the cloud provider accepted, with nodes upcoming.
	resizeAccepted
)

func TestOriginalConstructor(t *testing.T) {
	var original func(*provreqclient.ProvisioningRequestClient) *bestEffortAtomicProvClass = New
	class := original(nil)
	assert.Nil(t, class.batchLimits)
	assert.Nil(t, class.awaiter)
	assert.Equal(t, config.DefaultBestEffortAtomicProvisioningRequestMaxBatchAttempts, class.maxBatchAttempts)
	assert.Equal(t, config.DefaultBestEffortAtomicProvisioningRequestMaxResizeAttempts, class.maxResizeAttempts)
	assert.Equal(t, config.DefaultBestEffortAtomicProvisioningRequestBatchTimebox, class.batchTimebox)
}

func TestBatchRetryBounds(t *testing.T) {
	for _, tc := range []struct {
		name           string
		requestCount   int
		outcome        attemptOutcome
		elapsed        time.Duration
		cancel         bool
		createdGroup   bool
		noHeldFailures bool
		noBatchLimits  bool
		// maxBatchAttempts, maxResizeAttempts and timebox override the defaults when set.
		maxBatchAttempts  int
		maxResizeAttempts int
		timebox           time.Duration
		wantSizes         []int
		wantNextBatch     int
	}{
		{name: "plan rejections stop at the attempt budget", requestCount: 256, outcome: planRejection, wantSizes: []int{256, 128, 64, 32, 16, 8, 4, 2}, wantNextBatch: 1},
		{name: "resize rejections stop at the resize budget", requestCount: 8, outcome: resizeRejection, wantSizes: []int{8, 4, 2, 1}, wantNextBatch: 1},
		{name: "exhausted search without a batch limiter", requestCount: 8, outcome: resizeRejection, noBatchLimits: true, wantSizes: []int{8, 4, 2, 1}},
		{name: "resize failure with unknown outcome is not retried", requestCount: 8, outcome: resizeWithUnknownOutcome, wantSizes: []int{8}, wantNextBatch: 4},
		{name: "resize rejection is not retried while failures can't be held", requestCount: 8, outcome: resizeRejection, noHeldFailures: true, wantSizes: []int{8}, wantNextBatch: 4},
		{name: "time budget", requestCount: 8, outcome: planRejection, elapsed: config.DefaultBestEffortAtomicProvisioningRequestBatchTimebox, wantSizes: []int{8}, wantNextBatch: 4},
		{name: "configured attempt budget", requestCount: 8, outcome: planRejection, maxBatchAttempts: 2, wantSizes: []int{8, 4}, wantNextBatch: 2},
		{name: "configured resize budget", requestCount: 8, outcome: resizeRejection, maxResizeAttempts: 2, wantSizes: []int{8, 4}, wantNextBatch: 2},
		{name: "configured time budget", requestCount: 8, outcome: planRejection, elapsed: 30 * time.Second, timebox: time.Minute, wantSizes: []int{8, 4}, wantNextBatch: 2},
		{name: "cancelled between attempts", requestCount: 8, outcome: planRejection, cancel: true, wantSizes: []int{8}, wantNextBatch: 4},
		{name: "node group already created", requestCount: 8, outcome: planRejection, createdGroup: true, wantSizes: []int{8}},
		{name: "single request remains indivisible", requestCount: 1, outcome: planRejection, wantSizes: []int{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			env := newBatchTestEnv(t, tc.requestCount)
			if tc.noHeldFailures {
				env.class.heldFailures = nil
			}
			if tc.noBatchLimits {
				env.class.batchLimits = nil
			}
			if tc.maxBatchAttempts > 0 {
				env.class.maxBatchAttempts = tc.maxBatchAttempts
			}
			if tc.maxResizeAttempts > 0 {
				env.class.maxResizeAttempts = tc.maxResizeAttempts
			}
			if tc.timebox > 0 {
				env.class.batchTimebox = tc.timebox
			}
			var sizes []int
			env.scaleUp = func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
				sizes = append(sizes, len(attemptPods))
				env.clock.SetTime(env.clock.Now().Add(tc.elapsed))
				if tc.cancel {
					cancel()
				}
				st, err := env.outcome(tc.outcome, attemptPods)
				if tc.createdGroup {
					st.CreateNodeGroupResults = []nodegroups.CreateNodeGroupResult{{}}
				}
				return st, err
			}

			result, scaleErr := env.class.Provision(ctx, env.pods, nil, nil, nil)
			require.NotNil(t, result)
			assert.Equal(t, tc.outcome != planRejection, scaleErr != nil)
			assert.Equal(t, tc.wantSizes, sizes)
			// Only the last attempted subset was evaluated; requests left out keep their conditions.
			attempted := tc.wantSizes[len(tc.wantSizes)-1]
			env.assertConditions(t, nil, env.requests[:attempted])
			assert.Len(t, result.PodsAwaitEvaluation, tc.requestCount-attempted)
			assert.Zero(t, env.snapshot.depth, "no snapshot fork should remain after Provision")
			if tc.wantNextBatch > 0 {
				for i := 0; i < 8; i++ {
					request := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
						Name: fmt.Sprintf("new-request-%d", i), CPU: "100m", Memory: "1", PodCount: 1,
						Class: v1.ProvisioningClassBestEffortAtomicScaleUp,
					})
					request.Spec.PodSets[0].PodTemplateRef = env.requests[0].Spec.PodSets[0].PodTemplateRef
					request.PodTemplates = env.requests[0].PodTemplates
					require.NoError(t, env.client.CreateProvisioningRequestForTesting(t.Context(), request))
				}
				batch, err := env.podsInjector.GetBestEffortAtomicBatch(t.Context(), tc.requestCount)
				require.NoError(t, err)
				assert.Len(t, batch, tc.wantNextBatch, "new arrivals must not restart the exhausted search")
			}
		})
	}
}

// TestInitializeAppliesBatchRetryOptions checks that the batch retry settings come from the
// autoscaling options, and that options that aren't set keep the defaults.
func TestInitializeAppliesBatchRetryOptions(t *testing.T) {
	batching := config.AutoscalingOptions{BestEffortAtomicBatchProcessing: true, BestEffortAtomicProvisioningRequestMaxBatchSize: 10}
	configured := batching
	configured.BestEffortAtomicProvisioningRequestMaxBatchAttempts = 3
	configured.BestEffortAtomicProvisioningRequestMaxResizeAttempts = 2
	configured.BestEffortAtomicProvisioningRequestBatchTimebox = 30 * time.Second
	for _, tc := range []struct {
		name               string
		options            config.AutoscalingOptions
		wantBatchAttempts  int
		wantResizeAttempts int
		wantTimebox        time.Duration
	}{
		{
			name:               "defaults",
			options:            batching,
			wantBatchAttempts:  config.DefaultBestEffortAtomicProvisioningRequestMaxBatchAttempts,
			wantResizeAttempts: config.DefaultBestEffortAtomicProvisioningRequestMaxResizeAttempts,
			wantTimebox:        config.DefaultBestEffortAtomicProvisioningRequestBatchTimebox,
		},
		{
			name:               "configured",
			options:            configured,
			wantBatchAttempts:  3,
			wantResizeAttempts: 2,
			wantTimebox:        30 * time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			class := NewWithPodsInjector(nil, nil)
			class.scaleUpOrchestrator = initializeOnlyOrchestrator{}
			processors := &ca_processors.AutoscalingProcessors{ScaleStateNotifier: nodegroupchange.NewNodeGroupChangeObserversList()}
			class.Initialize(&ca_context.AutoscalingContext{AutoscalingOptions: tc.options}, processors, nil, nil, taints.TaintConfig{}, nil, nil)

			assert.Equal(t, tc.wantBatchAttempts, class.maxBatchAttempts)
			assert.Equal(t, tc.wantResizeAttempts, class.maxResizeAttempts)
			assert.Equal(t, tc.wantTimebox, class.batchTimebox)
		})
	}
}

func TestBatchSkipsOldestRequestThatCannotFitAlone(t *testing.T) {
	for _, tc := range []struct {
		name      string
		podCounts []int32
		outcome   attemptOutcome
		// wantSizes are the pods of every attempt.
		wantSizes []int
	}{
		{name: "plan rejection", podCounts: []int32{1, 1, 1, 1, 1, 1, 1, 1}, outcome: planRejection, wantSizes: []int{8, 4, 2, 1, 7}},
		// The cloud provider rejected the oldest request's resize, and the requests behind it need fewer pods.
		{name: "resize rejection", podCounts: []int32{2, 1, 1, 1}, outcome: resizeRejection, wantSizes: []int{5, 3, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newBatchTestEnvWithPods(t, tc.podCounts...)
			oldest := env.requests[0].Name
			var sizes []int
			env.scaleUp = func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
				sizes = append(sizes, len(attemptPods))
				for _, pod := range attemptPods {
					if pod.OwnerReferences[0].Name == oldest {
						return env.outcome(tc.outcome, attemptPods)
					}
				}
				return env.outcome(scaleUpSuccess, attemptPods)
			}

			result, scaleErr := env.class.Provision(t.Context(), env.pods, nil, nil, nil)
			require.NoError(t, scaleErr)
			assert.Equal(t, status.ScaleUpSuccessful, result.Result)
			assert.Equal(t, tc.wantSizes, sizes)
			env.assertConditions(t, env.requests[1:], env.requests[:1])
			assert.Len(t, result.PodsRemainUnschedulable, 1, "the skipped request's pod remains unschedulable")
			assert.Zero(t, env.snapshot.depth, "no snapshot fork should remain after Provision")
		})
	}
}

// TestBatchStopsWhenNoSmallerRequestRemains checks that after the cloud provider rejected the
// resize for the oldest request on its own, the requests behind it are only tried in the same
// iteration if one of them needs fewer pods; any other would need at least as many nodes.
func TestBatchStopsWhenNoSmallerRequestRemains(t *testing.T) {
	env := newBatchTestEnv(t, 4)
	var sizes []int
	env.scaleUp = func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
		sizes = append(sizes, len(attemptPods))
		return env.outcome(resizeRejection, attemptPods)
	}
	result, scaleErr := env.class.Provision(t.Context(), env.pods, nil, nil, nil)
	require.Error(t, scaleErr)
	assert.Equal(t, []int{4, 2, 1}, sizes)
	env.assertConditions(t, nil, env.requests[:1])
	assert.Len(t, result.PodsAwaitEvaluation, 3, "requests that weren't tried wait for the next iteration")
	assert.Zero(t, env.snapshot.depth, "no snapshot fork should remain after Provision")
}

// TestBatchSizeCountsPods checks that a rejected batch is halved by its pods, not its requests, and
// that the size remembered for later iterations is a number of pods.
func TestBatchSizeCountsPods(t *testing.T) {
	for _, tc := range []struct {
		name      string
		podCounts []int32
		// fitting is the largest number of pods that can be planned.
		fitting   int
		outcome   attemptOutcome
		wantSizes []int
		wantCalls []limiterCall
	}{
		{
			// Halving by requests would try the oldest three, then two, before the oldest one fits.
			name: "halving skips sizes that a large request can't fit", podCounts: []int32{1, 6, 1, 1, 1}, fitting: 3,
			outcome: planRejection, wantSizes: []int{10, 1}, wantCalls: []limiterCall{{"remember", 1}},
		},
		{
			// Remembering half of two requests would allow only one request, whatever its size.
			name: "a failed resize remembers half of the pods", podCounts: []int32{8, 1},
			outcome: resizeWithUnknownOutcome, wantSizes: []int{9}, wantCalls: []limiterCall{{"remember", 5}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newBatchTestEnvWithPods(t, tc.podCounts...)
			limiter := &recordingLimiter{}
			env.class.batchLimits = limiter
			var sizes []int
			env.scaleUp = func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
				sizes = append(sizes, len(attemptPods))
				if len(attemptPods) <= tc.fitting {
					return env.outcome(scaleUpSuccess, attemptPods)
				}
				return env.outcome(tc.outcome, attemptPods)
			}
			_, _ = env.class.Provision(t.Context(), env.pods, nil, nil, nil)
			assert.Equal(t, tc.wantSizes, sizes)
			assert.Equal(t, tc.wantCalls, limiter.calls)
		})
	}
}

// TestBatchAwaitsOnlyWrittenConditions checks that the class awaits the Provisioned conditions it
// writes, while requests left out of the last attempt aren't awaited, so that the next iteration
// can pick them right away.
func TestBatchAwaitsOnlyWrittenConditions(t *testing.T) {
	env := newBatchTestEnv(t, 8)
	awaiter := &recordingAwaiter{}
	env.class.awaiter = awaiter
	env.scaleUp = func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
		if len(attemptPods) > 2 {
			return env.outcome(planRejection, attemptPods)
		}
		return env.outcome(scaleUpSuccess, attemptPods)
	}
	result, scaleErr := env.class.Provision(t.Context(), env.pods, nil, nil, nil)
	require.NoError(t, scaleErr)
	assert.Equal(t, status.ScaleUpSuccessful, result.Result)
	assert.ElementsMatch(t, []string{env.requests[0].Name, env.requests[1].Name}, awaiter.names)
	assert.Same(t, env.podsInjector, NewWithPodsInjector(nil, env.podsInjector).awaiter, "the pods injector awaits the conditions in production")
}

// TestBatchLimitFollowsOutcomes checks how an iteration adjusts the remembered batch size: a batch
// that succeeds right away lets it grow, a batch that had to shrink before succeeding remembers the
// size that worked, and an exhausted search remembers the next smaller size.
func TestBatchLimitFollowsOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name         string
		requestCount int
		// fitting is the largest batch that succeeds; larger ones fail with outcome.
		fitting   int
		outcome   attemptOutcome
		wantCalls []limiterCall
	}{
		{name: "immediate success lets the limit grow", requestCount: 8, fitting: 8, outcome: resizeRejection, wantCalls: []limiterCall{{"success", 8}}},
		{name: "success after shrinking remembers the size that worked", requestCount: 8, fitting: 2, outcome: resizeRejection, wantCalls: []limiterCall{{"remember", 2}}},
		{name: "planning rejections are remembered the same way", requestCount: 8, fitting: 4, outcome: planRejection, wantCalls: []limiterCall{{"remember", 4}}},
		{name: "exhausted search remembers the next smaller size", requestCount: 8, outcome: resizeRejection, wantCalls: []limiterCall{{"remember", 1}}},
		{name: "a single request lets the limit grow", requestCount: 1, fitting: 1, outcome: planRejection, wantCalls: []limiterCall{{"success", 1}}},
		{name: "unknown outcomes halve the next batch", requestCount: 8, outcome: resizeWithUnknownOutcome, wantCalls: []limiterCall{{"remember", 4}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newBatchTestEnv(t, tc.requestCount)
			limiter := &recordingLimiter{}
			env.class.batchLimits = limiter
			env.scaleUp = func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
				if len(attemptPods) <= tc.fitting {
					return env.outcome(scaleUpSuccess, attemptPods)
				}
				return env.outcome(tc.outcome, attemptPods)
			}
			_, _ = env.class.Provision(t.Context(), env.pods, nil, nil, nil)
			assert.Equal(t, tc.wantCalls, limiter.calls)
		})
	}
	assert.Nil(t, NewWithPodsInjector(nil, nil).batchLimits, "a nil pods injector must not become a non-nil limiter")
}

func TestBatchLimitFeedbackSurvivesStatusUpdateFailure(t *testing.T) {
	for _, tc := range []struct {
		name         string
		existingPods int
		resizeFits   int
		wantFeedback limiterCall
		wantAttempts int
		wantResult   status.ScaleUpResult
	}{
		{name: "existing capacity", existingPods: 4, wantFeedback: limiterCall{"success", 4}, wantResult: status.ScaleUpError},
		{name: "existing capacity after shrinking", existingPods: 1, wantFeedback: limiterCall{"remember", 1}, wantAttempts: 2, wantResult: status.ScaleUpError},
		{name: "successful resize", resizeFits: 4, wantFeedback: limiterCall{"success", 4}, wantAttempts: 1, wantResult: status.ScaleUpSuccessful},
		{name: "successful resize after shrinking", resizeFits: 1, wantFeedback: limiterCall{"remember", 1}, wantAttempts: 3, wantResult: status.ScaleUpSuccessful},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newBatchTestEnv(t, 4)
			limiter := &recordingLimiter{}
			env.class.batchLimits = limiter
			if tc.existingPods > 0 {
				node := testutils.BuildTestNode("existing", int64(tc.existingPods*100), 1000)
				clustersnapshot.InitializeClusterSnapshotOrDie(t, env.snapshot, []*apiv1.Node{node}, nil)
			}
			attempts := 0
			env.scaleUp = func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
				attempts++
				if len(attemptPods) > tc.resizeFits {
					return env.outcome(planRejection, attemptPods)
				}
				return env.outcome(scaleUpSuccess, attemptPods)
			}
			var objects []runtime.Object
			for _, request := range env.requests {
				objects = append(objects, request.DeepCopy())
			}
			apiClient := provreqfake.NewSimpleClientset(objects...)
			apiClient.PrependReactor("patch", "provisioningrequests", func(action clienttesting.Action) (bool, runtime.Object, error) {
				assert.Equal(t, "status", action.GetSubresource())
				assert.Equal(t, []limiterCall{tc.wantFeedback}, limiter.calls, "capacity feedback must precede publishing the condition")
				return true, nil, goerrors.New("simulated status update failure")
			})
			// Call the batch attempt directly: it only needs status writes, not informer reads.
			env.class.client = provreqclient.NewProvisioningRequestClient(apiClient, nil, nil)
			st, err := env.class.provisionRequests(t.Context(), env.requests, env.pods, nil, nil, nil)
			require.ErrorContains(t, err, "simulated status update failure")
			require.NotNil(t, st)
			assert.Equal(t, tc.wantResult, st.Result)
			assert.Equal(t, tc.wantAttempts, attempts, "status update failures must not retry capacity operations")
			assert.Equal(t, []limiterCall{tc.wantFeedback}, limiter.calls)
			assert.Len(t, apiClient.Actions(), tc.wantFeedback.size)
			for _, request := range env.requests {
				stored, getErr := apiClient.AutoscalingV1().ProvisioningRequests(request.Namespace).Get(t.Context(), request.Name, metav1.GetOptions{})
				require.NoError(t, getErr)
				assert.Nil(t, apimeta.FindStatusCondition(stored.Status.Conditions, v1.Provisioned))
			}
			assert.Zero(t, env.snapshot.depth)
		})
	}
}

// TestUnknownOutcomeAfterShrinking checks that a resize with an unknown outcome ends the search
// and halves the size that was tried last, not the size the iteration started with.
func TestUnknownOutcomeAfterShrinking(t *testing.T) {
	env := newBatchTestEnv(t, 8)
	limiter := &recordingLimiter{}
	env.class.batchLimits = limiter
	var sizes []int
	env.scaleUp = func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
		sizes = append(sizes, len(attemptPods))
		if len(attemptPods) > 4 {
			return env.outcome(planRejection, attemptPods)
		}
		return env.outcome(resizeWithUnknownOutcome, attemptPods)
	}
	_, scaleErr := env.class.Provision(t.Context(), env.pods, nil, nil, nil)
	require.Error(t, scaleErr)
	assert.Equal(t, []int{8, 4}, sizes, "an unknown outcome is never followed by another attempt")
	assert.Equal(t, []limiterCall{{"remember", 2}}, limiter.calls)
	env.assertConditions(t, nil, env.requests[:4])
	assert.Zero(t, env.snapshot.depth, "no snapshot fork should remain after Provision")
}

// TestBatchGrowthWaitsForNodes checks that a batch whose resize the cloud provider accepted only
// lets the remembered size grow once its nodes arrive, and that a failure the class registers while
// it provisions doesn't halve the size for an earlier batch, which still grows it once the backoff
// of its node group ends and its nodes can be seen to have arrived.
func TestBatchGrowthWaitsForNodes(t *testing.T) {
	ctx := t.Context()
	env := newBatchTestEnv(t, 8)
	limiter := &recordingLimiter{}
	env.class.batchLimits = limiter
	env.class.outcomes = newBatchOutcomes(limiter, env.upcoming, env.clock)
	accept := func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
		return env.outcome(resizeAccepted, attemptPods)
	}
	env.scaleUp = accept
	result, scaleErr := env.class.Provision(ctx, env.pods, nil, nil, nil)
	require.NoError(t, scaleErr)
	assert.Equal(t, status.ScaleUpSuccessful, result.Result)
	env.assertConditions(t, env.requests, nil)
	_, _ = env.class.Provision(ctx, nil, nil, nil, nil)
	assert.Empty(t, limiter.calls, "the remembered size can't grow while the batch's nodes are upcoming")
	env.upcoming.counts[env.group.Id()] = 0
	_, _ = env.class.Provision(ctx, nil, nil, nil, nil)
	assert.Equal(t, []limiterCall{{"success", 8}}, limiter.calls, "the batch's nodes arrived")

	_, scaleErr = env.class.Provision(ctx, env.pods, nil, nil, nil)
	require.NoError(t, scaleErr)
	env.scaleUp = func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
		// The class's own scale-up failure reaches the shared observers while it provisions, and
		// backs the node group off.
		env.class.outcomes.RegisterFailedScaleUp(ctx, env.group, len(attemptPods), cloudprovider.InstanceErrorInfo{}, env.clock.Now())
		env.upcoming.backedOff[env.group.Id()] = true
		return env.outcome(resizeWithUnknownOutcome, attemptPods)
	}
	_, scaleErr = env.class.Provision(ctx, env.pods, nil, nil, nil)
	require.Error(t, scaleErr)
	env.upcoming.counts[env.group.Id()] = 0
	_, _ = env.class.Provision(ctx, nil, nil, nil, nil)
	assert.Equal(t, []limiterCall{{"success", 8}, {"remember", 4}}, limiter.calls, "only the failed attempt changes the size")
	env.upcoming.backedOff[env.group.Id()] = false
	_, _ = env.class.Provision(ctx, nil, nil, nil, nil)
	assert.Equal(t, []limiterCall{{"success", 8}, {"remember", 4}, {"success", 8}}, limiter.calls, "the earlier batch's nodes arrived")
	assert.Zero(t, env.snapshot.depth, "no snapshot fork should remain after Provision")
}

// TestFailedBatchReportsAdmissionErrors checks that failing to admit the requests that fit existing
// nodes, after no capacity could be requested for the rest of the batch, is reported together with
// the scale-up error.
func TestFailedBatchReportsAdmissionErrors(t *testing.T) {
	env := newBatchTestEnv(t, 4)
	node := testutils.BuildTestNode("existing", 200, 1000)
	clustersnapshot.InitializeClusterSnapshotOrDie(t, env.snapshot, []*apiv1.Node{node}, nil)
	env.scaleUp = func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
		return env.outcome(resizeWithUnknownOutcome, attemptPods)
	}
	var objects []runtime.Object
	for _, request := range env.requests {
		objects = append(objects, request.DeepCopy())
	}
	apiClient := provreqfake.NewSimpleClientset(objects...)
	apiClient.PrependReactor("patch", "provisioningrequests", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, goerrors.New("simulated status update failure")
	})
	// Call the batch attempt directly: it only needs status writes, not informer reads.
	env.class.client = provreqclient.NewProvisioningRequestClient(apiClient, nil, nil)
	st, err := env.class.provisionRequests(t.Context(), env.requests, env.pods, nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to admit ProvisioningRequests that fit existing capacity")
	assert.Contains(t, err.Error(), "timed out waiting for the resize")
	require.NotNil(t, st)
	assert.Equal(t, status.ScaleUpError, st.Result)
	assert.Zero(t, env.snapshot.depth)
}

// TestPartialSuccessKeepsBatchLimit checks that admitting whole requests against the resizes that
// succeeded, while another node group's resize failed, leaves the remembered batch size alone.
func TestPartialSuccessKeepsBatchLimit(t *testing.T) {
	env := newBatchTestEnv(t, 8)
	limiter := &recordingLimiter{}
	env.class.batchLimits = limiter
	env.scaleUp = func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
		return env.outcome(partialSuccess, attemptPods)
	}
	template := framework.NewTestNodeInfo(testutils.BuildTestNode("pool-template", 1000, 1000))
	_, scaleErr := env.class.Provision(t.Context(), env.pods, nil, nil, map[string]*framework.NodeInfo{env.group.Id(): template})
	require.Error(t, scaleErr, "the failed resize is still reported")
	env.assertConditions(t, env.requests, nil)
	assert.Empty(t, limiter.calls)
	assert.Zero(t, env.snapshot.depth, "no snapshot fork should remain after Provision")
}

type limiterCall struct {
	method string
	size   int
}

type recordingLimiter struct {
	calls []limiterCall
}

func (l *recordingLimiter) RememberBestEffortAtomicBatchLimit(_ context.Context, _ *provreqwrapper.ProvisioningRequest, size int) {
	l.calls = append(l.calls, limiterCall{"remember", size})
}

func (l *recordingLimiter) RecordBestEffortAtomicBatchSuccess(_ context.Context, _ *provreqwrapper.ProvisioningRequest, _, pods int) {
	l.calls = append(l.calls, limiterCall{"success", pods})
}

// recordingAwaiter records the requests whose Provisioned condition the class writes.
type recordingAwaiter struct {
	mu    sync.Mutex
	names []string
}

func (a *recordingAwaiter) AwaitProvisionedCondition(pr *provreqwrapper.ProvisioningRequest) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.names = append(a.names, pr.Name)
}

type batchTestEnv struct {
	class        *bestEffortAtomicProvClass
	client       *provreqclient.ProvisioningRequestClient
	requests     []*provreqwrapper.ProvisioningRequest
	pods         []*apiv1.Pod
	snapshot     *forkTrackingSnapshot
	clock        *clocktesting.FakePassiveClock
	group        *testprovider.TestNodeGroup
	otherGroup   *testprovider.TestNodeGroup
	upcoming     *fakeUpcomingNodes
	podsInjector *provreq.ProvisioningRequestPodsInjector
	scaleUp      func([]*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError)
}

func newBatchTestEnv(t *testing.T, requestCount int) *batchTestEnv {
	podCounts := make([]int32, requestCount)
	for i := range podCounts {
		podCounts[i] = 1
	}
	return newBatchTestEnvWithPods(t, podCounts...)
}

// newBatchTestEnvWithPods builds a batch of requests, oldest first, with the given numbers of pods.
func newBatchTestEnvWithPods(t *testing.T, podCounts ...int32) *batchTestEnv {
	env := &batchTestEnv{}
	for i, podCount := range podCounts {
		request := provreqwrapper.BuildValidTestProvisioningRequestFromOptions(provreqwrapper.TestProvReqOptions{
			Name: fmt.Sprintf("request-%03d", i), CPU: "100m", Memory: "1", PodCount: podCount,
			CreationTimestamp: time.Now().Add(time.Duration(i) * time.Second), Class: v1.ProvisioningClassBestEffortAtomicScaleUp,
		})
		requestPods, err := pods.PodsForProvisioningRequest(request)
		require.NoError(t, err)
		env.requests = append(env.requests, request)
		env.pods = append(env.pods, requestPods...)
	}
	env.client = provreqclient.NewFakeProvisioningRequestClient(t.Context(), t, env.requests...)
	handle, err := framework.NewTestFrameworkHandle()
	require.NoError(t, err)
	snapshot := predicate.NewPredicateSnapshot(store.NewDeltaSnapshotStore(), handle, false, 1, false, 0)
	clustersnapshot.InitializeClusterSnapshotOrDie(t, snapshot, nil, nil)
	env.snapshot = &forkTrackingSnapshot{ClusterSnapshot: snapshot}
	env.clock = clocktesting.NewFakePassiveClock(time.Now())
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup("pool", 0, 1000, 1)
	provider.AddNodeGroup("other", 0, 1000, 1)
	env.group = provider.GetNodeGroup("pool").(*testprovider.TestNodeGroup)
	env.otherGroup = provider.GetNodeGroup("other").(*testprovider.TestNodeGroup)
	env.upcoming = newFakeUpcomingNodes()

	env.podsInjector = provreq.NewProvisioningRequestPodsInjectorWithOptions(env.client, provreq.PodsInjectorOptions{
		InitialBackoffTime:              time.Minute,
		MaxBackoffTime:                  10 * time.Minute,
		MaxBackoffCacheSize:             1000,
		BestEffortAtomicBatchProcessing: true,
		BestEffortAtomicMaxBatchSize:    max(2, len(podCounts)),
		KubeClientBurst:                 10,
	})
	env.class = NewWithPodsInjector(env.client, env.podsInjector)
	env.class.autoscalingCtx = &ca_context.AutoscalingContext{ClusterSnapshot: env.snapshot}
	env.class.injector = scheduling.NewHintingSimulator()
	env.class.batchProcessing = true
	env.class.maxConcurrentUpdates = 10
	env.class.clock = env.clock
	env.class.heldFailures = newHeldFailureObserver(&recordingObserver{}, nil)
	env.class.scaleUpOrchestrator = &batchScaleUpStub{scaleUp: func(attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
		return env.scaleUp(attemptPods)
	}}
	return env
}

// outcome builds the result of an attempt. A resize rejection is reported as a clean rejection of
// the test node group's increase; a resize with unknown outcome fails with any other error. A
// partial success scales up the test node group with a node per pod while another group fails.
func (env *batchTestEnv) outcome(outcome attemptOutcome, attemptPods []*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError) {
	unschedulable := []status.NoScaleUpInfo{{Pod: attemptPods[0], RejectedNodeGroups: map[string]status.Reasons{"pool": orchestrator.AllOrNothingReason}}}
	failedResize := &status.ScaleUpStatus{FailedResizeNodeGroups: []cloudprovider.NodeGroup{env.group}, PodsRemainUnschedulable: unschedulable}
	switch outcome {
	case planRejection:
		return &status.ScaleUpStatus{Result: status.ScaleUpNoOptionsAvailable, PodsRemainUnschedulable: unschedulable}, nil
	case resizeRejection:
		err := errors.ToAutoscalerError(errors.CloudProviderError,
			fmt.Errorf("%w: insufficient capacity for %d nodes", cloudprovider.ErrAtomicIncreaseRejected, len(attemptPods))).AddPrefix("failed to increase node group size: ")
		failedResize.FailedResizeErrors = map[string]errors.AutoscalerError{env.group.Id(): err}
		return status.UpdateScaleUpError(failedResize, err)
	case resizeWithUnknownOutcome:
		err := errors.NewAutoscalerError(errors.CloudProviderError, "timed out waiting for the resize")
		failedResize.FailedResizeErrors = map[string]errors.AutoscalerError{env.group.Id(): err}
		return status.UpdateScaleUpError(failedResize, err)
	case partialSuccess:
		err := errors.NewAutoscalerError(errors.CloudProviderError, "resize of the other node group failed")
		return status.UpdateScaleUpError(&status.ScaleUpStatus{
			ScaleUpInfos:           []nodegroupset.ScaleUpInfo{{Group: env.group, CurrentSize: 1, NewSize: 1 + len(attemptPods)}},
			FailedResizeNodeGroups: []cloudprovider.NodeGroup{env.otherGroup},
			FailedResizeErrors:     map[string]errors.AutoscalerError{env.otherGroup.Id(): err},
		}, err)
	case resizeAccepted:
		size, err := env.group.TargetSize(context.Background())
		if err != nil {
			return status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.ToAutoscalerError(errors.InternalError, err))
		}
		env.group.SetTargetSize(size + len(attemptPods))
		env.upcoming.counts[env.group.Id()] += len(attemptPods)
		return &status.ScaleUpStatus{
			Result:       status.ScaleUpSuccessful,
			ScaleUpInfos: []nodegroupset.ScaleUpInfo{{Group: env.group, CurrentSize: size, NewSize: size + len(attemptPods)}},
		}, nil
	default:
		return &status.ScaleUpStatus{Result: status.ScaleUpSuccessful}, nil
	}
}

// assertConditions checks that provisioned requests have Provisioned=True, notFound requests
// have Provisioned=False, and every other request has no Provisioned condition at all.
func (env *batchTestEnv) assertConditions(t *testing.T, provisioned, notFound []*provreqwrapper.ProvisioningRequest) {
	t.Helper()
	want := map[string]metav1.ConditionStatus{}
	for _, request := range provisioned {
		want[request.Name] = metav1.ConditionTrue
	}
	for _, request := range notFound {
		want[request.Name] = metav1.ConditionFalse
	}
	updated, err := env.client.ProvisioningRequestsNoCache()
	require.NoError(t, err)
	for _, request := range updated {
		var got metav1.ConditionStatus
		if condition := apimeta.FindStatusCondition(request.Status.Conditions, v1.Provisioned); condition != nil {
			got = condition.Status
		}
		assert.Equal(t, want[request.Name], got, "Provisioned condition of %s", request.Name)
		assert.False(t, apimeta.IsStatusConditionTrue(request.Status.Conditions, v1.Failed), "%s must stay retryable", request.Name)
	}
}

type batchScaleUpStub struct {
	scaleup.Orchestrator
	scaleUp func([]*apiv1.Pod) (*status.ScaleUpStatus, errors.AutoscalerError)
}

func (s *batchScaleUpStub) ScaleUp(_ context.Context, pods []*apiv1.Pod, _ []*apiv1.Node, _ []*appsv1.DaemonSet, _ map[string]*framework.NodeInfo, allOrNothing bool) (*status.ScaleUpStatus, errors.AutoscalerError) {
	if !allOrNothing {
		return status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerError(errors.InternalError, "batch must remain atomic"))
	}
	return s.scaleUp(pods)
}

type forkTrackingSnapshot struct {
	clustersnapshot.ClusterSnapshot
	depth int
}

func (s *forkTrackingSnapshot) Fork() {
	s.depth++
	s.ClusterSnapshot.Fork()
}

func (s *forkTrackingSnapshot) Revert() {
	s.depth--
	s.ClusterSnapshot.Revert()
}

func (s *forkTrackingSnapshot) Commit() error {
	err := s.ClusterSnapshot.Commit()
	if err == nil {
		s.depth--
	}
	return err
}

func TestCleanResizeRejectionRequiresEveryGroupOutcome(t *testing.T) {
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup("a", 0, 10, 1)
	provider.AddNodeGroup("b", 0, 10, 1)
	a, b := provider.GetNodeGroup("a"), provider.GetNodeGroup("b")
	cleanA := errors.ToAutoscalerError(errors.CloudProviderError, fmt.Errorf("group a: %w", cloudprovider.ErrAtomicIncreaseRejected))
	cleanB := errors.ToAutoscalerError(errors.CloudProviderError, fmt.Errorf("group b: %w", cloudprovider.ErrAtomicIncreaseRejected))
	unknown := errors.NewAutoscalerError(errors.CloudProviderError, "timeout")
	for _, tc := range []struct {
		name   string
		errors map[string]errors.AutoscalerError
		want   bool
	}{
		{name: "all clean", errors: map[string]errors.AutoscalerError{"a": cleanA, "b": cleanB}, want: true},
		{name: "missing classification", errors: map[string]errors.AutoscalerError{"a": cleanA}},
		{name: "nil classification", errors: map[string]errors.AutoscalerError{"a": cleanA, "b": nil}},
		{name: "mixed outcomes", errors: map[string]errors.AutoscalerError{"a": cleanA, "b": unknown}},
		{name: "no outcomes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &status.ScaleUpStatus{FailedResizeNodeGroups: []cloudprovider.NodeGroup{a, b}, FailedResizeErrors: tc.errors}
			// The aggregate intentionally matches a clean rejection even though it also
			// contains a timeout. It must never substitute for per-group classification.
			aggregate := errors.ToAutoscalerError(errors.CloudProviderError, goerrors.Join(cleanA, unknown))
			assert.True(t, goerrors.Is(aggregate, cloudprovider.ErrAtomicIncreaseRejected))
			assert.Equal(t, tc.want, isCleanResizeRejection(st, aggregate))
		})
	}
	st := &status.ScaleUpStatus{
		FailedResizeNodeGroups: []cloudprovider.NodeGroup{a},
		FailedResizeErrors:     map[string]errors.AutoscalerError{"a": cleanA},
		ScaleUpInfos:           []nodegroupset.ScaleUpInfo{{Group: b, CurrentSize: 1, NewSize: 2}},
	}
	assert.False(t, isCleanResizeRejection(st, cleanA), "partial success must be admitted, not retried")
	st.ScaleUpInfos = nil
	st.CreateNodeGroupResults = []nodegroups.CreateNodeGroupResult{{MainCreatedNodeGroup: b}}
	assert.False(t, isCleanResizeRejection(st, cleanA), "node-group creation is a side effect")
}
