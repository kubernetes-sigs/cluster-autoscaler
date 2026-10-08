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

package besteffortatomic

import (
	"context"
	goerrors "errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"

	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	v1ac "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/applyconfiguration/autoscaling.x-k8s.io/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"

	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/orchestrator"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/metrics"
	"sigs.k8s.io/cluster-autoscaler/pkg/observers/nodegroupchange"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupset"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/provreq"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/conditions"
	provreqpods "sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"

	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
)

// fieldManager is the field manager used when server-side applying ProvisioningRequest conditions.
const fieldManager = "cluster-autoscaler"

// Best effort atomic provisionig class requests scale-up only if it's possible
// to atomically request enough resources for all pods specified in a
// ProvisioningRequest. It's "best effort" as it admits workload immediately
// after successful request, without waiting to verify that resources started.
type bestEffortAtomicProvClass struct {
	autoscalingCtx       *ca_context.AutoscalingContext
	client               *provreqclient.ProvisioningRequestClient
	injector             *scheduling.HintingSimulator
	scaleUpOrchestrator  scaleup.Orchestrator
	batchProcessing      bool
	maxConcurrentUpdates int
	// maxBatchAttempts bounds the scale-up attempts made for one batch in a single iteration.
	maxBatchAttempts int
	// maxResizeAttempts bounds how many of those attempts may end in a rejected cloud resize.
	maxResizeAttempts int
	// batchTimebox stops further attempts from starting; it doesn't interrupt one in progress.
	batchTimebox time.Duration
	heldFailures *heldFailureObserver
	batchLimits  batchLimiter
	awaiter      conditionAwaiter
	selector     batchSelector
	outcomes     *batchOutcomes
	clock        clock.PassiveClock
}

// batchLimiter remembers how many pods of compatible ProvisioningRequests later iterations batch, so
// that the search for a batch that fits continues where it stopped instead of restarting every
// iteration.
type batchLimiter interface {
	RememberBestEffortAtomicBatchLimit(ctx context.Context, pr *provreqwrapper.ProvisioningRequest, pods int)
	RecordBestEffortAtomicBatchSuccess(ctx context.Context, pr *provreqwrapper.ProvisioningRequest, requests, pods int)
}

// conditionAwaiter keeps a ProvisioningRequest from being picked again before the Provisioned
// condition written for it shows in the informer cache.
type conditionAwaiter interface {
	AwaitProvisionedCondition(pr *provreqwrapper.ProvisioningRequest)
}

// batchSelector selects the batches the class provisions. It settles the outcomes of earlier
// batches before selecting each one, so that a batch size they change applies without delay.
type batchSelector interface {
	SetBestEffortAtomicBatchSettler(settle func(ctx context.Context))
}

// New creates best effort atomic provisioning class supporting create capacity scale-up mode.
//
// Deprecated: Use NewWithPodsInjector to share batch selection and retry state.
func New(client *provreqclient.ProvisioningRequestClient) *bestEffortAtomicProvClass {
	return NewWithPodsInjector(client, nil)
}

// NewWithPodsInjector creates a best effort atomic provisioning class sharing the pods injector's
// batch selection and retry state.
// The pods injector selects batches and remembers batch sizes across iterations; it may be nil.
func NewWithPodsInjector(
	client *provreqclient.ProvisioningRequestClient,
	podsInjector *provreq.ProvisioningRequestPodsInjector,
) *bestEffortAtomicProvClass {
	o := &bestEffortAtomicProvClass{
		client:              client,
		scaleUpOrchestrator: orchestrator.New(),
		maxBatchAttempts:    config.DefaultBestEffortAtomicProvisioningRequestMaxBatchAttempts,
		maxResizeAttempts:   config.DefaultBestEffortAtomicProvisioningRequestMaxResizeAttempts,
		batchTimebox:        config.DefaultBestEffortAtomicProvisioningRequestBatchTimebox,
		clock:               clock.RealClock{},
	}
	if podsInjector != nil {
		o.batchLimits = podsInjector
		o.awaiter = podsInjector
		o.selector = podsInjector
	}
	return o
}

func (o *bestEffortAtomicProvClass) Initialize(
	autoscalingCtx *ca_context.AutoscalingContext,
	processors *ca_processors.AutoscalingProcessors,
	clusterStateRegistry *clusterstate.ClusterStateRegistry,
	estimatorBuilder estimator.EstimatorBuilder,
	taintConfig taints.TaintConfig,
	injector *scheduling.HintingSimulator,
	quotasTrackerFactory *resourcequotas.TrackerFactory,
) {
	o.autoscalingCtx = autoscalingCtx
	o.injector = injector
	o.batchProcessing = autoscalingCtx.BestEffortAtomicBatchProcessing && autoscalingCtx.BestEffortAtomicProvisioningRequestMaxBatchSize > 1
	o.maxConcurrentUpdates = max(1, autoscalingCtx.KubeClientOpts.KubeClientBurst)
	// Options that aren't set keep their defaults.
	if attempts := autoscalingCtx.BestEffortAtomicProvisioningRequestMaxBatchAttempts; attempts > 0 {
		o.maxBatchAttempts = attempts
	}
	if attempts := autoscalingCtx.BestEffortAtomicProvisioningRequestMaxResizeAttempts; attempts > 0 {
		o.maxResizeAttempts = attempts
	}
	if timebox := autoscalingCtx.BestEffortAtomicProvisioningRequestBatchTimebox; timebox > 0 {
		o.batchTimebox = timebox
	}
	if processors != nil && processors.ScaleStateNotifier != nil {
		if o.batchProcessing && o.batchLimits != nil && clusterStateRegistry != nil {
			// Cluster state reports failures it only detects later, such as scale-up timeouts, to
			// the shared observers rather than to this class's private scale-up orchestrator.
			o.outcomes = newBatchOutcomes(o.batchLimits, clusterStateRegistry, o.clock)
			processors.ScaleStateNotifier.Register(o.outcomes)
			if o.selector != nil {
				o.selector.SetBestEffortAtomicBatchSettler(o.outcomes.settle)
			}
		}
		// The scale-up orchestrator is private to this class, so routing its notifications through
		// heldFailures can't delay failure reporting for any other scale-up. Failures it doesn't
		// forward are recorded by a metrics producer of its own, because the shared one would only
		// see them together with the backoff.
		var failureMetrics nodegroupchange.NodeGroupChangeObserver
		if autoscalingCtx.CloudProvider != nil {
			failureMetrics = nodegroupchange.NewNodeGroupChangeMetricsProducer(autoscalingCtx.CloudProvider, metrics.DefaultMetrics, autoscalingCtx.TemplateNodeInfoRegistry)
		}
		o.heldFailures = newHeldFailureObserver(processors.ScaleStateNotifier, failureMetrics)
		scaleUpProcessors := *processors
		scaleUpProcessors.ScaleStateNotifier = nodegroupchange.NewNodeGroupChangeObserversList()
		scaleUpProcessors.ScaleStateNotifier.Register(o.heldFailures)
		processors = &scaleUpProcessors
	}
	o.scaleUpOrchestrator.Initialize(autoscalingCtx, processors, clusterStateRegistry, estimatorBuilder, taintConfig, quotasTrackerFactory)
}

// Provision returns success if there is, or has just been requested, sufficient capacity in the cluster for pods from ProvisioningRequest.
//
// When batch processing is disabled, exactly one ProvisioningRequest is handled per iteration.
// When it is enabled, all ProvisioningRequests whose pods the injector added to
// unschedulablePods are flattened into one all-or-nothing scale-up calculation. This allows
// compatible requests to be coalesced into a single infrastructure resize. If the batch can't
// be planned, or the cloud provider reports that it rejected the resize without changing the node
// group, the oldest requests with at most half of its pods are retried within the same iteration,
// so every request stays whole. Any other failed resize isn't retried within the iteration, but
// later iterations batch half as many pods. Requests left out keep their conditions and are picked
// up again by the next iteration. If only some resizes succeed, or none, whole requests are
// admitted against the capacity that was requested and existing nodes.
// Batches whose resizes were accepted are followed until their nodes arrive, because a scale-up
// that fails later also halves the size of later batches.
func (o *bestEffortAtomicProvClass) Provision(
	ctx context.Context,
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	if o.outcomes != nil {
		// The batch selector normally settled the batches already; doing so again costs little.
		o.outcomes.settle(ctx)
		// Failures registered from now on are this class's own, and it handles them as they happen.
		o.outcomes.setBusy(true)
		defer o.outcomes.setBusy(false)
	}
	if len(unschedulablePods) == 0 {
		return &status.ScaleUpStatus{Result: status.ScaleUpNotTried}, nil
	}
	prs := provreqclient.ProvisioningRequestsForPods(ctx, o.client, unschedulablePods)
	prs = provreqclient.FilterOutProvisioningClass(ctx, prs, v1.ProvisioningClassBestEffortAtomicScaleUp, "")
	if len(prs) == 0 {
		return &status.ScaleUpStatus{Result: status.ScaleUpNotTried}, nil
	}
	// ProvisioningRequestsForPods returns requests in map iteration order. Sort them so that a
	// batch is processed oldest-first and the outcome of an iteration is reproducible.
	provreqwrapper.SortProvisioningRequests(prs)

	o.autoscalingCtx.ClusterSnapshot.Fork()
	defer o.autoscalingCtx.ClusterSnapshot.Revert()

	if !o.batchProcessing {
		// Pick 1 ProvisioningRequest.
		return o.provisionRequests(ctx, prs[:1], unschedulablePods, nodes, daemonSets, nodeInfos)
	}

	return o.provisionBatch(ctx, prs, unschedulablePods, nodes, daemonSets, nodeInfos)
}

// provisionBatch flattens all pods from the selected ProvisioningRequests into one atomic
// capacity plan, preserving per-request admission when execution only partially succeeds.
func (o *bestEffortAtomicProvClass) provisionBatch(
	ctx context.Context,
	prs []*provreqwrapper.ProvisioningRequest,
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	logger := klog.FromContext(ctx)
	logger.Info("Processing best-effort-atomic provisioning requests as one flattened batch", "batchSize", len(prs), "podsCount", len(unschedulablePods))

	return o.provisionRequests(ctx, prs, unschedulablePods, nodes, daemonSets, nodeInfos)
}

// provisionRequests runs an atomic scale-up for the supplied ProvisioningRequests. In batch mode
// a planning rejection, or a resize the cloud provider rejected cleanly, is retried with the
// oldest requests holding at most half of the batch's pods within this iteration; any other failed
// resize instead halves the batch size remembered for later iterations. Whole requests are
// admitted, even when the scale-up fails for the rest of the batch, and requests that weren't
// evaluated are left for the next iteration.
func (o *bestEffortAtomicProvClass) provisionRequests(
	ctx context.Context,
	prs []*provreqwrapper.ProvisioningRequest,
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
) (result *status.ScaleUpStatus, resultErr errors.AutoscalerError) {
	logger := klog.FromContext(ctx)
	allPods := unschedulablePods
	// deferred holds requests that weren't evaluated, oldest first. Their conditions are left
	// untouched so that the next iteration can pick them up without a retry delay.
	var deferred []*provreqwrapper.ProvisioningRequest
	var skipped []status.NoScaleUpInfo
	defer func() {
		if result != nil {
			result.PodsRemainUnschedulable = append(result.PodsRemainUnschedulable, skipped...)
			result.PodsAwaitEvaluation = append(result.PodsAwaitEvaluation, podsForRequests(deferred, allPods)...)
		}
	}()

	retry := o.batchProcessing && len(prs) > 1
	if retry && o.heldFailures != nil {
		o.heldFailures.hold()
		defer o.heldFailures.release(ctx)
	}
	start := o.clock.Now()
	resizeRejections := 0

	for attempt := 1; ; attempt++ {
		// Each attempt starts from the same snapshot, including capacity reserved by earlier requests.
		snapshot := o.autoscalingCtx.ClusterSnapshot
		snapshot.Fork()
		actuallyUnschedulablePods, err := o.filterOutSchedulable(ctx, unschedulablePods)
		if err != nil {
			snapshot.Revert()
			_ = o.updateConditions(ctx, prs, v1.Provisioned, metav1.ConditionFalse, conditions.FailedToCheckCapacityReason, conditions.FailedToCheckCapacityMsg)
			st, aErr := status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "error during ScaleUp: %s", err.Error()))
			return st, aErr
		}

		if len(actuallyUnschedulablePods) == 0 {
			snapshot.Revert()
			o.adjustBatchLimit(ctx, prs, attempt > 1, nil)
			if updateErr := o.updateConditions(ctx, prs, v1.Provisioned, metav1.ConditionTrue, conditions.CapacityIsFoundReason, conditions.CapacityIsFoundMsg); updateErr != nil {
				st, aErr := status.UpdateScaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "capacity available, but failed to admit ProvisioningRequest batch: %s", updateErr.Error()))
				return st, aErr
			}
			return &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}, nil
		}

		st, scaleUpErr := o.scaleUpOrchestrator.ScaleUp(ctx, actuallyUnschedulablePods, nodes, daemonSets, nodeInfos, true)
		snapshot.Revert()

		if retry {
			planRejected := isPlanningRejection(st, scaleUpErr)
			resizeRejected := !planRejected && o.heldFailures != nil && isCleanResizeRejection(st, scaleUpErr)
			if resizeRejected {
				resizeRejections++
			}
			if (planRejected || (resizeRejected && resizeRejections < o.maxResizeAttempts)) &&
				canNarrow(prs, deferred, resizeRejected) &&
				attempt < o.maxBatchAttempts && o.clock.Since(start) < o.batchTimebox && ctx.Err() == nil {
				if len(prs) > 1 {
					size := halfOfBatch(prs)
					deferred = slices.Concat(prs[size:], deferred)
					prs = prs[:size]
				} else {
					// The oldest request can't be provisioned even on its own, so it mustn't hold
					// back the requests behind it.
					_ = o.updateConditions(ctx, prs, v1.Provisioned, metav1.ConditionFalse, conditions.CapacityIsNotFoundReason, "Capacity is not found for the ProvisioningRequest, CA will try to find it later.")
					skipped = append(skipped, st.PodsRemainUnschedulable...)
					prs, deferred = deferred, nil
				}
				unschedulablePods = podsForRequests(prs, allPods)
				logger.V(2).Info("Retrying part of the best-effort-atomic ProvisioningRequest batch", "batchSize", len(prs), "batchPods", podCount(prs), "attempt", attempt+1, "planRejected", planRejected, "resizeRejected", resizeRejected)
				continue
			}
			// A failed resize whose outcome is unknown mustn't be retried in this iteration, but later
			// iterations should still request less, as after a search that ran out of attempts.
			if (planRejected || isFailedResize(st, scaleUpErr)) && o.batchLimits != nil {
				o.batchLimits.RememberBestEffortAtomicBatchLimit(ctx, prs[0], (podCount(prs)+1)/2)
			}
		}

		if scaleUpErr == nil && st.Result == status.ScaleUpSuccessful {
			o.adjustBatchLimit(ctx, prs, attempt > 1, st)
			if updateErr := o.updateConditions(ctx, prs, v1.Provisioned, metav1.ConditionTrue, conditions.CapacityIsProvisionedReason, conditions.CapacityIsProvisionedMsg); updateErr != nil {
				return st, errors.NewAutoscalerErrorf(errors.InternalError, "scale up requested, but failed to admit ProvisioningRequest batch: %s", updateErr.Error())
			}
			return st, nil
		}

		if o.batchProcessing && st != nil && len(st.ScaleUpInfos) > 0 {
			// A partial success is a mixed signal about capacity, so the remembered batch size
			// is left unchanged, unless the capacity that was requested fails to arrive.
			o.trackScaleUp(prs, st, false)
			if admissionErr := o.admitPartialBatch(ctx, prs, st.ScaleUpInfos, nodeInfos); admissionErr != nil {
				return st, errors.NewAutoscalerErrorf(errors.InternalError, "partial scale up, but failed to admit ProvisioningRequests: %v; scale-up error: %v", admissionErr, scaleUpErr)
			}
			return st, scaleUpErr
		}

		if o.batchProcessing && len(prs) > 1 {
			// None of the requested capacity is coming, but whole requests that fit existing nodes,
			// for example nodes freed while the node group is backed off, don't have to wait for the
			// rest of the batch.
			if admissionErr := o.admitPartialBatch(ctx, prs, nil, nodeInfos); admissionErr != nil {
				message := fmt.Sprintf("failed to admit ProvisioningRequests that fit existing capacity: %v", admissionErr)
				if scaleUpErr != nil {
					message += fmt.Sprintf("; scale-up error: %v", scaleUpErr)
				}
				if st == nil {
					st = &status.ScaleUpStatus{}
				}
				return status.UpdateScaleUpError(st, errors.NewAutoscalerError(errors.InternalError, message))
			}
		} else {
			_ = o.updateConditions(ctx, prs, v1.Provisioned, metav1.ConditionFalse, conditions.CapacityIsNotFoundReason, "Capacity is not found for the ProvisioningRequest batch, CA will try to find it later.")
		}
		if scaleUpErr != nil {
			errStatus, aErr := status.UpdateScaleUpError(st, errors.NewAutoscalerErrorf(errors.InternalError, "error during ScaleUp: %s", scaleUpErr.Error()))
			return errStatus, aErr
		}
		return st, nil
	}
}

// adjustBatchLimit lets the remembered batch size follow successful provisioning, whether using
// existing capacity or a scale-up. A batch that had to shrink first remembers the size that
// worked, so later iterations don't repeat the rejected sizes; a batch that succeeded right
// away lets the remembered size grow again. A batch that needed a scale-up only does so once its
// nodes have arrived, because until then a failure can still halve the remembered size. It
// runs before the requests' conditions are updated, because the capacity outcome is known whether
// or not that update succeeds.
func (o *bestEffortAtomicProvClass) adjustBatchLimit(ctx context.Context, prs []*provreqwrapper.ProvisioningRequest, shrunk bool, st *status.ScaleUpStatus) {
	if !o.batchProcessing || o.batchLimits == nil {
		return
	}
	if shrunk {
		o.batchLimits.RememberBestEffortAtomicBatchLimit(ctx, prs[0], podCount(prs))
	}
	if !o.trackScaleUp(prs, st, !shrunk) && !shrunk {
		o.batchLimits.RecordBestEffortAtomicBatchSuccess(ctx, prs[0], len(prs), podCount(prs))
	}
}

// trackScaleUp follows the node groups resized for a batch until their nodes arrive, and reports
// whether there were any to follow. Only a batch that may grow the remembered size, and didn't
// create node groups, does so once they arrive: a node group created for the batch may only get
// its final identity once it's initialized, so its nodes can't be followed reliably.
func (o *bestEffortAtomicProvClass) trackScaleUp(prs []*provreqwrapper.ProvisioningRequest, st *status.ScaleUpStatus, grow bool) bool {
	if o.outcomes == nil || st == nil {
		return false
	}
	var resizes []nodegroupset.ScaleUpInfo
	for _, info := range st.ScaleUpInfos {
		if info.NewSize > info.CurrentSize {
			resizes = append(resizes, info)
		}
	}
	if len(resizes) == 0 {
		return false
	}
	o.outcomes.track(prs[0], len(prs), podCount(prs), resizes, grow && len(st.CreateNodeGroupResults) == 0)
	return true
}

// podCount returns the number of pods the requests ask for.
func podCount(prs []*provreqwrapper.ProvisioningRequest) int {
	count := 0
	for _, pr := range prs {
		count += pr.PodCount()
	}
	return count
}

// halfOfBatch returns how many of the oldest requests of a rejected batch to try next: as many as
// fit in half of the batch's pods, but at least one and fewer than all of them.
func halfOfBatch(prs []*provreqwrapper.ProvisioningRequest) int {
	limit := (podCount(prs) + 1) / 2
	size, pods := 1, prs[0].PodCount()
	for size < len(prs)-1 && pods+prs[size].PodCount() <= limit {
		pods += prs[size].PodCount()
		size++
	}
	return size
}

// canNarrow reports whether a rejected attempt can be followed by a smaller one: half of the
// batch, or the requests behind the oldest one when that was rejected on its own. If the cloud
// provider rejected the oldest request's resize, only a request that needs less can fit.
func canNarrow(prs, deferred []*provreqwrapper.ProvisioningRequest, resizeRejected bool) bool {
	if len(prs) > 1 {
		return true
	}
	return slices.ContainsFunc(deferred, func(request *provreqwrapper.ProvisioningRequest) bool {
		return !resizeRejected || !needsAtLeast(request, prs[0])
	})
}

// needsAtLeast reports whether request needs at least as much capacity as other. The requests of a
// batch have the same pod sets, so it does when it has at least as many pods in each of them.
func needsAtLeast(request, other *provreqwrapper.ProvisioningRequest) bool {
	if len(request.Spec.PodSets) != len(other.Spec.PodSets) {
		return false
	}
	for i := range request.Spec.PodSets {
		if request.Spec.PodSets[i].Count < other.Spec.PodSets[i].Count {
			return false
		}
	}
	return true
}

// isPlanningRejection reports whether the all-or-nothing plan was rejected before any cloud
// provider operation, such as creating a node group, so a smaller batch can be tried without
// side effects to account for.
func isPlanningRejection(st *status.ScaleUpStatus, scaleUpErr errors.AutoscalerError) bool {
	if scaleUpErr != nil || st == nil || st.Result != status.ScaleUpNoOptionsAvailable || len(st.CreateNodeGroupResults) > 0 ||
		len(st.ScaleUpInfos) > 0 || len(st.FailedResizeNodeGroups) > 0 || len(st.FailedCreationNodeGroups) > 0 {
		return false
	}
	for _, pod := range st.PodsRemainUnschedulable {
		for _, reason := range pod.RejectedNodeGroups {
			if reason == orchestrator.AllOrNothingReason {
				return true
			}
		}
	}
	return false
}

// isCleanResizeRejection reports whether the cloud provider rejected every resize of an attempt
// without changing any node group, which it signals by wrapping
// cloudprovider.ErrAtomicIncreaseRejected. Any other resize error may have changed capacity, for
// example after a timeout, so requesting a smaller increase right away could request capacity
// twice or, with providers that set absolute sizes from a cache, even shrink the node group.
func isCleanResizeRejection(st *status.ScaleUpStatus, scaleUpErr errors.AutoscalerError) bool {
	if scaleUpErr == nil || st == nil || len(st.FailedResizeNodeGroups) == 0 || len(st.ScaleUpInfos) > 0 ||
		len(st.CreateNodeGroupResults) > 0 || len(st.FailedCreationNodeGroups) > 0 ||
		len(st.FailedResizeErrors) != len(st.FailedResizeNodeGroups) {
		return false
	}
	// Inspect every outcome, not errors.Is on an aggregate: one clean rejection must
	// never authorize a retry when a different group's resize has an unknown outcome.
	for _, group := range st.FailedResizeNodeGroups {
		if group == nil || !goerrors.Is(st.FailedResizeErrors[group.Id()], cloudprovider.ErrAtomicIncreaseRejected) {
			return false
		}
	}
	return true
}

// isFailedResize reports whether an attempt failed to resize a node group while no resize
// succeeded, whatever the outcome. Shrinking the batch that later iterations request is safe even
// when the outcome is unknown, because they only resize after the node group's state is refreshed.
// A partial success is excluded: it is admitted against the capacity that was requested.
func isFailedResize(st *status.ScaleUpStatus, scaleUpErr errors.AutoscalerError) bool {
	return scaleUpErr != nil && st != nil && len(st.FailedResizeNodeGroups) > 0 && len(st.ScaleUpInfos) == 0
}

// podsForRequests returns the pods owned by the given ProvisioningRequests.
func podsForRequests(prs []*provreqwrapper.ProvisioningRequest, pods []*apiv1.Pod) []*apiv1.Pod {
	requests := make(map[types.NamespacedName]bool, len(prs))
	for _, pr := range prs {
		requests[types.NamespacedName{Namespace: pr.Namespace, Name: pr.Name}] = true
	}
	var selected []*apiv1.Pod
	for _, pod := range pods {
		if len(pod.OwnerReferences) > 0 && requests[types.NamespacedName{Namespace: pod.Namespace, Name: pod.OwnerReferences[0].Name}] {
			selected = append(selected, pod)
		}
	}
	return selected
}

// admitPartialBatch admits whole ProvisioningRequests, oldest first, that fit existing nodes together
// with the nodes of the successful scale-ups, if any, and records that capacity wasn't found for the
// rest. Requests admitted without any scale-up are admitted against capacity found in the cluster.
func (o *bestEffortAtomicProvClass) admitPartialBatch(
	ctx context.Context,
	prs []*provreqwrapper.ProvisioningRequest,
	successfulScaleUps []nodegroupset.ScaleUpInfo,
	nodeInfos map[string]*framework.NodeInfo,
) error {
	reason, message := conditions.CapacityIsProvisionedReason, conditions.CapacityIsProvisionedMsg
	if len(successfulScaleUps) == 0 {
		reason, message = conditions.CapacityIsFoundReason, conditions.CapacityIsFoundMsg
	}
	snapshot := o.autoscalingCtx.ClusterSnapshot
	snapshot.Fork()
	defer snapshot.Revert()

	for groupIndex, scaleUpInfo := range successfulScaleUps {
		template, found := nodeInfos[scaleUpInfo.Group.Id()]
		if !found {
			return fmt.Errorf("no node template for successful scale-up of %s", scaleUpInfo.Group.Id())
		}
		for nodeIndex := 0; nodeIndex < scaleUpInfo.NewSize-scaleUpInfo.CurrentSize; nodeIndex++ {
			nodeInfo, err := simulator.SanitizedNodeInfo(ctx, template, fmt.Sprintf("provreq-%d-%d", groupIndex, nodeIndex))
			if err != nil {
				return err
			}
			if err := snapshot.AddNodeInfo(nodeInfo); err != nil {
				return err
			}
		}
	}

	var admitted, unfulfilled []*provreqwrapper.ProvisioningRequest
	var failures []string
	for _, request := range prs {
		requestPods, err := provreqpods.PodsForProvisioningRequest(request)
		if err != nil {
			unfulfilled = append(unfulfilled, request)
			failures = append(failures, err.Error())
			continue
		}
		snapshot.Fork()
		result, err := o.injector.TrySchedulePods(ctx, snapshot, requestPods, true, clustersnapshot.SchedulingOptions{})
		if err == nil && len(result.Statuses) == len(requestPods) {
			if err = snapshot.Commit(); err == nil {
				admitted = append(admitted, request)
				continue
			}
		}
		snapshot.Revert()
		unfulfilled = append(unfulfilled, request)
		if err != nil {
			failures = append(failures, err.Error())
		}
	}
	if err := o.updateConditions(ctx, admitted, v1.Provisioned, metav1.ConditionTrue, reason, message); err != nil {
		failures = append(failures, err.Error())
	}
	if err := o.updateConditions(ctx, unfulfilled, v1.Provisioned, metav1.ConditionFalse, conditions.CapacityIsNotFoundReason, "Capacity is not found for the ProvisioningRequest, CA will try to find it later."); err != nil {
		failures = append(failures, err.Error())
	}
	if len(failures) > 0 {
		return fmt.Errorf("failed to admit partial ProvisioningRequest batch: %s", strings.Join(failures, "; "))
	}
	return nil
}

func (o *bestEffortAtomicProvClass) updateCondition(
	ctx context.Context,
	pr *provreqwrapper.ProvisioningRequest,
	conditionType string,
	conditionStatus metav1.ConditionStatus,
	reason string,
	message string,
) error {
	logger := klog.FromContext(ctx)
	prAC := v1ac.ProvisioningRequest(pr.Name, pr.Namespace)
	condition := metav1ac.Condition().
		WithType(conditionType).
		WithStatus(conditionStatus).
		WithReason(reason).
		WithMessage(message).
		WithLastTransitionTime(metav1.Now())
	prAC.WithStatus(v1ac.ProvisioningRequestStatus().WithConditions(condition))
	if conditionType == v1.Provisioned && o.awaiter != nil {
		o.awaiter.AwaitProvisionedCondition(pr)
	}
	if _, err := o.client.ApplyProvisioningRequest(prAC, fieldManager); err != nil {
		logger.Error(err, "failed to add condition to ProvReq", "provReq", klog.KObj(pr), "conditionType", conditionType, "conditionStatus", conditionStatus)
		return err
	}
	return nil
}

// updateConditions applies the same condition to every ProvisioningRequest in a flattened batch.
// API calls are concurrent but bounded so large batches don't exhaust client-side rate limits.
func (o *bestEffortAtomicProvClass) updateConditions(
	ctx context.Context,
	prs []*provreqwrapper.ProvisioningRequest,
	conditionType string,
	conditionStatus metav1.ConditionStatus,
	reason string,
	message string,
) error {
	if len(prs) == 0 {
		return nil
	}

	updateErrors := make([]error, len(prs))
	semaphore := make(chan struct{}, min(o.maxConcurrentUpdates, len(prs)))
	var wg sync.WaitGroup
	for i, pr := range prs {
		wg.Add(1)
		go func(index int, request *provreqwrapper.ProvisioningRequest) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			updateErrors[index] = o.updateCondition(ctx, request, conditionType, conditionStatus, reason, message)
		}(i, pr)
	}
	wg.Wait()

	var failures []string
	for i, updateErr := range updateErrors {
		if updateErr != nil {
			failures = append(failures, fmt.Sprintf("%s/%s: %v", prs[i].Namespace, prs[i].Name, updateErr))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("failed to update %s condition for %d of %d ProvisioningRequests: %s", conditionType, len(failures), len(prs), strings.Join(failures, "; "))
	}
	return nil
}

func (o *bestEffortAtomicProvClass) filterOutSchedulable(ctx context.Context, pods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	schedulingResult, err := o.injector.TrySchedulePods(ctx, o.autoscalingCtx.ClusterSnapshot, pods, false, clustersnapshot.SchedulingOptions{})
	if err != nil {
		return nil, err
	}

	scheduledPods := make(map[types.UID]bool)
	for _, status := range schedulingResult.Statuses {
		scheduledPods[status.Pod.UID] = true
	}

	var unschedulablePods []*apiv1.Pod
	for _, pod := range pods {
		if !scheduledPods[pod.UID] {
			unschedulablePods = append(unschedulablePods, pod)
		}
	}
	return unschedulablePods, nil

}
