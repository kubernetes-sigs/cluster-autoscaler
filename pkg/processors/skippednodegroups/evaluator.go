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

// Package skippednodegroups attributes node groups that were skipped during scale-up (e.g. in
// backoff or at max size) to the unschedulable pod equivalence groups that could actually use
// them, without doing any of that work on the main autoscaling loop.
//
// Every loop, the ScaleUpStatusProcessor applies the verdicts of the latest finished evaluation and
// hands the current (pod group, skipped node group) pairs to the Evaluator, a single background
// goroutine with its own scheduler framework handle. Verdicts are therefore applied one evaluation
// after a pair is first seen; until then the node group is left in NoScaleUpInfo.SkippedNodeGroups,
// which is exactly the behaviour without the feature.
package skippednodegroups

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"golang.org/x/time/rate"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/klog/v2"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	schedulerconfiglatest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

const (
	// DefaultMaxPairsPerLoop bounds the (pod group, skipped node group) pairs considered per loop,
	// and with it the main-loop lookups and the size of a job.
	// Pod groups beyond the budget keep the plain skipped list, i.e. the output without this feature.
	DefaultMaxPairsPerLoop = 200_000
	// DefaultPairsPerSecond throttles the worker so that it never monopolises a core; at roughly
	// 15us per predicate check this is ~30% of one core while a job is being processed.
	DefaultPairsPerSecond = 20_000

	// evaluatorProfileSuffix is appended to the scheduler profile name of the evaluator's handle.
	evaluatorProfileSuffix = "-skipped-node-groups-evaluator"
)

// EvaluatorOptions configures the Evaluator.
type EvaluatorOptions struct {
	// MaxPairsPerLoop is the maximum number of (pod group, skipped node group) pairs the processor
	// considers in one loop. Zero or negative means unlimited.
	MaxPairsPerLoop int
	// PairsPerSecond rate-limits the background worker. Zero or negative means unlimited.
	PairsPerSecond float64
}

// DefaultEvaluatorOptions returns the production defaults.
func DefaultEvaluatorOptions() EvaluatorOptions {
	return EvaluatorOptions{
		MaxPairsPerLoop: DefaultMaxPairsPerLoop,
		PairsPerSecond:  DefaultPairsPerSecond,
	}
}

// NewEvaluatorFrameworkHandle builds the scheduler framework handle dedicated to the Evaluator.
// Predicate checks swap framework.Handle.DelegatingLister for the duration of a run, so a handle
// must never be shared between goroutines; the Evaluator therefore gets its own, configured like
// the main one (same informers, scheduler config and feature flags) but under a distinct profile
// name: plugins register informer indexers keyed by profile name (e.g. the DynamicResources
// pod-by-claim index), so a second framework on the shared informer factory must not reuse the
// main profile's name. Plugins do not use the profile name for predicate decisions.
func NewEvaluatorFrameworkHandle(ctx context.Context, informerFactory informers.SharedInformerFactory, schedConfig *schedulerconfig.KubeSchedulerConfiguration, draEnabled, csiEnabled bool) (*framework.Handle, error) {
	cfg := schedConfig
	if cfg == nil {
		var err error
		if cfg, err = schedulerconfiglatest.Default(); err != nil {
			return nil, fmt.Errorf("couldn't create default scheduler config: %w", err)
		}
	} else {
		cfg = cfg.DeepCopy()
	}
	for i := range cfg.Profiles {
		cfg.Profiles[i].SchedulerName += evaluatorProfileSuffix
	}
	return framework.NewHandle(ctx, informerFactory, cfg, draEnabled, csiEnabled)
}

// verdictKey identifies one (pod, node group) verdict. Pod objects and template NodeInfos are
// rebuilt every loop, but pod UIDs and node group ids are stable, so a verdict computed for the
// status of one loop can be applied to the status of a later loop.
type verdictKey struct {
	pod       types.UID
	nodeGroup string
}

// snapshotOptions carries the AutoscalingContext knobs needed to build a PredicateSnapshot.
type snapshotOptions struct {
	draEnabled      bool
	parallelism     int
	csiEnabled      bool
	verbosityOffset int
}

// jobGroup is one pod equivalence group to evaluate: a private copy of the exemplar pod with
// inter-pod affinity already stripped, and the UIDs of all pods of the group (they all get the
// exemplar's verdict).
type jobGroup struct {
	pod  *apiv1.Pod
	uids []types.UID
}

// jobTemplate is one skipped node group to evaluate: its id and a private copy of the template NodeInfo.
type jobTemplate struct {
	nodeGroup string
	nodeInfo  *framework.NodeInfo
}

// job is the unit of work handed from the main loop to the worker: every evaluable pod group
// against every skipped node group of one loop. Everything it references is owned by the job (deep
// copies), so the worker never shares mutable state with the main loop.
type job struct {
	snapshotOpts snapshotOptions
	groups       []jobGroup
	templates    []jobTemplate
}

// Evaluator answers "could this pod equivalence group schedule on that skipped node group?" off
// the main autoscaling loop. See the package documentation.
//
// Hand-off: each loop hands over one job with all of its pairs. A newer job replaces a pending
// (not yet started) one, so the worker always evaluates the most recent loop and can never fall
// behind; a job that is already running is finished first.
//
// Results: the verdicts of the last finished job replace the previous ones as a whole, so verdicts
// of pods that are gone or of node groups that are no longer skipped disappear with the next job,
// and a changed template is picked up by the next job.
type Evaluator struct {
	fwHandle *framework.Handle
	opts     EvaluatorOptions
	limiter  *rate.Limiter

	mu      sync.Mutex
	results map[verdictKey]clustersnapshot.SchedulingError
	pending *job
	wake    chan struct{}

	// Counters for tests and debugging, guarded by mu.
	evaluatedPairs int
	completedJobs  int
}

// NewEvaluator creates an Evaluator using the given scheduler framework handle, which must be
// dedicated to it (see NewEvaluatorFrameworkHandle).
func NewEvaluator(fwHandle *framework.Handle, opts EvaluatorOptions) *Evaluator {
	limit := rate.Inf
	burst := 1
	if opts.PairsPerSecond > 0 {
		limit = rate.Limit(opts.PairsPerSecond)
		burst = max(1, int(opts.PairsPerSecond))
	}
	return &Evaluator{
		fwHandle: fwHandle,
		opts:     opts,
		limiter:  rate.NewLimiter(limit, burst),
		results:  make(map[verdictKey]clustersnapshot.SchedulingError),
		wake:     make(chan struct{}, 1),
	}
}

// Run processes jobs until ctx is cancelled. It must run in its own goroutine.
func (e *Evaluator) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.wake:
			e.ProcessPendingNow(ctx)
		}
	}
}

// ProcessPendingNow synchronously evaluates all pending jobs on the calling goroutine and returns
// the number of pairs evaluated. Run uses it; tests and benchmarks can call it directly.
func (e *Evaluator) ProcessPendingNow(ctx context.Context) int {
	evaluated := 0
	for j := e.takePending(); j != nil; j = e.takePending() {
		evaluated += e.processJob(ctx, j)
	}
	return evaluated
}

// Stats returns the number of pairs evaluated so far, jobs completed and the number of pod-level
// verdicts currently published.
func (e *Evaluator) Stats() (evaluatedPairs, completedJobs, publishedVerdicts int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.evaluatedPairs, e.completedJobs, len(e.results)
}

// HasPendingJob reports whether a job is waiting for the worker.
func (e *Evaluator) HasPendingJob() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pending != nil
}

func (e *Evaluator) enqueue(j *job) {
	e.mu.Lock()
	e.pending = j // Newest status wins.
	e.mu.Unlock()
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Evaluator) takePending() *job {
	e.mu.Lock()
	defer e.mu.Unlock()
	j := e.pending
	e.pending = nil
	return j
}

func (e *Evaluator) newSnapshot(opts snapshotOptions) *predicate.PredicateSnapshot {
	return predicate.NewPredicateSnapshot(store.NewBasicSnapshotStore(), e.fwHandle, opts.draEnabled, opts.parallelism, opts.csiEnabled, opts.verbosityOffset)
}

// processJob evaluates the job's pairs one template at a time: add the template node to the
// private snapshot, check every group against it, remove it again. Each predicate check therefore
// sees a one-node snapshot, so its cost depends neither on the cluster nor on the number of
// skipped node groups, and every node group is judged in isolation. The verdicts are published
// together once the whole job is done; a cancelled job publishes nothing.
func (e *Evaluator) processJob(ctx context.Context, j *job) int {
	logger := klog.FromContext(ctx)
	start := time.Now()
	snapshot := e.newSnapshot(j.snapshotOpts)
	results := make(map[verdictKey]clustersnapshot.SchedulingError)
	evaluated := 0
	for _, tmpl := range j.templates {
		if ctx.Err() != nil {
			return evaluated
		}
		nodeName := tmpl.nodeInfo.Node().Name
		if err := snapshot.AddNodeInfo(tmpl.nodeInfo); err != nil {
			logger.Error(err, "Skipped node groups evaluator: failed to add template NodeInfo to snapshot", "nodeGroupId", tmpl.nodeGroup)
			continue
		}
		for _, group := range j.groups {
			if err := e.limiter.Wait(ctx); err != nil {
				return evaluated
			}
			schedErr := snapshot.CheckPredicates(group.pod, nodeName)
			for _, uid := range group.uids {
				results[verdictKey{pod: uid, nodeGroup: tmpl.nodeGroup}] = schedErr
			}
			evaluated++
		}
		if err := snapshot.RemoveNodeInfo(ctx, nodeName); err != nil {
			logger.Error(err, "Skipped node groups evaluator: failed to remove template NodeInfo from snapshot, recreating it", "nodeGroupId", tmpl.nodeGroup)
			snapshot = e.newSnapshot(j.snapshotOpts)
		}
	}
	e.mu.Lock()
	e.results = results
	e.evaluatedPairs += evaluated
	e.completedJobs++
	e.mu.Unlock()
	logger.V(4).Info("Skipped node groups evaluator: job done", "pairs", evaluated, "templates", len(j.templates), "groups", len(j.groups), "duration", time.Since(start))
	return evaluated
}

// verdicts holds, per (group, template) pair, whether a verdict is known and what it is.
type verdicts struct {
	templates int
	errs      []clustersnapshot.SchedulingError
	known     []bool
}

func newVerdicts(groups, templates int) verdicts {
	return verdicts{
		templates: templates,
		errs:      make([]clustersnapshot.SchedulingError, groups*templates),
		known:     make([]bool, groups*templates),
	}
}

func (v verdicts) set(gi, ti int, schedErr clustersnapshot.SchedulingError) {
	v.errs[gi*v.templates+ti] = schedErr
	v.known[gi*v.templates+ti] = true
}

func (v verdicts) get(gi, ti int) (clustersnapshot.SchedulingError, bool) {
	return v.errs[gi*v.templates+ti], v.known[gi*v.templates+ti]
}

// lookupAndSchedule resolves the pairs from the latest published verdicts and hands all current
// pairs to the worker as one job. It runs on the main loop and does no predicate evaluation itself.
// A group's verdict is taken from the first of its pods that has one: all pods of an equivalence
// group get the same verdict, so new replicas of a known group are served right away. Groups flagged
// as not evaluable are neither looked up nor enqueued.
func (e *Evaluator) lookupAndSchedule(opts snapshotOptions, templates []skippedTemplate, samplePods []*apiv1.Pod, groupUIDs [][]types.UID, evaluable []bool) verdicts {
	results := newVerdicts(len(samplePods), len(templates))

	e.mu.Lock()
	for gi := range samplePods {
		if !evaluable[gi] {
			continue
		}
		for ti, tmpl := range templates {
			for _, uid := range groupUIDs[gi] {
				if schedErr, found := e.results[verdictKey{pod: uid, nodeGroup: tmpl.nodeGroup}]; found {
					results.set(gi, ti, schedErr)
					break
				}
			}
		}
	}
	e.mu.Unlock()

	// Build the job from private copies: the worker must not share the registry's NodeInfos (they
	// can be reused and mutated by node info providers in the next loop) or the pods.
	j := &job{snapshotOpts: opts}
	for gi, pod := range samplePods {
		if evaluable[gi] {
			j.groups = append(j.groups, jobGroup{pod: pod.DeepCopy(), uids: slices.Clone(groupUIDs[gi])})
		}
	}
	if len(j.groups) == 0 {
		return results
	}
	for _, tmpl := range templates {
		j.templates = append(j.templates, jobTemplate{nodeGroup: tmpl.nodeGroup, nodeInfo: tmpl.nodeInfo.DeepCopy()})
	}
	e.enqueue(j)
	return results
}

// stripPodAffinity drops inter-pod affinity/anti-affinity, which cannot be evaluated meaningfully
// against a lone template node, from an Affinity (which may be nil). It returns nil if nothing is
// left, so that the result serialises the same way as a pod that never had any affinity.
func stripPodAffinity(affinity *apiv1.Affinity) *apiv1.Affinity {
	if affinity == nil {
		return nil
	}
	affinity.PodAffinity = nil
	affinity.PodAntiAffinity = nil
	if affinity.NodeAffinity == nil {
		return nil
	}
	return affinity
}
