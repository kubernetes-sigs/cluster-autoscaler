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
// The main loop (ScaleUpStatusProcessor) only looks verdicts up in a cache and hands unknown
// (pod group, node group) pairs to the Evaluator, a single background goroutine with its own
// scheduler framework handle. Verdicts become visible one loop after a pair is first seen; until
// then the node group is left in NoScaleUpInfo.SkippedNodeGroups, which is exactly the behaviour
// without the feature.
package skippednodegroups

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"time"

	"golang.org/x/time/rate"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/klog/v2"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	schedulerconfiglatest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	"k8s.io/utils/clock"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/drain"
)

const (
	// DefaultRefreshAfter is how old a verdict may get before the worker re-checks it in the
	// background. The verdict keeps being used meanwhile (stale-while-revalidate): the cache key
	// already captures the pod spec and the node template, so only informer-backed predicate inputs
	// (PVC/PV binding state, CSINode limits) can change a verdict over time.
	DefaultRefreshAfter = 10 * time.Minute
	// DefaultEvictAfter drops verdicts that no loop has looked up for this long. Live pairs are looked
	// up every loop, so this only decides how long verdicts of vanished pods, of node groups that are
	// no longer skipped and of replaced templates linger.
	DefaultEvictAfter = 30 * time.Minute
	// DefaultMaxPairsPerLoop bounds the (pod group, skipped node group) pairs considered per loop,
	// and with it the main-loop lookups (~1us each), the size of a job and the growth of the cache.
	// Pod groups beyond the budget keep the plain skipped list, i.e. the output without this feature.
	DefaultMaxPairsPerLoop = 200_000
	// DefaultPairsPerSecond throttles the worker so that it never monopolises a core; at roughly
	// 15us per predicate check this is ~30% of one core while a job is being processed.
	DefaultPairsPerSecond = 20_000

	// evaluatorProfileSuffix is appended to the scheduler profile name of the evaluator's handle.
	evaluatorProfileSuffix = "-skipped-node-groups-evaluator"
	// sweepInterval bounds how often the worker scans the cache for idle verdicts.
	sweepInterval = time.Minute
)

// EvaluatorOptions configures the Evaluator.
type EvaluatorOptions struct {
	// RefreshAfter is the age after which a cached verdict is re-evaluated in the background.
	// The old verdict stays in use until the new one replaces it, so a verdict never regresses to
	// "unknown", however long a loop or a job takes.
	RefreshAfter time.Duration
	// EvictAfter is how long a verdict may go without being looked up before it is dropped.
	EvictAfter time.Duration
	// MaxPairsPerLoop is the maximum number of (pod group, skipped node group) pairs the processor
	// considers in one loop. Zero or negative means unlimited.
	MaxPairsPerLoop int
	// PairsPerSecond rate-limits the background worker. Zero or negative means unlimited.
	PairsPerSecond float64
	// Clock is used for verdict ageing; nil means the real clock.
	Clock clock.PassiveClock
}

// DefaultEvaluatorOptions returns the production defaults.
func DefaultEvaluatorOptions() EvaluatorOptions {
	return EvaluatorOptions{
		RefreshAfter:    DefaultRefreshAfter,
		EvictAfter:      DefaultEvictAfter,
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

// verdictKey identifies one (pod equivalence group, node group) verdict. The template signature is
// part of the key so that a node group update (labels, taints, machine type) invalidates the
// verdict without any explicit bookkeeping.
type verdictKey struct {
	group     string
	nodeGroup string
	template  string
}

type verdict struct {
	schedErr    clustersnapshot.SchedulingError
	evaluatedAt time.Time
	lastUsed    time.Time
}

// snapshotOptions carries the AutoscalingContext knobs needed to build a PredicateSnapshot.
type snapshotOptions struct {
	draEnabled      bool
	parallelism     int
	csiEnabled      bool
	verbosityOffset int
}

// jobGroup is one pod equivalence group to evaluate: its signature and a private copy of the
// exemplar pod with inter-pod affinity already stripped.
type jobGroup struct {
	signature string
	pod       *apiv1.Pod
}

// key identifies the group when jobs are merged.
func (g jobGroup) key() string {
	return g.signature
}

// jobTemplate is one skipped node group to evaluate: its id, template signature, a private copy of
// the template NodeInfo and the indices (into job.groups) of the groups to check against it.
type jobTemplate struct {
	nodeGroup string
	signature string
	nodeInfo  *framework.NodeInfo
	groups    []int
}

// job is the unit of work handed from the main loop to the worker. Everything it references is
// owned by the job (deep copies), so the worker never shares mutable state with the main loop.
type job struct {
	snapshotOpts snapshotOptions
	groups       []jobGroup
	templates    []jobTemplate
	pairs        int
}

// mergeJobs returns a job with the pairs of first, followed by the pairs of second that first does not
// contain yet, so that first's pairs are evaluated first. The result holds at most maxPairs pairs (zero
// or negative means unlimited) and shares the inputs' groups and NodeInfos, which jobs never modify.
// Either input may be nil.
func mergeJobs(first, second *job, maxPairs int) *job {
	if first == nil {
		return second
	}
	if second == nil {
		return first
	}
	merged := &job{snapshotOpts: first.snapshotOpts}
	groupIndex := make(map[string]int)
	templateIndex := make(map[string]int)
	seen := make(map[[2]string]bool)
	add := func(src *job) {
		for _, tmpl := range src.templates {
			templateKey := tmpl.nodeGroup + "/" + tmpl.signature
			for _, gi := range tmpl.groups {
				if maxPairs > 0 && merged.pairs >= maxPairs {
					return
				}
				group := src.groups[gi]
				pair := [2]string{templateKey, group.key()}
				if seen[pair] {
					continue
				}
				seen[pair] = true
				ti, found := templateIndex[templateKey]
				if !found {
					ti = len(merged.templates)
					templateIndex[templateKey] = ti
					merged.templates = append(merged.templates, jobTemplate{nodeGroup: tmpl.nodeGroup, signature: tmpl.signature, nodeInfo: tmpl.nodeInfo})
				}
				mgi, found := groupIndex[group.key()]
				if !found {
					mgi = len(merged.groups)
					groupIndex[group.key()] = mgi
					merged.groups = append(merged.groups, group)
				}
				merged.templates[ti].groups = append(merged.templates[ti].groups, mgi)
				merged.pairs++
			}
		}
	}
	add(first)
	add(second)
	return merged
}

// Evaluator answers "could this pod equivalence group schedule on that skipped node group?" off
// the main autoscaling loop. See the package documentation.
//
// Hand-off: every processed status hands over one job holding each of its pairs that is still unknown
// (or due for a refresh) once, grouped by template. Several statuses can be processed per loop (e.g.
// one per scale-up salvo iteration) with different pods, so a new job is merged into the pending one
// (newest pairs first, deduplicated, bounded by MaxPairsPerLoop) instead of replacing it. A running
// job yields to a pending one between templates and is merged back behind it, so no pair is dropped
// and the newest pairs are evaluated first. Whatever the worker finished is in the cache.
//
// Verdict lifecycle: evaluated once, applied on every loop that asks for it, re-evaluated in the
// background after RefreshAfter (while still being applied) and dropped once no loop has asked for
// it for EvictAfter. There is no other eviction; the per-loop pair budget bounds the growth.
type Evaluator struct {
	fwHandle *framework.Handle
	opts     EvaluatorOptions
	limiter  *rate.Limiter
	clock    clock.PassiveClock

	mu        sync.Mutex
	cache     map[verdictKey]verdict
	pending   *job
	wake      chan struct{}
	lastSweep time.Time

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
	c := opts.Clock
	if c == nil {
		c = clock.RealClock{}
	}
	return &Evaluator{
		fwHandle: fwHandle,
		opts:     opts,
		limiter:  rate.NewLimiter(limit, burst),
		clock:    c,
		cache:    make(map[verdictKey]verdict),
		wake:     make(chan struct{}, 1),
	}
}

// Run processes jobs until ctx is cancelled. It must run in its own goroutine.
func (e *Evaluator) Run(ctx context.Context) {
	var sweepCh <-chan time.Time
	if e.opts.EvictAfter > 0 {
		ticker := time.NewTicker(sweepInterval)
		defer ticker.Stop()
		sweepCh = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.wake:
			e.ProcessPendingNow(ctx)
		case <-sweepCh:
			e.evictIdle(e.clock.Now())
		}
	}
}

// ProcessPendingNow synchronously evaluates all pending jobs on the calling goroutine, drops idle
// verdicts and returns the number of pairs evaluated. Run uses it; tests and benchmarks can call it
// directly.
func (e *Evaluator) ProcessPendingNow(ctx context.Context) int {
	evaluated := 0
	for j := e.takePending(); j != nil; j = e.takePending() {
		evaluated += e.processJob(ctx, j)
	}
	e.evictIdle(e.clock.Now())
	return evaluated
}

// Stats returns the number of pairs evaluated so far, jobs completed and the cache size.
func (e *Evaluator) Stats() (evaluatedPairs, completedJobs, cacheSize int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.evaluatedPairs, e.completedJobs, len(e.cache)
}

// HasPendingJob reports whether a job is waiting for the worker.
func (e *Evaluator) HasPendingJob() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pending != nil
}

// lookupLocked returns the cached verdict for key if there is one, marks it as used, and reports
// whether it is old enough to be re-evaluated in the background. Callers must hold e.mu.
func (e *Evaluator) lookupLocked(key verdictKey, now time.Time) (schedErr clustersnapshot.SchedulingError, found, stale bool) {
	v, found := e.cache[key]
	if !found {
		return nil, false, false
	}
	v.lastUsed = now
	e.cache[key] = v
	return v.schedErr, true, now.Sub(v.evaluatedAt) > e.opts.RefreshAfter
}

func (e *Evaluator) store(key verdictKey, schedErr clustersnapshot.SchedulingError, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cache[key] = verdict{schedErr: schedErr, evaluatedAt: now, lastUsed: now}
	e.evaluatedPairs++
}

// evictIdle drops the verdicts that no loop has looked up for EvictAfter: their pods are gone, their
// node group is no longer skipped or their template was replaced. Live pairs are looked up every loop
// and therefore never qualify. The sweep runs at most once per sweepInterval; since the cache only
// grows through the worker, running it after each job is enough to bound the cache to the pairs
// looked up within the last EvictAfter.
func (e *Evaluator) evictIdle(now time.Time) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	if now.Sub(e.lastSweep) < sweepInterval {
		return 0
	}
	e.lastSweep = now
	evicted := 0
	for k, v := range e.cache {
		if now.Sub(v.lastUsed) > e.opts.EvictAfter {
			delete(e.cache, k)
			evicted++
		}
	}
	return evicted
}

func (e *Evaluator) enqueue(j *job) {
	e.mu.Lock()
	e.pending = mergeJobs(j, e.pending, e.opts.MaxPairsPerLoop) // Newest pairs first.
	e.mu.Unlock()
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// requeue merges the unprocessed rest of a job that yielded to a pending one behind the pending pairs.
func (e *Evaluator) requeue(rest *job) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending = mergeJobs(e.pending, rest, e.opts.MaxPairsPerLoop)
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
// private snapshot, check every requested group against it, remove it again. Each predicate check
// therefore sees a one-node snapshot, so its cost depends neither on the cluster nor on the number
// of skipped node groups, and every node group is judged in isolation.
func (e *Evaluator) processJob(ctx context.Context, j *job) int {
	logger := klog.FromContext(ctx)
	start := e.clock.Now()
	snapshot := e.newSnapshot(j.snapshotOpts)
	evaluated := 0
	for ti, tmpl := range j.templates {
		if ctx.Err() != nil {
			return evaluated
		}
		// A newer status arrived: evaluate its pairs first and merge the rest of this job back behind
		// them. Nothing is dropped; pairs that are no longer needed cost at most one evaluation.
		if ti > 0 && e.HasPendingJob() {
			e.requeue(&job{snapshotOpts: j.snapshotOpts, groups: j.groups, templates: j.templates[ti:]})
			logger.V(4).Info("Skipped node groups evaluator: newer job pending, requeueing the rest of the current one", "evaluatedPairs", evaluated, "totalPairs", j.pairs)
			return evaluated
		}
		nodeName := tmpl.nodeInfo.Node().Name
		if err := snapshot.AddNodeInfo(tmpl.nodeInfo); err != nil {
			logger.Error(err, "Skipped node groups evaluator: failed to add template NodeInfo to snapshot", "nodeGroupId", tmpl.nodeGroup)
			continue
		}
		for _, gi := range tmpl.groups {
			if err := e.limiter.Wait(ctx); err != nil {
				return evaluated
			}
			group := j.groups[gi]
			schedErr := snapshot.CheckPredicates(group.pod, nodeName)
			e.store(verdictKey{group: group.signature, nodeGroup: tmpl.nodeGroup, template: tmpl.signature}, schedErr, e.clock.Now())
			evaluated++
		}
		if err := snapshot.RemoveNodeInfo(ctx, nodeName); err != nil {
			logger.Error(err, "Skipped node groups evaluator: failed to remove template NodeInfo from snapshot, recreating it", "nodeGroupId", tmpl.nodeGroup)
			snapshot = e.newSnapshot(j.snapshotOpts)
		}
	}
	e.mu.Lock()
	e.completedJobs++
	e.mu.Unlock()
	logger.V(4).Info("Skipped node groups evaluator: job done", "pairs", evaluated, "templates", len(j.templates), "groups", len(j.groups), "duration", e.clock.Since(start))
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

// lookupAndSchedule resolves as many pairs as possible from the cache and hands the unknown ones
// (plus the ones due for a refresh) to the worker as one job. It runs on the main loop and does no
// predicate evaluation itself. Groups flagged as not evaluable are neither looked up nor enqueued.
func (e *Evaluator) lookupAndSchedule(opts snapshotOptions, templates []skippedTemplate, samplePods []*apiv1.Pod, evaluable []bool) verdicts {
	now := e.clock.Now()
	results := newVerdicts(len(samplePods), len(templates))

	groupSigs := make([]string, len(samplePods))
	for gi, pod := range samplePods {
		if evaluable[gi] {
			groupSigs[gi] = podGroupSignature(pod)
		}
	}
	templateSigs := make([]string, len(templates))
	for ti, tmpl := range templates {
		templateSigs[ti] = templateSignature(tmpl.nodeInfo)
	}

	unknownByTemplate := make([][]int, len(templates))
	refreshByTemplate := make([][]int, len(templates))
	unknown, refresh := 0, 0
	e.mu.Lock()
	for ti, tmpl := range templates {
		for gi := range samplePods {
			if !evaluable[gi] {
				continue
			}
			key := verdictKey{group: groupSigs[gi], nodeGroup: tmpl.nodeGroup, template: templateSigs[ti]}
			schedErr, found, stale := e.lookupLocked(key, now)
			if found {
				// Known verdicts are always applied; old ones are additionally re-checked in the
				// background so that a slow worker or a long loop never makes a verdict regress to unknown.
				results.set(gi, ti, schedErr)
				if stale {
					refreshByTemplate[ti] = append(refreshByTemplate[ti], gi)
					refresh++
				}
				continue
			}
			unknownByTemplate[ti] = append(unknownByTemplate[ti], gi)
			unknown++
		}
	}
	e.mu.Unlock()
	if unknown+refresh == 0 {
		return results
	}

	// Build the job from private copies: the worker must not share the registry's NodeInfos (they
	// can be reused and mutated by node info providers in the next loop) or the pods. Templates
	// with unknown pairs go first so that the worker produces new verdicts before refreshing old ones.
	j := &job{snapshotOpts: opts, pairs: unknown + refresh}
	groupIndex := make(map[int]int)
	addTemplate := func(ti int, groupLists ...[]int) {
		tmpl := templates[ti]
		jt := jobTemplate{nodeGroup: tmpl.nodeGroup, signature: templateSigs[ti], nodeInfo: tmpl.nodeInfo.DeepCopy()}
		for _, gis := range groupLists {
			for _, gi := range gis {
				jgi, seen := groupIndex[gi]
				if !seen {
					jgi = len(j.groups)
					groupIndex[gi] = jgi
					j.groups = append(j.groups, jobGroup{signature: groupSigs[gi], pod: samplePods[gi].DeepCopy()})
				}
				jt.groups = append(jt.groups, jgi)
			}
		}
		j.templates = append(j.templates, jt)
	}
	for ti := range templates {
		if len(unknownByTemplate[ti]) > 0 {
			addTemplate(ti, unknownByTemplate[ti], refreshByTemplate[ti])
		}
	}
	for ti := range templates {
		if len(unknownByTemplate[ti]) == 0 && len(refreshByTemplate[ti]) > 0 {
			addTemplate(ti, refreshByTemplate[ti])
		}
	}
	e.enqueue(j)
	return results
}

// podGroupSignature identifies a pod equivalence group across loops. It hashes what
// equivalence.BuildPodGroups compares: the controller, the labels and the scheduling-relevant part
// of the spec. Two pods of one group always get the same signature; a spec change (and therefore a
// different group) gets a new one.
func podGroupSignature(pod *apiv1.Pod) string {
	h := fnv.New64a()
	h.Write([]byte(pod.Namespace))
	h.Write([]byte{0})
	if ref := drain.ControllerRef(pod); ref != nil {
		h.Write([]byte(ref.UID))
	}
	h.Write([]byte{0})
	writeSortedMap(h, pod.Labels)
	spec := pod.Spec.DeepCopy()
	spec.NodeName = ""
	spec.Hostname = ""
	spec.Affinity = stripPodAffinity(spec.Affinity)
	projected := map[string]bool{}
	volumes := spec.Volumes[:0]
	for _, v := range spec.Volumes {
		if v.Projected != nil {
			projected[v.Name] = true
			continue
		}
		volumes = append(volumes, v)
	}
	spec.Volumes = volumes
	for _, containers := range [][]apiv1.Container{spec.InitContainers, spec.Containers} {
		for i := range containers {
			containers[i].Env = nil
			mounts := containers[i].VolumeMounts[:0]
			for _, m := range containers[i].VolumeMounts {
				if !projected[m.Name] {
					mounts = append(mounts, m)
				}
			}
			containers[i].VolumeMounts = mounts
		}
	}
	if b, err := json.Marshal(spec); err == nil {
		h.Write(b)
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

// templateSignature identifies what matters for predicates on a template node: labels, taints,
// allocatable, schedulability and what its (DaemonSet) pods already consume. Template NodeInfos are
// regenerated every loop under a fresh random name that is also written into the hostname label
// (see simulator.SanitizedTemplateNodeInfoFromNodeGroup), so the name and the labels carrying it
// are excluded; otherwise no verdict would ever survive to the next loop.
func templateSignature(nodeInfo *framework.NodeInfo) string {
	h := fnv.New64a()
	node := nodeInfo.Node()
	labels := make(map[string]string, len(node.Labels))
	for k, v := range node.Labels {
		if k == apiv1.LabelHostname || v == node.Name {
			continue
		}
		labels[k] = v
	}
	writeSortedMap(h, labels)
	if b, err := json.Marshal(node.Spec.Taints); err == nil {
		h.Write(b)
	}
	allocatable := make(map[string]string, len(node.Status.Allocatable))
	for name, q := range node.Status.Allocatable {
		allocatable[string(name)] = q.String()
	}
	writeSortedMap(h, allocatable)
	if node.Spec.Unschedulable {
		h.Write([]byte("unschedulable"))
	}
	if requested := nodeInfo.GetRequested(); requested != nil {
		if b, err := json.Marshal(requested); err == nil {
			h.Write(b)
		}
	}
	h.Write([]byte{0})
	h.Write([]byte(strconv.Itoa(len(nodeInfo.GetPods()))))
	if usedPorts := nodeInfo.GetUsedPorts(); len(usedPorts) > 0 {
		var ports []string
		for ip, portMap := range usedPorts {
			for pp := range portMap {
				ports = append(ports, fmt.Sprintf("%s/%s/%d", ip, pp.Protocol, pp.Port))
			}
		}
		sort.Strings(ports)
		for _, p := range ports {
			h.Write([]byte(p))
			h.Write([]byte{';'})
		}
	}
	if nodeInfo.CSINode != nil {
		if b, err := json.Marshal(nodeInfo.CSINode.Spec); err == nil {
			h.Write(b)
		}
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

func writeSortedMap(h interface{ Write([]byte) (int, error) }, m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{'='})
		h.Write([]byte(m[k]))
		h.Write([]byte{';'})
	}
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
