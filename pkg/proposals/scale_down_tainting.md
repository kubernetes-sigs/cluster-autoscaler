# Scale-Down Node Tainting

## Scope

`Actuator.StartDeletion` in `pkg/core/scaledown/actuation/actuator.go` runs scale-down in two phases:

1. **Synchronous phase (`RunOnce`):** Crops candidate nodes to parallelism budgets, taints selected nodes with `ToBeDeletedByClusterAutoscaler` (`NoSchedule`), records which deletions started, and returns.
2. **Asynchronous phase (background):** Waits for taints to reach `kube-scheduler`, drains non-empty nodes, and deletes the underlying VM instances.

### Origin

The original parallel scale-down design in [`parallel_drain.md`](parallel_drain.md) described the synchronous phase in one sentence: *"synchronously taint the nodes to be scaled down as we currently do"*, followed by *"schedule draining and node deletion as a separate go routine for each node"*. It did not specify error handling or failure recovery.

Current tainting behavior evolved accidentally over time:
- **PR #4908 (Initial Actuator):** Tainted and deleted empty nodes sequentially, stopping on first error without rollback. For drain nodes, it introduced an all-or-nothing rollback (*"If tainting fails for any of the nodes, already applied taints are cleaned up"*) without documenting the design rationale.
- **PR #5237 (Taint delay):** Reused the drain-node `taintNodesSync` helper for empty nodes to support `--node-delete-delay-after-taint`. This inadvertently made empty-node tainting all-or-nothing as well.
- **Atomic node groups (`ZeroOrMaxNodeScaling`):** Layered all-or-nothing group semantics on top of these two separate sequential passes.

Because these behaviors grew incrementally rather than by design, scale-down tainting contains several bugs and performance bottlenecks. This proposal defines the requirements for synchronous scale-down actuation and outlines the changes needed to close the gap.

## Requirements

### Functional Requirements

- **F1. Taint all drain nodes before the first pod eviction.**
  Every node scheduled for drain in the current loop must be tainted before any pod is evicted. Otherwise, `kube-scheduler` can reschedule an evicted pod onto another node in the same batch before CA begins draining that second node. This applies only to drain nodes, as empty nodes do not evict pods.

- **F2. Wait after tainting for scheduler and informer caches to catch up.**
  Deleting or draining a node immediately after tainting creates two cache-sync races:
  1. `kube-scheduler` has not received the taint yet and binds a new pod to the node.
  2. `kube-scheduler` bound a pod right before receiving the taint, but CA's own pod informer has not observed the binding yet.

  In both cases, CA's pre-deletion pod check misses the pod, risking deleting a node with active workloads. CA must wait after tainting until both caches settle before taking its final pod snapshot and deleting nodes.

- **F3. Never drain or delete an untainted node.**
  If CA fails to taint a node, it must not drain or delete that node in the current loop. The node remains in the cluster, and the planner re-evaluates it in subsequent loops.

- **F4. Never leave taints on nodes that CA will not delete.**
  If CA decides not to delete a node in the current loop, it must not leave `ToBeDeletedByClusterAutoscaler` on that node. Leftover taints permanently block pod scheduling on nodes that CA never terminates.

- **F5. Scale down atomic node groups atomically.**
  An atomic node group (`ZeroOrMaxNodeScaling`) must scale down all candidate nodes together. In a single loop, CA must either successfully taint every candidate in the group and start deletion for all of them, or delete none of them and remove any taints already applied to that group.

- **F6. Complete all taint additions and rollbacks before the next loop starts.**
  CA must finish adding taints to selected nodes, and removing taints from aborted nodes, before `StartDeletion` returns. The current loop needs settled taint results to report which nodes started deletion (F7), and subsequent `RunOnce` loops require an accurate cluster taint state to make correct scaling decisions.

- **F7. Clear contract for `StartDeletion` return values.**
  `StartDeletion` returns `(status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError)`:
  - **`ScaleDownResult`:** Returns `ScaleDownNoNodeDeleted` if no candidate nodes remain after budget cropping, `ScaleDownNodeDeleteStarted` if deletion starts for at least one node, and `ScaleDownError` only if candidates remained but CA could not taint any of them.
  - **`nodes`:** Returns the exact list of nodes sent to the asynchronous deletion phase (`nil` if no nodes started deletion).
  - **`err`:** Signals whether `StaticAutoscaler` should delay the next scale-down attempt (`--scale-down-delay-after-failure`). Returns `nil` on success or isolated taint failures. Returns a non-nil error if no nodes could be tainted, or if the rate of API server throttling (`429`) or server (`5xx`) errors exceeds a failure threshold. Any node whose taint succeeded still proceeds to deletion even when an error is returned.

### Non-Functional Requirements

- **N1. Keep `RunOnce` duration short.**
  `StartDeletion` runs synchronously inside `RunOnce`. Any synchronous delay blocks the rest of the loop, delaying scale-up evaluation and future scale-down cycles. Actuation must avoid blocking `RunOnce` unless correctness strictly requires it.

- **N2. Sustain throughput for batches of thousands of nodes.**
  Actuation must support `--max-scale-down-parallelism` values from the default of 10 up to thousands of nodes per loop. Scale-down throughput must not drop to zero as batch sizes grow.

- **N3. Tolerate transient API errors without aborting `RunOnce`.**
  Isolated API server errors (conflicts, timeouts, throttling) are normal in large clusters and must not abort entire batches or trigger global cooldowns. When heavy errors occur, CA should back off subsequent scale-down attempts while still completing deletion for nodes already tainted. Taint errors must never fail `RunOnce` or threaten CA liveness probe health.

- **N4. Minimize and bound load on `kube-apiserver`.**
  Tainting must limit concurrency and avoid redundant API traffic. When `kube-apiserver` is degraded or returning errors, CA must not amplify load by issuing rollback calls unless required for correctness.

## How the Current Code Fails

1. **Informers block `RunOnce` synchronously (violates N1):**
   When `--dynamic-node-delete-delay-after-taint-enabled` is set, `StartDeletion` blocks inside `RunOnce` waiting for CA's node informer to observe taints (up to 30 seconds per pass, twice per loop). This delay is only needed before deleting or draining nodes in the background, not before `RunOnce` finishes.

2. **All-or-nothing rollback kills throughput on large batches (violates N2, N3):**
   Each tainting pass is all-or-nothing. A single API failure in a batch of 500 nodes aborts the entire batch, rolls back all successful taints, returns `ScaleDownError`, fails `RunOnce`, and halts scale-down for `--scale-down-delay-after-failure`.

3. **Rollback amplifies API load during server distress (violates N4):**
   When CA cancels a batch of `N` nodes after an API failure, it sequentially cleans up taints. Because the rollback logic passes the pre-taint node pointer, `CleanToBeDeleted` issues both a `Get` and an `Update` for each node, sending up to `2 * (N - 1)` extra requests to an already struggling API server.

4. **Split passes leave mixed atomic node groups stuck (violates F4, F5):**
   `StartDeletion` runs two sequential passes: empty nodes first, drain nodes second. Empty nodes launch background deletion before drain nodes are even tainted. If an atomic group contains both empty and drain nodes, and a drain node fails to taint, the second pass rolls back all drain nodes. However, the empty nodes remain queued in the background deletion tracker, tainted forever. Subsequent loops skip the empty nodes (marked in-progress) and reject the remaining drain nodes (group incomplete), wedging the atomic group until CA restarts.

5. **Misleading error reporting (violates F7):**
   When the empty pass succeeds and the drain pass fails, `StartDeletion` returns `(ScaleDownError, emptyNodes, err)`. Returning a non-nil error while empty nodes are actively being deleted confuses callers, who treat errors as "no scale-down started".

## All-or-Nothing Tainting: Suspected Rationale vs Reality

Neither the original design document nor PR #4908 recorded why drain-node tainting was implemented with all-or-nothing rollback. Limiting pod churn is suspected as the most likely intent:

- **The suspected churn scenario:** Suppose under-utilized nodes A and B both run pods. CA taints node A, but fails to taint node B. If CA drains node A, `kube-scheduler` might move those evicted pods onto node B because it has free capacity and remains untainted. If node B stays under-utilized in the next loop, CA drains node B, moving those pods a second time. Rolling back node A's taint would allow CA to retry nodes A and B together in the next loop and avoid the second eviction.
- **Why this rationale does not hold up in practice:**
  1. **Zero throughput at scale:** In large batches (such as 500 nodes), transient API errors are practically guaranteed. Rolling back the entire batch means zero nodes scale down.
  2. **Severe API amplification:** Rolling back hundreds of taints floods `kube-apiserver` with extra read and write requests while it is already failing.
  3. **Ineffective churn prevention:** In active clusters with continuous pod churn, untainting all `N` candidate nodes leaves all `N` nodes open to new workloads during cooldown. When cooldown expires, CA must evict those newly scheduled workloads anyway. Dropping only the failed node leaves exactly 1 under-utilized node open to new pods, rather than `N`.

Users seeking strict disruption bounds can configure `PodDisruptionBudget`s, which the drain phase fully respects. Therefore, all-or-nothing rollback should be abandoned for standard node groups, and retained exclusively for atomic node groups where group-level atomicity is required for correctness.

## Proposed Design

1. **Single-pass unified tainting:**
   `StartDeletion` will taint all empty and drain candidates together in a single concurrent pass, starting background deletion only after the pass finishes.
   - **Higher throughput, shorter `RunOnce` (N1, N2):** All workers in `--max-concurrent-nodes-tainting` pull from a shared queue. Fast drain nodes no longer wait for slow empty nodes or conflict retries.
   - **Unified atomic group handling (F4, F5, F6):** Atomic groups are validated and rolled back as a complete unit before any node is registered for deletion, eliminating mixed-group wedge states.
   - **Accurate status reporting (F7):** `StartDeletion` determines the final set of successfully tainted nodes before launching any asynchronous deletions.
   - **Single latency observation (F2, N1):** Replaces two 30-second informer wait windows with a single post-taint wait per batch.

2. **Per-node failure isolation for standard groups (N2, N3, N4):**
   If a taint call fails for a node in a standard node group, CA drops only that node from the batch. Successfully tainted nodes continue to deletion without triggering batch rollback.

3. **Fast-fail and group rollback for atomic groups (F4, F5, F6, N4):**
   If a taint call fails for any node in an atomic group, workers skip remaining unstarted nodes in that group. CA excludes the entire group from deletion and removes taints from any nodes in that group that were already tainted in the current pass.

4. **Updated return contract and error backoff (F7, N3, N4):**
   `StartDeletion` returns `ScaleDownNodeDeleteStarted` whenever at least one node begins deletion, and `ScaleDownError` only when candidates remained but zero nodes could be tainted.
   - **Isolated errors:** If only a small share of taint calls fail, `StartDeletion` returns `nil` error so scale-down continues without cooldown next loop.
   - **Systemic errors:** If no nodes could be tainted, or if the proportion of throttling (`429`) or server (`5xx`) errors exceeds a threshold, CA deletes already-tainted nodes and returns a non-nil error. `StaticAutoscaler` pauses scale-down for `--scale-down-delay-after-failure` on subsequent loops without failing `RunOnce` or liveness checks.

5. **Asynchronous post-taint delay (F2, N1):**
   Latency tracking and informer waiting move entirely into background goroutines, out of the critical path of `RunOnce`.
   - Each node's propagation timer starts immediately before sending its taint request.
   - Once the synchronous pass completes, `StartDeletion` registers tainted nodes and hands them to a background worker.
   - When dynamic delay is enabled, the background worker observes taint propagation (up to 30 seconds), updates the 1-hour rolling maximum latency, and waits twice that duration before initiating drain and deletion.

## Consequences

- **Large scale-down parallelism is unlocked:** Operators can configure large `--max-scale-down-parallelism` values (hundreds or thousands of nodes) without transient errors stalling scale-down or failing `RunOnce`.
- **`RunOnce` latency is dramatically reduced:** `RunOnce` no longer blocks for up to 60 seconds waiting for informers to observe taints.
- **Mixed atomic node groups are protected:** Atomic groups containing both empty and drain candidates can no longer become permanently stuck due to partial taint failures.
- **Reduced API server pressure on errors:** Standard node groups eliminate `2 * (N - 1)` sequential rollback calls when a node fails to taint.
- **Controlled pod churn tradeoff:** Small batches may occasionally reschedule pods from drained nodes onto untainted candidate nodes if a taint fails, but workloads remain protected by PDBs, and cluster-wide churn is minimized compared to full batch rollbacks.
