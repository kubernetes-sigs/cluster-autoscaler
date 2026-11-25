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

- **F2. Delay drain and deletion after tainting to account for cache lag.**
  Deleting or draining a node immediately after tainting creates two cache-sync races:
  1. `kube-scheduler` has not received the taint yet and binds a new pod to the node.
  2. `kube-scheduler` bound a pod right before receiving the taint, but CA's own pod informer has not observed the binding yet.

  In both cases, CA's pre-deletion pod check misses the pod, risking deleting a node with active workloads. Because CA cannot observe `kube-scheduler`'s cache directly, it must wait for a delay after tainting (either the static `--node-delete-delay-after-taint`, or twice the maximum taint-observation latency seen by CA's own informer over the past hour when `--dynamic-node-delete-delay-after-taint-enabled` is set) before taking its final pod snapshot and deleting nodes. This shrinks the race window, though it cannot eliminate it completely.

- **F3. Never drain or delete an untainted node.**
  If CA fails to taint a node, it must not drain or delete that node in the current loop. The node remains in the cluster, and the planner re-evaluates it in subsequent loops.

- **F4. Never leave taints permanently on nodes that CA will not delete.**
  If CA aborts deleting a node that it already tainted (such as when rolling back an atomic node group, or when drain/deletion fails), it must remove `ToBeDeletedByClusterAutoscaler` from that node: in the same loop on a best-effort basis, and eventually if that removal call fails. Leftover taints block pod scheduling on nodes CA is not deleting.

- **F5. Scale down atomic node groups atomically.**
  An atomic node group (`ZeroOrMaxNodeScaling`) must scale down all candidate nodes together. In a single loop, CA must either successfully taint every candidate in the group and start deletion for all of them, or delete none of them and remove any taints already applied to that group.

- **F6. Keep cluster taint state consistent for subsequent loops' scheduling simulations.**
  In every loop, CA simulates scheduling pending and recently evicted pods onto existing nodes before deciding whether to scale up or scale down. Those simulations must know which nodes are being deleted and cannot accept pods:
  - If tainting is synchronous, `StartDeletion` must finish its taint and rollback attempts before returning (and any taint left behind by a failed rollback must eventually be cleaned up per F4).
  - If tainting were asynchronous, CA would need to track nodes pending tainting in memory and reflect those intended taints in the cluster snapshot on subsequent loops.

- **F7. Clear contract for `StartDeletion` return values.**
  `StartDeletion` returns `(status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError)`:
  - **`ScaleDownResult`:** Returns `ScaleDownNoNodeDeleted` if no candidate nodes remain after budget cropping, `ScaleDownNodeDeleteStarted` if deletion starts for at least one node, and `ScaleDownError` only if candidates remained but CA could not taint any of them.
  - **`nodes`:** Returns the exact list of nodes sent to the asynchronous deletion phase (`nil` if no nodes started deletion).
  - **`err`:** Signals whether `StaticAutoscaler` should delay the next scale-down attempt (`--scale-down-delay-after-failure`). Any node whose taint succeeded still proceeds to deletion even when an error is returned.

### Non-Functional Requirements

- **N1. Keep `RunOnce` duration short.**
  `StartDeletion` runs synchronously inside `RunOnce`. Any synchronous delay blocks the rest of the loop, delaying scale-up evaluation and future scale-down cycles. Actuation must avoid blocking `RunOnce` unless correctness strictly requires it.

- **N2. Do not make CA the bottleneck when the control plane has capacity.**
  If the Kubernetes control plane and cloud provider can handle the load (with client QPS and concurrency configured to match), CA's tainting logic must scale to batches of hundreds or thousands of nodes per loop without self-inflicted stalls or throughput collapse.

- **N3. Isolate per-node taint errors without aborting `RunOnce` or blocking healthy nodes.**
  Isolated taint errors, whether transient API errors (conflicts, timeouts) or persistent per-node rejections (such as an admission webhook), must not abort the entire batch or trigger global cooldowns. Nodes that repeatedly fail to taint should not starve healthy candidates of scale-down budget. When heavy API server errors occur, CA should back off subsequent scale-down attempts while still deleting nodes already tainted, and taint errors must never fail `RunOnce` or threaten CA liveness probe health.

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

5. **Misleading error and status reporting (violates F7, N3):**
   When the empty pass succeeds and the drain pass fails, `StartDeletion` returns `(ScaleDownError, emptyNodes, err)` even though `emptyNodes` are actively being deleted. `ScaleDownError` causes `StaticAutoscaler` to treat scale-down as not started (skipping `lastScaleDownDeleteTime` and `ClusterStateRegistry` recalculation), while the non-nil `err` needlessly fails `RunOnce` and triggers `--scale-down-delay-after-failure`.

## All-or-Nothing Tainting: Suspected Rationale vs Reality

Neither the original design document nor PR #4908 recorded why drain-node tainting was implemented with all-or-nothing rollback. Limiting pod churn is suspected as the most likely intent:

- **The suspected churn scenario:** Suppose under-utilized nodes A and B both run pods. CA taints node A, but fails to taint node B. If CA drains node A, `kube-scheduler` might move those evicted pods onto node B because it has free capacity and remains untainted. If node B stays under-utilized in the next loop, CA drains node B, moving those pods a second time. Rolling back node A's taint would allow CA to retry nodes A and B together in the next loop and avoid the second eviction.
- **Why this rationale does not hold up in practice:**
  1. **Zero throughput at scale:** In large batches (such as 500 nodes), transient API errors are practically guaranteed. Rolling back the entire batch means zero nodes scale down.
  2. **Severe API amplification:** Rolling back hundreds of taints floods `kube-apiserver` with extra read and write requests while it is already failing.
  3. **Ineffective churn prevention:** In active clusters with continuous pod churn, untainting all `N` candidate nodes leaves all `N` nodes open to new workloads during cooldown. When cooldown expires, CA must evict those newly scheduled workloads anyway. Dropping only the failed node leaves exactly 1 under-utilized node open to new pods, rather than `N`.

Therefore, all-or-nothing rollback should be abandoned for standard node groups, and retained exclusively for atomic node groups where group-level atomicity is required for correctness.

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

4. **Updated return contract (F7, N3, N4):**
   `StartDeletion` returns `(ScaleDownNodeDeleteStarted, nodes, nil)` whenever at least one node begins deletion, so partial taint failures do not trigger `--scale-down-delay-after-failure` or fail `RunOnce`. It returns `(ScaleDownError, nil, err)` only when candidates remained after budget cropping but zero nodes could be tainted.

5. **Asynchronous post-taint delay (F2, N1):**
   Latency tracking and informer waiting move entirely into background goroutines, out of the critical path of `RunOnce`.
   - Each node's propagation timer is captured immediately before its taint request and recorded with the tracker only if the taint succeeds.
   - Once the synchronous pass completes, `StartDeletion` drops any rolled-back atomic group nodes from the tracker, registers the remaining tainted nodes, and hands them to a background worker.
   - When dynamic delay is enabled, the background worker observes taint propagation for the remaining nodes (up to 30 seconds), updates the 1-hour rolling maximum latency, and waits twice that duration before initiating drain and deletion.

## Consequences

- **Large scale-down parallelism is unlocked:** Operators can configure large `--max-scale-down-parallelism` values (hundreds or thousands of nodes) without transient errors stalling scale-down or failing `RunOnce`.
- **`RunOnce` no longer blocks on informer propagation:** Moving taint-observation waiting to the background removes up to 60 seconds of idle waiting from `RunOnce`. Because the tainting RPCs themselves remain synchronous, operators raising `--max-scale-down-parallelism` must size `--max-concurrent-nodes-tainting` and `--kube-client-qps`/`--kube-client-burst` proportionally so the tainting pass stays short.
- **Mixed atomic node groups are protected:** Atomic groups containing both empty and drain candidates can no longer become permanently stuck due to partial taint failures.
- **Reduced API server pressure on errors:** Standard node groups eliminate `2 * (N - 1)` sequential rollback calls when a node fails to taint.
- **Pod churn tradeoff:** If a candidate node fails to taint, pods evicted from other nodes in the batch may land on that untainted candidate and be evicted again in a later loop. In exchange, CA avoids leaving the entire batch of `N` under-utilized nodes untainted and open to new workloads during cooldown, which in busy clusters causes more repeated evictions overall.

## Observability

Because partial taint failures no longer fail `RunOnce` (and therefore no longer increment `errors_total`), partial scale-downs are surfaced through per-node and taint-phase signals:
- **Node removal latency (`node_removal_latency_seconds`):** Nodes whose deletion starts record `deleted=true`. Nodes that fail to taint or are rolled back in an atomic group record `NodeDeleteErrorFailedToMarkToBeDeleted` in `ScaleDownStatus.NodeDeleteResults`, attributing their delay to `delay_reason=FailedToMarkToBeDeleted` while they remain tracked across subsequent loops ([PR #168](https://github.com/kubernetes-sigs/cluster-autoscaler/pull/168)).
- **Events and logs:** Each node that fails to taint emits a `ScaleDownFailed` warning event, and `StartDeletion` logs a summary of taint failures grouped by API status reason/code alongside any rollback failures ([PR #146](https://github.com/kubernetes-sigs/cluster-autoscaler/pull/146)).
- **`scaled_down_nodes_total`:** Continues to count only nodes whose VM deletion completes in the cloud provider.

## Potential Follow-ups

- **Clean up leftover `ToBeDeletedByClusterAutoscaler` taints (F4, F6):** If removing a taint fails during an atomic group rollback or after a failed drain/deletion, clean up `ToBeDeletedByClusterAutoscaler` in subsequent loops on any node that is not currently being deleted.
- **Per-node backoff after taint failure (N3):** Back off nodes that repeatedly fail to taint (for example, due to an admission webhook) for `--unremovable-node-recheck-timeout` so they do not consume scale-down budget on every loop.
- **Scale-down backoff on heavy API server errors (`429`/`5xx`) (N3, N4):** When the proportion of throttling (`429`) or server (`5xx`) errors during a taint pass exceeds a threshold, pause scale-down for `--scale-down-delay-after-failure` on subsequent loops (while still deleting already-tainted nodes and without failing `RunOnce` or liveness checks).
- **Asynchronous tainting:** Move tainting off `RunOnce` into a background controller, with `StartDeletion` recording selected nodes in memory and returning immediately. Doing so requires:
  1. **In-memory taint state for scheduling simulations (F6):** Overlaying pending taints onto `ClusterSnapshot` so `filterOutSchedulablePodListProcessor` and `Planner.injectPods` do not pack pending or evicted pods onto nodes queued for tainting.
  2. **Asynchronous status reporting (F7):** Updating `StartDeletion` callers and `ScaleDownStatusProcessor` so they no longer assume `ScaledDownNodes` have already been tainted synchronously.
