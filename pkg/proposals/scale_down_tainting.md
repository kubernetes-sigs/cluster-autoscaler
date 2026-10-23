# Scale-down tainting

## Scope

`Actuator.StartDeletion` in `pkg/core/scaledown/actuation/actuator.go` has two phases:

- The synchronous phase runs in `RunOnce`. It crops the candidate nodes to the scale-down parallelism budgets, adds the `ToBeDeletedByClusterAutoscaler` (`NoSchedule`) taint to the selected nodes, records the nodes that start deletion, and returns.
- The asynchronous phase runs in the background. It waits for the taint to reach `kube-scheduler`, drains the non-empty nodes, and deletes the node instances.

The original parallel scale-down design in [`parallel_drain.md`](parallel_drain.md) describes the synchronous phase in one sentence ("synchronously taint the nodes to be scaled down as we currently do", and then "schedule draining and node deletion as a separate go routine for each node") and does not specify how CA should handle taint failures. In the first `Actuator` implementation (kubernetes/autoscaler#4908), CA tainted and started deleting empty nodes one by one (stopping at the first error without removing previous taints), while for drain nodes `taintNodesSync` added all-or-nothing taint rollback ("If tainting fails for any of the nodes, already applied taints are cleaned up") without a recorded reason. Later, kubernetes/autoscaler#5237 reused `taintNodesSync` for empty nodes when it added `--node-delete-delay-after-taint`, which made empty-node tainting all-or-nothing as well, and atomic node groups were added on top of those two passes.

Because of this history, the way tainting works and handles errors today is accidental rather than intentional, and it contains several bugs and performance problems. This proposal exists to specify how the synchronous phase of actuation must behave and what changes are needed to close the gap between the current code and those requirements.

## Requirements

### Functional

F1. Taint all drain nodes before the first pod eviction. CA must add the taint to every node that it will drain in the current loop before it evicts the first pod. Otherwise, `kube-scheduler` can schedule an evicted pod onto another node in the same batch before CA drains that second node. This rule applies only to drain nodes because empty nodes do not evict pods.

F2. Wait after tainting before draining or deleting. A taint stops pod scheduling on a node only after the `kube-scheduler` informer receives the taint. Right before CA drains or deletes a node, CA takes a pod snapshot from its own pod informer. If an empty node has a pod in that snapshot, CA aborts the deletion of that node. If a drain node has a new pod in that snapshot, CA includes that pod in the drain. However, if CA takes the snapshot too early, two problems can occur:
   - `kube-scheduler` has not received the taint yet and binds a pod to the node after the snapshot.
   - `kube-scheduler` bound a pod to the node right before it received the taint, and CA's pod informer has not received that binding yet.

   In both cases, the snapshot misses the pod, and CA deletes the node while the pod runs on it. Therefore, after CA adds the taints, it must wait long enough for `kube-scheduler` to receive the taints and for CA's pod informer to receive any earlier pod bindings.

F3. Do not drain or delete an untainted node. If CA fails to add the taint to a node, CA must not drain or delete that node in the current loop. The node stays in the cluster, and the planner evaluates it again in the next loop.

F4. Do not leave taints on nodes that CA will not delete. If CA will not delete a node in the current loop, the node must not keep the `ToBeDeletedByClusterAutoscaler` taint. A leftover taint prevents pod scheduling on a node that CA never deletes.

F5. Scale down atomic node groups as a whole or not at all. An atomic node group (`ZeroOrMaxNodeScaling`) must scale down all of its candidate nodes together. In a single loop, CA must either add the taint to every candidate node in the group and start deletion for all of them, or start deletion for none of them and remove the taints that it already added to that group.

F6. Finish all taint additions and taint removals before the next loop starts. CA must add the taints to the selected nodes, and remove the taints from any aborted nodes, before `StartDeletion` returns and the next `RunOnce` loop starts. `StartDeletion` needs the final taint results to return the list of nodes that started deletion (F7). The next `RunOnce` loop also needs the final taint state on the nodes to make correct scale-up and scale-down decisions.

F7. Return values of `StartDeletion`. `StartDeletion` returns `(status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError)`:
   - `ScaleDownResult` is `ScaleDownNoNodeDeleted` when no candidate nodes remain after budget cropping, `ScaleDownNodeDeleteStarted` when deletion starts for at least one node, and `ScaleDownError` when candidate nodes remain after budget cropping but CA fails to taint all of them.
   - `nodes` is the exact list of nodes that CA sends to the asynchronous phase (`nil` when no node starts deletion).
   - `err` tells `StaticAutoscaler` whether to back off scale-down for `--scale-down-delay-after-failure` before the next attempt. `err` is nil when all taints succeed or when only a small share of taint calls in the batch fail. `err` is non-nil when CA cannot taint any node in the batch, or when the share of `kube-apiserver` throttling or server errors in the batch exceeds a threshold. Even when `err` is non-nil, any nodes in `nodes` whose taint succeeded still proceed to deletion.

### Non-functional

N1. Keep `RunOnce` duration short. `StartDeletion` runs inside `RunOnce`. Any synchronous work in `StartDeletion` delays the rest of the current loop and the next loop (including scale-up). `StartDeletion` must not delay `RunOnce` unless correctness requires it.

N2. Maintain scale-down throughput for batches of thousands of nodes. Actuation must support `--max-scale-down-parallelism` values from the default of 10 up to thousands of nodes per loop. Throughput must not drop to zero as the batch size grows.

N3. Tolerate isolated `kube-apiserver` errors, and back off on heavy errors without failing `RunOnce`. Isolated `kube-apiserver` errors (conflicts, timeouts, throttling) occur during normal operation and must not stop scale-down or trigger cooldown. When all taint calls fail or the share of throttling/server errors in a batch exceeds a threshold, CA must still delete the nodes that it already tainted and delay the next scale-down attempt (`--scale-down-delay-after-failure`), without aborting `RunOnce` or failing the CA liveness check.

N4. Minimize and limit the request load on `kube-apiserver`. Tainting must limit the number of concurrent API requests and avoid unnecessary calls. When `kube-apiserver` returns errors, CA must not send extra API requests unless correctness requires them.

## How the current code fails to meet the requirements

- When `--dynamic-node-delete-delay-after-taint-enabled` is true, `StartDeletion` waits inside `RunOnce` until CA's node informer receives the new taints (up to 30 seconds per pass, twice per loop) to compute the delay after tainting (N1). CA needs this delay only before it drains or deletes nodes in the background, not before `RunOnce` continues.
- Each tainting pass is all-or-nothing. If one taint call fails, CA cancels the pass, removes the taints that it already added in that pass, returns `ScaleDownError`, fails `RunOnce`, and blocks scale-down for `--scale-down-delay-after-failure` (N2, N3).
- When CA cancels a pass of `N` nodes after one failure, it sends up to `N - 1` extra `Get` and `Update` requests while `kube-apiserver` is already returning errors (N4).
- `StartDeletion` runs two sequential passes: empty nodes first, and drain nodes second. It starts background deletion for the empty nodes before it taints the drain nodes. This splits an atomic node group that has both empty and drain candidates across the two passes. If the empty pass succeeds and any drain node in the second pass fails to taint (even a drain node from a different node group), the second pass removes the taints from all drain nodes in the batch. The atomic group's empty nodes are already in the background deletion queue waiting for the rest of the group, and they keep their taint. In every later loop, CA excludes the empty nodes because their deletion is in progress, and rejects the drain nodes because the atomic group is incomplete (F4, F5). Only a CA restart clears this state.
- When the empty pass succeeds and the drain pass fails, `StartDeletion` returns `(ScaleDownError, emptyNodes, err)`. It returns a non-nil error while the empty nodes are already being deleted in the background (F7). Callers treat any non-nil error as "no scale-down started".

## All-or-nothing tainting of drain nodes

All-or-nothing tainting of drain nodes trades scale-down throughput and `kube-apiserver` load for a lower chance of pod churn:

- On a small cluster with a small batch, removing all taints after one failure can reduce pod churn. Suppose two under-utilized nodes A and B both run pods. CA taints node A, but fails to taint node B. If CA drains node A, `kube-scheduler` can schedule the evicted pods onto node B because node B is untainted and has free capacity. If node B is still below the utilization threshold in the next loop, CA drains node B too, and those pods move a second time. If CA instead removes the taint from node A, it retries nodes A and B together in the next loop and avoids the second pod move.
- On a large cluster with a large `--max-scale-down-parallelism` (for example, 500 nodes), this behavior causes three problems:
  1. It stops scale-down. In a batch of hundreds of nodes, at least one transient API error is likely in every loop. If CA removes all taints on the first failure and blocks scale-down for `--scale-down-delay-after-failure`, CA cannot scale down any nodes.
  2. It increases `kube-apiserver` load. Removing the taints sends up to `N - 1` extra `Get` and `Update` requests while `kube-apiserver` is already returning errors.
  3. It does not prevent extra pod churn. Large clusters have continuous pod creation. If CA removes the taints from all `N` under-utilized nodes, `kube-scheduler` can schedule new pods onto any of those `N` nodes during the failure cooldown. Any of those nodes that remains below the utilization threshold will scale down in a later loop, which evicts those new pods. Dropping only the failed node leaves one under-utilized node (B) open to new pods. Removing all taints leaves `N` nodes open.

On a small cluster, users who want to limit pod disruption can configure a `PodDisruptionBudget`, which the drain respects.

Therefore, we stop using all-or-nothing tainting for standard node groups, and keep it only for atomic node groups where correctness requires it.

## Proposal

1. Taint empty and drain nodes in one pass. `StartDeletion` taints all empty and drain candidates together in a single pass, and starts background deletion goroutines only after the pass finishes. This single pass has four benefits:
   - Higher tainting throughput and shorter `RunOnce` duration (N1, N2). All `--max-concurrent-nodes-tainting` workers pull from one queue of empty and drain nodes. Workers do not wait for the slowest empty node (for example, a node retrying a conflict) before they start tainting drain nodes. When `--dynamic-node-delete-delay-after-taint-enabled` is true, workers also do not wait up to 30 seconds for empty-node taints to reach the informer cache before they taint drain nodes.
   - Simpler atomic group validation and fail-fast (F4, F5, F6). CA validates and rolls back an atomic node group as a whole before it registers any node for deletion, even when the group has both empty and drain candidates.
   - Valid `StartDeletion` return values (F7). Because CA does not start empty-node deletions before it taints drain nodes, `StartDeletion` knows the full set of tainted nodes before it starts any background deletion.
   - One latency measurement and one post-taint wait per batch (F2, N1), instead of two per loop.
2. Isolate taint failures per node in standard node groups. If a taint call fails for a node in a standard node group, CA drops only that node from the batch and continues with the remaining nodes (N2, N3, N4).
3. Fail fast and roll back per group in atomic node groups. If a taint call fails for a node in an atomic node group, workers skip any remaining nodes in that group whose taint call has not started. CA excludes that group from deletion and removes the taint from any nodes in that group that it tainted during the current pass before `StartDeletion` returns (F4, F5, F6, N4).
4. Return contract and scale-down backoff on heavy API errors (F7, N3, N4). `StartDeletion` returns `ScaleDownNodeDeleteStarted` whenever deletion starts for at least one node, and `ScaleDownError` only when candidate nodes remained after cropping but CA could not taint any of them. When only a small share of taint calls fail, `StartDeletion` returns a nil error so scale-down continues without cooldown in the next loop. When no node could be tainted, or when the share of `kube-apiserver` throttling (`429`) or server (`5xx`) errors in the batch exceeds a threshold, CA still deletes the nodes that it already tainted in the current loop and returns a non-nil error so `StaticAutoscaler` delays the next scale-down attempt for `--scale-down-delay-after-failure` without aborting `RunOnce`.
5. Wait for the post-taint delay once per batch in the background (F2, N1). CA records taint propagation latency only for nodes whose taint succeeded and was not removed during an atomic group rollback. Each node's timer starts right before CA sends the taint API call for that node. After the synchronous taint and rollback pass finishes, `StartDeletion` registers the tainted nodes for deletion and starts one background goroutine:
   - When `--dynamic-node-delete-delay-after-taint-enabled` is false, the goroutine sleeps `--node-delete-delay-after-taint`.
   - When `--dynamic-node-delete-delay-after-taint-enabled` is true, the goroutine waits until CA's node informer receives the batch's taints (up to 30 seconds) to record the current batch's propagation latency. It then sleeps twice the maximum propagation latency observed in the last hour (or `--node-delete-delay-after-taint` if no latency was recorded in the last hour).
   - After the sleep finishes, the goroutine starts draining and deleting the nodes.

## Status

Proposed.

## Consequences

- Operators can use large `--max-scale-down-parallelism` values (hundreds or thousands of nodes). Large batches have a high probability of hitting isolated `kube-apiserver` errors, and a small number of taint failures in a standard node group no longer aborts the batch, sends `N - 1` rollback calls, fails `RunOnce`, or blocks scale-down. If the share of throttling or server errors in the batch exceeds a threshold, CA still deletes the nodes that it already tainted and backs off scale-down for `--scale-down-delay-after-failure` on subsequent loops.
- When `--dynamic-node-delete-delay-after-taint-enabled` is true, `RunOnce` no longer blocks for up to 60 seconds waiting for taints to reach the node informer, and drain nodes no longer wait for empty-node taints to propagate before CA taints them.
- Mixed atomic node groups cannot get stuck when a drain taint fails in the batch, because CA taints and validates the entire group before it registers any of the group's nodes for deletion.
- Keeping the remaining nodes in a standard node group when one drain node fails to taint avoids scale-down stalls and avoids `N - 1` rollback calls to `kube-apiserver`. In exchange, small batches can see extra pod churn: if CA taints drain node A and fails to taint drain node B, `kube-scheduler` can schedule pods evicted from node A onto node B. If node B is still below the utilization threshold in a later loop (and no `PodDisruptionBudget` blocks eviction), CA drains node B too and evicts those pods a second time.
- Taint failures no longer abort `RunOnce` or jeopardize the CA liveness check. CA reports individual taint failures through `ScaleDownFailed` events on the failed nodes and error log entries.
