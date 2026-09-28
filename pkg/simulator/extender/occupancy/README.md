# Complete node-local occupancy

This opt-in adapter sends complete Node and Pod objects, including every
resident in the current snapshot, to the existing extender Filter endpoint.
This extension is disabled by default, requires explicit support from the
selected endpoint, and is not part of the standard Kubernetes scheduler extender
API. The current wire version is `experimental-full-occupancy-v1alpha1`; the
contract remains subject to community review. Callers and evaluators implement
the JSON contract independently; this package is not a shared device SDK.

## Configuration

Configure the usual scheduler extender, with `filterVerb` and explicit
`managedResources`, then select its exact `urlPrefix` using
`--extender-occupancy-urls=https://device-evaluator:443`.
Only selected extenders send the extension. Others retain the standard
Kubernetes request and failure behavior.

A selected extender must not be ignorable. Its HTTP timeout and explicit TLS
client configuration apply. TLS verification remains enabled unless explicitly
disabled. The adapter sends full Nodes even when `nodeCacheCapable` is true,
without changing another scheduler's configuration.

## Request and response contract

The `simulation` object contains `version`, `resources` (the configured managed
resource names), and `nodes`, a map from candidate node name to its complete
resident list. Each resident has a full `pod` and `allocation: {"mode": ...}`:

- `preserve`: the evaluator extracts its assignment metadata from the Pod and
  validates the binding, target inventory, resource usage and user constraints.
  Missing or invalid evidence for an interested Pod is an error; it must not
  fall back to fresh allocation or zero occupancy.
- `allocate`: the evaluator recomputes the assignment, ignoring only its own
  generated allocation results while retaining user device-selection constraints.

CA does not interpret or synthesize device allocation metadata. Bound residents
use preserve; simulated placements, nominated unbound Pods and template copies
use allocate. A resident outside this extender's managed resources also uses
allocate and is still sent in full, so the evaluator receives complete context.
No caller lifecycle operation or execution order is sent over the wire.

Empty resident arrays mean empty occupancy; absent or null arrays are invalid.
The evaluator must not supplement occupancy from live caches or earlier calls.
Each candidate is evaluated independently. The response's `simulation` object
must acknowledge the same version and the SHA-256 digest of the raw request
body (`requestDigest`). This correlates the response, not authenticates it.
Every candidate must appear exactly once among passed or failed nodes.
Missing acknowledgement, unsupported resource declarations, unknown versions,
invalid responses and transport errors fail closed. Bodies above 1 MiB are
rejected; occupancy is never truncated.

## Scope and limitations

Device support and policies outside node-local occupancy belong to each
evaluator. A generic protocol does not add device backend support or establish
real hardware feasibility. Templates without usable inventory remain unsupported
by evaluators that require that inventory. Node-scoped template device identities
do not prove the UUIDs of future machines.
