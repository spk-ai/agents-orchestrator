# Prepared Workload Controllers

This branch now contains the dependent [resource-anchor controller integration](RESOURCE-ANCHORS.md).
The preparation-recovery lifecycle below describes its base. New starts require
the newer API/native/registry revisions in that report, including registry
migrations through `0024`; the old reproduction combination below is not a
substitute for those dependencies.

Dependent preparation-recovery follow-up on `754e935`, requiring the prepared
registry (`spk-ai/runners` `e7c42f4`, unchanged) and the API/native runner proposals
`feat/prepared-outcome-observation`. This is a source proposal, not a published
API, compatible stock image or production deployment. This focused branch does
not include the separately reviewed workload DNS correction; an installed
prepared/DNS stack must not be replaced with this branch alone.

## Contract Owners

Shared new-start authority is documented beside `startPreparedWorkload` in
[prepared_start.go](internal/reconciler/prepared_start.go).
Exact identity, monotonic evidence, removal and read-only activation-ACK recovery
live in [prepared_workloads.go](internal/reconciler/prepared_workloads.go).
Native owner persistence lives in
[resource_anchors.go](internal/reconciler/resource_anchors.go); workspace
generation checks live in [checked_volumes.go](internal/reconciler/checked_volumes.go).

Existing legacy records keep their removal path. Unsupported prepared/anchored
RPCs do not authorize fallback or agent-message replay.

## Lost Preparation Recovery

The retirement-only discovery contract lives beside `recoverPreparedRemovalBinding`
in [prepared_recovery.go](internal/reconciler/prepared_recovery.go).
Subsequent anchored proof recovery lives in
[preparation_revocation.go](internal/reconciler/preparation_revocation.go).
The dated acceptance below describes its original preparation-observation scope.

NotFound, Unimplemented, absent/invalid ownership markers, changed snapshots,
incomplete volume sets and identity/generation conflicts retain admission. This
is not initially-absent/late-create fencing, a credential revocation journal,
automatic adoption of old resources, or an exactly-once side-effect guarantee.

The source suite passes 663 ordinary and 662 selected race-test entries on
2026-09-15. Five opt-in/child entries skip outside their gates. The selected
suite excludes exactly the previously documented group-consumer race. All
1,320 focused recovery entries pass over 20 race-enabled repetitions, including
both owner kinds, first/existing/zero-volume workspaces, invalid observations,
competing-controller RPC interleavings, and failures before/after volume bind,
workload bind, native removal and confirmation. Build and vet excluding `assign`
pass; the unchanged full-vet self-assignment remains. Generated LLM API churn is
excluded from the contribution and these final source checks.

## Recovery Process Acceptance

The final opt-in execution run on 2026-09-15 passes **32 scenarios plus both
owner groups and parent** (35 entries), with no failures/skips, under the race
detector. It uses real PostgreSQL, registry/native RPCs, separate controller
processes and Kubernetes, with independent SQL and Pod/PVC identity checks.

Both owner paths recover a lost prepare response after SIGKILL/replacement of
all three application processes. Recovery itself survives SIGKILL after native
observation, volume binding and workload binding. Overlapping controllers also
converge: a stale observer accepts another controller's exact durable removal
without repeating native removal. Cancellation retires an unbound Pod before
its delayed prepare reply arrives. An initially missing preparation retains
admission; it does not gain an invented absence receipt.

Each recovered unexecuted Pod is followed by a new-Pod/same-PVC first turn that
verifies no prior effects. The previous parallel/follow-up, activation-ACK loss,
removal-crash and unused-reservation scenarios remain enabled. Agents metadata,
authorization tuple writes and the sandbox owner lookup are still explicit
stubs; no native agent, model credentials, A2A workflow or actual Ziti policy is
under test. Production rollout, initially absent/late-operation fencing and
durable credential revocation remain open.

## Earlier Verification

On 2026-09-14:

- All **597 ordinary tests including subtests** pass.
- All **596 selected race-test entries** pass. Exactly the pre-existing
  `TestGroupMembershipConsumerLoopRetriesWithoutBlocking` is excluded; the
  unfiltered race run still fails in that unchanged fake-subscription test.
- **2,360 focused entries across 20 repetitions** pass, covering both owners,
  real assembler entry points, preserved identity/placement metadata,
  first provision/follow-up, unknown replies, cancellation interleavings,
  removal restart windows, malformed evidence and unsafe volume plans.
- Build and `go vet -assign=false ./...` pass. Full vet still reports the
  pre-existing `ctx = ctx` in `start_decision.go:186`.

Those component controller tests use explicit in-memory registry and native fixtures;
they do not redirect legacy RPC callbacks into new ones. Replacement reconciler
objects model restart recovery, not process SIGKILL. The existing opt-in
`TestLiveCheckedVolumeStack`, its process entry point and `TestLiveVolumeRetention`
were not enabled in these runs. Native inspection has separate real Kubernetes
acceptance, not a combined controller/registry/A2A pass.

Generate all required local API packages using the repository's `buf.gen.yaml`
template. In adjacent `api-prepared-observation`, run:

```bash
buf generate . --template ../orchestrator-prepared-recovery/buf.gen.yaml \
  --output ../orchestrator-prepared-recovery --include-imports \
  --path proto/agynio/api/runner/v1 --path proto/agynio/api/runners/v1 \
  --path proto/agynio/api/threads/v1 --path proto/agynio/api/notifications/v1 \
  --path proto/agynio/api/metering/v1 --path proto/agynio/api/agents/v1 \
  --path proto/agynio/api/secrets/v1 --path proto/agynio/api/ziti_management/v1 \
  --path proto/agynio/api/groups/v1 --path proto/agynio/api/identity/v1 \
  --path proto/agynio/api/users/v1 --path proto/agynio/api/organizations/v1 \
  --path proto/agynio/api/tracing/v1 --path proto/agynio/api/images/v1 \
  --path proto/agynio/api/image_proxy/v1
```

The separately tracked LLM bindings are not regenerated by this command.
Generated API dependencies remain outside this contribution.

## Earlier Combined Execution Acceptance

The separate [prepared execution fixture](testdata/runner-prepared-fixture/README.md)
now passes **22 live scenarios plus the two owner groups and parent** under the
race detector. It uses real PostgreSQL/migrations, registry and native runner
RPC servers, Kubernetes Pods/PVCs, and production controller lifecycle methods
in separate OS processes. Independent SQL reads verify phase/revision, complete
bindings, exact removal observations/timestamps and durable owner/backend pins.

Both agent-instance and sandbox cases cover parallel owners, same-owner
admission, new-Pod/same-PVC follow-up, cancellation while preparation is in flight
and after binding/activation authorization, and removal crash windows. Sixteen
deliberate process deaths are joined as SIGKILL, including replacement of the
controller, registry and native runner after a lost activation acknowledgement.
Recovery observes the active Pod without another prepare/activate RPC or a
repeated workspace effect. Unused reservations can abort without native evidence.

The final whole-repository selected race run passes **625 test entries** with
`TestLivePreparedExecutionStack`, `TestLiveCheckedVolumeStack` and
`TestLiveVolumeRetention` all enabled. Only the unchanged group-consumer test
named above is excluded; the two child-process entry points skip at top level
and run through their parents. A fresh ordinary run passes 597 entries. Build
and vet excluding `assign` pass; fresh unfiltered race/full-vet runs reproduce
only the previously documented group-consumer failure and self-assignment.

That earlier unknown-prepare case proved quarantine, not resource recovery. The registry
never receives the lost receipt and keeps admission even after the test uses its
intercepted exact receipt to remove the disposable Pod. No production native
intent-discovery API is implied by that test-only cleanup.

The executed workload is a fixed, bounded Node program, not an A2A agent. Agent
assembly, the complete controller event loop, daemon/native sessions, provider
credentials, real overlay authorization and storage/node failover are outside
this fixture. Registry display metadata/authorization tuple writes and the
sandbox owner lookup are explicit stubs. Both native and registry responses are
real, not adapter shims. Temporary databases, namespaces, Pod-owned Secrets and
GET-only cluster RBAC are cleaned up with identity/absence checks; no existing
workspace, installed database or deployment is modified.

## Remaining Release Gates

Full A2A acceptance on the prepared stack is next; model-free combined execution
does not establish it.
Present, verified unexecuted preparations now have a retirement path. Initially
absent and delayed Pod/PVC creates still need fencing and reconciliation;
retaining admission for those cases is quarantine, not complete resource
recovery. Old ownerless Secrets and credential cleanup after a lost removal confirmation
still needs durable retry; zero-volume owner placement must retain backend pins.

All writers and routes must enforce the contract. Native receipts and registry
CAS are not authenticated owner authorization or node/storage fencing. Legacy
adoption/draining, migration rollout, forced deletion and late operations remain
open. No installed database, task workspace, deployment or quota was changed by
this contribution. Keep original repository licenses and review boundaries.
