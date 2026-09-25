# Resource Anchor Controllers

Dependent controller contribution on preparation-recovery base `c2bb0e5`.
Requires API `6fe4cab`, native k8s-runner `72a1cc8`, and Runners `e1a3b7f`
including migrations `0023` and `0024`. It is not installed. This branch does
not include the separately reviewed DNS correction and must not replace the
installed prepared/DNS combination by itself.

The dependent [anchored retirement proposal](ANCHORED-VOLUME-RETIREMENT.md)
adds migration `0025` and explicit PVC-and-owner removal to that baseline.
Historical anchor verification below retains its original scope.

## Contract Owners

[prepared_start.go](internal/reconciler/prepared_start.go) owns shared agent and
sandbox execution ordering; [resource_anchors.go](internal/reconciler/resource_anchors.go)
owns request-label projection, exact native owner persistence and dual revisions.
[checked_volumes.go](internal/reconciler/checked_volumes.go) owns allocation versus
adoption provenance. New starts never fall back to old APIs; existing legacy
records retain their explicit reconciliation path.

## Recovery Boundaries

Lost-reply cleanup lives in
[prepared_recovery.go](internal/reconciler/prepared_recovery.go) and
[preparation_revocation.go](internal/reconciler/preparation_revocation.go).
Exact bound removal and unused-reservation abort live in
[prepared_workloads.go](internal/reconciler/prepared_workloads.go).
Metadata cleanup is not a complete garbage collector or child-absence proof.

The following limitation records the original anchor-only contribution:

Anchored volume retirement/failure/reopen through old commands is refused until
the independent checked retirement contract exists. No fixture deletion grants
production authority to remove an installed workspace.

## Inbox Thread Identity

The real assembler uses the inbox thread in the native `thread-id` label.
Registry `thread_id` is a legacy instance alias in the existing orchestrator
creation path. Neither that registry field nor persistent volume identity is
rewritten here. Instead, the actual canonical inbox thread is pinned inside the
immutable workload anchor and compared against native request/recovery evidence.

Registry follow-up `e1a3b7f` corrects the Go/database validators. Additive
migration `0024` preserves prior records and existing anchors. A subsequent
workload can use another inbox thread while retaining its instance workspace;
an already-bound workload cannot change its thread. These are identity checks,
not proof of authenticated authority to select a thread or owner.

## Verification

The latest ordinary source run passes 761 test entries with five explicitly
gated live/child skips. The selected full race run passes 760 entries with those
same skips, excluding exactly the pre-existing
`TestGroupMembershipConsumerLoopRetriesWithoutBlocking` race. Build and
`go vet -assign=false ./...` pass; unfiltered vet retains the existing
`start_decision.go` self-assignment. Do not describe either excluded check as
passing. The final focused 20-run anchor race suite passes 1,920 entries,
including the additional cancellation-boundary tests, with no failures/skips.

Regression coverage includes both owner kinds, native request projection,
distinct APIs without fallback, immutable anchors and both revisions, volume
reservation receipts, lost replies, pre-preparation cancellation, exact removal,
unknown preparation retention and native/legacy thread separation.

The [combined process fixture](testdata/runner-prepared-fixture/README.md) uses
real PostgreSQL, registry/native RPCs, controller subprocesses and Kubernetes.
Its new assertions compare complete anchor/receipt JSON with independent SQL,
native ConfigMap UIDs with Pod/PVC ownership, and require workload-anchor absence
while preserving the volume owner. It also adds cancellation before preparation
authority and SIGKILL at anchor-removal PENDING/ABSENT. Run this fixture with
the exact dependent source revisions above; source tests alone are not its
acceptance evidence.

The final race-enabled combined run on 2026-09-15 passes all 38 scenarios plus
the two owner groups and parent (41 entries), with no failures/skips. Actual
inbox-thread IDs differ from legacy registry instance aliases. All three
application processes are replaced during recovery; the database is not crashed.
Fixture cleanup completes, and the independent installed-state comparison retains
all 105 PVCs and 52 deployment snapshots with zero task Pods. Authorization and
Agents display metadata remain fixtures, and the probe is credential-free Node,
not a native agent or A2A client. This does not close the release requirements.

## Remaining Release Work

- Complete initially absent/late-create reconciliation and durable cleanup.
- Add checked anchored-volume retirement and explicit legacy adoption.
- Persist/reconcile external credential revocation and delayed holds.
- Integrate any remaining wire/client paths and the separately reviewed DNS fix.
- Coordinate all writers, then rerun both complete Codex/Claude A2A matrices.
- Close backend/owner authentication, node/storage fencing, sandbox/network,
  TLS, backup/failover, packaging and sustained-load gates.

No A2A controller, workflow, agent profile or installed deployment changes in
this contribution. No upstream PR has been submitted.
