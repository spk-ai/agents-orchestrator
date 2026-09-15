# Resource Anchor Controllers

## Lab Combination

This `lab/resource-anchors-native-dns` branch combines focused controller
`2fddf9c` (cherry-picked as `48de4fc`) with prepared/DNS recovery base `b3ec0e2`.
Unlike the focused proposal, this combination retains the installed DNS
correction. It is not an installed image or a bundled upstream proposal.

The combined source also includes test-only `fdf60f9` (cherry-picked as
`ce2049c`), removing the group-consumer race exclusion. It passes 765 ordinary
and 765 full race entries, with five gated skips and no test exclusion.
Build and `go vet -assign=false ./...` pass. The combined Kubernetes/process
subset passes all four scenarios plus two owner groups and parent (seven
entries), with no failures/skips: parallel durable follow-up and lost preparation
across replacement of all three application processes, for each owner kind.
The final preservation snapshot matches all 105 PVCs, 52 deployments and cluster
RBAC, with zero installed task Pods. This verifies source compatibility, not a
new live DNS interception test, real-agent A2A rollout or production readiness.

## Focused Contribution

Dependent controller contribution on preparation-recovery base `c2bb0e5`.
Requires API `6fe4cab`, native k8s-runner `72a1cc8`, and Runners `e1a3b7f`
including migrations `0023` and `0024`. It is not installed. The focused
`feat/resource-anchor-controllers` branch does not include the separately
reviewed DNS correction and must not replace the installed prepared/DNS
combination by itself.

## Execution Contract

Both agent and sandbox starts share the same anchored implementation:

1. Validate the complete backend-bound inventory and checked volume records.
   Legacy bound volumes are not implicitly adopted. An unanchored unbound
   volume still needs this attempt's successful initial-create receipt.
2. Call `CreateAnchoredWorkload` to persist the unused reservation. An old
   registry cannot cause fallback to an unanchored start.
3. Project native ownership from the actual assembled labels, including the
   assembler's `label.*` properties and explicit-label precedence. Reject
   reserved labels and owner mismatches before reserving native metadata.
   Sandbox human ownership is also checked against Agents.
4. Reserve the workload anchor and any missing volume anchors. Bind each volume
   anchor and its exact workload/two-revision receipt before binding the full
   owner set to the workload. Existing volume anchors are reused, never replaced.
5. Advance both revisions before `PrepareAnchoredWorkload`. Validate every
   returned Pod/PVC owner and identity before binding or activation.
6. Remove the exact Pod, then require exact workload-anchor ABSENT before
   confirming removal. Pending, unsupported, changed and ambiguous responses
   retain admission. A new workload retains the original volume anchor/receipt.

All new starts require the distinct anchored APIs. Old persisted unanchored
workloads still reconcile/remove through their old contract. A workload cannot
switch between the two contracts, and the new controller cannot silently adopt
an old workspace into native ownership.

## Recovery Boundaries

For a lost preparation reply, persist REMOVING before read-only observation.
A verified gated Pod can be bound and retired without preparation/activation
replay. Unbound anchored volumes must still be the original revision-2
first-provision generation; an old bound record cannot become new again.

If the Pod outcome is unverified or absent, attempt exact workload-anchor
revocation but retain the unknown preparation's admission. Owner absence alone
is not child-cleanup evidence. An unused RESERVED record can retire without a
Pod receipt; known workload anchors must first be revoked. Lost metadata replies
can still leave metadata for future cleanup, even when execution was never
authorized. This contribution does not pretend to be a complete garbage collector.

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

The final race-enabled run of the focused contribution on 2026-09-15 passes all
38 scenarios plus the two owner groups and parent (41 entries), with no failures
or skips. Actual inbox-thread IDs differ from legacy registry instance aliases.
Application-process replacement and exact native ownership/cleanup are verified;
the database is not crashed. Fixture cleanup completes and the independent
installed-state comparison retains all 105 PVCs and 52 deployment snapshots with
zero task Pods. Authorization and Agents display metadata remain fixtures, and
the probe is credential-free Node, not a native agent or A2A client. The separate
passing lab subset above verifies this combination's compatibility.

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
