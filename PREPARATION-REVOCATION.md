# Interrupted Preparation Recovery

Dependent controller proposal, using API `23d3073`, native runner `3260fb4` and
registry `302b7c8` (migration 0026). These contributions are not installed and do
not change A2A routing, agent profiles, workflow code or task/workspace ownership.
Existing repository licensing is unchanged.

## DNS-Compatible Lab Combination

This lab branch combines controller `b08d43b` with the independent workload DNS
correction `204b5e9`, group-consumer test repair `fdf60f9` and no-op context cleanup
`0035aff`. The resulting source is `6669c08`. The focused contribution's older
selected-race/vet limitations below describe that branch, not this combination.

Both ordinary and unfiltered race suites pass 879 entries, with seven opt-in
live/helper skips. Unfiltered `go vet ./...` and `go build ./...` pass. The full
revocation process matrix also passes all 19 entries on this combined source,
with no failures or skips. It uses the same native/registry binaries and pinned
images as the focused acceptance; it does not exercise an actual Ziti overlay,
provider authentication or A2A. The existing 41-entry execution regression
passes on focused source `b08d43b`; it was not rerun on this combination. After
both fixtures cleaned up, the shared installed baseline matched exactly. No
deployment or installed workspace is changed.

## Behavior

The shared agent-instance/sandbox stop path first persists `REMOVING`. A fully
observed, gated Pod still uses the established exact-binding cleanup protocol.
If native preparation observation fails, an anchored workload can instead use
`RevokeWorkloadPreparation`. The controller validates and persists that complete
receipt before asking `ObservePreparationRevocation` for cleanup evidence.

A stored proof resumes through observation, never ordinary Pod discovery.
Pending/unsupported/invalid responses retain admission. The revocation failure's
status remains actionable; an earlier NotFound does not mask an Aborted CAS.
Failed recovery no longer deletes the native owner without retaining a proof.

Before any discovered PVC is bound, the entire observation partition and all
owner-scoped checked records are validated. A known UID must match exactly; an
absent or newly discovered allocation must still be its original unbound
anchored generation. Existing checked-volume CAS calls bind newly found PVCs.
The registry repeats its checks under the admission lock before confirmation.

Proof and observation use separate preparation/resource revision updates.
Responses must preserve exact immutable evidence, and completed history cannot
acquire replacement evidence. A late prepare reply cannot replace a recorded
revocation with a Pod binding. Lost replies resume from the persisted state;
no recovery path prepares, activates or resends an agent turn.

After confirmed cleanup, an explicitly requested new workload may use the same
workspace anchor and physical UID. An original first-provision reservation is
not reset merely because its PVC was initially absent. The old workload remains
immutable history with nil ordinary Pod binding/removal observation.

## Verification

On 2026-09-15 the ordinary source suite passed 875 entries with seven gated
live/helper skips. The selected full race suite passed 874 entries with those
skips, excluding exactly the previously documented unsynchronized
`TestGroupMembershipConsumerLoopRetriesWithoutBlocking` fixture. Build and
`go vet -assign=false ./...` passed. Unfiltered vet still reports the existing
`start_decision.go:186` self-assignment; this is not an unfiltered race/vet pass.

Focused tests cover zero/absent/found/known volumes, explicit follow-up, pending
cleanup, changed native or registry evidence, and failures before/after each
revocation/record/observe/volume-bind/confirm RPC. Fake protocol responses are
not Kubernetes garbage-collection evidence.

The [real process acceptance fixture](testdata/runner-prepared-fixture/README.md)
uses disposable PostgreSQL, scoped native Kubernetes handlers and actual
SIGKILL checkpoints. It separately checks retained workspace identity, no
execution replay, peer progress and exact cleanup.

The new revocation matrix passed all 16 scenarios plus two owner groups and
parent (19 entries), with no failures or skips. Independent before/after checks
preserved all 108 installed PVCs and 52 deployment specifications/readiness.

The existing execution matrix's first regression run passed 38 entries but hit
the shared 12-minute owner-group deadline during the last sandbox cleanup.
That run failed; its owned fixtures were removed and installed snapshots match.
The expanded nineteen-scenario group now has a 15-minute aggregate budget,
without changing each controller child's independent 120-second deadline or any
assertion. The complete rerun finished at 09:48 UTC with all 38 scenarios, two
owner groups and parent passing (41 entries), with no failures or skips. This
checks the existing execution/removal paths on source `b08d43b`; the failed first
attempt remains failed evidence, not a passing subset.

After fixture cleanup, an independent snapshot again matched all 108 installed
PVCs, 52 deployment specifications/readiness values, ten namespaces, 96
ClusterRoles and 76 ClusterRoleBindings. No installed task Pods remained.

## Remaining Gates

This is trusted-local protocol acceptance, not production completion. The native
receipt and current Pod absence do not authenticate every future writer or fence
a partitioned node. All-writer rollout, durable credential cleanup/retention,
network/sandbox enforcement, receipt lifecycle and storage/HA recovery remain
required. Recovery during an executed, interrupted agent turn still requires
explicit side-effect reconciliation; restoring its thread is not safe retry proof.
