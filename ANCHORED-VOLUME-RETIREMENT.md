# Anchored Volume Retirement Controllers

Dependent on `feat/resource-anchor-controllers` and the matching API, Runners
and k8s-runner `feat/anchored-volume-removal` branches. This focused proposal is
not installed and does not include the separate native DNS correction.

The intent-before-delete and exact-receipt contract lives beside
`advanceAnchoredVolumeRemoval` in
[anchored_volume_removal.go](internal/reconciler/anchored_volume_removal.go).
[checked_volumes.go](internal/reconciler/checked_volumes.go) owns immutable
provenance and dispatch for anchored versus unanchored records.
There is no legacy fallback. Idle compute release retains the workspace; unbound
first provisioning requires separate reconciliation.

## Verification

On 2026-09-15, the combined race-enabled native/process run passed all eight
scenarios plus two owner groups and parent (11 entries), with no failures or
skips. Independent before/after snapshots preserved all 108 installed PVCs,
52 deployments, ten namespaces and cluster RBAC identities; zero task Pods
remain. Temporary databases, namespaces and fixture processes were removed.

The ordinary suite passes 789 entries with six gated live/child skips. The
selected full race suite passes 788 entries with those same skips, excluding
exactly the previously documented `TestGroupMembershipConsumerLoopRetriesWithoutBlocking`
test race. The independent test-only repair is available separately; it is not
silently included in this focused runtime contribution. Vet with `-assign=false`
passes; unfiltered vet retains the pre-existing `start_decision.go` self-assignment.

The combined fixture adds four actual controller SIGKILL checkpoints for each
owner kind: after intent, native PENDING, native ABSENT and persisted
confirmation. At ABSENT it also replaces the registry and native processes.
Every case runs a second owner concurrently, verifies distinct PVCs and continued
peer effects, and compares complete registry history through independent SQL.
All starts, stops and retirements call production lifecycle methods via real
registry/native RPCs. The full event loop, Agents metadata and authorization
writes are not exercised; credential-free Node substitutes for the agent.

Follow [the prepared fixture instructions](testdata/runner-prepared-fixture/README.md)
with all four matching checkouts and migration `0025`, replacing the test selector
with `^TestLiveAnchoredVolumeRetirementStack$`. Both fixture builders use the
race detector. Shared replacement processes must live through the owner group,
not be torn down by an earlier checkpoint's cleanup.

The fixture verifies both PVC and volume-owner absence before confirmation and
disposes only its captured resources. It does not prove backend authentication,
future-write exclusion, node/storage fencing, durable credential cleanup,
initially absent/late-prepare recovery or production garbage collection. No
installed schema, deployment, existing workspace or provider credential changes.
Coordinated DNS-compatible rollout and the real-agent A2A matrix remain required.
