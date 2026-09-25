# Agents Orchestrator Service

Current implementation contracts live beside the Go owners listed in
[AGENTS.md](AGENTS.md); [docs/catalog.json](docs/catalog.json) indexes the
operational and historical documents. The operator adoption coordinator is now
in [coordinator.go](internal/volumemigration/coordinator.go), with its runbook in
[VOLUME-ANCHOR-MIGRATION.md](VOLUME-ANCHOR-MIGRATION.md).

## Integration Snapshot (2026-09-24)

The following dependency summary and older test reports retain their original
scope; they are not a current build manifest or deployment authorization.

The `sync/2026-09-24-revocation-native-dns` branch retains the DNS and independent
race/vet fixes on the rebased upstream `c88b608` contribution stack. Use API
`spk-ai/api` `c21440b`, the matching `spk-ai/k8s-runner`
`sync/2026-09-24-volume-adoption` branch, and registry `spk-ai/runners` `302b7c8`.
Earlier revisions below are historical records. This is an acceptance
combination, not a single upstream proposal or permission to upgrade installed
workspaces. That snapshot did not include the later operator adoption coordinator
linked above.

See [preparation revocation](PREPARATION-REVOCATION.md) for recovery rollout limits.

Architecture: https://github.com/agynio/architecture/blob/main/architecture/agents-orchestrator.md

## Backend-Bound Volume Lifecycle

Backend-bound inventory and durable deletion targets are owned by
[checked_volumes.go](internal/reconciler/checked_volumes.go) and
[volume_reconcile.go](internal/reconciler/volume_reconcile.go). Their comments
define fail-closed transitions and the distinct native removal capability.

The matching API, registry migration `0021` and native runner must be coordinated.
The migration refuses previously unidentified checked bindings; do not infer an
identity or rebind a record to whichever backend happens to answer. Stored
runner/owner identity and the physical namespace identity have different roles;
these checks are not authentication of the route or caller.

Historical backend-identity acceptance:

Tests include actual old-runner gRPC capability rejection and both owner kinds.
The native fixture verifies wrong-runner routing against real Kubernetes,
PostgreSQL and controller subprocesses, retaining original PVC/binding identity
before exercising the existing process-crash cases. Namespace identity uses
explicit GET-only RBAC; fixture namespace listing, foreign-namespace access and
Secret access remain denied. No installed task data or service is upgraded.

Workload-start backend pinning, authenticated route/policy audit, cloned-cluster
identity handling, late operations, partitioned-node fencing and coordinated
A2A rollout remain open. The existing unrelated group-consumer race and
`start_decision.go` self-assignment vet failure remain separate limitations;
selected race and `go vet -assign=false` results are not unfiltered passes.

## Confirmed Workload Removal

Legacy absence confirmation lives beside `handleMissingRunnerWorkload` in
[workload_reconcile.go](internal/reconciler/workload_reconcile.go).
Prepared exact-binding cleanup lives beside `stopPreparedWorkload` in
[prepared_workloads.go](internal/reconciler/prepared_workloads.go).
Retention from confirmed absence is documented beside `agentInstanceActivity`
in [volume_reconcile.go](internal/reconciler/volume_reconcile.go).

This trusts the runner's inspection contract. It is not fencing against a
partitioned Kubernetes node, force-deleted pods, reused workload IDs or a delayed
start request that creates a workload after an absence check. Operators must
drain and audit records written by older versions before relying on confirmation;
historical timestamps cannot be retroactively verified. Permanently lost runners
need an explicit infrastructure reconciliation procedure, not an automatic claim
that their workloads stopped.

The API and Runners service `feat/workload-removal-confirmation` branches add
the explicit field and its storage. Generate against that API checkout for this
branch; published BSR schemas and older Gateways do not yet carry the field.

```sh
cd ../api
buf generate . --template ../agents-orchestrator/buf.gen.yaml \
  --path proto/agynio/api/runners/v1 --include-imports \
  --output ../agents-orchestrator
cd ../agents-orchestrator
go test ./...
```

Other generated packages must already exist, as in the upstream build. No
generated source or unrelated API changes belong in this contribution.

## Checked Volume Lifecycle

Shared checked creation, identity validation, compensation and removal live in
[checked_volumes.go](internal/reconciler/checked_volumes.go).
[prepared_start.go](internal/reconciler/prepared_start.go) and
[resource_anchors.go](internal/reconciler/resource_anchors.go) own new-start
authority and persistent owner provenance. Original inventory is never a
name-only deletion permit; unbound or legacy history needs explicit reconciliation.

Dependencies are not published or permanently deployed: generate the combined
API `ec2bfed`, use checked runner integration `3c461c5`, and require Runners
owner-admission guard `f05b479` with migrations `0017`-`0019`. This branch is based
on orchestrator integration `f65a9f6`; review its incremental diff, not the entire
acceptance stack as one upstream proposal. The checked flag/RPC alone does not
prove the database admission guard is installed. Old records, all writers and
already-issued deletion requests need an explicit drain/upgrade audit. Do not
deploy this client against stock services or silently promote legacy records.

This is not backend-incarnation authentication, late-create/node/storage fencing,
ownership-aware garbage collection or a production rollout. A2A retains its own
interrupted-execution quarantine and explicit recovery policy; reopening a
volume is not permission to retry a possibly executed agent turn.

Historical checked-volume acceptance:

Validation includes ordinary Go tests, checked lifecycle/compensation/ownership
regressions, malformed page/reply cases and controller reconstruction after lost
begin/native/confirm replies. The optional native fixture below exercises both
agent and sandbox volume paths, but its registry and Agents clients are fakes.
The full unfiltered race suite still fails in the unchanged
`TestGroupMembershipConsumerLoopRetriesWithoutBlocking` fake subscription. The
selected suite excluding exactly that test passes with the native gate enabled.
`go vet ./...` also flags the existing `ctx = ctx` in `start_decision.go`;
`go vet -assign=false ./...` passes, not the unfiltered command.

## Immediate Stop On Pause (Opt-in)

Set `STOP_INACTIVE_INSTANCES=true` when explicit pause or termination must stop
an active workload without waiting for the daemon to go idle. The policy checks
live in [instance_stop.go](internal/reconciler/instance_stop.go); configuration
validation is in [config.go](internal/config/config.go).

Stopping is asynchronous. `POLL_INTERVAL`, `STOP_TIMEOUT_SEC`, runner health and
API availability determine latency; an accepted pause is not a stopped-workload
receipt. Callers must observe removal and reconcile possible side effects before
resuming interrupted work. This is an operator policy, not an exactly-once
execution guarantee or a new public cancellation API.

## Opt-in compute resource bounds

Allocation and validation live in [assembler.go](internal/assembler/assembler.go)
and [compute_resources.go](internal/assembler/compute_resources.go); accounting
limits are documented at [resources.go](internal/assembler/resources.go).

This needs the `ContainerSpec.resources` addition in `agynio/api` and a runner
implementing `compute-resources`. Upgrade those before enabling the capability
on profiles. Do not assume this agent capability configures sandbox allocation.
Resource quotas, aggregate admission and hardening are separate deployment controls.

To test the pending API change in sibling source checkouts, first generate the
published baseline as usual, then overlay only the changed runner contract:

```bash
buf generate --include-imports
cd ../api
buf generate . --template ../agents-orchestrator/buf.gen.yaml \
  --path proto/agynio/api/runner/v1 --include-imports \
  --output ../agents-orchestrator
cd ../agents-orchestrator
go test ./...
```

Do not include unrelated generated API changes in this contribution.

## Volume Reconciliation Safety

The scoped-inventory retention rule is documented beside `reconcileVolumes` and
`indexRunnerVolumes` in
[volume_reconcile.go](internal/reconciler/volume_reconcile.go).

This deliberately changes the architecture's automatic orphan-deletion policy.
Truly orphaned disks are also retained; operators must account for that storage
until an ownership- and generation-checked garbage collector is available. Do not
replace this with a second lookup followed by a name-only delete: creation and
reopening can still race that lookup.

Name/UID binding and revision guards do not authenticate backend routes or fence
node partitions and late creates. Those require separate enforcement/acceptance.

Focused regression tests (generate the APIs as in the development setup first):

```bash
go test -race ./internal/reconciler -run '^TestReconcileVolumes' -count=1
```

### Native Runner Acceptance

The [live fixture](internal/reconciler/volume_retention_live_test.go) uses a
reviewed native runner against Kubernetes, with fake registry and Agents clients.
This is not a deployed platform or database test. Both checkouts require matching
checked-volume API generation, not only the published baseline.

Generate both repositories' APIs first. Build the native fixture inside the
reviewed runner checkout using [the builder](testdata/runner-volume-fixture/build.sh)
so Go's internal-package boundary is preserved:

```bash
bash testdata/runner-volume-fixture/build.sh /absolute/k8s-runner /absolute/private/native-runner-fixture
RETENTION_LIVE_TEST=trusted-local \
RETENTION_KUBECONFIG=/absolute/private/kubeconfig \
RETENTION_RUNNER_BINARY=/absolute/private/native-runner-fixture \
go test -race ./internal/reconciler -run '^TestLiveVolumeRetention$' -count=1
```

The explicit kubeconfig must permit creating a disposable namespace and
impersonating its PVC-only test service account. Use only disposable resources,
never an installed runner deployment. Review the fixture's cleanup guards rather
than bypassing a refusal to delete unknown or backed storage. The control plane's
credentials are never put in prompts, copied to another credential file, or
passed as command arguments. This is trusted-local test infrastructure, not a
production runner authentication or cleanup implementation.

The separate [process fixture](testdata/runners-volume-fixture/README.md) covers
application-process replacement, not database failover.

## Workload DNS With Ziti

Resolver policy and the reason ordinary DNS cannot be a workload fallback live
in [assembler.go](internal/assembler/assembler.go), with sandbox assembly in
[sandbox.go](internal/assembler/sandbox.go). Operator upstream settings are
defined in [config.go](internal/config/config.go).

This is a resolver-routing correction, not an adversarial egress boundary:
direct-IP traffic, custom resolvers, privileges and network policy require
separate enforcement. Existing Pods are not rewritten by this change.

## Local Development

Full setup: https://github.com/agynio/architecture/blob/main/architecture/operations/local-development.md

### Prepare environment

```bash
git clone https://github.com/agynio/bootstrap.git
cd bootstrap
chmod +x apply.sh
./apply.sh -y
```

### Run from sources

Use the repository's [DevSpace workflow](devspace.yaml) after bootstrap:

```bash
# Deploy once (exit when healthy)
devspace dev

# Watch mode (streams logs, re-syncs on changes)
devspace dev -w
```

## Test Validation

[Group consumer synchronization](GROUP-CONSUMER-TEST.md) fixes two races in the
retry/cancellation test without changing the production consumer or retry policy.
