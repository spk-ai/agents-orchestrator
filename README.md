# Agents Orchestrator Service

The Agents Orchestrator runs a background reconciler that ensures agent workloads
exist for threads with unacknowledged agent messages.

Architecture: https://github.com/agynio/architecture/blob/main/architecture/agents-orchestrator.md

## Confirmed Workload Removal

`StopWorkload` may acknowledge a deletion request before the runtime has gone.
The reconciler keeps `STOPPING` workloads pending until the runner's
`InspectWorkload` reports `NotFound` for the persisted request ID and any returned
runtime ID, including legacy aliases. A stopped main container is not sufficient
because sidecars may still exist. Removal checks retry on later reconciliation
ticks rather than blocking all tasks during a termination grace period.

Runner dial/list/inspect failures and unenrollment are not removal evidence.
Agent failures retain an unset `removal_confirmed_at` until confirmed absent;
these records remain in reconciliation and prevent replacements even when
`removed_at` has already ended billing. Unconfirmed stopped records are also
tracked. A lost start reply is inspected and
stopped using the ID persisted before the request. Existing failure reasons and
retry backoff survive cleanup. Agent-instance persistent-volume TTL starts from confirmed
removal, never a failure's update timestamp; any unremoved workload holds its
instance's volume even when older removed workloads have expired retention.

This trusts the runner's inspection contract. It is not fencing against a
partitioned Kubernetes node, force-deleted pods, reused workload IDs or a delayed
start request that creates a workload after an absence check. Operators must
drain and audit records written by older versions before relying on confirmation;
historical timestamps cannot be retroactively verified. Permanently lost runners
need an explicit infrastructure reconciliation procedure, not an automatic claim
that their workloads stopped. Listing unremoved failures currently pages failed
and stopped history because the Runners API has no confirmation-time filter.

The API and Runners service `feat/workload-removal-confirmation` branches add
the explicit field and its storage. Generate against that API checkout for this
branch; published BSR schemas and older Gateways do not yet carry the field.
The AgentInstance lifecycle and its volume TTL use confirmation. Metering and
failure backoff keep their existing billing timestamps. The separate sandbox
reconciler's broader removal contract still needs migration and acceptance.

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

## Immediate Stop On Pause (Opt-in)

Set `STOP_INACTIVE_INSTANCES=true` when explicit pause or termination must stop
an active workload without waiting for the daemon to go idle. On each main
reconciliation tick, workloads no longer in the active desired set have their
instance lifecycle checked. Confirmed `paused` or `terminated` instances go
through the existing Runner `StopWorkload` path, even with recent keepalives.
Inbox items and persistent volumes are retained; no SDK-specific signal or
message acknowledgement is introduced.

Unset or false preserves the existing idle-timeout behavior. Invalid boolean
values stop configuration loading. Missing, mismatched or unreadable instance
state does not authorize an immediate stop. Active idle instances retain the
class idle timeout, and duplicate/STOPPING workload handling is unchanged.

Stopping is asynchronous. `POLL_INTERVAL`, `STOP_TIMEOUT_SEC`, runner health and
API availability determine latency; an accepted pause is not a stopped-workload
receipt. Callers must observe removal and reconcile possible side effects before
resuming interrupted work. This is an operator policy, not an exactly-once
execution guarantee or a new public cancellation API.

## Opt-in compute resource bounds

Agents requiring `compute-resources` use the selected environment flavor's
CPU/memory requests and limits for the main container. The deprecated
`Agent.resources` is not used for this opted-in allocation. Explicit MCP bounds
are passed to their sidecars. Missing MCP/supporting bounds delegate to the
runner's operator-configured defaults; explicit partial or invalid messages fail
assembly. The required capability is retained on `StartWorkload` so an old
runner cannot silently discard the new fields.

This needs the `ContainerSpec.resources` addition in `agynio/api` and a runner
implementing `compute-resources`. Upgrade those before enabling the capability
on profiles. Agents without it retain the legacy assembly and accounting
behavior. This change covers agent workloads, not the separate sandbox API.

Declared allocation accounting now uses the flavor plus explicit MCP requests
for opted-in agents. It does **not** include runner-owned supporting defaults or
all Kubernetes Pod overhead, and is not a complete whole-task budget. Resource
quotas, aggregate admission and hardening are separate deployment controls.

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

Runner volumes without a match in the scoped, active registry snapshot are
retained and logged for ownership reconciliation. The two lists are not atomic:
a disk can belong to another organization, a closed record, or a record created
after the registry scan. Absence from that snapshot is not deletion authority.

This deliberately changes the architecture's automatic orphan-deletion policy.
Truly orphaned disks are also retained; operators must account for that storage
until an ownership- and generation-checked garbage collector is available. Do not
replace this with a second lookup followed by a name-only delete: creation and
reopening can still race that lookup.

A nil, malformed or duplicate runner inventory is rejected in full for that
runner before updating records or deleting disks. Both volume keys and physical
instance names must be present and unique. Other runners can still reconcile.
Valid tracked provisioning, persistent-volume reuse, TTL and deprovisioning
continue through their existing paths. This change does not make those paths
safe against stale requests, backend name reuse, node partitions or late creates;
the runner API still lacks a caller-pinned deletion incarnation.

Focused regression tests (generate the APIs as in the development setup first):

```bash
go test -race ./internal/reconciler -run '^TestReconcileVolumes' -count=1
```

### Native Runner Acceptance

The optional live test uses a reviewed k8s-runner checkout's real `ListVolumes`
and `RemoveVolume` over loopback gRPC, plus Kubernetes PVCs. The registry and
Agents clients are deterministic fakes; this is not a deployed platform or
database test. The native runner must also reject missing/duplicate PVC keys,
instead of silently returning a partial inventory. No new API fields are needed.

Generate both repositories' APIs first. Build the native fixture inside the
reviewed runner checkout so Go's internal-package boundary is preserved:

```bash
bash testdata/runner-volume-fixture/build.sh /absolute/k8s-runner /absolute/private/native-runner-fixture
RETENTION_LIVE_TEST=trusted-local \
RETENTION_KUBECONFIG=/absolute/private/kubeconfig \
RETENTION_RUNNER_BINARY=/absolute/private/native-runner-fixture \
go test -race ./internal/reconciler -run '^TestLiveVolumeRetention$' -count=1
```

The explicit kubeconfig must permit creating a disposable namespace and
impersonating its PVC-only test service account. The fixture refuses cross-
namespace PVC or Secret-list access, exposes only two RPCs, and permits removal
only of its own empty claims. No existing runner deployment is used or changed.
Six 1 MiB claims name an absent storage class, so they acquire no backing disks;
quota forbids any Pod. Namespace cleanup checks owned UIDs, unexpected objects
and backing storage, then uses UID/resource-version deletion preconditions.
Unknown data prevents cleanup instead of being removed. The control plane's
credentials are never put in prompts, copied to another credential file, or
passed as command arguments. This is trusted-local test infrastructure, not a
production runner authentication or cleanup implementation.

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

```bash
# Deploy once (exit when healthy)
devspace dev

# Watch mode (streams logs, re-syncs on changes)
devspace dev -w
```
