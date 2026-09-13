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
Failures retain an unset `removed_at` until confirmed absent; these records remain
in reconciliation and prevent replacements. A lost start reply is inspected and
stopped using the ID persisted before the request. Existing failure reasons and
retry backoff survive cleanup. Sandbox workspace deletion likewise waits for the
workload stop to be confirmed. Persistent-volume TTL starts from confirmed
removal, never a failure's update timestamp; any unremoved workload holds its
instance's volume even when older removed workloads have expired retention.

This trusts the runner's inspection contract. It is not fencing against a
partitioned Kubernetes node, force-deleted pods, reused workload IDs or a delayed
start request that creates a workload after an absence check. Operators must
drain and audit records written by older versions before relying on `removed_at`;
historical timestamps cannot be retroactively verified. Permanently lost runners
need an explicit infrastructure reconciliation procedure, not an automatic claim
that their workloads stopped. Listing unremoved failures currently pages failed
history because the Runners API has no removal-time filter.

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
