# Agents Orchestrator Service

The Agents Orchestrator runs a background reconciler that ensures agent workloads
exist for threads with unacknowledged agent messages.

Architecture: https://github.com/agynio/architecture/blob/main/architecture/agents-orchestrator.md

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
