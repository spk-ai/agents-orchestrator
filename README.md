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
