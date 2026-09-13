# Agents Orchestrator Service

The Agents Orchestrator runs a background reconciler that ensures agent workloads
exist for threads with unacknowledged agent messages.

Architecture: https://github.com/agynio/architecture/blob/main/architecture/agents-orchestrator.md

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
