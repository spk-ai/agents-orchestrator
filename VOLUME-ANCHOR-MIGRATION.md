# Existing Workspace Migration

This dependent contribution uses the API and registry `feat/volume-anchor-migration`
branches, registry migration `0027`, and native runner `0ed8c5c` adoption RPCs.
Generate matching local API bindings as described in
[PREPARED-WORKLOADS.md](PREPARED-WORKLOADS.md).
It is an operator path, not an automatic controller upgrade or task retry.

## Operator Contract

Drain all controllers, clients and other writers first. Verify a coordinated,
restore-tested registry and workspace backup, including native ConfigMaps and
original PVC identities. Apply the reviewed additive schema before adoption.
Do not widen an older rollout guard or run old controllers against adopted rows.

Use the operator command in [cmd/volume-migration](cmd/volume-migration/main.go)
over the private registry and authenticated Ziti native-runner transport.
Use `-inspect-runner UUID` to obtain actual native observations. Build an
immutable `BeginVolumeAnchorMigrationRequest` for each complete owner inventory,
with original SQL revisions and matching native bindings. Never reconstruct an
allocation receipt for existing storage. Plans are bounded protobuf JSON.

```sh
volume-migration -trusted-local-drained \
  -runners-address runners:50051 \
  -ziti-management-address ziti-management:50051 \
  -plan /private/plans.json -batch -continue-quarantined -timeout 30m
```

The acknowledgement flag does not itself drain the cluster or verify backups.
The deployment coordinator must enforce both. The private transport has the
same trusted-platform boundary as the existing orchestrator; expose neither
registry nor Ziti management publicly.

Implementation ordering and independent-read requirements live beside
`Coordinator` and `Run` in
[coordinator.go](internal/volumemigration/coordinator.go), using the RPC contracts
owned by the API and registry. The original PVC UID/spec/content must survive.

Any ambiguous response stops the invocation. Resume the **same immutable plan**;
do not manufacture a new migration ID, clear a block or retry an agent turn.
An unbound failed historical generation stays quarantined without a fabricated
PVC. `-continue-quarantined` processes other owners and returns exit 3 if any
remain blocked. There is deliberately no abort/unblock operation.

Controller provenance checks live in
[checked_volumes.go](internal/reconciler/checked_volumes.go).
Session continuity remains the runtime's responsibility; storage
adoption does not establish safe retry of an interrupted task.

## Acceptance

With the [prepared fixture environment](testdata/runner-prepared-fixture/README.md), run:

```sh
go test -race -json ./internal/reconciler \
  -run '^TestLiveVolumeAnchorMigrationStack$' -count=1 -timeout=40m
```

Use the migration registry checkout for the registry fixture binary. The native
fixture needs the matching adoption API. The checkpoint matrix and assertions
live in [volume_migration_stack_live_test.go](internal/reconciler/volume_migration_stack_live_test.go).
Only temporary credential-free fixture resources may be disposed of.

These tests do not crash PostgreSQL, fence a failed node, establish hostile-code
isolation, test model approvals or prove automatic replay safety. Production
disaster recovery must restore database, storage and native authority together.
