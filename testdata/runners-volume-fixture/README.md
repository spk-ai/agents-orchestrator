# Checked Volume Stack Fixture

This opt-in test combines the actual Runners service and embedded migrations,
PostgreSQL, controller methods in separate OS processes, and the actual native
runner against Kubernetes. It requires a trusted local cluster and a local
Unix-socket Docker daemon. It does not update any platform deployment or use
the platform database, provider credentials, or model calls.

Generate compatible reviewed APIs in all three checkouts first. The historical
initial combination is API `ec2bfed`, Runners `f05b479` (migrations 0017-0019), native
runner `3c461c5`, and orchestrator checked-lifecycle code `eebf4cf` plus this
fixture. These are coordinated contribution branches, not published releases.

The separate [prepared execution fixture](../runner-prepared-fixture/README.md)
requires its own matching API/registry revisions. Do not treat a volume-only
fixture build as prepared-execution acceptance; the helper's compatibility
boundary is documented in [main.go](main.go).

From the orchestrator checkout, with absolute paths set by the operator:

```bash
bash testdata/runners-volume-fixture/build.sh "$RUNNERS_CHECKOUT" "$REGISTRY_BINARY"
GOFLAGS=-race bash testdata/runner-volume-fixture/build.sh "$RUNNER_CHECKOUT" "$RUNNER_BINARY"

CHECKED_VOLUME_STACK_TEST=trusted-local \
RETENTION_KUBECONFIG="$KUBECONFIG" \
CHECKED_REGISTRY_BINARY="$REGISTRY_BINARY" \
RETENTION_RUNNER_BINARY="$RUNNER_BINARY" \
CHECKED_POSTGRES_IMAGE="$POSTGRES_IMAGE" \
go test -race -json ./internal/reconciler \
  -run '^TestLiveCheckedVolumeStack$' -count=1 -timeout=15m
```

Use the [registry builder](build.sh) and
[native builder](../runner-volume-fixture/build.sh) with their required generated
bindings. `POSTGRES_IMAGE` must be an already-local, reviewed `name@sha256:...`
image. The original acceptance used PostgreSQL 16.6 Alpine. New source/image
combinations require separate compatibility review.

## Coverage

See
[checked_volume_stack_live_test.go](../../internal/reconciler/checked_volume_stack_live_test.go)
and its [process harness](../../internal/reconciler/checked_volume_stack_process_test.go)
for the executable checks.

## Boundaries And Cleanup

Agents metadata (including the sandbox finalization counter) and authorization
tuple writes are explicit stubs. Controller runner routing is a checked fixture
ID mapped to the loopback native process, not the production overlay dialer.
Registry RPCs require a private fixture token and an allowlist; this is not
production caller authentication. The native fixture permits PVC inspection
and checked deletion only, with namespace-scoped impersonated RBAC.

Review the [database/process guards](../../internal/reconciler/checked_volume_stack_helpers_test.go)
and [namespace cleanup](../../internal/reconciler/volume_retention_live_test.go)
before authorizing a run. Use private fixture configuration; do not print
credentials, bypass an ownership refusal, strip finalizers or delete installed
task data. Exact fixture limits belong to those helpers.

Workload rows are unused admission reservations: no native `StartWorkload` is
sent. Their synthetic historical retirement timestamps exercise TTL eligibility,
not actual Pod-removal confirmation. The PostgreSQL process is not crashed;
this proves application-process recovery, not storage or node failover.

This fixture does not establish A2A lifecycle acceptance, real Agents-service
finalization, production authorization/backend identity, legacy-data migration,
late-start/node fencing, garbage collection, or permanent rollout. Those remain
separate acceptance gates.
