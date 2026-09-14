# Checked Volume Stack Fixture

This opt-in test combines the actual Runners service and embedded migrations,
PostgreSQL, controller methods in separate OS processes, and the actual native
runner against Kubernetes. It requires a trusted local cluster and a local
Unix-socket Docker daemon. It does not update any platform deployment or use
the platform database, provider credentials, or model calls.

Generate compatible reviewed APIs in all three checkouts first. The initial
combination is API `ec2bfed`, Runners `f05b479` (migrations 0017-0019), native
runner `3c461c5`, and orchestrator checked-lifecycle code `eebf4cf` plus this
fixture. These are coordinated contribution branches, not published releases.

The separate [prepared execution fixture](../runner-prepared-fixture/README.md)
reuses this registry helper with an explicit `preparedWorkloads` configuration
flag. That mode requires migrations `0020`-`0022` and the actual prepared RPCs,
and refuses legacy workload creation. The default volume-only mode retains its
existing allowlist and no-execution scope.

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

`POSTGRES_IMAGE` must be an already-local, reviewed `name@sha256:...` image.
Acceptance uses PostgreSQL 16.6 Alpine. No image is downloaded by the fixture.
The registry builder always enables Go's race detector and uses temporary module
files inside the Runners checkout without changing its source or dependencies.
The parent test's race-enabled executable is also the controller subprocess.

## Coverage

For both agent-instance and sandbox owners, the test:

- Creates and binds a checked generation through controller methods and real
  registry RPCs. Independent `psql` connections compare committed state with
  API responses, including identity, revision, binding and removal intent.
- Pauses the controller after its idle scan but before begin-removal, admits
  new work, and verifies stale deletion is rejected without touching the PVC.
  Another owner can admit work independently. A failed but unconfirmed workload
  remains reserved even when its billing end is set.
- Forces the reverse ordering: once begin-removal commits, new admission fails.
- SIGKILLs and joins controller and registry processes at three boundaries:
  after begin commit, after native deletion but before the controller consumes
  its reply, and after registry confirmation but before owner finalization.
  Fresh processes retain the original intent. Native `PENDING` is not registry
  confirmation; only observed absence permits confirmation. Already-confirmed
  deletion is not repeated when sandbox finalization resumes.
- Explicitly reopens a generation, creates a replacement PVC under the old
  name, and verifies old-target replay cannot delete the new UID. Binding and
  reuse preserve the new generation and compensation ownership.

## Boundaries And Cleanup

Agents metadata (including the sandbox finalization counter) and authorization
tuple writes are explicit stubs. Controller runner routing is a checked fixture
ID mapped to the loopback native process, not the production overlay dialer.
Registry RPCs require a private fixture token and an allowlist; this is not
production caller authentication. The native fixture permits PVC inspection
and checked deletion only, with namespace-scoped impersonated RBAC.

Each owner case creates one labeled PostgreSQL container: loopback port only,
one CPU, 512 MiB memory, 128 PIDs, and a 256 MiB tmpfs database. Configuration
and passwords use private files and are not printed. Cleanup validates the
container ID, run label and absence of external mounts before removing it.
Unexpected nonzero process exits, including race-detector exits, fail the test.
Only confirmed injected SIGKILL exits are exempted.

Each case also creates a unique namespace with Pod quota zero and 1 MiB empty
PVCs using a nonexistent storage class. Namespace cleanup verifies recorded
UIDs and ownership, refuses unknown resources or backed claims, and observes
namespace absence. It never strips finalizers or deletes existing task data.

Workload rows are unused admission reservations: no native `StartWorkload` is
sent. Their synthetic historical retirement timestamps exercise TTL eligibility,
not actual Pod-removal confirmation. The PostgreSQL process is not crashed;
this proves application-process recovery, not storage or node failover.

This fixture does not establish A2A lifecycle acceptance, real Agents-service
finalization, production authorization/backend identity, legacy-data migration,
late-start/node fencing, garbage collection, or permanent rollout. Those remain
separate acceptance gates.
