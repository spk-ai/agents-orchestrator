# Agents Orchestrator Service

The Agents Orchestrator runs a background reconciler that ensures agent workloads
exist for threads with unacknowledged agent messages.

Architecture: https://github.com/agynio/architecture/blob/main/architecture/agents-orchestrator.md

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
