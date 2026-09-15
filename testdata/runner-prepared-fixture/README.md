# Prepared Execution Stack Fixture

This opt-in fixture combines real Runners migrations/PostgreSQL, production
controller lifecycle methods in separate OS processes, and the native runner's
real Kubernetes handlers. It is separate from the existing volume-only fixture,
whose namespace still prohibits Pod execution.

## Reproduce

Use matching `feat/anchored-volume-removal` checkouts for API, k8s-runner,
Runners and agents-orchestrator, including registry migration `0025`. The
parent anchor fixture used API `6fe4cab`, k8s-runner `72a1cc8` and Runners
`e1a3b7f` through migration `0024`; those alone do not implement this branch's
retirement contract. API generation is in
[`PREPARED-WORKLOADS.md`](../../PREPARED-WORKLOADS.md). These are dependent
contribution proposals, not stock Agyn releases.

From the orchestrator checkout, with absolute operator-selected paths:

```bash
bash testdata/runners-volume-fixture/build.sh "$RUNNERS_CHECKOUT" "$REGISTRY_BINARY"
bash testdata/runner-prepared-fixture/build.sh "$RUNNER_CHECKOUT" "$RUNNER_BINARY"

PREPARED_STACK_TEST=trusted-local \
RETENTION_KUBECONFIG="$KUBECONFIG" \
CHECKED_REGISTRY_BINARY="$REGISTRY_BINARY" \
PREPARED_RUNNER_BINARY="$RUNNER_BINARY" \
PREPARED_RUNNER_CHART="$RUNNER_CHECKOUT/charts/k8s-runner/values.yaml" \
CHECKED_POSTGRES_IMAGE="$POSTGRES_IMAGE" \
PREPARED_NODE_IMAGE="$NODE_IMAGE" \
go test -race -vet=off -json ./internal/reconciler \
  -run '^TestLivePreparedExecutionStack$' -count=1 -timeout=30m
```

Both images must be reviewed digest-pinned references. PostgreSQL must already
exist on the local Unix-socket Docker daemon. The native Node image must be
available to Kubernetes. No provider credential, agent CLI, platform database,
deployment update or installed workspace is used. Both fixture builders and
the controller test executable enable the race detector. Vet is disabled here
only because of the documented pre-existing self-assignment; separate vet with
`-assign=false` must still pass.

## Assertions

For both agent-instance and sandbox owners:

- A controller stops at BOUND while another owner runs. Independent Pod GETs
  prove the first Pod stays gated, unscheduled and unexecuted. The same owner
  cannot acquire a second reservation. Different owners use distinct PVC UIDs.
- Actual workspace effects survive confirmed Pod removal and a new Pod for the
  same owner. The other owner's heartbeat keeps advancing without changing its
  effects. Every turn checks its owner marker and complete prior file contents.
- SIGKILL after native activation, before the controller consumes its reply,
  leaves SQL at ACTIVATING. All three application processes are replaced.
  Read-only health reconciliation confirms ACTIVE/RUNNING, with no new prepare
  or activate RPC and no repeated workspace effect.
- Cancellation during prepare discovers and removes the unbound Pod before the
  late reply reaches its caller. Cancellation at BOUND and ACTIVATING prevents the paused startup
  from executing after the exact Pod has been retired. A following turn verifies
  the workspace still contains no canceled-turn effect.
- SIGKILL after prepare but before binding leaves an unknown outcome. All three
  application processes are replaced. Fresh production recovery observes the
  gated Pod through the new RPC, validates/persists exact volume and workload
  bindings into REMOVING and confirms exact native removal before follow-up.
- Recovery itself is SIGKILLed after observation, volume binding and workload
  binding. Fresh controllers finish retirement without prepare/activate replay.
- Overlapping controller processes converge on the same removal: a paused stale
  observer accepts another controller's durable confirmation without repeating
  removal. The next turn uses the same PVC and verifies no earlier execution.
- A killed PREPARING caller with no native Pod retains admission on NotFound.
  The fixture does not turn that observation into late-create fencing.
- SIGKILL after removal intent, native ABSENT and registry confirmation preserves
  committed state. A new controller observes/removes the exact predecessor
  before a new turn; billing timestamps and pending removal do not release it.
- A killed unused reservation can abort without fabricating native evidence.
- Cancellation after complete anchor persistence but before preparation
  revokes the workload owner and prevents a paused starter from creating a Pod.
  A following workload uses the existing volume owner for its first PVC.
- SIGKILL at anchor-removal PENDING and ABSENT retains admission until a fresh
  controller confirms exact removal. Persistent volume owners survive.
- Actual inbox threads differ from the registry's legacy instance alias.
  Both remain unchanged through recovery; native anchors retain the real thread.

Independent `psql` connections compare lifecycle phase/revision, complete native
binding, removal observation/timestamp, resource-anchor set, volume reservation
receipts and durable owner/backend pins against RPC responses. Native ConfigMap
UIDs and exact Pod/PVC owner references are compared independently too. The database
itself is not crashed, so this is application-process recovery, not disk-failure
or node-fencing acceptance.

## Boundaries And Cleanup

Registry Agents display metadata and authorization tuple writes are stubs.
The sandbox owner lookup is a narrowly scoped fixture. The shared production
starter, checked volume creation, health and stop methods use real RPC clients;
the fixed Node program substitutes for agent assembly and execution. It proves
neither the complete orchestrator event loop nor A2A/daemon inbox recovery,
native sessions, model approvals, credential revocation or actual Ziti policy.

Each owner group uses a new namespace with chart-derived scoped runner RBAC and
GET-only access to that namespace's UID. The native subprocess verifies denied
cross-namespace PVC access, namespace listing and Secret listing. Private
loopback RPC tokens and allowlists protect the fixture; legacy native startup
and all streams are denied. These tokens are not production authentication.

The namespace has a deny-network policy, at most four Pods and twenty 1 MiB
PVCs, aggregate CPU/memory quotas, and bounded per-container compute. Workloads
have no service-account token and their credential-free probes expire after
three minutes. This fixture does not establish adversarial network enforcement.
PostgreSQL uses the existing helper's one CPU, 512 MiB memory, 128 PID and
256 MiB tmpfs limits, with one loopback port and no external storage.

Cleanup uses captured exact bindings and native ABSENT observations, verifies
Pod/PVC UIDs, refuses unknown resources and remaining workload holds, observes
temporary Secret GC, then removes only the owned namespace and exact GET-only
cluster RBAC objects. It does not strip finalizers. Production recovery uses the
actual native observation RPC; parent-captured bindings independently validate
its result and are retained solely for bounded fixture cleanup. Process exit failures,
including race reports, fail acceptance; only joined deliberate SIGKILL exits
are exempted.

Namespace deletion also removes this fixture's unused anchor metadata and
retained test PVCs. This is explicit fixture disposal, not production anchored
volume retirement or proof that an absent anchor excludes delayed child creation.

The separate `TestLiveAnchoredVolumeRetirementStack` selector exercises the
[explicit retirement contract](../../ANCHORED-VOLUME-RETIREMENT.md), including
four controller SIGKILL checkpoints per owner kind and registry/native process
replacement. It verifies native PVC-and-owner absence before registry
confirmation. It does not substitute namespace deletion for that evidence.

Full A2A acceptance, initially absent/late-prepare resource recovery, authenticated
routes, node/storage fencing, legacy adoption, durable credential cleanup and
coordinated production rollout remain separate release requirements.
