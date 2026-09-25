# Prepared Execution Stack Fixture

This opt-in fixture combines real Runners migrations/PostgreSQL, production
controller lifecycle methods in separate OS processes, and the native runner's
real Kubernetes handlers. It is separate from the existing volume-only fixture,
whose namespace still prohibits Pod execution.

## Reproduce

The revisions below record the original acceptance combination, not current
release recommendations. A new combination needs separate compatibility review.
Use a trusted local Kubernetes cluster and local Unix-socket Docker daemon.

Use matching `feat/preparation-revocation` checkouts for API, k8s-runner,
Runners and agents-orchestrator, including registry migration `0026`. The
revocation dependencies are API `23d3073`, native runner `3260fb4` and registry
`302b7c8`. The preceding anchored-retirement branch used migration `0025`. The
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
  -run '^TestLivePreparedExecutionStack$' -count=1 -timeout=40m
```

Use the [registry builder](../runners-volume-fixture/build.sh) and
[native builder](build.sh); their generated-binding prerequisites must be met.
Both images must be reviewed digest-pinned references. PostgreSQL must already
exist on the local Unix-socket Docker daemon. The native Node image must be
available to Kubernetes. No provider credential, agent CLI, platform database,
deployment update or installed workspace is used. The historical recipe disables
vet because of the documented self-assignment in those revisions; its separate
`-assign=false` check is not an unfiltered vet pass.

## Assertions

The executable assertions live in
[prepared_stack_live_test.go](../../internal/reconciler/prepared_stack_live_test.go)
and its [process harness](../../internal/reconciler/prepared_stack_process_test.go).
These are application-process recovery checks: PostgreSQL itself is not crashed,
so they do not establish disk-failure or node-fencing acceptance.

## Unbound Revocation Recovery

With the same fixture environment, run:

```sh
go test -race -json ./internal/reconciler \
  -run '^TestLivePreparationRevocationStack$' -count=1 -timeout=18m
```

The checkpoint matrix lives in
[preparation_revocation_stack_live_test.go](../../internal/reconciler/preparation_revocation_stack_live_test.go).
Late PVCs are explicit fixture CREATEs, not a simulation of already-admitted
requests. Native delayed-CREATE acceptance is separate; neither scope authorizes
retrying an interrupted task or provider request.

## Boundaries And Cleanup

Registry Agents display metadata and authorization tuple writes are stubs.
The sandbox owner lookup is a narrowly scoped fixture. The shared production
starter, checked volume creation, health and stop methods use real RPC clients;
the fixed Node program substitutes for agent assembly and execution. It proves
neither the complete orchestrator event loop nor A2A/daemon inbox recovery,
native sessions, model approvals, credential revocation or actual Ziti policy.

Treat the loopback RPC tokens and scoped Kubernetes access as trusted-local
test isolation, not production authentication or adversarial network enforcement.
Review [native fixture access](main.go),
[namespace and cleanup guards](../../internal/reconciler/prepared_stack_helpers_test.go)
and [database/process guards](../../internal/reconciler/checked_volume_stack_helpers_test.go)
before authorizing a run. Do not bypass a cleanup refusal or strip finalizers;
unknown resources need operator investigation.

Namespace disposal removes retained fixture workspaces and metadata. It is not
production retirement evidence or proof against delayed children. Use the
separate `^TestLiveAnchoredVolumeRetirementStack$` selector with this environment
for [explicit retirement acceptance](../../ANCHORED-VOLUME-RETIREMENT.md);
its assertions live in
[anchored_volume_stack_live_test.go](../../internal/reconciler/anchored_volume_stack_live_test.go).

Full A2A acceptance, initially absent/late-prepare resource recovery, authenticated
routes, node/storage fencing, legacy adoption, durable credential cleanup and
coordinated production rollout remain separate release requirements.
