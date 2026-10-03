# Failed Workload Retention And Evidence

When a workload fails, the orchestrator records which check failed. Before the
Pod is removed it stores the final container statuses and the newest redacted
output of each container on the workload record. Optionally it keeps the failed
Pod for a bounded time first, so an operator can inspect it. Evidence is also
stored when retention starts, so it can be read while the Pod is kept. The code
owners are [failed_retention.go](internal/reconciler/failed_retention.go) and
[failure_evidence.go](internal/reconciler/failure_evidence.go).

## Settings

| Variable | Default | Meaning |
| --- | --- | --- |
| `FAILED_WORKLOAD_RETENTION` | `0` | Go duration, at most `24h`, for which a failed agent Pod is kept. `0` removes failed workloads at once, as before. |
| `FAILED_WORKLOAD_RETENTION_MAX` | `1` | Maximum number of failed Pods kept at once (0 to 16). Later failures are removed at once, after their evidence is stored. |
| `FAILED_WORKLOAD_EVIDENCE_LOG_BYTES` | `65536` | Newest output kept per container (at most `262144`; `0` stores statuses only). |

The suggested production profile is `FAILED_WORKLOAD_RETENTION=30m` and
`FAILED_WORKLOAD_RETENTION_MAX=1`, keeping the default evidence size. On
startup the orchestrator logs
`orchestrator: retaining up to N failed workload Pod(s) for D each`.

## What retention does

- The window starts when the record became FAILED (its `removed_at`). An
  orchestrator restart re-admits a retained Pod for the rest of its window.
- A retained workload stays FAILED with `removal_confirmed_at` unset. It keeps
  its capacity slot, and its agent instance is not started again: the
  confirmed-removal fence is unchanged.
- After the window the Pod is removed through the normal confirmed removal.
  The instance is then retried at once, because the start backoff is measured
  from the failure. The usual limit of 10 consecutive failures still pauses the
  instance.
- Retention ends early when the agent or its environment changes after the
  failure (the explicit signal for a newer start), when the instance is paused
  or terminated (`STOP_INACTIVE_INSTANCES=true`), or when the Pod no longer
  exists. Waiting messages do not end it, because a failed start always leaves
  its message waiting.
- Only agent-instance workloads are retained. Sandbox failures are removed at
  once, with evidence. The lifetime after `removed_at` is not metered.
- Only prepared workloads have evidence and retention. Unprepared records
  written by older versions are removed as before.

Release a Pod early by deleting it, for example
`kubectl delete pod -n agyn-workloads workload-<workload-id>`. On its next
tick the orchestrator confirms the removal. It keeps the evidence stored when
retention started, because a Pod that can no longer be read never overwrites
stored evidence. Do not edit the workload record.

## Reading the evidence

Read the record with `GetWorkload`, through the Gateway (`RunnersGateway`) or,
inside the cluster, through Runners:

- `failure_reason` and `failure_message` name the failed check and the
  container. Examples are `start check: main container "main" could not be
  created within 1m0s (state=WAITING reason=InvalidImageName restarts=0): ...`
  and `runner reported the Pod failed: init container "ziti-enroll" exited
  (...)`. The message is at most 2 KiB and never holds container output,
  because Runners copies it into workload notifications.
- `containers[]` holds the final status of each container: state, reason, exit
  code, message and restart count. `output_tail` holds the newest output, up
  to the configured size. The tail starts with `[agyn: earlier output
  truncated]` when output was cut. It reads `[output unavailable: ...]` when
  the runner could not supply output (a container that never started, a Pod
  that was already gone, or a runner without `TailWorkloadLogs`), and
  `[no output]` when the container printed nothing.

List responses (`ListWorkloads*`) omit `output_tail`. The orchestrator pages
through every tracked record each cycle, and evidence of up to 1 MiB per record
would push those pages past gRPC message limits.

Output and messages pass through a redactor before they leave the
orchestrator. It covers JWTs (including OpenZiti enrollment tokens),
three-segment tokens, provider API-key formats (OpenAI/Anthropic `sk-`,
GitHub, Slack, AWS, Google, Stripe, Doppler, Hugging Face, npm), bearer, basic
and authorization headers, URL credentials, PEM private keys, and values of
keys named like secret, token, password, api key or credential. Redaction is
pattern-based and deliberately broad, but it is not a guarantee: anyone who can
read workloads in the organization can read this evidence, as they can already
stream live logs.

## Design choice

The smallest design consistent with the API keeps evidence on the existing
workload record instead of in a new store. The parts are:

- **API.** It adds the optional `Container.output_tail` field and the bounded
  `RunnerService.TailWorkloadLogs` RPC. A distinct RPC means an older runner
  answers `Unimplemented` instead of ignoring a byte bound on
  `StreamWorkloadLogs`.
- **Runners.** It stores the tail in the existing `containers` JSON column, so
  no migration is needed. It caps the tail at 256 KiB per container and 1 MiB
  per workload, and omits it from list responses.
- **k8s-runner.** It serves `TailWorkloadLogs`, capped at 256 KiB, optionally
  from the previous container instance.

`failure_message` was rejected as the store because Runners publishes it with
notifications. The Files service was rejected because a thread attachment
would reach the agent.

An older Runners server drops the field. In that case the orchestrator logs
the redacted output (4 KiB per container) instead of storing it. An older
runner leaves an "unavailable" note per container. Container statuses are
stored either way.

## Rollout order

Deploy these together, in this order:

1. API `feat/failed-workload-evidence`.
2. Runners `feat/container-output-tail`. Because its API pin moves, the
   classification's `apiRevision` changes. The infra `rpc-access.json` parity
   copy must move with it.
3. k8s-runner `feat/tail-workload-logs`.
4. This orchestrator.

Keep `FAILED_WORKLOAD_RETENTION=0` until the new orchestrator is confirmed
healthy, then set the production profile.
