# Group Consumer Test Synchronization

Focused test-only correction on upstream `ae7d0bf`. No production consumer,
retry policy, API, generated code, workload or deployment behavior changes.

The unmodified `TestGroupMembershipConsumerLoopRetriesWithoutBlocking` reports
two races under `-race`: reading a fake subscription's boolean concurrently
with `Unsubscribe`, and restoring package retry globals without synchronizing
with the consumer's final reads. The initial repeated baseline run fails.

The test now observes subscription and unsubscription through channel signals.
It uses the existing production retry delay instead of mutating global values;
the five-second test context bounds retry/startup and cancellation has a separate
one-second unsubscribe deadline. The first subscription still fails, the retry
must succeed, unsubscribe must not happen early, and cancellation must trigger
unsubscribe. Waiting for these events no longer polls shared mutable state.

## Verification

Using the upstream generated API set from the independent DNS checkout:

```bash
go test -race -vet=off ./internal/reconciler \
  -run '^TestGroupMembershipConsumerLoopRetriesWithoutBlocking$' \
  -count=20 -cpu=1,2,4 -timeout=2m
go test -vet=off ./... -count=1
go test -race -vet=off ./... -count=1
go build ./...
go vet -assign=false ./...
```

All 60 repeated cases and both 257-entry ordinary/full race suites pass with
zero test exclusions, failures or skips. Build and vet excluding `assign` pass.
Unfiltered vet still fails on the unrelated `start_decision.go:182`
self-assignment. This patch does not fix or hide that result.

The test now takes approximately one second per case because it exercises the
actual retry delay. It has no Kubernetes, NATS server or provider prerequisite.
This source evidence does not establish production deployment or overall
orchestrator correctness. Existing repository licensing is retained; no
upstream PR has been submitted.
