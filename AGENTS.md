# Agents Orchestrator Contribution Guide

## Owners
- `cmd/volume-migration/main.go` and `internal/volumemigration/coordinator.go`:
  operator input and resumable existing-workspace adoption.
- `internal/reconciler/{prepared_start,prepared_workloads,resource_anchors}.go`:
  shared agent/sandbox execution authority and exact identities.
- `internal/reconciler/{prepared_recovery,preparation_revocation,checked_volumes,anchored_volume_removal,workload_reconcile,volume_reconcile}.go`:
  recovery, admission retention and explicit removal.
- The API defines wire contracts; Runners owns database guards; k8s-runner owns
  native resources. Keep those cross-repository dependencies explicit.

## Documentation
- Keep implementation invariants beside their handwritten Go or protobuf owner.
  Update those comments and focused tests when behavior changes; Markdown holds
  operations, cross-repository decisions, security boundaries and dated evidence.
- Start at `docs/catalog.json`. Maintain its version-1 document entries
  (`id`, `path`, `title`, `purpose`, `kind`) for meaningful Markdown and
  `AGENTS.md` only, using repository-relative paths. Do not index generated code.
- When a compatible structural navigator is available, discover repositories and
  components first, then batch-inspect selected owners and their related tests.
  Otherwise use native declarations, imports, RPC types and adjacent tests;
  `git diff upstream/main...HEAD` (or the reviewed base) identifies the changes.
  Do not add a navigator dependency or machine-specific paths to this repository.
  Navigator commands are `repos [--worktrees]`,
  `scan --repo REPO`, `inspect REPO::path` and `docs --repo REPO`.
  IDs here are `api`, `runners`, `orchestrator`, `k8s-runner` and `gateway`.
  Worktrees use Git-discovered basenames, optionally selected by `--worktree`.
  At genuine cross-repo owners, optional `@see repo::extensionless/component`
  references can aid navigation; same-repo `@see` paths retain the extension.
- Preserve dated verification, failures, skips and dependency revisions as
  historical evidence; do not silently turn them into current acceptance claims.
- Do not edit generated sources or applied SQL migrations, including comments.
  Migration bytes participate in recovery/backup checks. Explain SQL behavior
  beside the owning Go caller and link the original migration; schema changes
  require a separately reviewed additive migration. Preserve licensing/notices.

## Verification
Use the existing matching `.gen/` bindings. Run model-free
`go test -mod=readonly -race ./internal/volumemigration ./cmd/volume-migration`
and focused `./internal/reconciler` tests. Keep `PREPARED_STACK_TEST`,
`CHECKED_VOLUME_STACK_TEST`, `RETENTION_LIVE_TEST` and process-helper gates unset.
Live fixtures mutate disposable databases/namespaces and require separate
operator authorization. Never run the migration command or deployment tooling
as documentation verification. Report exclusions and skips explicitly.
