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
- Keep `docs/catalog.json` as the version-1 index of meaningful Markdown and
  `AGENTS.md`, not a source inventory. Preserve its stable document IDs.
- Use the workspace Navigator first when available: `repos [--worktrees]`,
  then `scan --repo orchestrator`, then batch `inspect orchestrator::owner orchestrator::related-owner`.
  Use `docs --repo orchestrator` and `doc orchestrator::document-id` for guides. Worktrees use
  Git-discovered basenames, optionally selected by `--worktree`.
  Standalone contributors need no Navigator: use `git diff upstream/main...HEAD`
  (or the reviewed base), `rg`, Go/Buf tools and adjacent tests.
  Do not copy Navigator tooling, dependencies or machine-specific paths here.
  Optional cross-repo `@see repo::extensionless/component` links belong at genuine
  contract owners; same-repo `@see` paths retain the source extension.
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
