# Main merge and branch cleanup

Date: 2026-04-22

## Scope

- Fast-forwarded `main` to the validated integration line at `4077e37`.
- Deleted local branches:
  - `codex/pg-branch-integration`
  - `codex/pg-stress-expansion`
  - `codex/pg16-ddl-partition`
  - `codex/pg16-ddl-partition-live`
  - `codex/pg16-ddl-partition-pr`
- Deleted remote branch:
  - `origin/codex/pg-branch-integration`

## Preservation

- Existing extra worktrees were not deleted.
- Those worktrees were switched to detached `HEAD` so any local untracked files or uncommitted changes remain on disk while the branch names are removed.

## Result

- Local branch list now contains only `main`.
- Remote branch list now contains only `main`.
- `main` includes the PostgreSQL-only line, partition DDL work, branch consolidation work, and the later `LIST` / `HASH` / multi-column partition expansion.
