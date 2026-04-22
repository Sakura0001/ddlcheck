# Task: branch consolidation

Date: 2026-04-22

## Scope

- Analyzed the three active worktree branches:
  - `codex/pg-only-no-graph`
  - `codex/pg-only-no-graph-remote`
  - `codex/pg-stress-expansion`
- Kept `codex/pg-stress-expansion` as the only surviving branch because it contains the full PostgreSQL-only line plus Task 1-5 work.
- Merged `codex/pg-only-no-graph-remote` into `codex/pg-stress-expansion` using `-s ours` with `--allow-unrelated-histories` so the legacy branch history is absorbed without reintroducing removed MySQL/SQLite/TiDB/CockroachDB code.
- Deleted local branches `codex/pg-only-no-graph`, `codex/pg-only-no-graph-remote`, and `main`.
- Deleted remote branch `codex/pg-only-no-graph-remote` from GitHub.

## Branch Analysis

- `codex/pg-only-no-graph`
  - Already fully contained in `codex/pg-stress-expansion`.
  - `git rev-list --left-right --count codex/pg-stress-expansion...codex/pg-only-no-graph` returned `7 0`.
- `codex/pg-only-no-graph-remote`
  - Diverged from a separate history line.
  - `git rev-list --left-right --count codex/pg-stress-expansion...codex/pg-only-no-graph-remote` returned `8 13`.
  - Unique commits on that branch were mostly older MySQL/JAR/resource-loading work and would have reintroduced files intentionally removed by the PostgreSQL-only cleanup.
- `codex/pg-stress-expansion`
  - Contains the PostgreSQL-only cleanup plus Task 1-5 changes:
    - bootstrap DDL/DML counts
    - stress mode
    - wider tables
    - generated columns
    - deterministic type coverage and semi-state replay fixes

## Worktree Handling

- Removed the clean extra worktree for `codex/pg-only-no-graph-remote`.
- The original workspace `/Users/yuyu/PyCharmMiscProject/ddlcheck-pg` had many pre-existing dirty changes, so it was moved to detached `HEAD` at `192f87d` instead of being deleted. This preserved all local file state while freeing the old branch for deletion.

## Validation

- Compiled main and test sources to `out/branch-merge` with:
  `javac -proc:none -encoding UTF-8 -cp 'libs/*' -d out/branch-merge $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')`
- Ran the serial local PostgreSQL regression suite against `127.0.0.1:5432`, user `postgres`, password `Taurus_123`:
  - `java -cp 'out/branch-merge:src/main/resources:libs/*' dbradar.MainOptionsTask1Test`
  - `java -cp 'out/branch-merge:src/main/resources:libs/*' dbradar.PostgreSQLOnlyProjectTest`
  - `java -cp 'out/branch-merge:src/main/resources:libs/*' dbradar.PostgreSQLEquationBootstrapCountTest`
  - `java -cp 'out/branch-merge:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest`
  - `java -cp 'out/branch-merge:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`
  - `java -cp 'out/branch-merge:src/main/resources:libs/*' dbradar.PostgreSQLCreateTableWidthTest`
  - `java -cp 'out/branch-merge:src/main/resources:libs/*' dbradar.PostgreSQLWideTableSmokeTest`
  - `java -cp 'out/branch-merge:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSmokeTest`
  - `java -cp 'out/branch-merge:src/main/resources:libs/*' dbradar.PostgreSQLSchemaTypeCoverageTest`
  - `java -cp 'out/branch-merge:src/main/resources:libs/*' dbradar.PostgreSQLTypeCoverageSmokeTest`

## Final State

- Local branch list now contains only:
  - `codex/pg-stress-expansion`
- Remote old branch removed:
  - `codex/pg-only-no-graph-remote`
