# Task 2: execution logging

Date: 2026-04-23

## Scope

- Added the global CLI flag `--log-global-execution`, defaulting to `false`.
- Added run-scoped `logs/postgresql/global-execution.log` initialization, truncation, and synchronized single-line writes.
- Simplified stress worker current-log names to `databasePrefix + 1-based threadId`.
- Wrapped `SQLConnection` statement creation so JDBC `execute*` calls log thread id, actual database name, SQL text, status, and flattened errors.
- Added explicit PostgreSQL `DROP DATABASE` / `CREATE DATABASE` global-log writes for lifecycle SQL that bypasses `SQLConnection`.
- Updated stress and generated-column smoke expectations for the simplified thread log names and global execution log presence/content.

## Validation

- Compiled main and test sources with:
  `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/execution-logging-check $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')`
- Ran local PostgreSQL validation on `127.0.0.1:5432`, user `postgres`, password `Taurus_123`:
  - `java -cp 'out/execution-logging-check:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest`
  - `java -cp 'out/execution-logging-check:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`
  - `java -cp 'out/execution-logging-check:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSmokeTest`
- Re-ran `PostgreSQLStressSmokeTest` alone to keep a fresh `global-execution.log` on disk for manual inspection.
- Re-ran `PostgreSQLGeneratedColumnSmokeTest` once more without `--log-global-execution` and verified the global log file was absent afterward, confirming the default-off behavior.

## Thread Log Review

- The stress thread log names are now simplified:
  - `logs/postgresql/task2_grouped_1-cur.log`
  - `logs/postgresql/task2_grouped_2-cur.log`
  - `logs/postgresql/task2_grouped_3-cur.log`
  - `logs/postgresql/task2_grouped_4-cur.log`
- All four grouped thread logs still contained mixed DDL, DML, and DQL activity.
- Shared stress logs now also use the simplified naming:
  - `logs/postgresql/task2_shared_1-cur.log`
  - `logs/postgresql/task2_shared_2-cur.log`

## Global Log Review

- `logs/postgresql/global-execution.log` was recreated for the enabled stress run and reached `182052` bytes in the sampled verification run.
- The log contained:
  - `thread=1` through `thread=4`
  - actual grouped database names `task2_grouped_0_g0` and `task2_grouped_0_g1`
  - explicit lifecycle SQL such as `DROP DATABASE IF EXISTS ...` and `CREATE DATABASE ...`
  - catalog and schema introspection SQL such as `SHOW server_version_num`, `SELECT ... FROM pg_type`, and `SELECT ... FROM information_schema.tables`
  - normal workload DDL/DML/DQL
  - both `status=SUCCESS` and `status=FAIL`
  - flattened `error=-` for successful statements and concrete PostgreSQL error messages for failed statements

## Notes

- The physical grouped database names remain unchanged, so the simplified per-thread `-cur.log` names now trade off local readability for shorter filenames; actual database routing is visible in `global-execution.log`.
- Connection-test lifecycle SQL currently logs with `thread=-1` because it runs before worker threads are assigned user-facing ids. Worker-executed SQL uses the expected `1-based` thread ids.
