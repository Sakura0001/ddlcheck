# Execution Logging Design

Date: 2026-04-23

## Problem

The current PostgreSQL stress logging is optimized for per-thread investigation only:

- grouped stress worker log names expose round and group details too aggressively
- there is no single run-level log that shows every executed SQL statement
- success, failure, database target, and error details are scattered across current logs and exception files

This makes it harder to reconstruct a full run across many concurrent threads and many databases.

## Goals

- Simplify per-thread log file names back to `databasePrefix + threadId`.
- Keep thread ids user-facing as `1, 2, 3, ...` instead of zero-based worker indexes.
- Add a single global execution log for one run.
- Gate global logging behind a new CLI flag and keep it disabled by default.
- Ensure the global log contains every SQL statement executed during one run, including:
  - DDL
  - DML
  - DQL
  - schema/catalog introspection
  - stress direct-execution SQL
  - database create/drop statements
- Rewrite the global log file from scratch on each run.

## Non-Goals

- Changing the content format of existing per-thread `-cur.log` files.
- Removing existing exception logs.
- Adding persistent multi-run log accumulation.

## User-Facing CLI

Add a global option:

- `--log-global-execution`

Semantics:

- default: `false`
- when `true`, emit a run-level global execution log
- when `false`, no global execution log file is written

## Per-Thread Log Naming

The physical database name remains unchanged.

Only the worker `logName` used by `StateLogger` changes for stress workers:

- grouped/shared/isolated worker current log files should use:
  - `databasePrefix + threadId`
- thread id is `workerIndex + 1`

Examples:

- `task2_grouped_1-cur.log`
- `task2_grouped_2-cur.log`

The global execution log will still record the real database name for each statement, so grouped database targeting remains visible there.

## Global Execution Log

File path:

- `logs/<dbms>/global-execution.log`

Lifecycle:

- initialize once per run
- truncate existing file contents before the run starts
- append one line per executed SQL statement during that run

Line format:

```text
thread=2 db=task2_grouped_0_g1 status=FAIL error=duplicate key value violates unique constraint sql=INSERT INTO ...
```

Rules:

- `thread` is the user-facing 1-based thread id when available
- `db` is the actual target database name
- `status` is `SUCCESS` or `FAIL`
- `error` is `-` for success, otherwise the single-line exception message
- `sql` is the executed SQL text in single-line form

## Coverage Strategy

To guarantee that all executed SQL is logged, global execution logging must be attached at the JDBC execution boundary instead of only at higher-level query orchestration points.

### Primary interception layer

Instrument `SQLConnection` so that:

- `createStatement()` returns a wrapped `Statement`
- `prepareStatement()` returns a wrapped `PreparedStatement`

The wrappers will log on:

- `execute(...)`
- `executeQuery(...)`
- `executeUpdate(...)`
- any other execution methods used by the current code path

This covers:

- normal query execution through `QueryManager`
- stress direct execution
- schema introspection
- comparator and EDC validation queries
- any direct JDBC use that goes through `SQLConnection`

### Supplemental interception

Database create/drop statements in PostgreSQL are executed through raw JDBC connections in `PostgreSQLGlobalState`, bypassing `SQLConnection`.

These statements must be explicitly recorded there to preserve the “all SQL in one run” guarantee.

## State and Context Propagation

Global logging needs access to:

- real database name
- user-facing thread id
- SQL text
- success/failure result
- error message

Recommended propagation model:

- `DBMSExecutor` computes the 1-based thread id for worker log naming and state context
- `StateLogger` owns the static global log writer and formatting helpers
- `SQLConnection` stores enough context to emit global log lines for wrapped statements

## Implementation Outline

### Option parsing

- Add `--log-global-execution` to `MainOptions`.

### Logger sink

- Extend `StateLogger` with:
  - global log file initialization
  - one-line event formatting
  - synchronized append methods
  - line sanitization for SQL and error text

### JDBC wrappers

- Extend `SQLConnection` with run context:
  - actual database name
  - user-facing thread id
  - global logging enabled flag
- wrap created statements so all SQL execution methods append one global log line.

### Executor integration

- `DBMSExecutor` should attach `StateLogger` to state early enough and pass thread-id context into the connection/logger path.

### Stress worker naming

- Change grouped/isolated/shared worker `logName` construction in `Main` to use `databasePrefix + threadId`.

### PostgreSQL database create/drop logging

- Add explicit global-log writes for `DROP DATABASE IF EXISTS ...` and `CREATE DATABASE ...` in `PostgreSQLGlobalState`.

## Testing

Add or extend tests for:

- option parsing for `--log-global-execution`
- grouped stress log naming now using `databasePrefix + threadId`
- global execution log file creation only when enabled
- global execution log rewrite behavior per run
- presence of thread id, actual database name, status, and SQL text
- grouped stress run capturing SQL from multiple databases in one global log
- local PostgreSQL validation that global log includes:
  - worker SQL
  - grouped-database SQL
  - bootstrap/generated-column SQL

## Risks

- If logging remains only at `QueryManager` level, direct JDBC execution paths will be missed.
- If `SQLConnection` wrappers miss one execution method, some SQL may silently escape logging.
- If global logging is not synchronized, multi-threaded runs can interleave and corrupt lines.
- If error messages are not flattened to one line, the global log becomes hard to parse.

## Verification Requirements

Implementation is complete only after all of the following are done:

- compile main and test sources
- run option parsing tests
- run local PostgreSQL grouped stress smoke with global logging enabled
- inspect per-thread log file names
- inspect the generated `global-execution.log`
- verify the global log contains statements from multiple threads and multiple databases
- write a short `/engineing` note
- commit and attempt to push the validated change to GitHub `main`
