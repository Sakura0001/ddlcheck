# MySQL Stress Round-Based Design

**Scope:** Only `mysql --oracle STRESS`

**Goal:** Redesign MySQL stress mode so execution semantics match the intended crash-finding workflow: each database is exercised in repeated rounds, each round runs DDL/DML/QUERY work against the same database, and multi-thread mode means multiple workers sharing one database.

## Requirements

- Single-thread mode:
  - `--num-threads` means how many databases are tested in parallel.
  - each database has exactly one stress worker.
  - each database repeats the configured number of rounds.
- Multi-thread mode:
  - `--num-threads` still means how many databases are tested in parallel.
  - `--stress-threads-per-db` means how many workers share one database.
  - those workers repeatedly stress the same database for the configured number of rounds.
- Round semantics:
  - create or recreate the database for the round
  - execute configured counts of DDL, DML, and random query statements
  - drop and recreate for the next round
- Metadata refresh rules:
  - refresh schema before each DDL
  - refresh schema before each DML
  - do not refresh schema before each query
- Error policy:
  - do not use statement success ratio as a failure criterion
  - worker disconnects may reconnect and continue
  - timeout/hang does not need to be converted into an explicit test failure in stress mode

## Chosen Design

### 1. Introduce a minimal stress-specific parameter set

Keep these stress controls:

- `--num-threads`
- `--stress-threads-per-db`
- `--stress-rounds-per-db`
- `--stress-ddl-per-thread`
- `--stress-dml-per-thread`
- `--stress-query-per-thread`

Other stress-specific controls become implementation defaults rather than required knobs for the intended workflow.

### 2. Give stress mode its own round-based execution path

Do not drive MySQL stress mode through the generic `nrQueries` and `maxGeneratedDatabases` loops.

Instead:

- `Main` still creates one top-level task per database name using `--num-threads`
- MySQL stress execution handles round repetition inside the stress path itself
- each top-level database task owns one database name for all of its rounds

This keeps the generic framework unchanged for other DBMS modes while making MySQL stress semantics explicit.

### 3. Make each round recreate the database

For each round of one database task:

1. create the database fresh
2. run stress workers against that database
3. close connections
4. drop and recreate on the next round by calling the existing database creation path again

This matches the required “run against one database, then delete and rerun” behavior.

### 4. Refresh schema only before DDL and DML

Inside each worker:

- before generating and executing each DDL statement, call `updateSchema()`
- before generating and executing each DML statement, call `updateSchema()`
- query generation uses the latest schema already produced by prior DDL/DML refreshes and does not force an extra refresh

This matches the required consistency model and avoids unnecessary query-phase metadata churn.

### 5. Preserve reconnect behavior

Worker exceptions should continue to log, reconnect once, and resume the round where practical. Stress mode is intended to keep probing the server, not to abort on ordinary statement-level failures.

### 6. Keep the FK metadata race fix

The previously added MySQL foreign-key metadata tolerance stays in place. Concurrent DDL can temporarily expose incomplete `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` rows, and stress mode should not fail inside the harness for that reason.

## Files Expected To Change

- `src/main/java/dbradar/mysql/MySQLOptions.java`
- `src/main/java/dbradar/mysql/oracle/MySQLStressOracle.java`
- `src/main/java/dbradar/ProviderAdapter.java`
- `src/test/java/dbradar/ddlCheck/TestMySQLEDCOracle.java`
- `src/test/java/dbradar/mysql/oracle/TestMySQLStressOracleConfig.java`
- `docs/change-logs/2026-03-31-mysql-stress-multithread-validation.md`

## Non-Goals

- No changes to `mysql --oracle EQUATION`
- No cleanup of unrelated global CLI options
- No crash-detection policy based on success ratio

## Risks

- Changing stress execution semantics inside the existing generic runner can easily produce off-by-one round behavior if the responsibility split is unclear.
- Recreating the database per round must not accidentally leak stale worker connections across rounds.
- Tests should verify semantics, not just successful command return codes.
