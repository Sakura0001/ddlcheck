# Task 3: wider PostgreSQL CREATE TABLE coverage

Date: 2026-04-22

## Scope

- Expanded PostgreSQL `CREATE TABLE` generation to emit 8-15 columns for every table-creation statement, without changing the shared grammar engine.
- Mirrored the same 8-15 column layout in `postgresql.txverifier.yy` so verifier-side parsing stays aligned with generated DDL.
- Fixed PostgreSQL schema hydration for insertion-sensitive metadata:
  - column order now follows `ordinal_position` instead of lexical `column_name`;
  - `NOT NULL` and generated-column flags are populated from `information_schema.columns`;
  - primary-key columns are marked from index metadata.
- Made bit-string generation length-aware for PostgreSQL `bit(n)` columns so inserts into wide tables with `bit(1)` columns can succeed.
- During Task 3 verification, also fixed shared-stress follow-up regressions that wide-table changes exposed:
  - shared bootstrap state is refreshed per thread before warm-up;
  - warm-up DDL now preserves at least one base table so the following DML step can always be generated;
  - PostgreSQL view detection now reads `information_schema.views` instead of relying on name prefixes, so prefixed shared-stress views and temporary views are replayed correctly.

## Validation

- Compiled main and test sources to `out/task3` with:
  `javac -proc:none -encoding UTF-8 -cp 'libs/*' -d out/task3 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')`
- Ran local PostgreSQL 16.13 direct generator validation against `127.0.0.1:5432`, user `postgres`, password `Taurus_123`:
  `java -cp 'out/task3:src/main/resources:libs/*' dbradar.PostgreSQLCreateTableWidthTest`
- Ran local PostgreSQL 16.13 log-based smoke validation against `127.0.0.1:5432`, user `postgres`, password `Taurus_123`:
  `java -cp 'out/task3:src/main/resources:libs/*' dbradar.PostgreSQLWideTableSmokeTest`
- Re-ran the stress and equation regressions after the Task 3 fixes:
  `java -cp 'out/task3:src/main/resources:libs/*' dbradar.PostgreSQLEquationBootstrapCountTest`
  `java -cp 'out/task3:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`
- Ran the combined local regression suite:
  `java -cp 'out/task3:src/main/resources:libs/*' dbradar.MainOptionsTask1Test && java -cp 'out/task3:src/main/resources:libs/*' dbradar.PostgreSQLOnlyProjectTest && java -cp 'out/task3:src/main/resources:libs/*' dbradar.PostgreSQLEquationBootstrapCountTest && java -cp 'out/task3:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest && java -cp 'out/task3:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest && java -cp 'out/task3:src/main/resources:libs/*' dbradar.PostgreSQLCreateTableWidthTest && java -cp 'out/task3:src/main/resources:libs/*' dbradar.PostgreSQLWideTableSmokeTest`

## Thread Log Review

- `logs/postgresql/task3_columns_0-cur.log` showed all logged `CREATE TABLE` statements using 10-column and 11-column layouts in this smoke run, satisfying the new 8-15 target.
- The same log showed a successful bootstrap insert:
  `INSERT INTO t0 (c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11) VALUES (...)`
- The smoke log still contained the expected semi-state replay block because Task 3 validation used `equation` mode rather than `stress` mode.
- The direct insert database `task3_insert_check` contained one user table and one inserted row after the generator check, confirming the widened-table insert path worked in PostgreSQL 16.13.
- `logs/postgresql/task2_isolated_0-cur.log` showed the isolated stress thread emitting DDL, DML, and DQL against PostgreSQL 16.13 after the regression fixes.
- `logs/postgresql/task2_shared_0-thread0-cur.log` and `logs/postgresql/task2_shared_0-thread1-cur.log` both showed separate per-thread logs in shared topology, each containing mixed DDL, DML, and DQL without semi-state blocks.

## Gaps Observed

- Task 3 fixed width and insertability for the currently supported PostgreSQL types, but it did not expand the type surface itself. Broader PostgreSQL-only type coverage and additional generator work remain for Task 5.
