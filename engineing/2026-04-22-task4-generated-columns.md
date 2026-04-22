# Task 4: PostgreSQL generated-column coverage

Date: 2026-04-22

## Scope

- Added a PostgreSQL bootstrap helper that creates one guaranteed `GENERATED ALWAYS AS (c1 + c2) STORED` table in every database round.
- The generated-column bootstrap table uses 8 columns, so it remains compatible with Task 3's wider-table target while staying simple enough for stable inserts.
- `equation` mode now replays that generated-column table into the semi-state and forces one successful insert into it before the remaining bootstrap DML budget is consumed.
- `stress` mode now also creates and inserts into the same generated-column table during bootstrap, so generated-column coverage is present in both `equation` and `stress`.
- PostgreSQL semi-state column replay now preserves `ordinal_position` and reuses correct nullability parsing, which keeps generated-column DDL reconstruction aligned with the live table definition.

## Validation

- Compiled main and test sources to `out/task4` with:
  `javac -proc:none -encoding UTF-8 -cp 'libs/*' -d out/task4 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')`
- Ran the new generated-column integration smoke:
  `java -cp 'out/task4:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSmokeTest`
- Re-ran the bootstrap and stress regressions after the generated-column change:
  `java -cp 'out/task4:src/main/resources:libs/*' dbradar.PostgreSQLEquationBootstrapCountTest`
  `java -cp 'out/task4:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`
  `java -cp 'out/task4:src/main/resources:libs/*' dbradar.PostgreSQLWideTableSmokeTest`
- Re-ran the combined local regression suite against PostgreSQL 16.13 on `127.0.0.1:5432`, user `postgres`, password `Taurus_123`:
  `java -cp 'out/task4:src/main/resources:libs/*' dbradar.MainOptionsTask1Test && java -cp 'out/task4:src/main/resources:libs/*' dbradar.PostgreSQLOnlyProjectTest && java -cp 'out/task4:src/main/resources:libs/*' dbradar.PostgreSQLEquationBootstrapCountTest && java -cp 'out/task4:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest && java -cp 'out/task4:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest && java -cp 'out/task4:src/main/resources:libs/*' dbradar.PostgreSQLCreateTableWidthTest && java -cp 'out/task4:src/main/resources:libs/*' dbradar.PostgreSQLWideTableSmokeTest && java -cp 'out/task4:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSmokeTest`

## Thread Log Review

- `logs/postgresql/task4_generated_eq_0-cur.log` showed the mandatory generated-column table:
  `CREATE TABLE t1 (..., c8 INT GENERATED ALWAYS AS (c1 + c2) STORED);`
- The same equation log showed the bootstrap insert omitting the generated column itself:
  `INSERT INTO t1 (c1,c2,c3,c4,c5,c6,c7) VALUES (...);`
- Local database inspection on `task4_generated_eq_0` confirmed `information_schema.columns` contains `t1.c8` with generation expression `(c1 + c2)`.
- A direct row check on `task4_generated_eq_0` returned `1,2,3` for `c1,c2,c8`, confirming the stored generated column computed and persisted the expected value.
- `logs/postgresql/task4_generated_stress_0-thread0-cur.log` showed the shared-stress bootstrap thread creating `thr0_t2` with `c8 INT GENERATED ALWAYS AS (c1 + c2) STORED` and then inserting rows without naming `c8`.
- `logs/postgresql/task4_generated_stress_0-thread1-cur.log` remained a separate per-thread log and continued to emit mixed DDL/DML/DQL against the same shared database after bootstrap.

## Gaps Observed

- Task 4 guarantees stored generated-column coverage for PostgreSQL 16.13, but it does not add PostgreSQL 18-only virtual generated columns.
- Task 5 still needs broader PostgreSQL-only type coverage and semi-state replay support for PostgreSQL-specific user-defined types.
