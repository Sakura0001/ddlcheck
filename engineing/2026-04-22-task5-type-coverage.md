# Task 5: PostgreSQL type coverage and semi-state replay

Date: 2026-04-22

## Scope

- Added deterministic PostgreSQL type coverage bootstrap objects for `equation` and `stress`.
- Reworked semi-state replay to rebuild enum/domain/composite prerequisites and preserve exact PostgreSQL column types via `pg_attribute + format_type(...)`.
- Expanded schema/type-generator support for additional PostgreSQL-native families used by the new coverage tables.
- Cleaned obvious non-PostgreSQL grammar fragments and synchronized verifier-side type parsing with PostgreSQL-only DDL coverage.

## Validation

- Database: PostgreSQL 16.13 (Homebrew)
- Connection: `127.0.0.1:5432`, user `postgres`, password `Taurus_123`
- Compile:
  - `javac -proc:none -encoding UTF-8 -cp 'libs/*' -d out/task5 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')`
- Serial verification:
  - `java -cp 'out/task5:src/main/resources:libs/*' dbradar.MainOptionsTask1Test`
  - `java -cp 'out/task5:src/main/resources:libs/*' dbradar.PostgreSQLOnlyProjectTest`
  - `java -cp 'out/task5:src/main/resources:libs/*' dbradar.PostgreSQLEquationBootstrapCountTest`
  - `java -cp 'out/task5:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest`
  - `java -cp 'out/task5:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`
  - `java -cp 'out/task5:src/main/resources:libs/*' dbradar.PostgreSQLCreateTableWidthTest`
  - `java -cp 'out/task5:src/main/resources:libs/*' dbradar.PostgreSQLWideTableSmokeTest`
  - `java -cp 'out/task5:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSmokeTest`
  - `java -cp 'out/task5:src/main/resources:libs/*' dbradar.PostgreSQLSchemaTypeCoverageTest`
  - `java -cp 'out/task5:src/main/resources:libs/*' dbradar.PostgreSQLTypeCoverageSmokeTest`

## Thread Log Review

- `logs/postgresql/task5_typecov_eq_0-cur.log`
  - Bootstrap DDL created generated-column, enum/domain/composite, built-in coverage, and user-defined coverage tables.
  - Semi-state replay rebuilt `coverage_enum`, `coverage_domain`, `coverage_composite`, then replayed `typecov_user` with exact types `int4multirange`, `coverage_domain`, `box`, `lseg`, `path`, `polygon`, `circle`, `macaddr8`, and `tsquery`.
- `logs/postgresql/task5_typecov_stress_0-thread0-cur.log`
  - Shared stress bootstrap created `thr0_coverage_*`, `thr0_typecov_builtin`, and `thr0_typecov_user`.
  - Subsequent mixed stress traffic hit the new coverage tables with `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `REINDEX`, and `SELECT`.
- `logs/postgresql/task5_typecov_stress_0-thread1-cur.log`
  - Shared stress traffic continued against the same coverage objects from another thread and kept emitting DDL/DML/DQL without semi-state markers.

## Catalog Checks

- `task5_typecov_eq_0.public.typecov_user` confirmed in `information_schema.columns` with:
  - `int4range`
  - `int4multirange`
  - enum/domain/composite identity preserved through `udt_name` / `domain_name`
  - `box`
  - `lseg`
  - `path`
  - `polygon`
  - `circle`
  - `macaddr8`
  - `tsquery`
- `pg_attribute + format_type(...)` on `task5_typecov_eq_0.public.typecov_user` confirmed semi-state replay uses exact type names instead of lossy `USER-DEFINED`.
- `task5_typecov_stress_0` retained `thr0_coverage_enum`, `thr0_coverage_domain`, `thr0_coverage_composite`, `thr0_typecov_builtin`, and `thr0_typecov_user` in `pg_type`.

## Grammar Checks

- Grepped Task 1-5 verification logs for:
  - `strftime`
  - `dayname`
  - `monthname`
  - `date_sub`
  - `date_diff`
  - `NOCASE`
  - `NOACCENT`
- Result: no matches in the verified PostgreSQL logs.

## Coverage Notes

- Deterministically covered in bootstrap tables:
  - `bytea`
  - `date`
  - `time`
  - `timestamp`
  - `interval`
  - `point`
  - `box`
  - `lseg`
  - `path`
  - `polygon`
  - `circle`
  - `cidr`
  - `macaddr`
  - `macaddr8`
  - `tsvector`
  - `tsquery`
  - `uuid`
  - `xml`
  - `json`
  - `jsonb`
  - `pg_lsn`
  - `oid`
  - arrays
  - `int4range`
  - `int4multirange`
  - enum
  - domain
  - composite
- Random `CREATE TABLE` generation intentionally stayed on the higher-yield subset (`INT/BOOLEAN/TEXT/DECIMAL/FLOAT/REAL/int4range/MONEY/BIT/INET`) because broadening the random grammar further made wide-table bootstrap success drop sharply under the current generic `DEFAULT/CHECK` rules.
