# Task 4: PG18 virtual generated-column support

Date: 2026-04-23

## Scope

- Kept the existing PostgreSQL 16 bootstrap path stable by retaining the guaranteed stored generated-column table.
- Added version-aware generated-column support so PostgreSQL 18+ can bootstrap an additional `GENERATED ALWAYS AS (...) VIRTUAL` table when the DDL budget is high enough.
- Extended semi-state replay to preserve the generated-column storage kind by reading `pg_attribute.attgenerated` and rebuilding either `STORED` or `VIRTUAL`.
- Added a pure support test for the PostgreSQL-version gate and updated the generated-column smoke to validate the storage kind from catalog metadata.

## Validation

- Compiled main and test sources with:
  `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/verify-20260423 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')`
- Ran:
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSupportTest`
- Re-ran local PostgreSQL regressions on `127.0.0.1:5432`, user `postgres`, password `Taurus_123`:
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLEquationBootstrapCountTest`
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSmokeTest`
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLTypeCoverageSmokeTest`

## Thread log review

- Local PostgreSQL is `16.13`, so the generated-column logs still show the stored bootstrap path only:
  - `logs/postgresql/task4_generated_eq_0-cur.log`
  - `logs/postgresql/task4_generated_stress_0-thread0-cur.log`
- The equation log still contains:
  `CREATE TABLE t0 (..., c8 INT GENERATED ALWAYS AS (c1 + c2) STORED);`
  followed by:
  `INSERT INTO t0 (c1,c2,c3,c4,c5,c6,c7) VALUES (...);`
- The updated smoke now also checks `pg_attribute.attgenerated`, confirming the local stored column stays kind `s`.

## Notes

- PostgreSQL 18 virtual generated columns cannot be executed against the local `16.13` server, so the new `VIRTUAL` path is version-gated and covered by the pure support test in this environment.
- On PostgreSQL 18+, the bootstrap adds the virtual generated-column table once `--ddl-count` reaches `5`, while `--ddl-count=4` keeps the previous task5 type-coverage budget unchanged.
