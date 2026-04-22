# Main task revalidation on PostgreSQL

Date: 2026-04-23

## Scope

- Re-read the existing `/engineing` history on `main` and verified that tasks 1-3 and 5 were already implemented in the current source tree.
- The initial sweep exposed one remaining gap in task 4: PostgreSQL 18 virtual generated columns were still missing.
- Revalidated every task against local PostgreSQL at `127.0.0.1:5432` with user `postgres` and password `Taurus_123`.

## Validation

- Compiled main and test sources with:
  `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/verify-20260423 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')`
- Task 1:
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.MainOptionsTask1Test`
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLEquationBootstrapCountTest`
- Task 2:
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest`
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`
- Task 3:
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLCreateTableWidthTest`
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLWideTableSmokeTest`
- Task 4:
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSmokeTest`
- Task 5:
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLSchemaTypeCoverageTest`
  `java -cp 'out/verify-20260423:src/main/resources:libs/*' dbradar.PostgreSQLTypeCoverageSmokeTest`

## Thread log review

- Task 1:
  `logs/postgresql/task1_bootstrap_0-cur.log` showed exactly 4 bootstrap DDL statements before `==== Start SemiState ====;` and exactly 3 bootstrap DML statements before the first `SELECT`.
- Task 2:
  `logs/postgresql/task2_isolated_0-cur.log` contained DDL, DML, and DQL in isolated mode.
  `logs/postgresql/task2_shared_0-thread0-cur.log` and `task2_shared_0-thread1-cur.log` both contained DDL/DML/DQL activity in shared mode, confirming the per-thread logging and shared-database pressure path.
- Task 3:
  `logs/postgresql/task3_columns_0-cur.log` contained `CREATE TABLE` statements with 8, 11, and 15 columns, and the subsequent bootstrap inserts succeeded.
- Task 4:
  `logs/postgresql/task4_generated_eq_0-cur.log` contained `GENERATED ALWAYS AS (c1 + c2) STORED` and the matching generated-column insert.
  `logs/postgresql/task4_generated_stress_0-thread0-cur.log` contained the shared bootstrap generated-column create/insert pair; `thread1` continued concurrent workload without duplicating the shared bootstrap.
- Task 5:
  `logs/postgresql/task5_typecov_eq_0-cur.log` and `task5_typecov_stress_0-thread0-cur.log` both contained the built-in and PostgreSQL-specific coverage objects, including `INT4MULTIRANGE`, `MACADDR8`, `TSQUERY`, enum/domain/composite custom types, and inserts into both `typecov_builtin` and `typecov_user`.

## Conclusion

- The current `main` already contained tasks 1-3 and 5.
- Task 4 needed a follow-up for PostgreSQL 18 virtual generated columns and is documented separately in `engineing/2026-04-23-task4-virtual-generated-columns.md`.
- This file records the initial branch revalidation sweep before that task4 follow-up was applied.
