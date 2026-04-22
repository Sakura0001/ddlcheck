# Task 1: bootstrap DDL/DML counts

Date: 2026-04-22

## Scope

- Added global options `--ddl-count` and `--dml-count`.
- Changed equation-mode bootstrap so each database round now waits for the requested number of successful DDL and DML statements before entering the normal DQL loop.
- Tightened bootstrap retry handling so `IgnoreMeException` during DDL/DML generation is retried inside bootstrap instead of being silently swallowed by the outer oracle loop.
- Added standalone tests for option parsing and bootstrap-count validation against a local PostgreSQL instance.

## Validation

- Compiled main and test sources to `out/task1` with:
  `javac -proc:none -encoding UTF-8 -cp 'libs/*' -d out/task1 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')`
- Ran:
  `java -cp 'out/task1:src/main/resources:libs/*' dbradar.MainOptionsTask1Test`
- Ran:
  `java -cp 'out/task1:src/main/resources:libs/*' dbradar.PostgreSQLOnlyProjectTest`
- Ran local PostgreSQL bootstrap-count verification against `127.0.0.1:5432`, user `postgres`, password `Taurus_123`:
  `java -cp 'out/task1:src/main/resources:libs/*' dbradar.PostgreSQLEquationBootstrapCountTest`

## Thread log review

- `logs/postgresql/task1_bootstrap_0-cur.log` showed exactly 4 DDL statements before `==== Start SemiState ====;`.
- The same thread log showed exactly 3 successful bootstrap DML statements before the first non-bootstrap `SELECT`.
- The verification database was PostgreSQL 16.13 and the bootstrap log contained the expected semi-state replay block.
