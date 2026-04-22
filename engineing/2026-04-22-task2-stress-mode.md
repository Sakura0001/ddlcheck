# Task 2: PostgreSQL stress mode

Date: 2026-04-22

## Scope

- Added PostgreSQL `--oracle stress` and `--stress-topology=isolated|shared`.
- Split stress execution from equation-mode bootstrap and semi-state validation.
- Added per-thread log names for shared topology while multiple threads target the same database.
- Added worker-specific object-name prefixes such as `thr0_` and `thr1_` so shared-topology threads can keep creating tables, views, indexes, triggers, and constraints without constant name collisions.
- Added a dedicated `PostgreSQLStressOracle` that bootstraps exact DDL/DML quotas, then mixes DDL, DML, and DQL without building a semi-state.
- Hardened statement execution so connection-reset / backend-closed style errors surface as hard failures instead of being silently treated as ordinary statement failures.
- Kept equation mode exact-count semantics by preventing the last bootstrap DDL from deleting the only remaining base table.

## Validation

- Compiled main and test sources to `out/task2` with:
  `javac -proc:none -encoding UTF-8 -cp 'libs/*' -d out/task2 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')`
- Ran:
  `java -cp 'out/task2:src/main/resources:libs/*' dbradar.MainOptionsTask1Test`
- Ran:
  `java -cp 'out/task2:src/main/resources:libs/*' dbradar.PostgreSQLOnlyProjectTest`
- Ran local PostgreSQL 16.13 equation regression verification against `127.0.0.1:5432`, user `postgres`, password `Taurus_123`:
  `java -cp 'out/task2:src/main/resources:libs/*' dbradar.PostgreSQLEquationBootstrapCountTest`
- Ran:
  `java -cp 'out/task2:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest`
- Ran local PostgreSQL 16.13 stress smoke verification against `127.0.0.1:5432`, user `postgres`, password `Taurus_123`:
  `java -cp 'out/task2:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`

## Thread Log Review

- `logs/postgresql/task2_isolated_0-cur.log` contained bootstrap DDL, bootstrap DML, later mixed DDL/DML/DQL, and no `==== Start SemiState ====;` marker.
- `logs/postgresql/task2_shared_0-thread0-cur.log` and `logs/postgresql/task2_shared_0-thread1-cur.log` were written separately, proving shared-topology threads no longer overwrite one another's current logs.
- Shared-topology logs showed worker-specific object prefixes such as `thr0_t*`, `thr0_v*`, `thr0_i*`, `thr1_t*`, `thr1_v*`, and `thr1_i*`, confirming name-collision mitigation is active while the physical database stays shared.
- The regression log `logs/postgresql/task1_bootstrap_0-cur.log` again showed exactly 4 bootstrap DDL statements before `==== Start SemiState ====;`, so the Task 2 scheduler/oracle changes did not break Task 1 exact-count behavior.

## Gaps Observed

- Stress mode now behaves correctly at the scheduler/oracle/logging layer, but the current PostgreSQL grammar still emits non-PG fragments such as `strftime`, `dayname`, `NOCASE`, and `NOACCENT`. These statements are now treated as normal fuzz failures rather than hard crashes, but the grammar cleanup still needs to be completed in Task 5 so stress density is not diluted by invalid SQL.
