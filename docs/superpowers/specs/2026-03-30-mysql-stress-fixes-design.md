# MySQL Stress Fixes Design

**Scope:** Only `mysql --oracle STRESS`

**Goal:** Fix MySQL stress-mode bugs so the runtime behavior matches CLI parameters and the generated SQL reaches at least 80% execution success on local MySQL 8.4.

## Current Findings

Local reproduction against `127.0.0.1:3306` with `root/Taurus_123` shows:

- Overall success ratio is about 60%.
- `QUERY` is around 80% success, but many failures are deterministic alias bugs.
- `DML` is around 36% success and is the main source of instability.

Observed root causes:

1. `GROUP BY` and `ORDER BY` use `_column_alias`, but alias generation creates a fresh alias name instead of referencing an alias already emitted in the current `SELECT` list.
2. `_insert_select_same_table` generates `INSERT INTO table(insertable_columns) SELECT * FROM table`, which often produces mismatched column counts and triggers MySQL error `1136`.
3. `ON DUPLICATE KEY UPDATE _column = VALUES(_column)` can target generated columns or unrelated columns, which triggers MySQL error `3105`.
4. Stress execution retries deterministic statement failures up to five times, inflating failure counts without improving success ratio.
5. Stress-mode DML generation uses generic value generation paths that are valid in principle but too aggressive for MySQL generated columns and narrow physical column types after repeated schema mutation.

## Chosen Design

### 1. Keep the fix isolated to stress mode

Do not redesign the full MySQL grammar. Apply targeted safeguards in the MySQL stress path and MySQL-specific generator helpers so the change stays bounded and testable.

### 2. Make alias references stable

Adjust MySQL query generation so `GROUP BY` and `ORDER BY` choose from aliases that were already returned by the current `SELECT` clause, instead of minting a new alias token.

### 3. Make `INSERT ... SELECT` structurally valid

Change `_insert_select_same_table` to project the same insertable columns on both sides:

- `INSERT INTO t (c1, c2) SELECT c1, c2 FROM t`

This removes the deterministic column-count mismatch while preserving same-table stress coverage.

### 4. Restrict duplicate-key updates to writable columns

Add a MySQL-specific key function for stress-safe duplicate-key updates so the update target:

- belongs to the same insert target table
- is not generated
- is actually present in the insert column list

This prevents `VALUES(...)` references from targeting generated or unrelated columns.

### 5. Reduce pointless retries in stress mode

Retry only for transient classes of MySQL failures that may succeed on a later attempt, such as lock or deadlock conditions. Deterministic semantic errors should be logged once and counted once.

### 6. Validate with local MySQL using an explicit metric

Acceptance metric:

- `successful statements / total statements >= 80%`

Validation will use the local MySQL instance at `127.0.0.1:3306` with the provided credentials and a reduced stress run that is large enough to be meaningful.

## Files Expected To Change

- `src/main/java/dbradar/mysql/oracle/MySQLStressOracle.java`
- `src/main/java/dbradar/mysql/MySQLKeyFuncManager.java`
- `src/main/java/dbradar/common/query/generator/QueryContext.java`
- `src/main/resources/dbradar/mysql/mysql.grammar.yy`
- `src/test/java/dbradar/mysql/oracle/TestMySQLStressOracleConfig.java`
- `src/test/java/dbradar/mysql/TestMySQLGrammarAndTypeSupport.java`
- New MySQL stress-focused regression test file if needed

## Non-Goals

- No changes to `mysql --oracle EQUATION`
- No broad rewrite of generic query generation
- No attempt to make every generated MySQL statement valid

## Risks

- Alias handling is shared generation infrastructure, so the fix must avoid breaking non-MySQL paths.
- Success ratio depends on random generation; validation needs a stable enough run size to avoid flukes.
- Some MySQL grammar constructs remain intentionally aggressive and may still fail, but the ratio must clear the agreed threshold.
