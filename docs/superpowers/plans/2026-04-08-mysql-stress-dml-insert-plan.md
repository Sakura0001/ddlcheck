# MySQL Stress DML Insert Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace MySQL stress mode's current no-op DML generation with real row-producing INSERT/REPLACE/UPDATE/DELETE behavior so stress runs leave observable table data and still avoid writing generated columns.

**Architecture:** Keep stress mode lightweight instead of routing through the full grammar-based DML generator, but generate real writable-column literals from schema metadata and bias empty tables toward INSERT/REPLACE. Validate the fix with a live MySQL stress run that leaves `TABLE_ROWS > 0` and logs at least one insert-like DML statement.

**Tech Stack:** Java 17, JUnit 5, MySQL 8.0.45, `MySQLStressOracle`, local MySQL CLI

**Observed Baseline (2026-04-08):**
- `src/main/java/dbradar/mysql/oracle/MySQLStressOracle.java:212-230` only emits:
  - `DELETE ... WHERE 1 = 0`
  - `UPDATE ... SET col = col WHERE 1 = 0`
- Local reproduction command:

```bash
java -cp 'build/classes:build/test-classes:src/main/resources:libs/*' dbradar.Main \
  --username root --password Taurus_123 --host 127.0.0.1 --port 3306 \
  --num-threads 1 --timeout-seconds 20 --log-each-select true --log-execution-time false \
  --print-progress-information false --database-prefix codexdmlrepro_ --use-connection-test true \
  mysql --oracle STRESS --stress-threads-per-db 1 --stress-rounds-per-db 1 \
  --stress-ddl-per-thread 5 --stress-dml-per-thread 20 --stress-query-per-thread 0
```

- Result after that run:
  - log file `logs/mysql/codexdmlrepro_0-cur.log` contains only no-op UPDATE/DELETE DML
  - `information_schema.TABLES` reports `t0:0`, `t1:0`, `t2:0`
- Equation mode already logs real `INSERT` / `REPLACE`, so the bug is isolated to stress mode, not the generic MySQL DML provider.

---

### Task 1: Add Failing Tests That Describe The Correct Stress-DML Semantics

**Files:**
- Create: `src/test/java/dbradar/mysql/oracle/TestMySQLStressDML.java`
- Create: `src/test/java/dbradar/mysql/oracle/TestMySQLStressDMLIntegration.java`

- [ ] **Step 1: Add a generation-level red test**

Create a focused unit test that repeatedly invokes the stress DML generator and asserts it can emit at least one `INSERT` or `REPLACE` when a table has writable columns.

Suggested expectation:

```java
assertTrue(generatedSql.stream().anyMatch(sql -> sql.startsWith("INSERT ") || sql.startsWith("REPLACE ")),
        "stress DML should be able to insert rows instead of only no-op UPDATE/DELETE");
```

Use reflection if the production method remains private, or make a small package-private helper specifically for testability.

- [ ] **Step 2: Add a live-MySQL red test for row creation**

Add or extend an integration test that runs one tiny stress round against the local MySQL instance, then queries row counts from the created database and asserts at least one base table has rows.

Suggested assertion:

```java
assertTrue(rowCounts.values().stream().anyMatch(count -> count > 0),
        "stress DML should leave at least one table with inserted data");
```

- [ ] **Step 3: Add a log-level red assertion**

Check the generated stress log for `INSERT` or `REPLACE` so the regression is visible even before querying row counts.

Suggested expectation:

```java
assertTrue(Files.lines(logPath).anyMatch(line -> line.contains("| INSERT ") || line.contains("| REPLACE ")),
        "stress log should record insert-like DML after the fix");
```

- [ ] **Step 4: Compile and run the new tests to confirm red state**

Run:

```bash
mkdir -p build/classes build/test-classes
javac -proc:none -cp 'libs/*:src/main/resources' -d build/classes $(find src/main/java -name '*.java')
javac -proc:none -cp 'libs/*:src/main/resources:build/classes' -d build/test-classes $(find src/test/java -name '*.java')
java -cp 'build/classes:build/test-classes:src/main/resources:libs/*' TestReflectionRunner \
  dbradar.mysql.oracle.TestMySQLStressDML \
  dbradar.mysql.oracle.TestMySQLStressDMLIntegration
```

Expected: at least one new assertion fails before implementation.

### Task 2: Extract Reusable Stress-Safe Literal Generation

**Files:**
- Create: `src/main/java/dbradar/mysql/oracle/MySQLStressValueHelper.java`
- Modify: `src/main/java/dbradar/mysql/MySQLKeyFuncManager.java`

- [ ] **Step 1: Move the literal-generation logic into a shared helper**

`MySQLKeyFuncManager.generateStressSafeValue(...)` already contains most of the type-specific literal rules stress mode needs, but it is private and trapped inside the grammar key-function class. Extract that logic into a helper that both the key-function layer and the stress oracle can call.

Suggested API:

```java
public final class MySQLStressValueHelper {
    public static String generateValue(MySQLSchema.MySQLColumn column, MySQLGlobalState state) { ... }
}
```

- [ ] **Step 2: Preserve generated-column and nullability safety**

The helper should:
- never target generated columns
- avoid returning `null` for `NOT NULL` columns
- keep existing safe literals for numeric, temporal, JSON, binary, enum/set, and text types

- [ ] **Step 3: Repoint `MySQLKeyFuncManager` to the new helper**

Update the existing stress-specific key functions so the extraction does not duplicate literal rules or create drift between grammar DML and stress DML.

### Task 3: Replace No-Op Stress DML With Real Row-Producing Statements

**Files:**
- Modify: `src/main/java/dbradar/mysql/oracle/MySQLStressOracle.java`

- [ ] **Step 1: Make empty tables prefer INSERT/REPLACE**

Replace the current `generateStressDML(...)` branch with logic that checks table emptiness and biases empty tables toward row creation.

Suggested high-level shape:

```java
if (writableColumns.isEmpty()) {
    return null;
}
if (table.getNrRows(state) == 0 || Randomly.getBooleanWithRatherHighProbability()) {
    return buildInsertOrReplace(table, writableColumns, state);
}
return Randomly.getBoolean() ? buildUpdate(table, writableColumns, state) : buildDelete(table, state);
```

- [ ] **Step 2: Build actual INSERT / REPLACE SQL from writable columns**

Generate statements like:

```sql
INSERT IGNORE INTO t0(c1, c2, c5) VALUES (1, 'a', '2000-01-01');
REPLACE INTO t0(c1, c2, c5) VALUES (42, 'b', '2024-12-31');
```

Rules:
- use only `!col.isGenerated()` columns
- allow inserting into all writable columns when possible
- if a table has awkward constraints, `INSERT IGNORE` / `REPLACE` is acceptable for stress mode

- [ ] **Step 3: Keep UPDATE / DELETE meaningful instead of no-op**

When row-producing DML has already populated a table, allow:

```sql
UPDATE IGNORE t0 SET c2 = 'test' LIMIT 1;
DELETE LOW_PRIORITY IGNORE FROM t0 LIMIT 1;
```

or another MySQL-safe equivalent that can touch real rows without requiring complex predicates.

- [ ] **Step 4: Preserve retry and schema-refresh behavior**

Do not change:
- retry policy in `executeWithRetries(...)`
- schema refresh cadence in `executeBatch(...)`
- `ensureAtLeastOneTable(...)` bootstrap behavior

This task is only about replacing the DML generator, not reworking the stress execution model.

### Task 4: Verify The Fix With Local MySQL

**Files:**
- No code changes in this task

- [ ] **Step 1: Recompile everything**

Run:

```bash
mkdir -p build/classes build/test-classes
javac -proc:none -cp 'libs/*:src/main/resources' -d build/classes $(find src/main/java -name '*.java')
javac -proc:none -cp 'libs/*:src/main/resources:build/classes' -d build/test-classes $(find src/test/java -name '*.java')
```

Expected: compilation succeeds.

- [ ] **Step 2: Run targeted automated tests**

Run:

```bash
java -cp 'build/classes:build/test-classes:src/main/resources:libs/*' TestReflectionRunner \
  dbradar.mysql.oracle.TestMySQLStressDML \
  dbradar.mysql.oracle.TestMySQLStressDMLIntegration
```

Expected: targeted stress-DML tests pass.

- [ ] **Step 3: Re-run the live stress reproduction and verify rows are inserted**

Run:

```bash
java -cp 'build/classes:build/test-classes:src/main/resources:libs/*' dbradar.Main \
  --username root --password Taurus_123 --host 127.0.0.1 --port 3306 \
  --num-threads 1 --timeout-seconds 20 --log-each-select true --log-execution-time false \
  --print-progress-information false --database-prefix codexdmlfix_ --use-connection-test true \
  mysql --oracle STRESS --stress-threads-per-db 1 --stress-rounds-per-db 1 \
  --stress-ddl-per-thread 5 --stress-dml-per-thread 20 --stress-query-per-thread 0

mysql -uroot -pTaurus_123 -h127.0.0.1 -P3306 -N -e "
SELECT CONCAT(TABLE_NAME, ':', TABLE_ROWS)
FROM information_schema.TABLES
WHERE TABLE_SCHEMA='codexdmlfix_0' AND TABLE_TYPE='BASE TABLE'
ORDER BY TABLE_NAME;
"

rg -n '\\| INSERT |\\| REPLACE ' logs/mysql/codexdmlfix_0-cur.log
```

Expected:
- at least one `INSERT` or `REPLACE` line is present in the stress log
- at least one base table reports `TABLE_ROWS > 0`

- [ ] **Step 4: Run the existing MySQL stress integration tests**

Run:

```bash
java -cp 'build/classes:build/test-classes:src/main/resources:libs/*' TestReflectionRunner \
  dbradar.mysql.oracle.TestMySQLStressOracleConfig \
  dbradar.mysql.oracle.TestMySQLStressDML \
  dbradar.mysql.oracle.TestMySQLStressDMLIntegration
```

Expected: the focused MySQL stress tests still pass after the DML change, without relying on unrelated long-running equation tests.
