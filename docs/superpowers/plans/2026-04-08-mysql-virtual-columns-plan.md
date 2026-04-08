# MySQL Virtual Column Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the MySQL virtual/generated-column gaps so CREATE TABLE and ALTER TABLE ADD COLUMN can both produce virtual columns, unsupported ALTER paths stop targeting generated columns, and replay/test helpers keep generated columns out of writable INSERT lists.

**Architecture:** Reuse one generated-column grammar fragment across CREATE TABLE and ALTER TABLE ADD COLUMN, then make the MySQL key-function layer and replay helpers generated-column-aware so runtime code never tries to write into generated columns. Validate both grammar coverage and live MySQL 8.0.45 behavior against `information_schema.columns` / `SHOW CREATE TABLE`.

**Tech Stack:** Java 17, JUnit 5, MySQL 8.0.45, `mysql.grammar.yy`, `MySQLSchema`, local MySQL CLI

**Observed Baseline (2026-04-08):**
- `src/main/resources/dbradar/mysql/mysql.grammar.yy` already supports generated columns during `CREATE TABLE` via `generated_column_definition`.
- `alter_table_add_column` at `src/main/resources/dbradar/mysql/mysql.grammar.yy:266-269` only accepts `type_name`, so generated columns cannot be added via ALTER today.
- Local stress logs already show generated-column-specific DDL failures (`Incorrect usage of DEFAULT and generated column`, `Changing the STORED status is not supported for generated columns`), so unsupported ALTER paths still target generated columns.
- `src/test/java/dbradar/ddlCheck/TestMySQLEDCOracle.java:396-469` reconstructs replay INSERTs from `SELECT *`, which will try to insert generated columns once schemas include them.
- `src/test/java/dbradar/ddlCheck/TestMySQLEDCOracle.java` is already dirty in the current workspace; preserve the user's existing stress-parameter edits while changing only the replay helper or moving that helper into a new dedicated test utility.

---

### Task 1: Add Failing Coverage For Virtual Column Grammar And Live Schema Behavior

**Files:**
- Modify: `src/test/java/dbradar/mysql/TestMySQLGrammarAndTypeSupport.java`
- Create: `src/test/java/dbradar/mysql/TestMySQLVirtualColumnCoverage.java`
- Modify: `src/test/java/dbradar/ddlCheck/TestMySQLEDCOracle.java`

- [ ] **Step 1: Extend the grammar assertions first**

Add assertions that the grammar contains generated-column syntax in both `create_table` and `alter_table_add_column`.

Target assertions:

```java
assertTrue(content.contains("generated_column_definition"),
        "generated column rule should exist");
assertTrue(content.contains("ALTER TABLE _table ADD COLUMN? _new_column_name type_name generated_column_definition"),
        "ALTER TABLE ADD COLUMN should support generated columns");
assertTrue(content.contains("ALTER TABLE _table ADD COLUMN? _new_column_name type_name generated_column_definition AFTER _column"),
        "ALTER TABLE ADD COLUMN ... AFTER should support generated columns");
```

- [ ] **Step 2: Add a live-MySQL regression test for CREATE + ALTER generated columns**

Create a dedicated integration test that:
- opens a local MySQL database with `root / Taurus_123`
- creates a base table
- adds one `VIRTUAL` generated column and one `STORED` generated column through `ALTER TABLE`
- refreshes schema with `state.updateSchema()`
- asserts `MySQLSchema.MySQLColumn#isGenerated()` is `true` for both new columns
- asserts `SHOW CREATE TABLE` still contains `GENERATED ALWAYS AS`

Suggested skeleton:

```java
@Test
public void testSchemaMarksAlterAddedGeneratedColumns() throws Exception {
    MySQLGlobalState state = new MySQLGlobalState();
    state.setDatabaseName("codex_virtual_columns");
    state.setConnection(state.createDatabase("127.0.0.1", 3306, "root", "Taurus_123", "codex_virtual_columns"));
    try (Statement s = state.getConnection().createStatement()) {
        s.execute("CREATE TABLE t0 (c1 INT, c2 INT)");
        s.execute("ALTER TABLE t0 ADD COLUMN c3 INT GENERATED ALWAYS AS ((c1 + c2)) VIRTUAL");
        s.execute("ALTER TABLE t0 ADD COLUMN c4 INT GENERATED ALWAYS AS ((c1 * 2)) STORED");
    }
    state.updateSchema();
    // assert c3/c4 are present and generated
}
```

- [ ] **Step 3: Add a red test for replay INSERT column filtering**

Cover the helper at `TestMySQLEDCOracle.fetchInsertStmts(...)` so replay INSERTs exclude generated columns when a table contains them.

Suggested expectation:

```java
assertFalse(insertSql.contains("(c1, c2, c3_generated)"),
        "replay INSERTs must not target generated columns");
assertTrue(insertSql.contains("(c1, c2)"),
        "replay INSERTs should contain only writable base columns");
```

- [ ] **Step 4: Compile and run the new tests to verify red state**

Run:

```bash
mkdir -p build/classes build/test-classes
javac -proc:none -cp 'libs/*:src/main/resources' -d build/classes $(find src/main/java -name '*.java')
javac -proc:none -cp 'libs/*:src/main/resources:build/classes' -d build/test-classes $(find src/test/java -name '*.java')
java -cp 'build/classes:build/test-classes:src/main/resources:libs/*' TestReflectionRunner \
  dbradar.mysql.TestMySQLGrammarAndTypeSupport \
  dbradar.mysql.TestMySQLVirtualColumnCoverage
```

Expected: at least one new assertion fails before implementation.

### Task 2: Add ALTER TABLE ADD COLUMN Generated-Column Grammar Support

**Files:**
- Modify: `src/main/resources/dbradar/mysql/mysql.grammar.yy`

- [ ] **Step 1: Refactor the grammar so CREATE and ALTER reuse the same generated-column fragment**

Replace the hard-coded `type_name`-only `alter_table_add_column` productions with a shared reusable definition such as:

```yy
column_definition:
    _new_column_name type_name column_constraint?
    | _new_column_name type_name generated_column_definition
```

Then wire both `new_column` and `alter_table_add_column` through that rule.

- [ ] **Step 2: Add generated-column variants for positional ALTER ADD**

Ensure all three ALTER forms work:

```yy
ALTER TABLE _table ADD COLUMN? column_definition algorithm?
ALTER TABLE _table ADD COLUMN? column_definition FIRST algorithm?
ALTER TABLE _table ADD COLUMN? column_definition AFTER _column algorithm?
```

- [ ] **Step 3: Keep generated storage optional and MySQL-compatible**

Do not regress the existing create-table forms:
- `GENERATED ALWAYS AS (...)`
- `AS (...)`
- optional `VIRTUAL`
- optional `STORED`

- [ ] **Step 4: Re-run the grammar-only tests**

Run:

```bash
java -cp 'build/classes:build/test-classes:src/main/resources:libs/*' TestReflectionRunner \
  dbradar.mysql.TestMySQLGrammarAndTypeSupport
```

Expected: grammar assertions pass.

### Task 3: Make Generated Columns Non-Writable Across ALTER Helpers And Replay Paths

**Files:**
- Modify: `src/main/resources/dbradar/mysql/mysql.grammar.yy`
- Modify: `src/main/java/dbradar/mysql/MySQLKeyFuncManager.java`
- Modify: `src/main/java/dbradar/common/query/generator/KeyFuncManager.java`
- Modify: `src/test/java/dbradar/ddlCheck/TestMySQLEDCOracle.java`

- [ ] **Step 1: Introduce a MySQL key function for non-generated target columns**

Add a dedicated key such as `_mutable_column` / `_non_generated_column` in `MySQLKeyFuncManager` that selects only columns where `!column.isGenerated()`.

Suggested behavior:

```java
List<AbstractTableColumn<?, ?>> mutableColumns = currentContext.getCurrentColumns().stream()
        .filter(column -> !column.isGenerated())
        .collect(Collectors.toList());
if (mutableColumns.isEmpty()) {
    throw new QueryGenerationException("There are no mutable columns.");
}
```

- [ ] **Step 2: Rewire generated-column-incompatible ALTER statements to use the new key**

Audit and update the MySQL grammar rules that are invalid on generated columns, at minimum:
- `alter_table_alter_column_set_default`
- `alter_table_alter_column_drop_default`
- `alter_table_alter_column_set_visible`
- `alter_table_alter_column_set_invisible`
- any `CHANGE COLUMN` / `MODIFY COLUMN` path that should not randomly target generated columns without a matching generated definition

Use the new non-generated key instead of bare `_column` in these productions.

- [ ] **Step 3: Fix the replay INSERT helper to skip generated columns**

Update `fetchInsertStmts(...)` so it does not blindly serialize `SELECT *`. Build the insert-column list from live schema metadata or `ResultSetMetaData` filtered through `state.getSchema()` and skip generated columns.

The final replay SQL should look like:

```sql
INSERT INTO t0(c1, c2) VALUES (1, 2);
```

not:

```sql
INSERT INTO t0(c1, c2, c3_generated) VALUES (...);
```

- [ ] **Step 4: Remove stale generated-column symbol references if they are dead**

`KeyFuncManager` still checks for `"generated_constraint"`. Update that to the actual grammar symbol name (`generated_column_definition`) or delete the dead branch if the check is no longer meaningful.

### Task 4: Verify Against Local MySQL 8.0.45

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

- [ ] **Step 2: Run targeted automated verification**

Run:

```bash
java -cp 'build/classes:build/test-classes:src/main/resources:libs/*' TestReflectionRunner \
  dbradar.mysql.TestMySQLGrammarAndTypeSupport \
  dbradar.mysql.TestMySQLVirtualColumnCoverage
```

Expected: all targeted tests pass.

- [ ] **Step 3: Run a manual local-MySQL smoke test**

Run:

```bash
mysql -uroot -pTaurus_123 -h127.0.0.1 -P3306 -e "
DROP DATABASE IF EXISTS codex_virtual_smoke;
CREATE DATABASE codex_virtual_smoke;
USE codex_virtual_smoke;
CREATE TABLE t0 (c1 INT, c2 INT, c3 INT GENERATED ALWAYS AS ((c1 + c2)) VIRTUAL);
ALTER TABLE t0 ADD COLUMN c4 INT GENERATED ALWAYS AS ((c1 * 2)) STORED;
SHOW CREATE TABLE t0;
SELECT COLUMN_NAME, EXTRA
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'codex_virtual_smoke' AND TABLE_NAME = 't0'
ORDER BY ORDINAL_POSITION;
INSERT INTO t0(c1, c2) VALUES (1, 2);
SELECT c1, c2, c3, c4 FROM t0;
"
```

Expected:
- `SHOW CREATE TABLE` includes both generated columns
- `EXTRA` shows `VIRTUAL GENERATED` / `STORED GENERATED`
- inserting base columns succeeds
- selecting the row returns computed values for `c3` and `c4`
