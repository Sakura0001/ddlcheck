# MySQL Remove JSON Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove MySQL-specific JSON support so the MySQL test program no longer generates, treats, or validates JSON types or JSON functions.

**Architecture:** Restrict the change to MySQL-only surfaces: grammar, Lua value config, schema type mapping, and MySQL-only oracle/value helper code. Keep other DBMS paths unchanged and verify with focused regression tests that JSON is absent from MySQL generation inputs and outputs.

**Tech Stack:** Java 17, JUnit 5, MySQL grammar/Lua resources

---

### Task 1: Lock Down The Removal Contract With Tests

**Files:**
- Modify: `src/test/java/dbradar/mysql/TestMySQLGrammarAndTypeSupport.java`

- [ ] **Step 1: Write the failing test**

```java
assertTrue(!content.contains("| JSON"), "MySQL grammar should not expose JSON as a type");
assertTrue(!content.contains("json_func:"), "MySQL grammar should not expose JSON functions");
assertEquals(MySQLSchema.MySQLDataType.TEXT, m.invoke(null, "json"));
assertEquals(MySQLSchema.MySQLDataType.CHAR, m.invoke(null, "enum"));
assertEquals(MySQLSchema.MySQLDataType.CHAR, m.invoke(null, "set"));
```

- [ ] **Step 2: Run test to verify it fails**

Run: `jshell --class-path 'build/classes:build/test-classes:libs/*'`
Expected: existing MySQL grammar/type support test fails because JSON is still present and mapped specially.

- [ ] **Step 3: Write minimal implementation**

```text
No production changes in this task.
```

- [ ] **Step 4: Run test to verify it still fails for the expected reason**

Run: `jshell --class-path 'build/classes:build/test-classes:libs/*'`
Expected: failure output points to JSON still existing in grammar/resources/type mapping.

- [ ] **Step 5: Commit**

```bash
git add src/test/java/dbradar/mysql/TestMySQLGrammarAndTypeSupport.java
git commit -m "test: lock down mysql json removal"
```

### Task 2: Remove MySQL JSON Generation Inputs

**Files:**
- Modify: `src/main/resources/dbradar/mysql/mysql.grammar.yy`
- Modify: `src/main/resources/dbradar/mysql/mysql.zz.lua`

- [ ] **Step 1: Write the failing test**

```java
assertTrue(!content.contains("| JSON"), "MySQL grammar should not expose JSON as a type");
assertTrue(!content.contains("json_func:"), "MySQL grammar should not expose JSON functions");
```

- [ ] **Step 2: Run test to verify it fails**

Run: `jshell --class-path 'build/classes:build/test-classes:libs/*'`
Expected: test fails until grammar/Lua JSON entries are removed.

- [ ] **Step 3: Write minimal implementation**

```text
Delete JSON from the MySQL type_name alternatives, constraint disable-regexes, expression grammar hook, json_func production, and Lua JSON value table.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `jshell --class-path 'build/classes:build/test-classes:libs/*'`
Expected: grammar assertions pass and no MySQL JSON generator entries remain.

- [ ] **Step 5: Commit**

```bash
git add src/main/resources/dbradar/mysql/mysql.grammar.yy src/main/resources/dbradar/mysql/mysql.zz.lua
git commit -m "feat: remove mysql json grammar support"
```

### Task 3: Remove MySQL JSON Type Handling In Java

**Files:**
- Modify: `src/main/java/dbradar/mysql/schema/MySQLSchema.java`
- Modify: `src/main/java/dbradar/mysql/oracle/MySQLStressValueHelper.java`
- Modify: `src/main/java/dbradar/mysql/oracle/MySQLEDCOracle.java`
- Modify: `src/test/java/dbradar/mysql/TestMySQLGrammarAndTypeSupport.java`

- [ ] **Step 1: Write the failing test**

```java
assertEquals(MySQLSchema.MySQLDataType.TEXT, m.invoke(null, "json"));
assertEquals(MySQLSchema.MySQLDataType.CHAR, m.invoke(null, "enum"));
assertEquals(MySQLSchema.MySQLDataType.CHAR, m.invoke(null, "set"));
```

- [ ] **Step 2: Run test to verify it fails**

Run: `jshell --class-path 'build/classes:build/test-classes:libs/*'`
Expected: failure because MySQL schema still exposes JSON and maps enum/set through JSON.

- [ ] **Step 3: Write minimal implementation**

```text
Remove MySQLDataType.JSON, remap enum/set to CHAR, let json fall back to TEXT, delete the JSON-specific stress value branch, and remove MySQL EDC expected JSON query error.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `jshell --class-path 'build/classes:build/test-classes:libs/*'`
Expected: MySQL grammar/type support test passes.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/dbradar/mysql/schema/MySQLSchema.java src/main/java/dbradar/mysql/oracle/MySQLStressValueHelper.java src/main/java/dbradar/mysql/oracle/MySQLEDCOracle.java src/test/java/dbradar/mysql/TestMySQLGrammarAndTypeSupport.java
git commit -m "feat: remove mysql json java handling"
```

### Task 4: Verify No MySQL JSON Is Generated

**Files:**
- Modify: `src/test/java/dbradar/mysql/TestMySQLGrammarAndTypeSupport.java`

- [ ] **Step 1: Write the failing test**

```java
assertTrue(!Files.readString(Path.of("src/main/resources/dbradar/mysql/mysql.zz.lua")).contains("JSON ="),
        "MySQL Lua generator config should not expose JSON");
```

- [ ] **Step 2: Run test to verify it fails**

Run: `jshell --class-path 'build/classes:build/test-classes:libs/*'`
Expected: failure until the Lua config is cleaned.

- [ ] **Step 3: Write minimal implementation**

```text
No additional production code if Task 2 already removed the entry; just keep the assertion as regression coverage.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `javac -proc:none -cp 'libs/*' -d build/classes $(find src/main/java -name '*.java') && javac -proc:none -cp 'build/classes:libs/*' -d build/test-classes $(find src/test/java -name '*.java')`
Expected: compilation succeeds.

Run: `jshell --class-path 'build/classes:build/test-classes:libs/*'`
Expected: MySQL grammar/type support tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/test/java/dbradar/mysql/TestMySQLGrammarAndTypeSupport.java
git commit -m "test: verify mysql json is fully removed"
```
