# MySQL Stress Round-Based Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make MySQL stress mode round-based per database, with a minimal parameter set and schema refresh only before DDL/DML.

**Architecture:** Keep MySQL stress isolated from the generic `nrQueries` / `maxGeneratedDatabases` loops. Use one top-level database task per `--num-threads`, then let MySQL stress own per-database rounds and per-database worker concurrency internally.

**Tech Stack:** Java 17, JUnit 5, existing `Main` / `ProviderAdapter` / `MySQLStressOracle` execution model

---

### Task 1: Add Failing Tests For The New Stress Semantics

**Files:**
- Modify: `src/test/java/dbradar/mysql/oracle/TestMySQLStressOracleConfig.java`
- Modify: `src/test/java/dbradar/ddlCheck/TestMySQLEDCOracle.java`

- [ ] **Step 1: Write a failing config test for the new minimal stress parameter**

Add a test asserting `MySQLOptions` exposes `stress-rounds-per-db` with a default greater than zero and no longer needs schema-refresh tuning in caller code.

- [ ] **Step 2: Write failing integration expectations for single-database round semantics**

Update stress tests so they describe:
- single-thread mode: one worker per database, repeated rounds
- multi-thread mode: multiple workers on one database, repeated rounds

- [ ] **Step 3: Compile tests to confirm red state**

Run:

```bash
mkdir -p build/classes build/test-classes
javac -proc:none -cp 'libs/*:src/main/resources' -d build/classes $(find src/main/java -name '*.java')
javac -proc:none -cp 'libs/*:src/main/resources:build/classes' -d build/test-classes $(find src/test/java -name '*.java')
```

Expected: compile may fail or tests would fail until new stress API is implemented.

### Task 2: Implement Minimal Stress Parameters

**Files:**
- Modify: `src/main/java/dbradar/mysql/MySQLOptions.java`

- [ ] **Step 1: Add `--stress-rounds-per-db`**

Add a dedicated rounds-per-database option with a positive default.

- [ ] **Step 2: Remove non-essential stress knobs from the public option surface**

Drop or stop using stress-specific schema refresh / warn-code knobs so the stress CLI surface matches the minimal model.

- [ ] **Step 3: Add getters matching the new semantics**

Expose:
- rounds per db
- threads per db
- ddl per thread
- dml per thread
- query per thread

### Task 3: Move MySQL Stress To Round-Based Execution

**Files:**
- Modify: `src/main/java/dbradar/ProviderAdapter.java`
- Modify: `src/main/java/dbradar/mysql/oracle/MySQLStressOracle.java`

- [ ] **Step 1: Stop using generic `nrQueries` loop for MySQL stress**

Make MySQL stress run its own round loop instead of piggybacking on `globalState.getOptions().getNrQueries()`.

- [ ] **Step 2: Implement one database task owning repeated rounds**

For one database task:
- create fresh database state
- run the configured stress round
- close worker connections
- proceed to next round on the same database name

- [ ] **Step 3: Refresh schema before each DDL and DML only**

Adjust worker execution so:
- DDL path updates schema immediately before generation/execution
- DML path updates schema immediately before generation/execution
- QUERY path does not force schema refresh

- [ ] **Step 4: Keep reconnect-and-continue behavior**

Preserve worker reconnect behavior for connection-loss scenarios instead of promoting them to global failure.

### Task 4: Align Tests With The New User-Facing Model

**Files:**
- Modify: `src/test/java/dbradar/ddlCheck/TestMySQLEDCOracle.java`
- Modify: `src/test/java/dbradar/mysql/oracle/TestMySQLStressOracleConfig.java`

- [ ] **Step 1: Rewrite single-thread stress test parameters**

Use only the minimal parameter set and configure:
- one worker per database
- explicit rounds per database
- explicit DDL/DML/QUERY counts

- [ ] **Step 2: Rewrite multi-thread stress test parameters**

Use only the minimal parameter set and configure:
- one database task
- multiple workers per database
- explicit rounds per database

- [ ] **Step 3: Keep assertions focused on semantics**

Validate:
- log file exists
- statements executed
- same database prefix is used for repeated rounds

Do not assert success ratio.

### Task 5: Verification And Documentation

**Files:**
- Modify: `docs/change-logs/2026-03-31-mysql-stress-multithread-validation.md`

- [ ] **Step 1: Recompile everything**

Run:

```bash
mkdir -p build/classes build/test-classes
javac -proc:none -cp 'libs/*:src/main/resources' -d build/classes $(find src/main/java -name '*.java')
javac -proc:none -cp 'libs/*:src/main/resources:build/classes' -d build/test-classes $(find src/test/java -name '*.java')
```

Expected: compilation succeeds.

- [ ] **Step 2: Run targeted verification**

Run:

```bash
jshell --class-path 'build/classes:build/test-classes:src/main/resources:libs/*'
```

Execute:

```java
import dbradar.ddlCheck.TestMySQLEDCOracle;
import dbradar.mysql.oracle.TestMySQLStressOracleConfig;
var cfg = new TestMySQLStressOracleConfig();
var stress = new TestMySQLEDCOracle();
// invoke targeted test methods covering new stress semantics
```

Expected: targeted stress tests complete without assertion failure.

- [ ] **Step 3: Update change log**

Document:
- old semantics
- new round-based semantics
- reduced parameter surface
- verification commands and results
