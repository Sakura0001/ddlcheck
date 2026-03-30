# MySQL Stress Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix MySQL stress-mode generation and execution behavior so local stress validation reaches at least 80% successful statements.

**Architecture:** Keep the repair local to the MySQL stress path. Patch deterministic generator bugs first, then reduce invalid stress retries, then verify with unit tests and a real MySQL stress run.

**Tech Stack:** Java 17, JUnit 5, MySQL 8.4, JCommander, existing grammar-based SQL generator

---

### Task 1: Add Regression Tests For Deterministic Stress Bugs

**Files:**
- Modify: `src/test/java/dbradar/mysql/oracle/TestMySQLStressOracleConfig.java`
- Modify: `src/test/java/dbradar/mysql/TestMySQLGrammarAndTypeSupport.java`
- Create: `src/test/java/dbradar/mysql/TestMySQLStressGenerationRegression.java`

- [ ] **Step 1: Write failing tests**

Add tests that assert:

- `INSERT ... SELECT` uses matching projected columns instead of `SELECT *`
- MySQL stress duplicate-key update targets are writable columns only
- alias references for `GROUP BY`/`ORDER BY` come from emitted aliases rather than creating fresh names
- stress retry classification does not retry deterministic semantic failures

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
mkdir -p build/test-classes && javac -proc:none -cp 'libs/*:src/main/resources:build/classes' -d build/test-classes $(find src/test/java -name '*.java')
java -jar libs/junit-platform-console-standalone.jar --class-path 'build/classes:build/test-classes:src/main/resources:libs/*' --select-class dbradar.mysql.TestMySQLStressGenerationRegression --select-class dbradar.mysql.oracle.TestMySQLStressOracleConfig --select-class dbradar.mysql.TestMySQLGrammarAndTypeSupport
```

Expected: at least one newly added test fails because current MySQL stress generation still has the known bugs.

### Task 2: Fix MySQL Generator And Stress Execution

**Files:**
- Modify: `src/main/java/dbradar/common/query/generator/QueryContext.java`
- Modify: `src/main/java/dbradar/mysql/MySQLKeyFuncManager.java`
- Modify: `src/main/resources/dbradar/mysql/mysql.grammar.yy`
- Modify: `src/main/java/dbradar/mysql/oracle/MySQLStressOracle.java`

- [ ] **Step 1: Implement stable alias references**

Store emitted select aliases in `QueryContext` and add a MySQL key function that reuses them for `GROUP BY` and `ORDER BY`.

- [ ] **Step 2: Implement valid same-table insert-select generation**

Generate:

```sql
INSERT INTO t (c1, c2) SELECT c1, c2 FROM t
```

using the same non-generated columns for both sides.

- [ ] **Step 3: Implement safe duplicate-key update column selection**

Track insertable columns for the current insert statement and only allow duplicate-key updates against that writable subset.

- [ ] **Step 4: Narrow stress retries to transient MySQL errors**

Keep retries for retryable failures like deadlocks and lock waits. Log deterministic failures once without repeating the same SQL five times.

- [ ] **Step 5: Run targeted tests**

Run:

```bash
mkdir -p build/classes build/test-classes && javac -proc:none -cp 'libs/*:src/main/resources' -d build/classes $(find src/main/java -name '*.java') && javac -proc:none -cp 'libs/*:src/main/resources:build/classes' -d build/test-classes $(find src/test/java -name '*.java')
```

Expected: compilation succeeds.

### Task 3: Validate Against Local MySQL

**Files:**
- No code changes required

- [ ] **Step 1: Run focused stress validation**

Run:

```bash
java -cp 'build/classes:src/main/resources:libs/*' dbradar.Main --username root --password Taurus_123 --host 127.0.0.1 --port 3306 --num-threads 1 --num-tries 1 --num-queries 20 --timeout-seconds 120 --max-generated-databases 1 --log-each-select true --log-execution-time false --print-progress-information false --database-prefix codexstress mysql --oracle STRESS --stress-enable true --stress-threads-per-db 1 --stress-ddl-per-thread 3 --stress-dml-per-thread 10 --stress-query-per-thread 10 --stress-schema-refresh-interval 5 --stress-log-each-sql true
```

- [ ] **Step 2: Compute success ratio from the generated log**

Run:

```bash
python3 - <<'PY'
from collections import Counter
import re
path='logs/mysql/codexstress0-cur.log'
stats=Counter()
with open(path) as f:
    for line in f:
        m=re.search(r'\\[kind=\\w+\\] (SUCCESS|FAIL)', line)
        if m:
            stats[m.group(1)] += 1
total = stats['SUCCESS'] + stats['FAIL']
print({'success': stats['SUCCESS'], 'fail': stats['FAIL'], 'ratio': round(stats['SUCCESS'] * 100 / total, 2)})
PY
```

Expected: ratio is at least `80.0`.

### Task 4: Final Verification

**Files:**
- No code changes required

- [ ] **Step 1: Re-run the focused unit suite**

Run:

```bash
java -jar libs/junit-platform-console-standalone.jar --class-path 'build/classes:build/test-classes:src/main/resources:libs/*' --scan-class-path --include-classname '.*MySQL.*'
```

Expected: MySQL-focused unit tests pass.

- [ ] **Step 2: Capture remaining risks**

Record any still-failing MySQL error classes seen in validation and confirm they no longer prevent the agreed success ratio target.
