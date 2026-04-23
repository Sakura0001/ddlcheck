# Execution Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a parameter-controlled global execution log for one run and simplify stress thread log names to `databasePrefix + threadId`.

**Architecture:** Add `--log-global-execution` in global options, route global log writes through `StateLogger`, and intercept SQL at the `SQLConnection` statement boundary so all executed SQL is captured. Keep PostgreSQL create/drop database SQL covered with explicit writes in `PostgreSQLGlobalState`.

**Tech Stack:** Java, JDBC `Statement`/`PreparedStatement` wrappers, PostgreSQL local smoke tests, existing logging infrastructure

---

### Task 1: Add the execution-log option and logger sink

**Files:**
- Modify: `src/main/java/dbradar/MainOptions.java`
- Modify: `src/main/java/dbradar/StateLogger.java`
- Test: `src/test/java/dbradar/PostgreSQLStressOptionsTest.java`

- [ ] **Step 1: Write the failing option test**

```java
private static void verifiesGlobalExecutionLoggingParsing() {
    MainOptions options = parseMainOptions("--log-global-execution", "true");
    require(options.logGlobalExecution(),
            "Expected --log-global-execution true to enable global execution logging");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-log-task1 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-log-task1:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest`

Expected: FAIL because `logGlobalExecution()` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```java
@Parameter(names = "--log-global-execution",
        description = "Logs every executed SQL statement for the current run into one global file", arity = 1)
private boolean logGlobalExecution = false;

public boolean logGlobalExecution() {
    return logGlobalExecution;
}
```

```java
public static synchronized void initializeGlobalExecutionLog(DatabaseProvider provider, MainOptions options) { ... }
public static synchronized void logGlobalExecutionEvent(... ) { ... }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-log-task1 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-log-task1:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/dbradar/MainOptions.java src/main/java/dbradar/StateLogger.java src/test/java/dbradar/PostgreSQLStressOptionsTest.java
git commit -m "feat: add global execution logging option"
```

### Task 2: Capture all SQL at the JDBC execution boundary

**Files:**
- Modify: `src/main/java/dbradar/SQLConnection.java`
- Modify: `src/main/java/dbradar/DBMSExecutor.java`
- Modify: `src/main/java/dbradar/GlobalState.java`
- Modify: `src/main/java/dbradar/postgresql/PostgreSQLGlobalState.java`

- [ ] **Step 1: Write the failing global-log smoke expectation**

```java
Path globalLog = Path.of("logs", "postgresql", "global-execution.log");
require(Files.exists(globalLog), "Expected global execution log to be created when enabled");
String content = Files.readString(globalLog);
require(content.contains("thread=1"), "Expected global log to include thread id");
require(content.contains("status=SUCCESS"), "Expected global log to include statement status");
```

- [ ] **Step 2: Run test to verify it fails**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-log-task2 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-log-task2:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`

Expected: FAIL because the global execution log does not exist yet.

- [ ] **Step 3: Write minimal execution-boundary implementation**

```java
public final class SQLConnection implements DatabaseConnection {
    private final Connection connection;
    private final String databaseName;
    private final int threadId;
    private final MainOptions options;
    ...
}
```

```java
public Statement createStatement() throws SQLException {
    return new LoggedStatement(connection.createStatement(), databaseName, threadId, options);
}
```

```java
StateLogger.logGlobalExecutionEvent(threadId, databaseName, sql, success, errorMessage);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-log-task2 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-log-task2:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/dbradar/SQLConnection.java src/main/java/dbradar/DBMSExecutor.java src/main/java/dbradar/GlobalState.java src/main/java/dbradar/postgresql/PostgreSQLGlobalState.java
git commit -m "feat: capture global execution log at jdbc boundary"
```

### Task 3: Simplify thread log names and verify run-level global log behavior

**Files:**
- Modify: `src/main/java/dbradar/Main.java`
- Modify: `src/test/java/dbradar/PostgreSQLStressSmokeTest.java`
- Modify: `src/test/java/dbradar/PostgreSQLGeneratedColumnSmokeTest.java`
- Create: `engineing/2026-04-23-execution-logging.md`

- [ ] **Step 1: Write the failing thread-log naming assertions**

```java
require(Files.exists(Path.of("logs", "postgresql", GROUPED_DATABASE_PREFIX + "1-cur.log")),
        "Expected grouped worker 1 log to use simplified naming");
require(Files.exists(Path.of("logs", "postgresql", GROUPED_DATABASE_PREFIX + "4-cur.log")),
        "Expected grouped worker 4 log to use simplified naming");
```

- [ ] **Step 2: Run test to verify it fails**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-log-task3 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-log-task3:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`

Expected: FAIL because stress thread logs still use database/group-heavy names.

- [ ] **Step 3: Write minimal implementation and documentation**

```java
String logName = options.getDatabasePrefix() + (workerIndex + 1);
```

```md
# Execution logging

Date: 2026-04-23

## Scope

- Added `--log-global-execution`, disabled by default.
- Added a run-level `global-execution.log`.
- Simplified stress worker log names to `databasePrefix + threadId`.
```

- [ ] **Step 4: Run verification to confirm it passes**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-log-task3 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-log-task3:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest && java -cp 'out/plan-log-task3:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest && java -cp 'out/plan-log-task3:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSmokeTest`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/dbradar/Main.java src/test/java/dbradar/PostgreSQLStressSmokeTest.java src/test/java/dbradar/PostgreSQLGeneratedColumnSmokeTest.java engineing/2026-04-23-execution-logging.md
git commit -m "feat: simplify stress logs and add execution log"
```

## Self-Review

- Spec coverage: CLI flag, per-thread log naming, run-level global log, JDBC-boundary interception, and PostgreSQL create/drop coverage are all represented.
- Placeholder scan: no TODO/TBD placeholders remain.
- Type consistency: `logGlobalExecution`, `global-execution.log`, `threadId`, and `databaseName` are named consistently across tasks.
