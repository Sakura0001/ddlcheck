# Stress Grouped Topology Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add PostgreSQL stress grouping so every `N` threads can exercise one database while keeping existing `isolated` and `shared` commands compatible.

**Architecture:** Resolve an effective `stressThreadsPerDb` value inside `PostgreSQLOptions`, then route all PostgreSQL stress scheduling through a grouped mapping in `Main`. Use per-group barriers and failures, while keeping database-name keyed bootstrap and per-thread object prefixes unchanged.

**Tech Stack:** Java, JCommander, PostgreSQL JDBC, local smoke tests driven through `Main.executeMain`

---

### Task 1: Add grouped stress option parsing

**Files:**
- Modify: `src/main/java/dbradar/postgresql/PostgreSQLOptions.java`
- Test: `src/test/java/dbradar/PostgreSQLStressOptionsTest.java`

- [ ] **Step 1: Write the failing option test**

```java
private static void verifiesStressThreadsPerDbParsing() {
    PostgreSQLOptions pgOptions = parseOptions("--oracle", "stress", "--stress-threads-per-db", "4");
    require(pgOptions.getStressThreadsPerDb() == 4,
            "Expected --stress-threads-per-db 4 to be parsed");
}

private static void verifiesEffectiveStressThreadsPerDbFallbacks() {
    PostgreSQLOptions sharedOptions = parseOptions("--oracle", "stress", "--stress-topology", "shared");
    require(sharedOptions.getEffectiveStressThreadsPerDb(8) == 8,
            "Expected shared fallback to use all threads");

    PostgreSQLOptions isolatedOptions = parseOptions("--oracle", "stress", "--stress-topology", "isolated");
    require(isolatedOptions.getEffectiveStressThreadsPerDb(8) == 1,
            "Expected isolated fallback to use one thread per database");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-task1 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-task1:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest`

Expected: FAIL because `getStressThreadsPerDb()` and `getEffectiveStressThreadsPerDb(int)` do not exist yet.

- [ ] **Step 3: Write minimal option implementation**

```java
@Parameter(names = "--stress-threads-per-db",
        description = "Specifies how many PostgreSQL stress threads should share a single database")
public int stressThreadsPerDb = -1;

public int getStressThreadsPerDb() {
    return stressThreadsPerDb;
}

public int getEffectiveStressThreadsPerDb(int concurrentThreads) {
    if (stressThreadsPerDb > 0) {
        return Math.min(stressThreadsPerDb, concurrentThreads);
    }
    if (stressTopology == PostgreSQLStressTopology.SHARED) {
        return concurrentThreads;
    }
    return 1;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-task1 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-task1:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/dbradar/postgresql/PostgreSQLOptions.java src/test/java/dbradar/PostgreSQLStressOptionsTest.java
git commit -m "feat: add grouped stress option parsing"
```

### Task 2: Replace shared-only scheduling with grouped stress scheduling

**Files:**
- Modify: `src/main/java/dbradar/Main.java`

- [ ] **Step 1: Write the failing grouped stress smoke test**

```java
private static void verifiesGroupedStressMode() throws Exception {
    int exitCode = Main.executeMain(
            "--num-threads", "4",
            "--num-queries", "18",
            "--max-generated-databases", "1",
            "--print-progress-information", "false",
            "--database-prefix", GROUPED_DATABASE_PREFIX,
            "--host", HOST,
            "--port", String.valueOf(PORT),
            "--username", USERNAME,
            "--password", PASSWORD,
            "postgresql", "--oracle", "stress", "--stress-threads-per-db", "2");
    require(exitCode == 0, "Expected grouped stress run to succeed");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-task2 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-task2:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`

Expected: FAIL because grouped `--stress-threads-per-db` does not affect scheduling, database naming, or grouped log expectations.

- [ ] **Step 3: Write minimal grouped scheduler implementation**

```java
int threadsPerDb = postgreSQLOptions.getEffectiveStressThreadsPerDb(options.getNumberConcurrentThreads());
if (threadsPerDb == 1) {
    return submitIsolatedStressTasks(...);
}
return submitGroupedStressTasks(options, execService, executorFactory, someOneFails, seqCounterList, threadsPerDb);
```

```java
int groupIndex = workerIndex / threadsPerDb;
int groupLeader = groupIndex * threadsPerDb;
String databaseName = options.getDatabasePrefix() + round + "_g" + groupIndex;
```

```java
private static final class StressThreadGroupRuntime {
    private final CyclicBarrier prepareBarrier;
    private final CyclicBarrier finishBarrier;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-task2 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-task2:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/dbradar/Main.java src/test/java/dbradar/PostgreSQLStressSmokeTest.java
git commit -m "feat: group postgres stress threads by database"
```

### Task 3: Verify grouped bootstrap resources and document the change

**Files:**
- Modify: `src/test/java/dbradar/PostgreSQLGeneratedColumnSmokeTest.java`
- Create: `engineing/2026-04-23-task2-grouped-stress-mode.md`

- [ ] **Step 1: Write the failing grouped bootstrap verification**

```java
int exitCode = Main.executeMain(
        "--num-threads", "4",
        "--num-queries", "12",
        "--max-generated-databases", "1",
        "--database-prefix", STRESS_PREFIX,
        "--ddl-count", supportsVirtualGeneratedColumns ? "5" : "4",
        "--dml-count", supportsVirtualGeneratedColumns ? "4" : "3",
        "--host", HOST,
        "--port", String.valueOf(PORT),
        "--username", USERNAME,
        "--password", PASSWORD,
        "postgresql", "--oracle", "stress", "--stress-threads-per-db", "2");

try (Connection group0 = createConnection(STRESS_PREFIX + "0_g0");
     Connection group1 = createConnection(STRESS_PREFIX + "0_g1")) {
    assertGeneratedColumnState(group0, storedTableNameForGroup0, "s");
    assertGeneratedColumnState(group1, storedTableNameForGroup1, "s");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-task3 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-task3:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSmokeTest`

Expected: FAIL because grouped stress databases `..._g0` and `..._g1` are not produced yet.

- [ ] **Step 3: Write minimal grouped bootstrap verification and engineering note**

```java
Path thread0Log = Path.of("logs", "postgresql", STRESS_PREFIX + "0_g0-thread0-cur.log");
Path thread2Log = Path.of("logs", "postgresql", STRESS_PREFIX + "0_g1-thread2-cur.log");
```

```md
# Task 2: grouped PostgreSQL stress mode

Date: 2026-04-23

## Scope

- Added `--stress-threads-per-db`.
- Mapped PostgreSQL stress execution to one database per thread group.
- Preserved `isolated` and `shared` CLI compatibility by resolving them to grouped sizes.
```

- [ ] **Step 4: Run verification to confirm it passes**

Run: `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/plan-task3 $(find src/main/java -name '*.java') $(find src/test/java -name '*.java') && java -cp 'out/plan-task3:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest && java -cp 'out/plan-task3:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest && java -cp 'out/plan-task3:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSmokeTest`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/test/java/dbradar/PostgreSQLGeneratedColumnSmokeTest.java engineing/2026-04-23-task2-grouped-stress-mode.md
git commit -m "test: cover grouped postgres stress bootstrap"
```

## Self-Review

- Spec coverage: option parsing, grouped scheduling, per-group bootstrap verification, and engineering notes are all covered by Tasks 1-3.
- Placeholder scan: no TBD/TODO markers remain; every task includes concrete files, commands, and code snippets.
- Type consistency: `stressThreadsPerDb` and `getEffectiveStressThreadsPerDb(int)` are named consistently across options, scheduler, and tests.
