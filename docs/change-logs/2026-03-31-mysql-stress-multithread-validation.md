# MySQL Stress Round-Based Refactor Change Log

## Background

The previous MySQL stress path mixed generic framework controls with stress-specific behavior:

- `--num-threads` controlled only concurrency, not the number of databases under stress
- `--num-tries`, `--num-queries`, and `--max-generated-databases` indirectly shaped stress execution
- single-thread and multi-thread tests did not map cleanly to the intended crash-finding workflow
- stress-specific parameters included non-essential tuning knobs that were not part of the desired user-facing model

The target behavior was clarified as:

- single-thread mode: each database has one worker and repeats a fixed number of rounds
- multi-thread mode: each database has `k` workers sharing the same database and repeating the same round model
- each round recreates the database
- each worker executes configured counts of DDL, DML, and random query statements
- schema metadata is refreshed before each DDL and DML, but not before each query

## Changes Made

### 1. Reduced the MySQL stress parameter surface

File:

- `src/main/java/dbradar/mysql/MySQLOptions.java`

Kept the minimal stress controls:

- `--stress-threads-per-db`
- `--stress-rounds-per-db`
- `--stress-ddl-per-thread`
- `--stress-dml-per-thread`
- `--stress-query-per-thread`

Removed the stress-specific knobs that were no longer part of the intended API:

- `--stress-enable`
- `--stress-schema-refresh-interval`
- `--stress-log-each-sql`
- `--stress-warn-error-code`

Behavioral simplification:

- `useStress()` now depends only on `--oracle STRESS`
- stress logging now follows the existing global `--log-each-select` behavior

### 2. Made MySQL stress round-based per database

Files:

- `src/main/java/dbradar/Main.java`
- `src/main/java/dbradar/ProviderAdapter.java`

Changes:

- when MySQL runs in stress mode, the number of top-level database tasks now follows `--num-threads`
- stress mode no longer relies on `--num-tries` to decide how many database tasks exist
- stress mode no longer relies on `--max-generated-databases` to repeat rounds
- `ProviderAdapter` now runs stress internally as `stress-rounds-per-db` rounds for the same database name
- each round recreates the database before the next round starts

Result:

- one top-level database task owns one database name
- that task repeats the configured number of rounds
- single-thread vs multi-thread behavior is determined only by `stress-threads-per-db`

### 3. Changed schema refresh semantics to match the requested workflow

File:

- `src/main/java/dbradar/mysql/oracle/MySQLStressOracle.java`

Changes:

- removed the interval-based `SchemaRefreshController`
- refresh schema before each DDL statement
- refresh schema before each DML statement
- do not force a schema refresh before each random query

This now matches the requested consistency rule:

- DDL and DML see fresh metadata
- query execution can race naturally with prior schema changes

### 4. Preserved reconnect-and-continue behavior

File:

- `src/main/java/dbradar/mysql/oracle/MySQLStressOracle.java`

The stress worker behavior still:

- logs worker failures
- reconnects once when possible
- continues stress execution instead of treating ordinary worker failure as a global test failure

This matches the clarified requirement that crash / disconnect detection will be handled externally rather than by turning stress mode into a fail-fast test harness.

### 5. Kept the FK metadata race tolerance fix

File:

- `src/main/java/dbradar/mysql/schema/MySQLSchema.java`

The previous fix remains:

- transient foreign-key metadata rows with missing referenced tables or columns are skipped during schema refresh

This prevents the stress harness from crashing on temporary `INFORMATION_SCHEMA` inconsistencies during concurrent DDL.

### 6. Rewrote stress tests around the minimal parameter set

Files:

- `src/test/java/dbradar/ddlCheck/TestMySQLEDCOracle.java`
- `src/test/java/dbradar/mysql/oracle/TestMySQLStressOracleConfig.java`

Changes:

- single-thread stress test now uses only the minimal stress parameters and verifies repeated rounds on one database task
- multi-thread stress test now uses only the minimal stress parameters and verifies multiple worker threads on one shared database
- removed the old stress test that validated `--num-tries <= 0` for stress mode, because stress now no longer depends on that parameter
- config test now validates the new `stress-rounds-per-db` default instead of the removed schema-refresh interval

## Verification

### Compilation

Executed:

```bash
mkdir -p build/classes build/test-classes
javac -proc:none -cp 'libs/*:src/main/resources' -d build/classes $(find src/main/java -name '*.java')
javac -proc:none -cp 'libs/*:src/main/resources:build/classes' -d build/test-classes $(find src/test/java -name '*.java')
```

Result:

- main code compiled successfully
- test code compiled successfully

### Targeted verification

Executed via `jshell` because the repository still does not include a local `junit-platform-console-standalone.jar`.

1. Configuration and regression tests:

```java
import dbradar.mysql.oracle.TestMySQLStressOracleConfig;
import dbradar.mysql.TestMySQLStressGenerationRegression;
var cfg = new TestMySQLStressOracleConfig();
cfg.testStressDefaultsAreSafe();
cfg.testDeterministicSqlErrorsAreNotRetried();
var reg = new TestMySQLStressGenerationRegression();
reg.testFetchForeignKeysSkipsTransientMetadataRowsWithMissingReferencedTable();
System.out.println("CONFIG_AND_REGRESSION_OK");
```

Observed result:

- `CONFIG_AND_REGRESSION_OK`

2. Single-thread round-based stress test:

```java
import dbradar.ddlCheck.TestMySQLEDCOracle;
var t = new TestMySQLEDCOracle();
t.testMySQLStressOracle();
System.out.println("SINGLE_STRESS_OK");
```

Observed result:

- `SINGLE_STRESS_OK`

3. Multi-thread same-database round-based stress test:

```java
import dbradar.ddlCheck.TestMySQLEDCOracle;
var t = new TestMySQLEDCOracle();
t.testMySQLStressMultiThread();
System.out.println("MULTI_STRESS_OK");
```

Observed result:

- `MULTI_STRESS_OK`

## Files Changed

- `src/main/java/dbradar/Main.java`
- `src/main/java/dbradar/ProviderAdapter.java`
- `src/main/java/dbradar/mysql/MySQLOptions.java`
- `src/main/java/dbradar/mysql/oracle/MySQLStressOracle.java`
- `src/main/java/dbradar/mysql/schema/MySQLSchema.java`
- `src/test/java/dbradar/ddlCheck/TestMySQLEDCOracle.java`
- `src/test/java/dbradar/mysql/TestMySQLStressGenerationRegression.java`
- `src/test/java/dbradar/mysql/oracle/TestMySQLStressOracleConfig.java`
- `docs/superpowers/specs/2026-03-31-mysql-stress-round-based-design.md`
- `docs/superpowers/plans/2026-03-31-mysql-stress-round-based-plan.md`
- `docs/change-logs/2026-03-31-mysql-stress-multithread-validation.md`
