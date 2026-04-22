# Stress Grouped Topology Design

Date: 2026-04-23

## Problem

The current PostgreSQL `stress` mode supports only two extreme mappings:

- `isolated`: one thread exercises one database
- `shared`: all threads exercise the same database in each round

This is too coarse for the intended crash-finding workload. The target workload is a grouped model where every `N` threads exercise one database. For example, `120` threads with `N=4` should exercise `30` databases concurrently.

## Goals

- Add a grouped stress concurrency model for PostgreSQL.
- Keep existing `isolated` and `shared` invocations working without breaking old commands.
- Express the new model with one parameter instead of introducing another topology value.
- Preserve the existing shared-bootstrap guarantees inside each database group.
- Keep per-thread logs and unique object names so concurrent workers in the same database remain distinguishable.

## Non-Goals

- Changing non-stress execution.
- Changing equation-mode bootstrap behavior.
- Reworking PostgreSQL shared bootstrap tracking away from the current database-name keyed model.

## User-Facing CLI

Add a PostgreSQL-specific option:

- `--stress-threads-per-db`

Semantics:

- `1`: one thread per database
- `N > 1`: every `N` threads share one database
- `N >= numThreads`: all stress threads share one database

Compatibility behavior:

- If `--stress-threads-per-db` is explicitly set, it takes precedence.
- If it is not set:
  - `--stress-topology isolated` maps to `1`
  - `--stress-topology shared` maps to `numThreads`

This keeps old commands stable while making grouped stress the underlying execution model.

## Scheduling Model

Stress scheduling becomes a grouped model for both existing topologies and the new middle ground.

For each worker thread:

- `threadsPerDb = normalize(configuredValue, numThreads)`
- `groupIndex = workerIndex / threadsPerDb`
- `groupLeader = groupIndex * threadsPerDb`
- `databaseName = databasePrefix + round + "_g" + groupIndex`

Rules:

- Only the group leader prepares the database for that group and round.
- All workers in the same group wait on a group-local prepare barrier before running.
- All workers in the same group wait on a group-local finish barrier before advancing to the next round.
- Failures are tracked per group so one group does not stall unrelated groups.

## Database and Bootstrap Behavior

No structural change is required in PostgreSQL shared bootstrap tracking:

- `PostgreSQLGlobalState` already initializes shared stress databases once per `databaseName`.
- `PostgreSQLStressOracle` already bootstraps shared stress state once per `databaseName`.

Because grouped stress assigns a distinct `databaseName` per `(round, groupIndex)`, the existing database-name keyed locking and bootstrap tracking naturally become per-group.

## Logging and Object Naming

Keep per-thread log naming:

- each worker continues to log to its own `...-thread{workerIndex}` files

Keep object-name isolation by preserving the current global-thread prefix:

- generated object prefix remains `thr{workerIndex}_`

This avoids collisions when multiple threads operate on the same grouped database.

## Implementation Outline

### Option parsing

- Add `--stress-threads-per-db` to `PostgreSQLOptions`.
- Add a resolver method that returns the effective grouped value from:
  - explicit `--stress-threads-per-db`
  - otherwise `isolated -> 1`
  - otherwise `shared -> numThreads`

### Main stress scheduling

- Replace the current shared-only scheduler with a grouped scheduler.
- Reuse the existing isolated scheduler only when the effective grouped value is `1`, or unify both through one grouped path if that keeps the code simpler.
- Build group-local synchronization state keyed by `groupIndex`.
- Use group leaders instead of global worker `0` for `prepareDatabase()`.

### Validation utilities

- Keep the current `buildSharedObjectPrefix(workerIndex)` behavior.
- Introduce helper methods as needed for:
  - normalizing `threadsPerDb`
  - computing `groupIndex`
  - computing grouped `databaseName`
  - obtaining group synchronization state

## Test Plan

Add or extend the following tests:

- `PostgreSQLStressOptionsTest`
  - parse `--stress-threads-per-db`
  - verify fallback mapping:
    - `isolated -> 1`
    - `shared -> numThreads`
  - verify clamping and normalization rules

- `PostgreSQLStressSmokeTest`
  - grouped case: `numThreads=4`, `stressThreadsPerDb=2`
  - verify two grouped databases are created
  - verify four per-thread logs exist
  - verify each thread log still contains DDL, DML, and DQL

- One grouped shared-bootstrap regression
  - verify each grouped database bootstraps once
  - verify group-local bootstrap artifacts exist in each grouped database
  - an existing generated-column or type-coverage smoke can be extended for this

## Risks

- Reusing the old shared global barriers would incorrectly synchronize unrelated groups. The implementation must move to group-local barriers and group-local failure state.
- Database naming must remain stable and collision-free across groups and rounds, otherwise shared bootstrap tracking will merge unrelated groups.
- Group size normalization must be explicit so invalid values such as `0` or negative numbers do not create silent edge-case behavior.

## Verification Requirements

Implementation is complete only after all of the following are done:

- compile main and test sources
- run PostgreSQL stress option tests
- run grouped stress smoke tests against the local PostgreSQL instance
- inspect thread logs for grouped runs
- write a short `/engineing` note for the change
- commit and push the validated change to GitHub `main`
