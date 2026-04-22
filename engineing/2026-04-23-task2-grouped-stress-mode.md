# Task 2: grouped PostgreSQL stress mode

Date: 2026-04-23

## Scope

- Added PostgreSQL `--stress-threads-per-db` and resolved stress execution to a grouped model.
- Kept `--stress-topology=isolated|shared` compatible by mapping them to grouped sizes when the new option is unset.
- Replaced the old global shared barriers with per-group barriers and per-group failure tracking.
- Preserved per-thread log names and `thr{workerIndex}_` generated object prefixes inside grouped databases.
- Extended smoke coverage for `numThreads=4` and `stressThreadsPerDb=2`, including generated-column grouped bootstrap verification across both group databases.

## Implementation notes

- Effective grouped size semantics:
  - `1` behaves like isolated mode
  - `N >= numThreads` behaves like shared mode
  - intermediate `N` values map every `N` worker threads to one database
- Grouped database names now use `databasePrefix + round + "_g" + groupIndex`, which keeps PostgreSQL shared-bootstrap tracking naturally scoped per group because the existing tracking is keyed by database name.
- Each group has its own prepare barrier, finish barrier, and failure reference so unrelated groups no longer block one another.

## Suggested verification

- Compile:
  `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/grouped-stress-check $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')`
- Options test:
  `java -cp 'out/grouped-stress-check:src/main/resources:libs/*' dbradar.PostgreSQLStressOptionsTest`
- Grouped stress smoke:
  `java -cp 'out/grouped-stress-check:src/main/resources:libs/*' dbradar.PostgreSQLStressSmokeTest`
- Generated-column grouped smoke:
  `java -cp 'out/grouped-stress-check:src/main/resources:libs/*' dbradar.PostgreSQLGeneratedColumnSmokeTest`
