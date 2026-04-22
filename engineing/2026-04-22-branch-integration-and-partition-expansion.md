# Branch integration and partition expansion

Date: 2026-04-22

## Scope

- Consolidated the active PostgreSQL work branches into `codex/pg-branch-integration`.
- Merged `codex/pg-stress-expansion` as a real code merge.
- Merged `codex/pg16-ddl-partition-live` with `-s ours --allow-unrelated-histories` after confirming the surviving line already contained the partition fixes and replay repairs.
- `codex/pg16-ddl-partition` was already reachable from the integrated history and did not require an extra merge commit.

## Partition coverage added

- Extended partition strategy support from `RANGE` only to:
  - `RANGE`
  - `LIST`
  - `HASH`
- Added multi-column partition keys for:
  - `RANGE (partition_key1, partition_key2)`
  - `HASH (partition_key1, partition_key2)`
- Kept `LIST` single-column only because PostgreSQL 16 does not support multi-column list partitioning.
- Expanded partition child generation to cover:
  - `DEFAULT`
  - `FOR VALUES FROM (...) TO (...)`
  - `FOR VALUES IN (...)`
  - `FOR VALUES WITH (MODULUS ..., REMAINDER ...)`

## Safety controls

- Added partition metadata parsing so the schema model now understands:
  - strategy
  - key arity
  - key column names
  - default partitions
  - hash remainder coverage
- Added a partition-aware insert target selector:
  - normal tables are still allowed
  - partition parents are insert targets only when rows can be routed safely
  - leaf partitions are excluded from generic random inserts
- Added partition-aware key synthesis for parent-table inserts:
  - `RANGE` inserts can target an explicit partition or the default partition
  - `LIST` inserts can target an explicit value set or the default partition
  - `HASH` inserts are allowed only after full remainder coverage exists
- Kept random `UPDATE` off partition tables to avoid broad failures from partition-key rewrites.

## Validation

- Compiled main and test sources with:
  `javac -proc:none -encoding UTF-8 -cp "libs/*:src/main/resources" -d build/test-classes $(find src/main/java src/test/java -name '*.java')`
- Ran local PostgreSQL validation against `127.0.0.1:5432`, user `postgres`, password `Taurus_123`:
  - `dbradar.ddlCheck.PostgreSQLPartitionRegressionTest`
  - `dbradar.ddlCheck.PostgreSQLPartitionWorkload`
  - `dbradar.MainOptionsTask1Test`
  - `dbradar.PostgreSQLOnlyProjectTest`
  - `dbradar.PostgreSQLEquationBootstrapCountTest`
  - `dbradar.PostgreSQLStressOptionsTest`
  - `dbradar.PostgreSQLStressSmokeTest`
  - `dbradar.PostgreSQLCreateTableWidthTest`
  - `dbradar.PostgreSQLWideTableSmokeTest`
  - `dbradar.PostgreSQLGeneratedColumnSmokeTest`
  - `dbradar.PostgreSQLSchemaTypeCoverageTest`
  - `dbradar.PostgreSQLTypeCoverageSmokeTest`
  - `dbradar.Main --num-threads 2 --num-tries 24 --num-queries 20 --max-generated-databases 1 --random-seed 989898 --host 127.0.0.1 --port 5432 --username postgres --password Taurus_123 postgresql --oracle equation`
- Final random run finished successfully with:
  - `Executed 400 queries`
  - `successful statements: 78%`

## Log review

- General random thread logs now naturally include:
  - `PARTITION BY LIST`
  - `PARTITION BY HASH`
  - `PARTITION BY RANGE (partition_key1, partition_key2)`
  - `FOR VALUES IN (...)`
  - `FOR VALUES WITH (MODULUS ..., REMAINDER ...)`
  - `FOR VALUES FROM (...) TO (...)`
- Verified explicit parent inserts in the general workload:
  - `logs/postgresql/database11-cur.log` contains `INSERT INTO t2 (partition_key1,partition_key2) VALUES (50,50);`
  - `logs/postgresql/database21-cur.log` contains `INSERT INTO t2 (partition_key1,c1,c2) ...`
- Dedicated partition workload logs:
  - `logs/postgresql/partition_workload_0-cur.log`
  - `logs/postgresql/partition_workload_1-cur.log`
  both show:
  - generated detach/attach cycles
  - direct `LIST` insert verification
  - direct `HASH` insert verification with full remainder coverage
  - direct multi-column `RANGE` insert verification
  - no `-- ERROR` markers

## Deferred items

- Expression partition keys
- `ATTACH PARTITION ... FOR VALUES ...`
- mixed-modulus hash partition trees
- `LIST` multi-column keys
- random updates that modify partition keys

These were left out on purpose because they significantly increase invalid-statement risk and would likely reduce the main random success rate.
