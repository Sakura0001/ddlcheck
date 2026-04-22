# PG16 partition DDL support

Date: 2026-04-22

## Scope

- Audited current PostgreSQL DDL grammar against PostgreSQL 16 official docs with `gpt-5.4` subagents for `CREATE TABLE`, `ALTER TABLE`, `CREATE INDEX`, `REINDEX`, `TRUNCATE`, and `VIEW`-related DDL.
- Added a safe partition-specific DDL slice instead of widening the generic roots:
  - `CREATE TABLE ... PARTITION BY RANGE (...)`
  - `CREATE TABLE ... PARTITION OF ... DEFAULT`
  - `ALTER TABLE ... DETACH PARTITION ...`
  - `ALTER TABLE ... ATTACH PARTITION ... DEFAULT`
- Extended PostgreSQL schema metadata and key functions so partitioned parents, attached partitions, and detached attach candidates are selectable during random generation.
- Fixed semi-state replay so partitioned tables and partitions are rebuilt as `PARTITION BY` / `PARTITION OF`, not downgraded to plain `INHERITS`.

## Validation

- Compiled main and test sources with `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources'`.
- Ran `dbradar.ddlCheck.PostgreSQLPartitionRegressionTest` against local PostgreSQL 16 at `127.0.0.1:5432`, user `postgres`, password `Taurus_123`.
- Ran random PostgreSQL fuzzing through `dbradar.Main` with 2 concurrent threads and inspected generated logs.
- Ran `dbradar.ddlCheck.PostgreSQLPartitionWorkload` to force random partition-only coverage on 2 threads and verify attach/detach/create roots end-to-end.
- Hardened `PostgreSQLPartitionWorkload` so repeated runs first drop stale `partition_workload_*` databases and propagate worker-thread failures via `Future#get()`, preventing false green runs without thread logs.

## Thread log review

- `logs/postgresql/database1-cur.log`, `database3-cur.log`, `database10-cur.log`, `database11-cur.log`, `database18-cur.log`, `database19-cur.log`, `database22-cur.log`, `database23-cur.log`: the general random workload now emits `CREATE TABLE ... PARTITION BY RANGE (partition_key)` and semi-state replay emits the same shape, so partitioned parent tables are part of normal fuzz coverage.
- `logs/postgresql/partition_workload_0-cur.log`: covered `PARTITION BY RANGE`, `PARTITION OF ... DEFAULT`, `DETACH PARTITION`, and `ATTACH PARTITION ... DEFAULT` on one thread without execution errors.
- `logs/postgresql/partition_workload_1-cur.log`: independently covered the same partition DDL family on a second thread, including repeated detach/reattach cycles and multiple partitioned parents, without execution errors.

## Risk decision

- Did not widen generic `create_table` / `alter_table` with the full PG16 partition grammar, because that would couple partition-specific preconditions to the general workload and sharply increase invalid-statement rates.
- Deferred high-risk PG16 partition syntax such as multi-column keys, expression keys, `LIST` / `HASH`, `FOR VALUES` bound generation, `CONCURRENTLY`, and `FINALIZE` until the schema model and legality filters are rich enough to keep failure rates under control.
