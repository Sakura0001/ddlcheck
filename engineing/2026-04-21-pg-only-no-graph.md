# PG-only and no-graph cleanup

Date: 2026-04-21

## Scope

- Removed non-PostgreSQL providers, schemas, grammar resources, tests, docs, and runtime jars for MySQL, MariaDB, SQLite, TiDB, and CockroachDB.
- Removed reducer, duplicate-detection, SchemaGraph, and JUNG graph dependencies.
- Restricted runtime provider registration and explicit JDBC driver loading to PostgreSQL.
- Simplified PostgreSQL DDL generation so it no longer builds or compares schema graphs before fuzzing.

## Validation

- Compiled main and test sources with `javac -proc:none -encoding UTF-8 -cp 'libs/*'`.
- Ran `dbradar.PostgreSQLOnlyProjectTest` to verify only PostgreSQL artifacts remain and graph artifacts are absent.
- Ran a local PostgreSQL fuzz check with 2 threads against `127.0.0.1:5432`, user `postgres`, password `Taurus_123`.

## Thread log review

- `logs/postgresql/codex_pg_cleanup_0-cur.log`: generated and replayed PostgreSQL-only DDL/DML/DQL for `codex_pg_cleanup_0`; no graph, reducer, duplicate-detection, or non-PostgreSQL provider output appeared.
- `logs/postgresql/codex_pg_cleanup_1-cur.log`: generated and replayed PostgreSQL-only DDL/DML/DQL for `codex_pg_cleanup_1`; no graph, reducer, duplicate-detection, or non-PostgreSQL provider output appeared.
