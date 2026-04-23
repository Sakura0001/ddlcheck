# Executable fat jar packaging

Date: 2026-04-23

## Scope

- Extended Maven packaging so `/opt/homebrew/bin/mvn clean package -DskipTests` produces:
  - `target/ddlcheck-pg-1.0-SNAPSHOT.jar`
  - `target/ddlcheck-pg-1.0-SNAPSHOT-all.jar`
- Set the runnable jar entrypoint to `dbradar.Main`.
- Kept the existing `libs/` repository model and unpacked those jars into the packaged artifact before shading.
- Added source-level classpath/file resource loading support so the fat jar can read:
  - `dbradar/postgresql/postgresql.grammar.yy`
  - `dbradar/postgresql/postgresql.zz.lua`
  - default Lua config files
- Added a focused regression test for reading text resources from both a temporary jar and a filesystem path.
- Updated `readme.md` with the exact build and run commands.
- Added `target/` to `.gitignore`.

## Red to green

- Red 1:
  - `src/test/java/dbradar/ResourceUtilsClasspathTest.java` was added first.
  - Initial compile failed because `ResourceUtils` did not exist.
- Red 2:
  - An early fat-jar smoke run failed with:
    `java.lang.RuntimeException: GlobalState: Fail to parse grammar`
  - Root cause: runtime resource loading used file-path APIs that do not work for `jar:file:...!/resource` URLs.
- Green:
  - Added `dbradar.ResourceUtils`.
  - Switched grammar and Lua config loading to classpath/file-safe reads.
  - Rebuilt the fat jar and re-ran the executable smoke successfully.

## Verification

- Resource-loading regression:
  - `mkdir -p out/resource-utils-green && javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/resource-utils-green $(find src/main/java -name '*.java') src/test/java/dbradar/ResourceUtilsClasspathTest.java && java -cp 'out/resource-utils-green:src/main/resources:libs/*' dbradar.ResourceUtilsClasspathTest`
  - Result: exit code `0`
- Fat jar build:
  - `/opt/homebrew/bin/mvn clean package -DskipTests`
  - Result: `BUILD SUCCESS`
- Help path:
  - `java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar --help`
  - Result: usage text printed; exit code `255`
  - Note: `255` is the current CLI help/error-path behavior of `dbradar.Main`, not a packaging failure
- Local PostgreSQL smoke:
  - `java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar --num-threads 1 --num-tries 1 --num-queries 20 --max-generated-databases 1 --ddl-count 4 --dml-count 3 --host 127.0.0.1 --port 5432 --username postgres --password Taurus_123 postgresql --oracle equation`
  - Result: exit code `0`

## Thread log review

- Log inspected:
  - `logs/postgresql/database0-cur.log`
- Observed bootstrap DDL:
  - `CREATE TABLE t0 ... GENERATED ALWAYS AS ... STORED`
  - `CREATE TABLE typecov_builtin ...`
  - `CREATE TABLE typecov_user ...`
- Observed semi-state block:
  - `==== Start SemiState ====;`
  - `==== End SemiState ====;`
- Observed bootstrap DML:
  - `INSERT INTO t0 ...`
  - `INSERT INTO typecov_builtin ...`
  - `INSERT INTO typecov_user ...`
- Observed subsequent workload:
  - multiple `SELECT ...`
  - later `INSERT INTO typecov_builtin ...`
  - later `UPDATE ONLY typecov_user ...`

## Delivered commands

- Build:
  - `/opt/homebrew/bin/mvn clean package -DskipTests`
- Run help:
  - `java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar --help`
- Run PostgreSQL smoke:
  - `java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar --num-threads 1 --num-tries 1 --num-queries 20 --max-generated-databases 1 --ddl-count 4 --dml-count 3 --host 127.0.0.1 --port 5432 --username postgres --password Taurus_123 postgresql --oracle equation`
