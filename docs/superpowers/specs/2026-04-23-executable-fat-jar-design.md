# Executable Fat Jar Design

Date: 2026-04-23

## Goal

Make `mvn clean package -DskipTests` produce a single runnable jar that can be copied to another machine and launched with `java -jar ...`.

## Requirements

- Keep the existing `libs/` directory in the repository.
- Keep the current CLI entrypoint and argument model unchanged.
- The final runnable artifact must include:
  - project classes
  - project resources
  - all runtime classes from `libs/*.jar`
- The jar manifest must set `Main-Class: dbradar.Main`.
- The delivered operator flow must be:

```bash
mvn clean package -DskipTests
java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar --help
```

- The packaged jar must also be exercised against the local PostgreSQL instance using password `Taurus_123`.
- Thread logs produced by the fat jar run must be inspected as part of verification.

## Packaging approach

The current build already compiles against `libs/` through `systemPath` dependencies. For the runnable jar, the build will not rely on Maven Shade's handling of `system` scope dependencies. Instead, packaging will explicitly merge the repository jars.

`pom.xml` will be extended with a package-phase task that:

1. creates a staging directory under `target/`
2. copies compiled project output from `target/classes`
3. unpacks each jar under `libs/` into that staging directory
4. removes signature metadata such as:
   - `META-INF/*.SF`
   - `META-INF/*.DSA`
   - `META-INF/*.RSA`
5. writes a manifest with `Main-Class: dbradar.Main`
6. assembles `target/ddlcheck-pg-1.0-SNAPSHOT-all.jar`

This keeps the build deterministic and directly aligned with the repository's current local-jar model.

## Verification strategy

### Red check

Before changing the build, run:

```bash
/opt/homebrew/bin/mvn clean package -DskipTests
java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar --help
```

Expected before the change: the package step does not produce the `-all.jar`, so the `java -jar` step fails because the file does not exist.

### Green checks

After the build change:

1. run `/opt/homebrew/bin/mvn clean package -DskipTests`
2. confirm `target/ddlcheck-pg-1.0-SNAPSHOT-all.jar` exists
3. run `java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar --help`
4. run the fat jar against local PostgreSQL with a bounded smoke command
5. inspect the generated thread log and confirm the run executed SQL as expected

## Out of scope

- Changing the CLI syntax
- Removing `libs/`
- Publishing artifacts to a remote package registry
- Adding a Maven wrapper
