# Maven Maintenance Design

Date: 2026-04-23

## Goal

Bring the project under Maven-based maintenance without removing the existing `libs/` directory, while also cleaning obvious generated clutter from the repository view.

## Current state

- The project currently builds by invoking `javac` directly with `libs/*` on the classpath.
- There is no root `pom.xml`.
- The repository root contains generated directories such as `build/` and `out/`, both already ignored by Git.
- The source tree contains many generated `.class` files that are ignored by Git but make the project hard to inspect locally.
- One IDE file, `src/main/java/dbradar/common/query/query.iml`, is still tracked by Git.
- The local environment for this task does not have the `mvn` binary installed, so Maven verification cannot be executed inside this session.

## Scope

### In scope

- Add a root `pom.xml`.
- Keep `libs/` in the repository and reference the required jars from `pom.xml`.
- Configure Maven for:
  - Java 17 compilation
  - compilation behavior aligned with the current `javac -proc:none` flow
  - unit/integration test compilation under `src/test/java`
  - JUnit 5 test execution
- Remove local generated `.class` files and generated build directories from the working tree.
- Stop tracking the existing `.iml` file and ignore future IDE metadata.
- Update the README with Maven-oriented build commands.
- Record the change in `/engineing`.

### Out of scope

- Migrating dependencies from `libs/` to Maven Central coordinates.
- Adding Maven Wrapper files.
- Refactoring source layout away from the current Maven-standard directories.
- Rewriting tests or changing PostgreSQL runtime behavior.

## Dependency strategy

The `pom.xml` will use `system`-scoped dependencies with `${project.basedir}/libs/...` `systemPath` entries for the jars that are actually required by the current codebase:

- `jcommander`
- `grammar-core`
- `luaj-jse`
- `auto-service-annotations`
- `slf4j-simple`
- `postgresql`
- `junit-jupiter-api`
- `junit-jupiter-engine`
- `junit-jupiter-params`
- supporting JUnit platform jars that are already present in `libs/`

This is not the most portable Maven pattern, but it matches the user requirement to keep `libs/` and let the project be maintained through Maven with minimal behavioral change.

The project no longer relies on generated `ServiceLoader` metadata at runtime because `Main.getDBMSProviders()` manually constructs the PostgreSQL provider. Maven compilation will therefore mirror the current `javac -proc:none` behavior instead of introducing new annotation-processing requirements.

## Cleanup strategy

- Delete `src/**/*.class`.
- Delete `build/` and `out/`.
- Extend `.gitignore` to include `.idea/` and `*.iml`.
- Remove the tracked `query.iml` file from Git.

This keeps the repository view focused on source, resources, docs, and persistent assets only.

## Verification

Because `mvn` is not installed in the local environment, verification will be split:

1. Red check:
   - run `mvn -version` and record the local blocker (`command not found`)
2. Structural verification:
   - ensure `pom.xml` parses as XML
   - ensure every `systemPath` target exists in `libs/`
3. Compilation regression:
   - compile the project with the existing `javac -cp 'libs/*'` flow after cleanup to confirm the new metadata changes did not break the current buildable state
4. Repository hygiene verification:
   - confirm no `.class` files remain under `src/`
   - confirm `build/` and `out/` are absent
   - confirm `query.iml` is no longer tracked

## User-facing build commands

The delivered commands will be:

```bash
mvn compile
mvn test
mvn package
```

And, because the local session lacks Maven, the documentation will explicitly note that these commands require Maven to be installed on the machine first.
