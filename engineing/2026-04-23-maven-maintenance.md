# Maven maintenance cleanup

Date: 2026-04-23

## Scope

- Added a root `pom.xml` so the project can be maintained through Maven.
- Kept the existing `libs/` directory and referenced its jars directly from Maven with `systemPath`.
- Matched the current manual compile behavior by keeping Maven compilation on Java 17 and passing `-proc:none`.
- Cleaned local generated artifacts from the repository view:
  - removed `src/**/*.class`
  - removed `build/`
  - removed `out/`
  - stopped tracking `src/main/java/dbradar/common/query/query.iml`
- Extended `.gitignore` to hide `.idea/` and future `*.iml` files.
- Updated `readme.md` with Maven build commands.

## Validation

- Confirmed the local environment blocker:
  - `mvn -version`
  - Result: `zsh:1: command not found: mvn`
- Verified the `pom.xml` `systemPath` targets exist under `libs/`.
- Recompiled the project with the existing manual flow against `libs/*` after the cleanup:
  - `javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d <temp-dir> $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')`

## Repository hygiene checks

- `find src -name '*.class'` returned no results after cleanup.
- `git ls-files '*.iml'` returned no results after removing the tracked IDEA module file.
- `build/` and `out/` were removed from the project root after verification.

## Notes

- This session could not execute `mvn compile` or `mvn test` because Maven is not installed in the local shell environment.
- The delivered Maven commands are:
  - `mvn compile`
  - `mvn test`
  - `mvn package`
