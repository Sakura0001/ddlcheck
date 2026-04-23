# Executable Fat Jar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `mvn clean package -DskipTests` emit a single executable jar that runs DDLCheck with `java -jar`.

**Architecture:** Keep compilation on the current Java 17 plus local-`libs/` model, and extend package-time assembly rather than changing runtime code. The fat jar will be created by unpacking compiled classes plus every repository jar into a staging directory, then building an `-all.jar` with `dbradar.Main` as the manifest entrypoint.

**Tech Stack:** Maven, Java 17, local jars from `libs/`, PostgreSQL smoke verification.

---

### Task 1: Prove the current build is missing the runnable jar

**Files:**
- Modify: `docs/superpowers/specs/2026-04-23-executable-fat-jar-design.md`
- Modify: `docs/superpowers/plans/2026-04-23-executable-fat-jar.md`

- [ ] **Step 1: Run the current package command**

Run:

```bash
/opt/homebrew/bin/mvn clean package -DskipTests
```

Expected: Maven package succeeds or partially succeeds, but there is no `target/ddlcheck-pg-1.0-SNAPSHOT-all.jar`.

- [ ] **Step 2: Run the expected executable-jar command and watch it fail**

Run:

```bash
java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar --help
```

Expected: failure because the `-all.jar` file does not yet exist.

### Task 2: Add fat-jar assembly to Maven

**Files:**
- Modify: `pom.xml`
- Modify: `readme.md`

- [ ] **Step 1: Add the package-phase assembly plugin**

Extend `pom.xml` with a plugin that:

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-antrun-plugin</artifactId>
  <version>3.1.0</version>
</plugin>
```

Bind it to `package` and make it:
- create `${project.build.directory}/fat-jar-work`
- copy `${project.build.outputDirectory}` into it
- unzip `libs/*.jar` into it
- delete `META-INF/*.SF`, `META-INF/*.DSA`, `META-INF/*.RSA`
- emit `${project.build.directory}/${project.artifactId}-${project.version}-all.jar`

- [ ] **Step 2: Write the manifest entrypoint**

Make the assembled jar include:

```text
Main-Class: dbradar.Main
```

- [ ] **Step 3: Document the new operator flow**

Update `readme.md` with:

```bash
/opt/homebrew/bin/mvn clean package -DskipTests
java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar --help
```

### Task 3: Verify the fat jar

**Files:**
- Modify: `engineing/2026-04-23-executable-fat-jar.md`

- [ ] **Step 1: Re-run the build**

Run:

```bash
/opt/homebrew/bin/mvn clean package -DskipTests
```

Expected: exit code `0` and `target/ddlcheck-pg-1.0-SNAPSHOT-all.jar` exists.

- [ ] **Step 2: Verify the executable jar help path**

Run:

```bash
java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar --help
```

Expected: usage/help output from `dbradar.Main`.

- [ ] **Step 3: Verify the executable jar against local PostgreSQL**

Run:

```bash
java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar \
  --num-threads 1 \
  --num-tries 1 \
  --num-queries 20 \
  --max-generated-databases 1 \
  --ddl-count 4 \
  --dml-count 3 \
  --host 127.0.0.1 \
  --port 5432 \
  --username postgres \
  --password Taurus_123 \
  postgresql --oracle equation
```

Expected: exit code `0`.

- [ ] **Step 4: Inspect the generated thread log**

Run:

```bash
sed -n '1,120p' logs/postgresql/database0-cur.log
```

Expected: the log contains bootstrap DDL, bootstrap DML, and later query activity.

- [ ] **Step 5: Record the task**

Write `engineing/2026-04-23-executable-fat-jar.md` with:
- packaging approach
- exact build and run commands
- verification results
- thread log observations

- [ ] **Step 6: Commit**

```bash
git add pom.xml readme.md engineing/2026-04-23-executable-fat-jar.md docs/superpowers/specs/2026-04-23-executable-fat-jar-design.md docs/superpowers/plans/2026-04-23-executable-fat-jar.md
git commit -m "build: add executable fat jar packaging"
```
