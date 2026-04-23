# Maven Maintenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Maven project metadata that uses the existing `libs/` directory for dependencies, while cleaning generated clutter from the repository view.

**Architecture:** Keep the current source layout and runtime behavior intact. Introduce a root `pom.xml` that mirrors the existing manual classpath, then remove generated artifacts and IDE metadata so the repository is readable and Maven-aware without a broad refactor.

**Tech Stack:** Java 17, Maven POM metadata, local jar dependencies from `libs/`, JUnit 5, PostgreSQL JDBC, AutoService.

---

### Task 1: Capture the current build failure and dependency surface

**Files:**
- Create: `docs/superpowers/specs/2026-04-23-maven-maintenance-design.md`
- Create: `docs/superpowers/plans/2026-04-23-maven-maintenance.md`
- Modify: `readme.md`

- [ ] **Step 1: Run the failing Maven command**

Run: `mvn -version`
Expected: shell error indicating `mvn` is not installed in the local environment.

- [ ] **Step 2: Confirm the jars the code currently relies on**

Run: `rg -n "AutoService|com.beust|org.luaj|grammar\\.|org.junit|org.postgresql|org.slf4j" src/main/java src/test/java`
Expected: direct imports confirming the dependency list for the new `pom.xml`.

- [ ] **Step 3: Keep the build note aligned with reality**

Update `readme.md` to mention that the project is now Maven-managed but still depends on local jars in `libs/`, and that Maven must be installed before running the commands documented later in the file.

- [ ] **Step 4: Commit the planning docs if needed**

```bash
git add docs/superpowers/specs/2026-04-23-maven-maintenance-design.md docs/superpowers/plans/2026-04-23-maven-maintenance.md
git commit -m "docs: add maven maintenance design"
```

### Task 2: Add the Maven build definition

**Files:**
- Create: `pom.xml`
- Modify: `readme.md`

- [ ] **Step 1: Write the minimal Maven build definition**

Create `pom.xml` with:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>dbradar</groupId>
  <artifactId>ddlcheck-pg</artifactId>
  <version>1.0-SNAPSHOT</version>
  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <junit.version>5.8.1</junit.version>
  </properties>
</project>
```

- [ ] **Step 2: Add the local jar dependencies**

Extend `pom.xml` with `system`-scoped dependencies whose `systemPath` points into `${project.basedir}/libs/...`.

```xml
<dependency>
  <groupId>local.libs</groupId>
  <artifactId>jcommander</artifactId>
  <version>1.82</version>
  <scope>system</scope>
  <systemPath>${project.basedir}/libs/jcommander-1.82.jar</systemPath>
</dependency>
```

Repeat the same pattern for `grammar-core`, `luaj-jse`, `auto-service`, `auto-service-annotations`, `slf4j-simple`, `postgresql`, and the JUnit jars.

- [ ] **Step 3: Add build plugins**

Configure:

```xml
<build>
  <plugins>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-compiler-plugin</artifactId>
      <version>3.11.0</version>
    </plugin>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-surefire-plugin</artifactId>
      <version>3.2.5</version>
      <configuration>
        <useSystemClassLoader>true</useSystemClassLoader>
      </configuration>
    </plugin>
  </plugins>
</build>
```

Also add `annotationProcessorPaths` so AutoService processing uses the jars from `libs/`.

- [ ] **Step 4: Document the Maven commands**

Add to `readme.md`:

```bash
mvn compile
mvn test
mvn package
```

Plus one sentence that these commands require Maven to be installed locally and still resolve libraries from the repository `libs/` directory.

### Task 3: Clean the repository view

**Files:**
- Modify: `.gitignore`
- Delete: `src/main/java/dbradar/common/query/query.iml`

- [ ] **Step 1: Ignore IDE metadata**

Update `.gitignore` so it contains:

```gitignore
out/
report/
*.class
build/
logs/
.idea/
*.iml
```

- [ ] **Step 2: Stop tracking the committed IDEA module file**

Run:

```bash
git rm -- src/main/java/dbradar/common/query/query.iml
```

Expected: Git stages the deletion of the tracked `.iml` file.

- [ ] **Step 3: Remove generated local artifacts**

Run:

```bash
find src -name '*.class' -delete
rm -rf build out
```

Expected: source directories contain only source/resources, and the generated build folders are removed.

### Task 4: Verify the migrated project metadata

**Files:**
- Modify: `engineing/2026-04-23-maven-maintenance.md`

- [ ] **Step 1: Verify the repository cleanup**

Run:

```bash
find src -name '*.class'
git ls-files '*.iml'
test ! -d build && test ! -d out
```

Expected: no `.class` output, no tracked `.iml`, and both generated directories absent.

- [ ] **Step 2: Verify the `pom.xml` dependency paths**

Run:

```bash
python3 - <<'PY'
import xml.etree.ElementTree as ET
from pathlib import Path
root = ET.parse('pom.xml').getroot()
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
for node in root.findall('.//m:systemPath', ns):
    path = node.text.replace('${project.basedir}/', '')
    p = Path(path)
    print(f'{path}: {"OK" if p.exists() else "MISSING"}')
PY
```

Expected: every referenced jar reports `OK`.

- [ ] **Step 3: Re-run the existing manual compile flow**

Run:

```bash
mkdir -p out/manual-check
javac -proc:none -encoding UTF-8 -cp 'libs/*:src/main/resources' -d out/manual-check $(find src/main/java -name '*.java') $(find src/test/java -name '*.java')
```

Expected: exit code `0`.

- [ ] **Step 4: Record the task**

Write `engineing/2026-04-23-maven-maintenance.md` summarizing:
- the cleanup performed
- the added Maven metadata
- the fact that `mvn` was unavailable locally
- the verification commands and outcomes

- [ ] **Step 5: Commit**

```bash
git add .gitignore pom.xml readme.md engineing/2026-04-23-maven-maintenance.md
git commit -m "build: add maven project metadata"
```
