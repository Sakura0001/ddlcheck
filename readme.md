To run DDLCheck for PostgreSQL:

1. Deploy a PostgreSQL instance.
2. Revise the username and password in `src/test/java/dbradar/ddlCheck/TestPostgreSQLEDCOracle.java` or pass them on the command line.
3. Build the executable jar with Maven. This repository still resolves Java dependencies from the local `libs/` directory, so Maven must be installed on the machine first.

```bash
/opt/homebrew/bin/mvn clean package -DskipTests
```

The runnable fat jar is:

```bash
target/ddlcheck-pg-1.0-SNAPSHOT-all.jar
```

You can launch the tool directly with:

```bash
java -jar target/ddlcheck-pg-1.0-SNAPSHOT-all.jar --help
```

Example PostgreSQL run:

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

For IDE-based execution, you can still run:

```bash
TestPostgreSQLEDCOracle#testPostgreSQLEquationOracle
```
