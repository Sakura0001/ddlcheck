To run DDLCheck for PostgreSQL, please follow the steps:

1. Deploy a PostgreSQL instance.
2. Revise the username and password in `dbradar/ddlCheck/TestPostgreSQLEDCOracle.java` or pass them on the command line.
3. Run DDLCheck with the following testing method in IDE, e.g., IntelliJ:

```bash
TestPostgreSQLEDCOracle#testPostgreSQLEquationOracle
```

You can also run a small command-line check after compiling:

```bash
java -cp 'out:src/main/resources:libs/*' dbradar.Main \
  --num-threads 2 --num-tries 2 --num-queries 5 \
  --max-generated-databases 1 \
  --host 127.0.0.1 --port 5432 \
  --username postgres --password Taurus_123 \
  postgresql --oracle equation
```
