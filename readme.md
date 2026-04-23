To run DDLCheck for PostgreSQL:

1. Deploy a PostgreSQL instance.
2. Revise the username and password in `src/test/java/dbradar/ddlCheck/TestPostgreSQLEDCOracle.java` or pass them on the command line.
3. Build the project with Maven. This repository still resolves its Java dependencies from the local `libs/` directory, so Maven must be installed on the machine first.

```bash
mvn compile
mvn test
mvn package
```

For IDE-based execution, you can still run:

```bash
TestPostgreSQLEDCOracle#testPostgreSQLEquationOracle
```
