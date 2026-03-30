To run DDLCheck, please follow the steps:
1. Deploy target DBMSs, including MySQL, PostgreSQL, SQLite, MariaDB, CockroachDB, and TiDB
2. Revise the username and password based on the deployed option in the corresponding DBMS configure.
   1. dbradar/ddlCheck/TestMySQLEDCOracle.java
   2. dbradar/ddlCheck/TestPostgreSQLEDCOracle.java
   3. dbradar/ddlCheck/TestMariaDBEDCOracle.java 
   4. dbradar/ddlCheck/TestCockroachDBEDCOracle.java
   5. dbradar/ddlCheck/TestTiDBEDCOracle.java
4. Run DDLCheck with the following testing methods in IDE, e.g., Intellij
```bash
TestMySQLEDCOracle#testMySQLEquationOracle
TestPostgreSQLEDCOracle#testPostgreSQLEquationOracle
TestMariaDBEDCOracle#testMariaDBEquationOracle
TestCockroachDBEDCOracle#testCockroachDBEquationOracle
TestTiDBEDCOracle#testTiDBEquationOracle
TestSQLite3EDCOracle#testSQLite3EquationOracle
```