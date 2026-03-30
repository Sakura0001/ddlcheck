package dbradar.ddlCheck;

import dbradar.Main;
import dbradar.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import dbradar.cockroachdb.CockroachDBSchema;
import dbradar.cockroachdb.oracle.CockroachDBEDCOracle;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCockroachDBEDCOracle extends TestEDCOracleBase<CockroachDBGlobalState, CockroachDBEDCOracle> {

    String dbName = "cockroachdb";
    String host = "127.0.0.1";
    int port = 26257;
    String username = "cockroach";
    String password = "cockroach";

    @Test
    public void testCockroachDBEquationOracle() {
        assertEquals(0, Main.executeMain("--num-threads", "1",
                "--num-tries", "100000", "--num-queries", "5000", "--max-generated-databases", "1",
                "--host", host, "--port", String.valueOf(port), "--username", username, "--password", password,
                dbName, "--oracle", "equation"));
    }

    String folderPath = "folderPath";

    @Test
    public void filterOutCockroachDBBugs() throws IOException {
        checkExistenceOfBugs(folderPath);
    }

    String databaseName = null;
    String reportPath = "reportPath";
    List<String> witnessQueries = List.of(
            "SELECT1",
            "SELECT2"
    );

    @Test
    public void reproduceCockroachBug() throws SQLException, IOException {
        BugReport report = new BugReport(reportPath);
        reproduceBug(report, report.knownToReproduce, witnessQueries);
    }

    @Test
    public void testCockroachDBSchema() throws Exception {
        CockroachDBGlobalState state = getState();
        try (Statement createTable1 = state.getConnection().createStatement();
             Statement createTable2 = state.getConnection().createStatement();) {
            createTable1.execute("CREATE TABLE t1 ( c1 int PRIMARY KEY, UNIQUE (c1))");
            createTable2.execute("CREATE TABLE t2 ( c1 int DEFAULT NULL, FOREIGN KEY (c1) REFERENCES t1 (c1))");
        }

        CockroachDBSchema schema1 = state.getSchema();

        try (Statement dropTable1 = state.getConnection().createStatement();
             Statement dropTable2 = state.getConnection().createStatement();) {
            dropTable1.execute("DROP TABLE t2;");
            dropTable2.execute("DROP TABLE t1;");
        }
        try (Statement createTable1 = state.getConnection().createStatement();
             Statement createTable2 = state.getConnection().createStatement();) {
            createTable1.execute("CREATE TABLE t3 ( c1 int, UNIQUE (c1), PRIMARY KEY(c1))");
            createTable2.execute("CREATE TABLE t4 ( c1 int DEFAULT NULL, FOREIGN KEY (c1) REFERENCES t3 (c1))");
        }
        state.updateSchema();
        CockroachDBSchema schema2 = state.getSchema();
        CockroachDBEDCOracle oracle = getOracle(state);
        assertTrue(oracle.isEquivalentCockroachDBSchema(schema1, schema2));
    }

    @Override
    protected CockroachDBGlobalState getState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        if (databaseName == null) {
            databaseName = "test";
        }
        CockroachDBGlobalState state = new CockroachDBGlobalState();
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase(host, port, username, password, databaseName));

        return state;
    }

    @Override
    protected CockroachDBGlobalState getSemiState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        assert databaseName != null;
        CockroachDBGlobalState semiState = new CockroachDBGlobalState();
        String semiDB = databaseName + "_semi";
        semiState.setDatabaseName(semiDB);
        semiState.setConnection(semiState.createDatabase(host, port, username, password, semiDB));

        return semiState;
    }

    @Override
    protected CockroachDBEDCOracle getOracle(CockroachDBGlobalState state) {
        return new CockroachDBEDCOracle(state);
    }
}
