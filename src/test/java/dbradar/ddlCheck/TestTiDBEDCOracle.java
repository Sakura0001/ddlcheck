package dbradar.ddlCheck;

import dbradar.Main;
import dbradar.tidb.TiDBGlobalState;
import dbradar.tidb.oracle.TiDBEDCOracle;
import dbradar.tidb.schema.TiDBSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTiDBEDCOracle extends TestEDCOracleBase<TiDBGlobalState, TiDBEDCOracle> {

    String dbName = "tidb";
    String host = "127.0.0.1";
    int port = 4000;
    String username = "song";
    String password = "root";

    @Test
    public void testTiDBEquationOracle() {
        assertEquals(0, Main.executeMain("--num-threads", "1",
                "--num-tries", "100000", "--num-queries", "5000", "--max-generated-databases", "1",
                "--host", host, "--port", String.valueOf(port), "--username", username, "--password", password,
                dbName, "--oracle", "equation"));
    }

    String folderPath = "folderPath";

    @Test
    public void filterOutTiDBBugs() throws IOException {
        checkExistenceOfBugs(folderPath);
    }

    String databaseName = null;
    String reportPath = "reportPath";
    List<String> witnessQueries = List.of(
            "SELECT1",
            "SELECT2"
    );

    @Test
    public void reproduceTiDBBug() throws SQLException, IOException {
        BugReport report = new BugReport(reportPath);
        reproduceBug(report, report.knownToReproduce, witnessQueries);
    }


    @Test
    public void testTiDBSchema() throws Exception {
        TiDBGlobalState state = getState();
        try (Statement createTable1 = state.getConnection().createStatement();
             Statement createTable2 = state.getConnection().createStatement()) {
            createTable1.execute("CREATE TABLE `t1` (  `c1` int PRIMARY KEY, UNIQUE KEY `idx_1` (c1));");
            createTable2.execute("CREATE TABLE `t2` (  `c1` int DEFAULT NULL, FOREIGN KEY (`c1`) REFERENCES `t1` (`c1`));");
        }

        TiDBSchema schema1 = state.getSchema();

        try (Statement dropTable1 = state.getConnection().createStatement();
             Statement dropTable2 = state.getConnection().createStatement()) {
            dropTable1.execute("DROP TABLE t2;");
            dropTable2.execute("DROP TABLE t1;");
        }
        try (Statement createTable1 = state.getConnection().createStatement();
             Statement createTable2 = state.getConnection().createStatement()) {
            createTable1.execute("CREATE TABLE `t3` (  `c1` int UNIQUE, PRIMARY KEY(c1))");
            createTable2.execute("CREATE TABLE `t4` (  `c1` int DEFAULT NULL, FOREIGN KEY (`c1`) REFERENCES `t3` (`c1`))");
        }
        state.updateSchema();
        TiDBSchema schema2 = state.getSchema();
        TiDBEDCOracle oracle = getOracle(state);
        assertTrue(oracle.isEquivalentTiDBSchema(schema1, schema2));
    }


    @Override
    protected TiDBGlobalState getState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        if (databaseName == null) {
            databaseName = "test";
        }
        TiDBGlobalState state = new TiDBGlobalState();
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase(host, port, username, password, databaseName));

        return state;
    }

    @Override
    protected TiDBGlobalState getSemiState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        assert databaseName != null;
        TiDBGlobalState semiState = new TiDBGlobalState();
        String semiDB = databaseName + "_semi";
        semiState.setDatabaseName(semiDB);
        semiState.setConnection(semiState.createDatabase(host, port, username, password, semiDB));

        return semiState;
    }

    @Override
    protected TiDBEDCOracle getOracle(TiDBGlobalState state) {
        return new TiDBEDCOracle(state);
    }
}
