package dbradar.ddlCheck;

import dbradar.Main;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.oracle.PostgreSQLEDCOracle;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPostgreSQLEDCOracle extends TestEDCOracleBase<PostgreSQLGlobalState, PostgreSQLEDCOracle> {

    String dbName = "postgresql";
    String host = "127.0.0.1";
    int port = 5432;
    String username = "postgres";
    String password = "Taurus_123";

    @Test
    public void testPostgreSQLEquationOracle() {
        assertEquals(0, Main.executeMain("--num-threads", "40",
                "--num-tries", "100000", "--num-queries", "5000", "--max-generated-databases", "100",
                "--host", host, "--port", String.valueOf(port), "--username", username, "--password", password,"--log-global-execution","true",
                dbName, "--oracle", "equation"));
    }

    String folderPath = "folderPath";

    @Test
    public void filterOutPostgreSQLBugs() throws IOException {
        checkExistenceOfBugs(folderPath);
    }

    String databaseName = null;
    String reportPath = "reportPath";
    List<String> witnessQueries = List.of(
            "SELECT1",
            "SELECT2"
    );

    @Test
    public void reproducePostgreSQLBug() throws SQLException, IOException {
        BugReport report = new BugReport(reportPath);
        reproduceBug(report, report.knownToReproduce, witnessQueries);
    }

    @Override
    protected PostgreSQLGlobalState getState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        if (databaseName == null) {
            databaseName = "test";
        }
        PostgreSQLGlobalState state = new PostgreSQLGlobalState();
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase(host, port, username, password, databaseName));

        return state;
    }

    @Override
    protected PostgreSQLGlobalState getSemiState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        assert databaseName != null;
        PostgreSQLGlobalState semiState = new PostgreSQLGlobalState();
        String semiDB = databaseName + "_semi";
        semiState.setDatabaseName(semiDB);
        semiState.setConnection(semiState.createDatabase(host, port, username, password, semiDB));

        return semiState;
    }

    @Override
    protected PostgreSQLEDCOracle getOracle(PostgreSQLGlobalState state) {
        return new PostgreSQLEDCOracle(state);
    }
}
