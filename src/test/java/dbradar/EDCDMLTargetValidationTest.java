package dbradar;

import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLOptions;
import dbradar.postgresql.PostgreSQLProvider.PostgreSQLQueryProvider;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public final class EDCDMLTargetValidationTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 5432;
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";

    private EDCDMLTargetValidationTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyDmlTargetTableValidation(PostgreSQLQueryProvider.INSERT, "insert");
        verifyDmlTargetTableValidation(PostgreSQLQueryProvider.UPDATE_MULTI_ROW, "update");
        verifyDmlTargetTableValidation(PostgreSQLQueryProvider.DELETE_MULTI_ROW, "delete");
    }

    private static void verifyDmlTargetTableValidation(PostgreSQLQueryProvider queryProvider, String suffix)
            throws Exception {
        PostgreSQLGlobalState state = createState("dml_target_validation_" + suffix);
        PostgreSQLGlobalState semiState = createState("dml_target_validation_" + suffix + "_semi");
        try {
            createFixtureTable(state);
            createFixtureTable(semiState);
            state.updateSchema();
            semiState.updateSchema();

            state.setRandomly(new Randomly(10));
            SQLQueryAdapter dml = queryProvider.getQuery(state);
            CapturingOracle oracle = new CapturingOracle(state, semiState, dml);
            if (!oracle.runDmlValidation()) {
                throw new AssertionError("Expected DML to execute successfully: " + dml.getQueryString());
            }
            if (!List.of("SELECT * FROM t0;").equals(oracle.getCapturedValidationQueries())) {
                throw new AssertionError("Expected validation query to target t0, but got "
                        + oracle.getCapturedValidationQueries() + " for " + dml.getQueryString());
            }
        } finally {
            closeQuietly(state, semiState);
        }
    }

    private static PostgreSQLGlobalState createState(String databaseName) throws Exception {
        MainOptions options = new MainOptions();
        Randomly.initialize(options);

        PostgreSQLOptions dbOptions = new PostgreSQLOptions();
        PostgreSQLGlobalState state = new PostgreSQLGlobalState();
        state.setMainOptions(options);
        state.setDbmsSpecificOptions(dbOptions);
        state.setRandomly(new Randomly(0));
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase(HOST, PORT, USERNAME, PASSWORD, databaseName));
        return state;
    }

    private static void createFixtureTable(PostgreSQLGlobalState state) throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute("CREATE TABLE t0 (c1 INT, c2 TEXT)");
            statement.execute("INSERT INTO t0 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        }
    }

    private static void closeQuietly(PostgreSQLGlobalState... states) {
        for (PostgreSQLGlobalState state : states) {
            if (state == null || state.getConnection() == null) {
                continue;
            }
            try {
                state.getConnection().close();
            } catch (Exception ignored) {
            }
        }
    }

    private static final class CapturingOracle extends EDCBase<PostgreSQLGlobalState> {
        private final PostgreSQLGlobalState semiStateRef;
        private final SQLQueryAdapter dml;
        private final List<String> capturedValidationQueries = new java.util.ArrayList<>();

        private CapturingOracle(PostgreSQLGlobalState state, PostgreSQLGlobalState semiState, SQLQueryAdapter dml) {
            super(state);
            this.synState = semiState;
            this.semiStateRef = semiState;
            this.dml = dml;
        }

        private boolean runDmlValidation() throws SQLException {
            return checkDMLStmt(false);
        }

        private List<String> getCapturedValidationQueries() {
            return capturedValidationQueries;
        }

        @Override
        protected boolean checkStmt(String stmt, boolean logExecutionAttempt) {
            return checkStmt(stmt, genState, semiStateRef);
        }

        @Override
        protected void checkDQLStmt(SQLQueryAdapter query) throws SQLException {
            capturedValidationQueries.add(query.getQueryString());
            super.checkDQLStmt(query);
        }

        @Override
        public void cleanDatabase() {
        }

        @Override
        public List<SQLQueryAdapter> fetchCreateStmts(PostgreSQLGlobalState state) {
            return List.of();
        }

        @Override
        public String generateSelectStmt(PostgreSQLGlobalState state) {
            return null;
        }

        @Override
        public SQLQueryAdapter generateDMLStmt(PostgreSQLGlobalState state) {
            return dml;
        }

        @Override
        public String checkQueryPlan(String query, PostgreSQLGlobalState state) {
            return "";
        }

        @Override
        public String getExecutionResult(String query, PostgreSQLGlobalState state) {
            try (Statement statement = state.getConnection().createStatement()) {
                statement.execute(query);
                return null;
            } catch (SQLException e) {
                return e.getMessage();
            }
        }
    }
}
