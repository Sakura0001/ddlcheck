package dbradar.ddlCheck;

import dbradar.MainOptions;
import dbradar.Randomly;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLOptions;
import dbradar.postgresql.oracle.PostgreSQLEDCOracle;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public final class PostgreSQLConstraintReplayRegressionTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 5432;
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";

    private PostgreSQLConstraintReplayRegressionTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyConstraintNameCollisionsReplayIntoSemiState();
        verifyForeignKeysReplayAfterStandaloneUniqueIndexes();
    }

    private static void verifyConstraintNameCollisionsReplayIntoSemiState() throws Exception {
        PostgreSQLGlobalState state = createState("constraint_collision_replay_state");
        PostgreSQLGlobalState semiState = createState("constraint_collision_replay_state_semi");
        try {
            execute(state, "CREATE TEMP TABLE constraint_collision_temp (c4 INT, c5 INT, CONSTRAINT shared_pkey PRIMARY KEY (c5))");
            execute(state, "CREATE TABLE constraint_collision_public (c4 INT, c5 INT, CONSTRAINT shared_pkey PRIMARY KEY (c4))");
            state.updateSchema();

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            List<SQLQueryAdapter> fetchedStatements = oracle.fetchCreateStmts(state);
            List<String> fetchedSql = fetchedStatements.stream().map(SQLQueryAdapter::getQueryString).toList();
            requireContains(fetchedSql, "CREATE TABLE constraint_collision_public (c4 integer NOT NULL, c5 integer, PRIMARY KEY (c4))");

            oracle.replayCreateStmts(semiState, new ArrayList<>(fetchedStatements));

            String insertRows = "INSERT INTO constraint_collision_public (c4, c5) VALUES (1, 10), (2, 20)";
            execute(state, insertRows);
            execute(semiState, insertRows);

            String updateNonKeyColumnToDuplicate = "UPDATE constraint_collision_public SET c5 = 10 WHERE TRUE";
            execute(state, updateNonKeyColumnToDuplicate);
            execute(semiState, updateNonKeyColumnToDuplicate);
        } finally {
            closeQuietly(state, semiState);
        }
    }

    private static void verifyForeignKeysReplayAfterStandaloneUniqueIndexes() throws Exception {
        PostgreSQLGlobalState state = createState("fk_after_index_replay_state");
        PostgreSQLGlobalState semiState = createState("fk_after_index_replay_state_semi");
        try {
            execute(state, "CREATE TABLE fk_after_index (c4 INT, c5 INT)");
            execute(state, "CREATE UNIQUE INDEX fk_after_index_c4_idx ON fk_after_index (c4)");
            execute(state, "ALTER TABLE fk_after_index ADD FOREIGN KEY (c5) REFERENCES fk_after_index(c4)");
            state.updateSchema();

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            List<SQLQueryAdapter> fetchedStatements = oracle.fetchCreateStmts(state);
            List<String> fetchedSql = fetchedStatements.stream().map(SQLQueryAdapter::getQueryString).toList();
            requireContains(fetchedSql, "CREATE TABLE fk_after_index (c4 integer, c5 integer)");
            requireContains(fetchedSql, "CREATE UNIQUE INDEX fk_after_index_c4_idx ON public.fk_after_index USING btree (c4)");
            requireContains(fetchedSql, "ALTER TABLE fk_after_index ADD FOREIGN KEY (c5) REFERENCES fk_after_index(c4)");

            oracle.replayCreateStmts(semiState, new ArrayList<>(fetchedStatements));

            String insertRows = "INSERT INTO fk_after_index (c4, c5) VALUES (1, NULL), (2, 1)";
            execute(state, insertRows);
            execute(semiState, insertRows);

            String updateViolatingForeignKey = "UPDATE fk_after_index SET c5 = 999 WHERE c4 = 2";
            requireExecutionFailure(state, updateViolatingForeignKey, "violates foreign key constraint");
            requireExecutionFailure(semiState, updateViolatingForeignKey, "violates foreign key constraint");
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

    private static void execute(PostgreSQLGlobalState state, String sql) throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
        }
    }

    private static void requireExecutionFailure(PostgreSQLGlobalState state, String sql, String expectedMessageFragment)
            throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
            throw new AssertionError("Expected statement to fail: " + sql);
        } catch (Exception exception) {
            String message = exception.getMessage();
            if (message == null || !message.contains(expectedMessageFragment)) {
                throw new AssertionError("Expected failure containing [" + expectedMessageFragment + "] for [" + sql
                        + "] but got: " + message, exception);
            }
        }
    }

    private static void requireContains(List<String> statements, String expectedFragment) {
        String normalizedFragment = normalize(expectedFragment);
        for (String statement : statements) {
            if (normalize(statement).contains(normalizedFragment)) {
                return;
            }
        }
        throw new AssertionError("Expected fragment not found: " + expectedFragment + "\nStatements:\n" + String.join("\n", statements));
    }

    private static String normalize(String sql) {
        return sql.replaceAll("\\s+", " ").trim().toUpperCase();
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
}
