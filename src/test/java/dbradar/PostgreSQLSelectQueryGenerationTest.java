package dbradar;

import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.IgnoreMeException;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLOptions;
import dbradar.postgresql.PostgreSQLProvider;

import java.sql.Statement;
import java.util.List;
import java.util.Locale;

public final class PostgreSQLSelectQueryGenerationTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;
    private static final List<String> UNSAFE_FUNCTION_TOKENS = List.of(
            "PG_SLEEP", "PG_SLEEP_FOR", "PG_SLEEP_UNTIL", "SETSEED", "RANDOM(", "PG_ADVISORY", "LOCK TABLE");

    private PostgreSQLSelectQueryGenerationTest() {
    }

    public static void main(String[] args) throws Exception {
        verifySelectDoesNotEmitUnsafeOrExpensiveFunctions();
        verifyLimitedSelectsAreDeterministicallyOrdered();
        verifyUnexpectedDqlErrorsAreNotReturnedAsEmptyResults();
        verifyGeneratedSelectsExecuteWithinOneSecond();
        verifyGeneratedCteSelectsExecute();
    }

    private static void verifyLimitedSelectsAreDeterministicallyOrdered() throws Exception {
        PostgreSQLGlobalState state = createState("select_query_order_state");
        try {
            createCoverageTables(state);
            state.updateSchema();

            int successfulGenerations = 0;
            for (long seed = 1; seed <= 1000 && successfulGenerations < 300; seed++) {
                state.setRandomly(new Randomly(seed));
                SQLQueryAdapter query;
                try {
                    query = PostgreSQLProvider.PostgreSQLQueryProvider.SELECT.getQuery(state);
                } catch (QueryGenerationException | IgnoreMeException ignored) {
                    continue;
                }
                successfulGenerations++;
                String normalized = query.getQueryString().toUpperCase(Locale.ROOT);
                if (normalized.contains(" LIMIT ") && !normalized.contains(" ORDER BY ")) {
                    throw new AssertionError("Generated SELECT uses LIMIT without deterministic ORDER BY: "
                            + query.getQueryString());
                }
            }
            if (successfulGenerations < 300) {
                throw new AssertionError("Expected at least 300 generated SELECT statements but got "
                        + successfulGenerations);
            }
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyGeneratedCteSelectsExecute() throws Exception {
        PostgreSQLGlobalState state = createState("select_query_cte_state");
        try {
            createCoverageTables(state);
            execute(state, "SET SESSION statement_timeout = 1000");
            state.updateSchema();

            for (long seed = 1; seed <= 500; seed++) {
                state.setRandomly(new Randomly(seed));
                SQLQueryAdapter query = PostgreSQLProvider.PostgreSQLQueryProvider.SELECT.getQuery(state);
                if (!query.getQueryString().toUpperCase(Locale.ROOT).startsWith("WITH ")) {
                    continue;
                }
                try (Statement statement = state.getConnection().createStatement()) {
                    statement.executeQuery(query.getQueryString());
                } catch (Exception e) {
                    throw new AssertionError("Generated CTE SELECT failed: " + query.getQueryString(), e);
                }
                return;
            }
            throw new AssertionError("Expected SELECT generation to emit an executable CTE query");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifySelectDoesNotEmitUnsafeOrExpensiveFunctions() throws Exception {
        PostgreSQLGlobalState state = createState("select_query_safety_state");
        try {
            createCoverageTables(state);
            state.updateSchema();

            int successfulGenerations = 0;
            for (long seed = 1; seed <= 1000 && successfulGenerations < 300; seed++) {
                state.setRandomly(new Randomly(seed));
                SQLQueryAdapter query;
                try {
                    query = PostgreSQLProvider.PostgreSQLQueryProvider.SELECT.getQuery(state);
                } catch (QueryGenerationException | IgnoreMeException ignored) {
                    continue;
                }
                successfulGenerations++;
                String normalized = query.getQueryString().toUpperCase(Locale.ROOT);
                for (String unsafeToken : UNSAFE_FUNCTION_TOKENS) {
                    if (normalized.contains(unsafeToken)) {
                        throw new AssertionError("Generated SELECT contains unsafe token [" + unsafeToken + "]: "
                                + query.getQueryString());
                    }
                }
            }
            if (successfulGenerations < 300) {
                throw new AssertionError("Expected at least 300 generated SELECT statements but got "
                        + successfulGenerations);
            }
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyGeneratedSelectsExecuteWithinOneSecond() throws Exception {
        PostgreSQLGlobalState state = createState("select_query_execution_state");
        try {
            createCoverageTables(state);
            execute(state, "SET SESSION statement_timeout = 1000");
            state.updateSchema();

            for (long seed = 1; seed <= 200; seed++) {
                state.setRandomly(new Randomly(seed));
                SQLQueryAdapter query = PostgreSQLProvider.PostgreSQLQueryProvider.SELECT.getQuery(state);
                long start = System.nanoTime();
                try (Statement statement = state.getConnection().createStatement()) {
                    statement.executeQuery(query.getQueryString());
                } catch (Exception e) {
                    throw new AssertionError("Generated SELECT failed: " + query.getQueryString(), e);
                }
                long elapsedNanos = System.nanoTime() - start;
                if (elapsedNanos > 1_000_000_000L) {
                    throw new AssertionError("Generated SELECT exceeded 1s: " + query.getQueryString());
                }
            }
        } finally {
            closeQuietly(state);
        }
    }


    private static void verifyUnexpectedDqlErrorsAreNotReturnedAsEmptyResults() throws Exception {
        PostgreSQLGlobalState state = createState("select_query_error_state");
        try {
            createCoverageTables(state);
            state.updateSchema();

            SQLQueryAdapter invalidQuery = new SQLQueryAdapter("SELECT missing_column FROM select_safety_a");
            try {
                EDCBase.getQueryResult(invalidQuery, state);
                throw new AssertionError("Expected unexpected DQL error to be reported instead of returning empty results");
            } catch (AssertionError expected) {
                if (!expected.getMessage().contains("SELECT missing_column FROM select_safety_a")) {
                    throw expected;
                }
            }
        } finally {
            closeQuietly(state);
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

    private static void createCoverageTables(PostgreSQLGlobalState state) throws Exception {
        execute(state, "CREATE TABLE select_safety_a (c1 INT, c2 INT, c3 TEXT, c4 BOOLEAN, c5 DATE)");
        execute(state, "CREATE TABLE select_safety_b (c1 INT, c2 INT, c3 TEXT, c4 BOOLEAN, c5 DATE)");
        for (int i = 0; i < 100; i++) {
            execute(state, String.format(Locale.ROOT,
                    "INSERT INTO select_safety_a VALUES (%d, %d, 'a_%d', %s, DATE '2024-01-01' + %d)",
                    i, i % 17, i, i % 2 == 0 ? "TRUE" : "FALSE", i));
            execute(state, String.format(Locale.ROOT,
                    "INSERT INTO select_safety_b VALUES (%d, %d, 'b_%d', %s, DATE '2024-02-01' + %d)",
                    i, i % 19, i, i % 2 == 0 ? "FALSE" : "TRUE", i));
        }
    }

    private static void execute(PostgreSQLGlobalState state, String sql) throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
        }
    }

    private static void closeQuietly(PostgreSQLGlobalState state) {
        if (state == null || state.getConnection() == null) {
            return;
        }
        try {
            state.getConnection().close();
        } catch (Exception ignored) {
        }
    }
}
