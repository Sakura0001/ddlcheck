package dbradar.ddlCheck;

import dbradar.MainOptions;
import dbradar.Randomly;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerator;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLKeyFunctionManager;
import dbradar.postgresql.PostgreSQLOptions;
import dbradar.postgresql.oracle.PostgreSQLEDCOracle;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public final class PostgreSQLPartitionRegressionTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 5432;
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";

    private PostgreSQLPartitionRegressionTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyPartitionedTablesReplayIntoSemiState();
        verifyPartitionGrammarRootsGenerateExecutableStatements();
    }

    private static void verifyPartitionedTablesReplayIntoSemiState() throws Exception {
        PostgreSQLGlobalState state = createState("partition_replay_state");
        PostgreSQLGlobalState semiState = createState("partition_replay_state_semi");
        try {
            execute(state, "CREATE TABLE sales (id INT, sale_date INT NOT NULL, region TEXT) PARTITION BY RANGE (sale_date)");
            execute(state, "CREATE TABLE sales_staging (id INT, sale_date INT NOT NULL, region TEXT)");
            execute(state, "ALTER TABLE sales ATTACH PARTITION sales_staging DEFAULT");
            state.updateSchema();

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            List<SQLQueryAdapter> fetchedStatements = oracle.fetchCreateStmts(state);
            List<String> fetchedSql = fetchedStatements.stream().map(SQLQueryAdapter::getQueryString).toList();

            requireContains(fetchedSql, "PARTITION BY RANGE (sale_date)");
            requireContains(fetchedSql, "PARTITION OF sales DEFAULT");

            List<SQLQueryAdapter> replayStatements = new ArrayList<>(fetchedStatements);
            List<String> replayedSql = oracle.replayCreateStmts(semiState, replayStatements);
            requireContains(replayedSql, "PARTITION BY RANGE (sale_date)");

            execute(state, "INSERT INTO sales (id, sale_date, region) VALUES (1, 10, 'north'), (2, 150, 'south')");
            execute(semiState, "INSERT INTO sales (id, sale_date, region) VALUES (1, 10, 'north'), (2, 150, 'south')");

            requireSingleValue(state, "SELECT count(*) FROM sales_staging", 2);
            requireSingleValue(semiState, "SELECT count(*) FROM sales_staging", 2);
        } finally {
            closeQuietly(state, semiState);
        }
    }

    private static void verifyPartitionGrammarRootsGenerateExecutableStatements() throws Exception {
        PostgreSQLGlobalState state = createState("partition_generator_state");
        try {
            String createPartitionedTable = generateQuery(state, "create_partitioned_table", 11L);
            if (!normalize(createPartitionedTable).contains("PARTITION BY RANGE")) {
                throw new AssertionError("Expected RANGE partitioned table, got: " + createPartitionedTable);
            }
            execute(state, createPartitionedTable);
            state.updateSchema();

            String createPartition = generateQuery(state, "create_table_partition", 29L);
            if (!normalize(createPartition).contains("PARTITION OF")) {
                throw new AssertionError("Expected partition child table, got: " + createPartition);
            }
            execute(state, createPartition);
            state.updateSchema();

            String detachPartition = generateQuery(state, "alter_table_detach_partition", 47L);
            if (!normalize(detachPartition).contains("DETACH PARTITION")) {
                throw new AssertionError("Expected detach partition statement, got: " + detachPartition);
            }
            execute(state, detachPartition);
            state.updateSchema();

            String attachPartition = generateQuery(state, "alter_table_attach_partition", 53L);
            if (!normalize(attachPartition).contains("ATTACH PARTITION")) {
                throw new AssertionError("Expected attach partition statement, got: " + attachPartition);
            }
            execute(state, attachPartition);
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

    private static String generateQuery(PostgreSQLGlobalState state, String root, long seed) {
        state.setRandomly(new Randomly(seed));
        QueryGenerator generator = new QueryGenerator(state, state.getGrammar(), root, new PostgreSQLKeyFunctionManager(state));
        return generator.getRandomQuery().toQueryString();
    }

    private static void execute(PostgreSQLGlobalState state, String sql) throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
        }
    }

    private static void requireSingleValue(PostgreSQLGlobalState state, String sql, int expected) throws Exception {
        try (Statement statement = state.getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                throw new AssertionError("No result for query: " + sql);
            }
            int actual = resultSet.getInt(1);
            if (actual != expected) {
                throw new AssertionError(String.format("Expected %d for [%s] but got %d", expected, sql, actual));
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
