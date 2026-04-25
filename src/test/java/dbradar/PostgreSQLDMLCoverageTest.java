package dbradar;

import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerator;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLKeyFunctionManager;
import dbradar.postgresql.PostgreSQLProvider;
import dbradar.postgresql.PostgreSQLOptions;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PostgreSQLDMLCoverageTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;
    private static final Pattern SECOND_INSERT_TEXT_VALUE_PATTERN = Pattern.compile("\\(-?\\d+,'([^']*)'\\)");

    private PostgreSQLDMLCoverageTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyGeneratedInsertCanEmitAndExecuteMultipleRows();
        verifyGeneratedUpdateCanAffectMultipleRows();
        verifyGeneratedDeleteCanAffectMultipleRows();
        verifyGeneratedInsertUsesDiverseCachedValues();
    }

    private static void verifyGeneratedInsertCanEmitAndExecuteMultipleRows() throws Exception {
        PostgreSQLGlobalState state = createState("dml_multi_insert_state");
        try {
            execute(state, "CREATE TABLE dml_multi_insert (c1 INT NOT NULL, c2 TEXT)");
            state.updateSchema();

            for (long seed = 1; seed <= 500; seed++) {
                state.setRandomly(new Randomly(seed));
                SQLQueryAdapter insert = PostgreSQLProvider.PostgreSQLQueryProvider.INSERT.getQuery(state);
                String sql = insert.getQueryString();
                if (isMultiRowValuesInsert(sql)) {
                    execute(state, sql);
                    requireAtLeast(state, "SELECT count(*) FROM dml_multi_insert", 2);
                    return;
                }
            }
            throw new AssertionError("Expected generated INSERT to emit a multi-row VALUES list");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyGeneratedUpdateCanAffectMultipleRows() throws Exception {
        PostgreSQLGlobalState state = createState("dml_multi_update_state");
        try {
            execute(state, "CREATE TABLE dml_multi_update (c1 INT NOT NULL, c2 INT NOT NULL)");
            execute(state, "INSERT INTO dml_multi_update (c1, c2) VALUES (1, 10), (1, 10), (1, 10)");
            state.updateSchema();

            String sql = generateQuery(state, "update_multi_row", 2001L);
            if (!normalize(sql).contains("WHERE TRUE")) {
                throw new AssertionError("Expected generated multi-row UPDATE to contain WHERE TRUE: " + sql);
            }
            int updatedRows = executeUpdate(state, sql);
            if (updatedRows < 2) {
                throw new AssertionError("Expected multi-row UPDATE to affect at least two rows but affected "
                        + updatedRows + ": " + sql);
            }
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyGeneratedDeleteCanAffectMultipleRows() throws Exception {
        PostgreSQLGlobalState state = createState("dml_multi_delete_state");
        try {
            execute(state, "CREATE TABLE dml_multi_delete (c1 INT NOT NULL, c2 TEXT)");
            execute(state, "INSERT INTO dml_multi_delete (c1, c2) VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            state.updateSchema();

            String sql = generateQuery(state, "delete_multi_row", 3001L);
            if (!normalize(sql).contains("WHERE TRUE")) {
                throw new AssertionError("Expected generated multi-row DELETE to contain WHERE TRUE: " + sql);
            }
            int deletedRows = executeUpdate(state, sql);
            if (deletedRows < 2) {
                throw new AssertionError("Expected multi-row DELETE to affect at least two rows but affected "
                        + deletedRows + ": " + sql);
            }
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyGeneratedInsertUsesDiverseCachedValues() throws Exception {
        PostgreSQLGlobalState state = createState("dml_cached_insert_state");
        try {
            execute(state, "CREATE TABLE dml_cached_insert (c1 INT NOT NULL, c2 TEXT NOT NULL)");
            state.updateSchema();

            boolean sawSameStatementDuplicate = false;
            for (long seed = 1; seed <= 40; seed++) {
                state.setRandomly(new Randomly(seed));
                SQLQueryAdapter insert = PostgreSQLProvider.PostgreSQLQueryProvider.INSERT.getQuery(state);
                String sql = insert.getQueryString();
                sawSameStatementDuplicate |= hasDuplicateAndDiverseTextValues(sql);
                execute(state, sql);
            }

            if (!sawSameStatementDuplicate) {
                throw new AssertionError("Expected one generated INSERT statement to reuse a cached c2 value while still producing diverse c2 values");
            }
            requireAtLeast(state, "SELECT count(DISTINCT c1) FROM dml_cached_insert", 2);
            requireAtLeast(state,
                    "SELECT count(*) FROM (SELECT c1 FROM dml_cached_insert GROUP BY c1 HAVING count(*) > 1) repeated",
                    1);
        } finally {
            closeQuietly(state);
        }
    }

    private static boolean isMultiRowValuesInsert(String sql) {
        String normalized = normalize(sql);
        return normalized.contains("), (") || normalized.contains("),(");
    }

    private static boolean hasDuplicateAndDiverseTextValues(String sql) {
        Matcher matcher = SECOND_INSERT_TEXT_VALUE_PATTERN.matcher(sql);
        Set<String> values = new HashSet<>();
        int valueCount = 0;
        while (matcher.find()) {
            values.add(matcher.group(1));
            valueCount++;
        }
        return valueCount > 1 && values.size() > 1 && values.size() < valueCount;
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

    private static int executeUpdate(PostgreSQLGlobalState state, String sql) throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            return statement.executeUpdate(sql);
        }
    }

    private static String generateQuery(PostgreSQLGlobalState state, String root, long seed) {
        state.setRandomly(new Randomly(seed));
        QueryGenerator generator = new QueryGenerator(state, state.getGrammar(), root, new PostgreSQLKeyFunctionManager(state));
        return generator.getRandomQuery().toQueryString();
    }

    private static void requireSingleValue(PostgreSQLGlobalState state, String sql, int expected) throws Exception {
        int actual = querySingleValue(state, sql);
        if (actual != expected) {
            throw new AssertionError(String.format("Expected %d for [%s] but got %d", expected, sql, actual));
        }
    }

    private static void requireAtLeast(PostgreSQLGlobalState state, String sql, int minimum) throws Exception {
        int actual = querySingleValue(state, sql);
        if (actual < minimum) {
            throw new AssertionError(String.format("Expected at least %d for [%s] but got %d", minimum, sql, actual));
        }
    }

    private static int querySingleValue(PostgreSQLGlobalState state, String sql) throws Exception {
        try (Statement statement = state.getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                throw new AssertionError("No result for query: " + sql);
            }
            return resultSet.getInt(1);
        }
    }

    private static String normalize(String sql) {
        return sql.replaceAll("\\s+", " ").trim().toUpperCase();
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
