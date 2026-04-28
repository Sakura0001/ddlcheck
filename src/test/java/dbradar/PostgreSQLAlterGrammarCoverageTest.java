package dbradar;

import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLProvider;
import dbradar.postgresql.PostgreSQLOptions;

import java.sql.Statement;
import java.util.Locale;

public final class PostgreSQLAlterGrammarCoverageTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;

    private PostgreSQLAlterGrammarCoverageTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyAlterTableSubrootsGenerateCompleteExecutableSql();
        verifyLowRiskAlterObjectRootsGenerateExecutableSql();
    }

    private static void verifyAlterTableSubrootsGenerateCompleteExecutableSql() throws Exception {
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ADD_COLUMN,
                "alter_table_add_column_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_DROP_COLUMN,
                "alter_table_drop_column_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_TYPE,
                "alter_table_type_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT,
                "alter_table_drop_default_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT,
                "alter_table_set_default_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_NOT_NULL,
                "alter_table_set_not_null_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL,
                "alter_table_drop_not_null_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_SET_COLUMN,
                "alter_table_set_column_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_RESET_COLUMN,
                "alter_table_reset_column_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_STORAGE,
                "alter_table_storage_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ADD_UNIQUE_KEY,
                "alter_table_unique_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ADD_PRIMARY_KEY,
                "alter_table_primary_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ADD_FOREIGN_KEY,
                "alter_table_foreign_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_OPTION,
                "alter_table_option_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_RENAME_TABLE,
                "alter_table_rename_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_RENAME_COLUMN,
                "alter_table_rename_column_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_CHANGE_COLUMN,
                "alter_table_change_column_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_MODIFY_COLUMN,
                "alter_table_modify_column_state");
        verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ADD_INDEX,
                "alter_table_add_index_state", "CREATE INDEX ");
        verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_DROP_INDEX,
                "alter_table_drop_index_state", "DROP INDEX ");
        verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_RENAME_INDEX,
                "alter_table_rename_index_state", "ALTER INDEX ");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_DROP_PRIMARY_KEY,
                "alter_table_drop_primary_key_state");
        verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_TABLE_ADD_CHECK,
                "alter_table_add_check_state");
    }

    private static void verifyLowRiskAlterObjectRootsGenerateExecutableSql() throws Exception {
        verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_INDEX, "alter_index_state",
                "ALTER INDEX ");
        verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_VIEW, "alter_view_state",
                "ALTER VIEW ");
        verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_SEQUENCE, "alter_sequence_state",
                "ALTER SEQUENCE ");
    }

    private static void verifyExecutableAlterTable(PostgreSQLProvider.PostgreSQLQueryProvider provider,
                                                   String databaseName) throws Exception {
        verifyExecutableRoot(provider, databaseName, "ALTER TABLE ");
    }

    private static void verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider provider, String databaseName,
                                             String expectedPrefix) throws Exception {
        PostgreSQLGlobalState state = createState(databaseName);
        try {
            bootstrapObjects(state, expectedPrefix.equals("ALTER VIEW "));
            SQLQueryAdapter query = null;
            for (long seed = 1; seed <= 100; seed++) {
                state.setRandomly(new Randomly(seed));
                try {
                    query = provider.getQuery(state);
                    break;
                } catch (IgnoreMeException | QueryGenerationException ignored) {
                }
            }
            require(query != null, "Expected provider to generate SQL: " + provider);

            String sql = query.getQueryString();
            String normalized = normalize(sql);
            require(normalized.startsWith(expectedPrefix),
                    "Expected " + provider + " to generate complete SQL starting with " + expectedPrefix + ": " + sql);
            require(!normalized.matches(".*ALTER TABLE .* ALTER TABLE .*"),
                    "ALTER TABLE SQL must not contain a nested ALTER TABLE statement: " + sql);

            execute(state, sql);
        } finally {
            closeQuietly(state);
        }
    }

    private static void bootstrapObjects(PostgreSQLGlobalState state, boolean includeView) throws Exception {
        execute(state, "CREATE TABLE alter_base (c1 INT NOT NULL, c2 INT NOT NULL, c3 INT, c4 TEXT)");
        execute(state, "CREATE TABLE alter_ref (c1 INT PRIMARY KEY, c2 INT NOT NULL, c3 INT)");
        execute(state, "CREATE TABLE alter_pk_base (c1 INT PRIMARY KEY, c2 INT NOT NULL, c3 INT)");
        execute(state, "CREATE UNIQUE INDEX alter_base_c2_idx ON alter_base (c2)");
        execute(state, "CREATE UNIQUE INDEX alter_base_c3_idx ON alter_base (c3)");
        if (includeView) {
            execute(state, "CREATE TABLE alter_view_base (c1 INT NOT NULL, c2 INT NOT NULL, c3 INT)");
            execute(state, "CREATE VIEW alter_view AS SELECT c1, c2, c3 FROM alter_view_base");
        }
        execute(state, "CREATE SEQUENCE alter_seq");
        state.updateSchema();
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

    private static String normalize(String sql) {
        return sql.replaceAll("\\s+", " ").trim().toUpperCase(Locale.ROOT);
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

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
