package dbradar;

import com.beust.jcommander.JCommander;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLOptions;
import dbradar.postgresql.PostgreSQLProvider;
import dbradar.postgresql.PostgreSQLSchema;

import java.util.HashSet;
import java.util.Set;

public final class PostgreSQLCreateTableWidthTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;
    private static final String WIDTH_DATABASE = "task3_width_check";
    private static final String INSERT_DATABASE = "task3_insert_check";
    private static final int REQUIRED_TABLES = 5;

    private PostgreSQLCreateTableWidthTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyGeneratedTablesHaveEightToFifteenColumns();
        verifyWideTableAllowsSuccessfulInsert();
    }

    private static void verifyGeneratedTablesHaveEightToFifteenColumns() throws Exception {
        PostgreSQLGlobalState state = createState(WIDTH_DATABASE);
        Set<String> seenTables = new HashSet<>();
        try {
            int successfulTables = 0;
            int attempts = 0;
            while (successfulTables < REQUIRED_TABLES && attempts++ < 200) {
                SQLQueryAdapter createTable;
                try {
                    createTable = PostgreSQLProvider.PostgreSQLQueryProvider.CREATE_TABLE.getQuery(state);
                } catch (IgnoreMeException ignored) {
                    continue;
                }
                if (!createTable.execute(state.getConnection(), false)) {
                    continue;
                }
                state.updateSchema();
                PostgreSQLSchema.PostgreSQLTable table = findNewBaseTable(state, seenTables);
                require(table != null, "Expected each successful CREATE TABLE to add a new base table");
                int columnCount = table.getColumns().size();
                require(columnCount >= 8 && columnCount <= 15,
                        "Expected generated table " + table.getName() + " to contain 8-15 columns but observed "
                                + columnCount + " from " + createTable.getQueryString());
                seenTables.add(table.getName());
                successfulTables++;
            }
            require(successfulTables == REQUIRED_TABLES,
                    "Expected to validate " + REQUIRED_TABLES + " successful CREATE TABLE statements");
        } finally {
            state.getConnection().close();
        }
    }

    private static void verifyWideTableAllowsSuccessfulInsert() throws Exception {
        PostgreSQLGlobalState state = createState(INSERT_DATABASE);
        try {
            PostgreSQLSchema.PostgreSQLTable table = createOneTable(state);
            int columnCount = table.getColumns().size();
            require(columnCount >= 8 && columnCount <= 15,
                    "Expected INSERT smoke table to contain 8-15 columns but observed " + columnCount);

            boolean inserted = false;
            for (int attempt = 0; attempt < 100; attempt++) {
                SQLQueryAdapter insert;
                try {
                    insert = PostgreSQLProvider.PostgreSQLQueryProvider.INSERT.getQuery(state);
                } catch (IgnoreMeException ignored) {
                    continue;
                }
                if (insert.execute(state.getConnection(), false)) {
                    inserted = true;
                    break;
                }
            }
            require(inserted, "Expected at least one generated INSERT to succeed for a wide table");
        } finally {
            state.getConnection().close();
        }
    }

    private static PostgreSQLSchema.PostgreSQLTable createOneTable(PostgreSQLGlobalState state) throws Exception {
        Set<String> seenTables = new HashSet<>();
        for (int attempt = 0; attempt < 100; attempt++) {
            SQLQueryAdapter createTable;
            try {
                createTable = PostgreSQLProvider.PostgreSQLQueryProvider.CREATE_TABLE.getQuery(state);
            } catch (IgnoreMeException ignored) {
                continue;
            }
            if (!createTable.execute(state.getConnection(), false)) {
                continue;
            }
            state.updateSchema();
            PostgreSQLSchema.PostgreSQLTable table = findNewBaseTable(state, seenTables);
            if (table != null) {
                return table;
            }
        }
        throw new AssertionError("Expected to create at least one PostgreSQL table");
    }

    private static PostgreSQLSchema.PostgreSQLTable findNewBaseTable(PostgreSQLGlobalState state, Set<String> seenTables) {
        for (PostgreSQLSchema.PostgreSQLTable table : state.getSchema().getDatabaseTablesWithoutViews()) {
            if (!seenTables.contains(table.getName())) {
                return table;
            }
        }
        return null;
    }

    private static PostgreSQLGlobalState createState(String databaseName) throws Exception {
        MainOptions options = new MainOptions();
        DBMSExecutorFactory<?> executorFactory = new DBMSExecutorFactory<>(new PostgreSQLProvider(), options);
        JCommander.newBuilder()
                .addObject(options)
                .addCommand("postgresql", executorFactory.getCommand())
                .build()
                .parse("--host", HOST, "--port", String.valueOf(PORT), "--username", USERNAME, "--password", PASSWORD,
                        "postgresql");

        PostgreSQLGlobalState state = new PostgreSQLGlobalState();
        Randomly.initialize(options);
        state.setMainOptions(options);
        state.setDbmsSpecificOptions((PostgreSQLOptions) executorFactory.getCommand());
        state.setRandomly(new Randomly(1));
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase());
        state.updateSchema();
        return state;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
