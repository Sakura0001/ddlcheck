package dbradar.cockroachdb;

import com.google.auto.service.AutoService;
import dbradar.DatabaseProvider;
import dbradar.GlobalState;
import dbradar.IgnoreMeException;
import dbradar.MainOptions;
import dbradar.QueryManager;
import dbradar.QueryProvider;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.SQLGlobalState;
import dbradar.SQLProviderAdapter;
import dbradar.StatementExecutor;
import dbradar.common.query.QueryConfig;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AutoService(DatabaseProvider.class)
public class CockroachDBProvider extends SQLProviderAdapter {

    public CockroachDBProvider() {
        super(CockroachDBGlobalState.class, CockroachDBOptions.class);
    }

    public enum CockroachDBQueryProvider implements QueryProvider {
        CREATE_TABLE("create_table", true),
        CREATE_INDEX("create_index", true),
        CREATE_VIEW("create_view", true),
        SHOW_TABLES("show_tables", false),
        TRUNCATE_TABLE("truncate", true),
        ALTER_TABLE("alter_table", true),
        ALTER_TABLE_ADD_COLUMN("alter_table_add_column", true),
        ALTER_TABLE_DROP_COLUMN("alter_table_drop_column", true),
        ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT("alter_table_alter_column_set_default", true),
        ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT("alter_table_alter_column_drop_default", true),
        ALTER_TABLE_ALTER_COLUMN_SET_VISIBLE("alter_table_alter_column_set_visible", true),
        ALTER_TABLE_ALTER_COLUMN_SET_INVISIBLE("alter_table_alter_column_set_invisible", true),
        ALTER_TABLE_CHANGE_COLUMN("alter_table_change_column", true),
        ALTER_TABLE_MODIFY_COLUMN("alter_table_modify_column", true),
        ALTER_TABLE_RENAME_COLUMN("alter_table_rename_column", true),
        ALTER_TABLE_ADD_PRIMARY_KEY("alter_table_add_primary_key", true),
        ALTER_TABLE_ADD_UNIQUE_KEY("alter_table_add_unique_key", true),
        ALTER_TABLE_ADD_FOREIGN_KEY("alter_table_add_foreign_key", true),
        ALTER_TABLE_RENAME_TABLE("alter_table_rename_table", true),
        DROP_TABLE("drop_table", true),
        DROP_INDEX("drop_index", true),
        DROP_VIEW("drop_view", true),
        INSERT("insert", false),
        UPSERT("upsert", false),
        UPDATE("update", false),
        DELETE("delete", false),
        SET_VARIABLE("set_variable", false),
        ANALYZE_TABLE("analyze_table", false),
        RESET("reset", false),
        SELECT("select", false),
        TRANSACTION_START("begin", false),
        TRANSACTION_ROLLBACK("rollback", true),
        TRANSACTION_COMMIT("commit", true),
        TRANSACTION_ISOLATION("set_isolation", true);

        private final QueryConfig queryConfig;

        CockroachDBQueryProvider(String queryRoot, boolean canAffectSchema) {
            this.queryConfig = new QueryConfig(queryRoot, canAffectSchema);
        }

        @Override
        public SQLQueryAdapter getQuery(GlobalState globalState) {
            CockroachDBGlobalState state = (CockroachDBGlobalState) globalState;
            QueryGenerator queryGenerator = new QueryGenerator(state, state.getGrammar(), queryConfig.getQueryRoot(),
                    new CockroachDBKeyFunctionManager(state));

            SQLQueryAdapter query = new SQLQueryAdapter(queryGenerator.getRandomQuery());
            query.setCanAffectSchema(queryConfig.canAffectSchema());
            return query;
        }

        @Override
        public boolean canBeRetried() {
            return queryConfig.canBeRetried();
        }
    }

    private static Map<QueryProvider, Integer> buildQueryWeights(GlobalState state) {
        CockroachDBGlobalState globalState = (CockroachDBGlobalState) state;

        Map<QueryProvider, Integer> queryWeights = new HashMap<>();
        queryWeights.put(CockroachDBQueryProvider.CREATE_TABLE, 0);
        queryWeights.put(CockroachDBQueryProvider.CREATE_INDEX, 5);
        queryWeights.put(CockroachDBQueryProvider.CREATE_VIEW, 5);
        queryWeights.put(CockroachDBQueryProvider.SHOW_TABLES, 2);
        queryWeights.put(CockroachDBQueryProvider.TRUNCATE_TABLE, 2);
        queryWeights.put(CockroachDBQueryProvider.ALTER_TABLE, 5);
        queryWeights.put(CockroachDBQueryProvider.DROP_INDEX, 5);
        queryWeights.put(CockroachDBQueryProvider.DROP_VIEW, 5);
        queryWeights.put(CockroachDBQueryProvider.INSERT, globalState.getOptions().getMaxNumberInserts());
        queryWeights.put(CockroachDBQueryProvider.UPSERT, globalState.getOptions().getMaxNumberInserts());
        queryWeights.put(CockroachDBQueryProvider.UPDATE, 20);
        queryWeights.put(CockroachDBQueryProvider.DELETE, 5);
        queryWeights.put(CockroachDBQueryProvider.SET_VARIABLE, 5);
        queryWeights.put(CockroachDBQueryProvider.ANALYZE_TABLE, 1);
        queryWeights.put(CockroachDBQueryProvider.RESET, 1);
        queryWeights.put(CockroachDBQueryProvider.TRANSACTION_START, 0);
        queryWeights.put(CockroachDBQueryProvider.TRANSACTION_COMMIT, 0);
        queryWeights.put(CockroachDBQueryProvider.TRANSACTION_ROLLBACK, 0);
        queryWeights.put(CockroachDBQueryProvider.TRANSACTION_ISOLATION, 0);

        return queryWeights;
    }

    public enum CockroachDBDDLStmt {

        CREATE_TABLE(CockroachDBQueryProvider.CREATE_TABLE),
        CREATE_INDEX(CockroachDBQueryProvider.CREATE_INDEX),
        CREATE_VIEW(CockroachDBQueryProvider.CREATE_VIEW),
        ALTER_TABLE_ADD_COLUMN(CockroachDBQueryProvider.ALTER_TABLE_ADD_COLUMN),
        ALTER_TABLE_DROP_COLUMN(CockroachDBQueryProvider.ALTER_TABLE_DROP_COLUMN),
        ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT(CockroachDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT),
        ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT(CockroachDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT),
        ALTER_TABLE_ALTER_COLUMN_SET_VISIBLE(CockroachDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_VISIBLE),
        ALTER_TABLE_ALTER_COLUMN_SET_INVISIBLE(CockroachDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_INVISIBLE),
        ALTER_TABLE_CHANGE_COLUMN(CockroachDBQueryProvider.ALTER_TABLE_CHANGE_COLUMN),
        ALTER_TABLE_MODIFY_COLUMN(CockroachDBQueryProvider.ALTER_TABLE_MODIFY_COLUMN),
        ALTER_TABLE_RENAME_COLUMN(CockroachDBQueryProvider.ALTER_TABLE_RENAME_COLUMN),
        ALTER_TABLE_ADD_PRIMARY_KEY(CockroachDBQueryProvider.ALTER_TABLE_ADD_PRIMARY_KEY),
        ALTER_TABLE_ADD_UNIQUE_KEY(CockroachDBQueryProvider.ALTER_TABLE_ADD_UNIQUE_KEY),
        ALTER_TABLE_ADD_FOREIGN_KEY(CockroachDBQueryProvider.ALTER_TABLE_ADD_FOREIGN_KEY),
        ALTER_TABLE_RENAME_TABLE(CockroachDBQueryProvider.ALTER_TABLE_RENAME_TABLE),
        TRUNCATE_TABLE(CockroachDBQueryProvider.TRUNCATE_TABLE),
        DROP_TABLE(CockroachDBQueryProvider.DROP_TABLE),
        //        DROP_INDEX(CockroachDBQueryProvider.DROP_INDEX), // https://www.cockroachlabs.com/docs/stable/drop-index#remove-an-index-with-no-dependencies
        DROP_VIEW(CockroachDBQueryProvider.DROP_VIEW);

        private final CockroachDBQueryProvider queryProvider;

        CockroachDBDDLStmt(CockroachDBQueryProvider queryProvider) {
            this.queryProvider = queryProvider;
        }

        public CockroachDBQueryProvider getQueryProvider() {
            return queryProvider;
        }

        public static CockroachDBQueryProvider getRandomDDL() {
            return Randomly.fromOptions(CockroachDBDDLStmt.values()).getQueryProvider();
        }

    }

    public enum CockroachDBDMLStmt {

        INSERT(CockroachDBQueryProvider.INSERT, 10),
        UPDATE(CockroachDBQueryProvider.UPDATE, 10),
        UPSERT(CockroachDBQueryProvider.UPSERT, 10),
        DELETE(CockroachDBQueryProvider.DELETE, 1);

        private final CockroachDBQueryProvider queryProvider;
        private final int weight;
        private static final List<CockroachDBDMLStmt> weightedMap = new ArrayList<>();

        CockroachDBDMLStmt(CockroachDBQueryProvider queryProvider, int weight) {
            this.queryProvider = queryProvider;
            this.weight = weight;
        }

        private SQLQueryAdapter getQuery(CockroachDBGlobalState state) {
            return queryProvider.getQuery(state);
        }

        public static SQLQueryAdapter getRandomDML(CockroachDBGlobalState state) {
            if (weightedMap.isEmpty()) {
                for (CockroachDBDMLStmt dmlStmt : CockroachDBDMLStmt.values()) {
                    for (int i = 0; i < dmlStmt.weight; i++) {
                        weightedMap.add(dmlStmt);
                    }
                }
            }

            return Randomly.fromList(weightedMap).getQuery(state);
        }
    }

    public enum DatabaseInitStmt {
        CREATE_TABLE(CockroachDBQueryProvider.CREATE_TABLE),
        CREATE_INDEX(CockroachDBQueryProvider.CREATE_INDEX),
        INSERT(CockroachDBQueryProvider.INSERT);

        private CockroachDBQueryProvider queryProvider;

        DatabaseInitStmt(CockroachDBQueryProvider queryProvider) {
            this.queryProvider = queryProvider;
        }

        public SQLQueryAdapter getQuery(CockroachDBGlobalState state) {
            return queryProvider.getQuery(state);
        }
    }


    public static class CockroachDBGlobalState extends SQLGlobalState {
        private static final String DEFAULT_GENERATOR_CONFIG_PATH = "dbradar/cockroachdb/cockroachdb.zz.lua";
        private static final String DEFAULT_GRAMMAR_PATH = "dbradar/cockroachdb/cockroachdb.grammar.yy";

        public CockroachDBGlobalState() {
            setGrammarPath(DEFAULT_GRAMMAR_PATH);
            // todo add error pattern using regex
            setRegexErrorTypes(new HashMap<>());
        }

        @Override
        protected CockroachDBSchema readSchema() throws SQLException {
            return CockroachDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

        @Override
        public String getGeneratorConfigPath() {
            String generatorConfigPath = null;
            if (getOptions() != null) {
                generatorConfigPath = getOptions().getGeneratorConfigPath();
            }
            if (generatorConfigPath == null) { // load default generator config path
                generatorConfigPath = DEFAULT_GENERATOR_CONFIG_PATH;
            }

            return generatorConfigPath;
        }

        public SQLConnection createDatabase() throws SQLException {
            String username = getOptions().getUserName();
            String password = getOptions().getPassword();
            String host = getOptions().getHost();
            int port = getOptions().getPort();
            String databaseName = getDatabaseName();

            if (!getDbmsSpecificOptions().useEquation()) {
                getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
                getState().logStatement("CREATE DATABASE " + databaseName);
                getState().logStatement(String.format("USE %s;", databaseName));
            }

            return createDatabase(host, port, username, password, databaseName);
        }

        public SQLConnection createDatabase(String host, int port, String username, String password, String databaseName) throws SQLException {
            String url = String.format("jdbc:postgresql://%s:%d/postgres", host, port);
            try (Connection conn = DriverManager.getConnection(url, username, password); Statement statement = conn.createStatement()) {
                statement.execute("DROP DATABASE IF EXISTS " + databaseName);
                statement.execute("CREATE DATABASE " + databaseName);
            }

            url = String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName);
            Connection conn = DriverManager.getConnection(url, username, password);

            return new SQLConnection(conn);
        }

        /**
         * Create a connection to an existing database, and do not delete it
         */
        public SQLConnection createConnection() throws SQLException {
            String username = getOptions().getUserName();
            String password = getOptions().getPassword();
            String host = getOptions().getHost();
            int port = getOptions().getPort();
            if (host == null) {
                host = CockroachDBOptions.DEFAULT_HOST;
            }
            if (port == MainOptions.NO_SET_PORT) {
                port = CockroachDBOptions.DEFAULT_PORT;
            }
            String databaseName = getDatabaseName();
            return createConnection(host, port, username, password, databaseName);
        }

        /**
         * Create a connection to an existing database, and do not delete it
         */
        public SQLConnection createConnection(String host, int port, String username, String password, String databaseName) throws SQLException {
            String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName);
            Connection conn = DriverManager.getConnection(url, username, password);
            return new SQLConnection(conn);
        }

        @Override
        public CockroachDBSchema getSchema() {
            return (CockroachDBSchema) super.getSchema();
        }

        @Override
        public CockroachDBOptions getDbmsSpecificOptions() {
            return (CockroachDBOptions) super.getDbmsSpecificOptions();
        }
    }

    @Override
    public void generateDatabase(GlobalState globalState) throws Exception {
        CockroachDBGlobalState state = (CockroachDBGlobalState) globalState;

        QueryManager manager = state.getManager();
        List<String> standardSettings = new ArrayList<>();
        standardSettings.add("--Don't send automatic bug reports");
        standardSettings.add("SET CLUSTER SETTING debug.panic_on_failed_assertions = true;");
        standardSettings.add("SET CLUSTER SETTING diagnostics.reporting.enabled    = false;");
        standardSettings.add("SET CLUSTER SETTING diagnostics.reporting.send_crash_reports = false;");

        standardSettings.add("-- Disable the collection of metrics and hope that it helps performance");
        standardSettings.add("SET CLUSTER SETTING sql.metrics.statement_details.enabled = 'off'");
        standardSettings.add("SET CLUSTER SETTING sql.metrics.statement_details.plan_collection.enabled = 'off'");
        standardSettings.add("SET CLUSTER SETTING sql.stats.automatic_collection.enabled = 'off'");
        standardSettings.add("SET CLUSTER SETTING timeseries.storage.enabled = 'off'");

        if (state.getDbmsSpecificOptions().testHashIndexes) {
            standardSettings.add("set experimental_enable_hash_sharded_indexes='on';");
        }
        if (state.getDbmsSpecificOptions().testTempTables) {
            standardSettings.add("SET experimental_enable_temp_tables = 'on'");
        }
        for (String s : standardSettings) {
            manager.execute(new SQLQueryAdapter(s));
        }

        if (state.getDbmsSpecificOptions().useEquation()) {
            return;
        }


        for (int i = 0; i < Randomly.fromOptions(2, 3); i++) {
            boolean success = false;
            do {
                try {
                    SQLQueryAdapter q = CockroachDBQueryProvider.CREATE_TABLE.getQuery(state);
                    success = state.executeStatement(q);
                } catch (IgnoreMeException e) {
                    // continue trying
                }
            } while (!success);
        }

        StatementExecutor se = new StatementExecutor(globalState, buildQueryWeights(globalState), (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(GlobalState state) throws SQLException {
        CockroachDBGlobalState globalState = (CockroachDBGlobalState) state;
        return globalState.createDatabase();
    }

    @Override
    public String getDBMSName() {
        return "cockroachdb";
    }

}
