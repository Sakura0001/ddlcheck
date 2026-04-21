package dbradar.mariadb;

import com.google.auto.service.AutoService;
import dbradar.DatabaseProvider;
import dbradar.GlobalState;
import dbradar.MainOptions;
import dbradar.QueryProvider;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.SQLGlobalState;
import dbradar.SQLProviderAdapter;
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

@AutoService(DatabaseProvider.class)
public class MariaDBProvider extends SQLProviderAdapter {

    public MariaDBProvider() {
        super(MariaDBGlobalState.class, MariaDBOptions.class);
    }

    public enum MariaDBQueryProvider implements QueryProvider {
        CREATE_TABLE("create_table", true),
        CREATE_INDEX("create_index", true),
        CREATE_VIEW("create_view", true),
        SHOW_TABLES("show_tables", false),
        CHECK_TABLE("check_table", false),
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
        ALTER_TABLE_ADD_INDEX("alter_table_add_index", true),
        ALTER_TABLE_DROP_INDEX("alter_table_drop_index", true),
        ALTER_TABLE_RENAME_INDEX("alter_table_rename_index", true),
        ALTER_TABLE_ADD_PRIMARY_KEY("alter_table_add_primary_key", true),
        ALTER_TABLE_DROP_PRIMARY_KEY("alter_table_drop_primary_key", true),
        ALTER_TABLE_ADD_UNIQUE_KEY("alter_table_add_unique_key", true),
        ALTER_TABLE_ADD_FOREIGN_KEY("alter_table_add_foreign_key", true),
        ALTER_TABLE_RENAME_TABLE("alter_table_rename_table", true),
        ALTER_TABLE_OPTION("alter_table_option", true),
        RENAME_TABLE("rename_table", true),
        DROP_TABLE("drop_table", true),
        DROP_INDEX("drop_index", true),
        DROP_VIEW("drop_view", true),
        INSERT("insert", false),
        REPLACE("replace", false),
        UPDATE("update", false),
        DELETE("delete", false),
        SET_VARIABLE("set_variable", false),
        REPAIR("repair", false),
        OPTIMIZE("optimize", false),
        CHECKSUM("checksum", false),
        ANALYZE_TABLE("analyze_table", false),
        FLUSH("flush", false),
        RESET("reset", false),
        SELECT("select", false),
        TRANSACTION_START("begin", false),
        TRANSACTION_ROLLBACK("rollback", true),
        TRANSACTION_COMMIT("commit", true),
        TRANSACTION_ISOLATION("set_isolation", true);

        private final QueryConfig queryConfig;

        MariaDBQueryProvider(String queryRoot, boolean canAffectSchema) {
            this.queryConfig = new QueryConfig(queryRoot, canAffectSchema);
        }

        @Override
        public SQLQueryAdapter getQuery(GlobalState globalState) {
            MariaDBGlobalState state = (MariaDBGlobalState) globalState;
            QueryGenerator queryGenerator = new QueryGenerator(state, state.getGrammar(), queryConfig.getQueryRoot(),
                    new MariaDBKeyFuncManager(state));

            SQLQueryAdapter query = new SQLQueryAdapter(queryGenerator.getRandomQuery());
            query.setCanAffectSchema(queryConfig.canAffectSchema());
            return query;
        }

        @Override
        public boolean canBeRetried() {
            return queryConfig.canBeRetried();
        }
    }

    public enum MariaDBDDLStmt {

        CREATE_TABLE(MariaDBQueryProvider.CREATE_TABLE),
        CREATE_INDEX(MariaDBQueryProvider.CREATE_INDEX),
        CREATE_VIEW(MariaDBQueryProvider.CREATE_VIEW),
        ALTER_TABLE_ADD_COLUMN(MariaDBQueryProvider.ALTER_TABLE_ADD_COLUMN),
        ALTER_TABLE_DROP_COLUMN(MariaDBQueryProvider.ALTER_TABLE_DROP_COLUMN),
        ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT(MariaDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT),
        ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT(MariaDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT),
        ALTER_TABLE_ALTER_COLUMN_SET_VISIBLE(MariaDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_VISIBLE),
        ALTER_TABLE_ALTER_COLUMN_SET_INVISIBLE(MariaDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_INVISIBLE),
        ALTER_TABLE_CHANGE_COLUMN(MariaDBQueryProvider.ALTER_TABLE_CHANGE_COLUMN),
        ALTER_TABLE_MODIFY_COLUMN(MariaDBQueryProvider.ALTER_TABLE_MODIFY_COLUMN),
        ALTER_TABLE_RENAME_COLUMN(MariaDBQueryProvider.ALTER_TABLE_RENAME_COLUMN),
        ALTER_TABLE_ADD_INDEX(MariaDBQueryProvider.ALTER_TABLE_ADD_INDEX),
        ALTER_TABLE_DROP_INDEX(MariaDBQueryProvider.ALTER_TABLE_DROP_INDEX),
        ALTER_TABLE_RENAME_INDEX(MariaDBQueryProvider.ALTER_TABLE_RENAME_INDEX),
        ALTER_TABLE_ADD_PRIMARY_KEY(MariaDBQueryProvider.ALTER_TABLE_ADD_PRIMARY_KEY),
        ALTER_TABLE_DROP_PRIMARY_KEY(MariaDBQueryProvider.ALTER_TABLE_DROP_PRIMARY_KEY),
        ALTER_TABLE_ADD_UNIQUE_KEY(MariaDBQueryProvider.ALTER_TABLE_ADD_UNIQUE_KEY),
        ALTER_TABLE_ADD_FOREIGN_KEY(MariaDBQueryProvider.ALTER_TABLE_ADD_FOREIGN_KEY),
        ALTER_TABLE_RENAME_TABLE(MariaDBQueryProvider.ALTER_TABLE_RENAME_TABLE),
        RENAME_TABLE(MariaDBQueryProvider.RENAME_TABLE),
        ALTER_TABLE_OPTION(MariaDBQueryProvider.ALTER_TABLE_OPTION),
        TRUNCATE_TABLE(MariaDBQueryProvider.TRUNCATE_TABLE),
        DROP_TABLE(MariaDBQueryProvider.DROP_TABLE),
        DROP_INDEX(MariaDBQueryProvider.DROP_INDEX),
        DROP_VIEW(MariaDBQueryProvider.DROP_VIEW);

        private final MariaDBQueryProvider queryProvider;

        MariaDBDDLStmt(MariaDBQueryProvider queryProvider) {
            this.queryProvider = queryProvider;
        }

        public MariaDBQueryProvider getQueryProvider() {
            return queryProvider;
        }

    }

    public enum MariaDBDMLStmt {

        INSERT(MariaDBQueryProvider.INSERT, 10),
        UPDATE(MariaDBQueryProvider.UPDATE, 10),
        REPLACE(MariaDBQueryProvider.REPLACE, 10),
        DELETE(MariaDBQueryProvider.DELETE, 1);

        private final MariaDBQueryProvider queryProvider;
        private final int weight;
        private static final List<MariaDBDMLStmt> weightedMap = new ArrayList<>();

        MariaDBDMLStmt(MariaDBQueryProvider queryProvider, int weight) {
            this.queryProvider = queryProvider;
            this.weight = weight;
        }

        public SQLQueryAdapter getQuery(MariaDBGlobalState state) {
            return queryProvider.getQuery(state);
        }

        public static SQLQueryAdapter getRandomDML(MariaDBGlobalState state) {
            if (weightedMap.isEmpty()) {
                for (MariaDBDMLStmt dmlStmt : MariaDBDMLStmt.values()) {
                    for (int i = 0; i < dmlStmt.weight; i++) {
                        weightedMap.add(dmlStmt);
                    }
                }
            }

            return Randomly.fromList(weightedMap).getQuery(state);
        }
    }

    @Override
    public void generateDatabase(GlobalState state) throws Exception {
    }

    public static class MariaDBGlobalState extends SQLGlobalState {

        private static final String DEFAULT_GENERATOR_CONFIG_PATH = "dbradar/mariadb/mariadb.zz.lua";

        private static final String DEFAULT_GRAMMAR_PATH = "dbradar/mariadb/mariadb.grammar.yy";

        public MariaDBGlobalState() {
            setGrammarPath(DEFAULT_GRAMMAR_PATH);
            // todo add error pattern using regex
            setRegexErrorTypes(new HashMap<>());
        }

        @Override
        protected MariaDBSchema readSchema() throws SQLException {
            return MariaDBSchema.fromConnection(getConnection(), getDatabaseName());
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

        @Override
        public SQLConnection createDatabase() throws SQLException {
            String username = getOptions().getUserName();
            String password = getOptions().getPassword();
            String host = getOptions().getHost();
            int port = getOptions().getPort();
            if (host == null) {
                host = MariaDBOptions.DEFAULT_HOST;
            }
            if (port == MainOptions.NO_SET_PORT) {
                port = MariaDBOptions.DEFAULT_PORT;
            }
            String databaseName = getDatabaseName();

            if (!getDbmsSpecificOptions().useEquation()) {
                getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
                getState().logStatement("CREATE DATABASE " + databaseName);
                getState().logStatement("USE " + databaseName);
            }

            return createDatabase(host, port, username, password, databaseName);
        }

        public SQLConnection createDatabase(String host, int port, String username, String password, String databaseName) throws SQLException {
            String url = String.format("jdbc:mariadb://%s:%d",
                    host, port);
            Connection conn = DriverManager.getConnection(url, username, password);
            try (Statement statement = conn.createStatement()) {
                statement.execute("DROP DATABASE IF EXISTS " + databaseName);
                statement.execute("CREATE DATABASE " + databaseName);
                statement.execute("USE " + databaseName);
            }
            return new SQLConnection(conn);
        }

        public MariaDBOptions getDbmsSpecificOptions() {
            return (MariaDBOptions) super.getDbmsSpecificOptions();
        }

        @Override
        public MariaDBSchema getSchema() {
            return (MariaDBSchema) super.getSchema();
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
                host = MariaDBOptions.DEFAULT_HOST;
            }
            if (port == MainOptions.NO_SET_PORT) {
                port = MariaDBOptions.DEFAULT_PORT;
            }
            String databaseName = getDatabaseName();
            return createConnection(host, port, username, password, databaseName);
        }

        /**
         * Create a connection to an existing database, and do not delete it
         */
        public SQLConnection createConnection(String host, int port, String username, String password, String databaseName) throws SQLException {
            String url = String.format("jdbc:mariadb://%s:%d/%s", host, port, databaseName);
            Connection conn = DriverManager.getConnection(url, username, password);
            return new SQLConnection(conn);
        }
    }

    @Override
    public SQLConnection createDatabase(GlobalState state) throws SQLException {
        MariaDBGlobalState globalState = (MariaDBGlobalState) state;
        return globalState.createDatabase();
    }

    @Override
    public String getDBMSName() {
        return "mariadb";
    }

}
