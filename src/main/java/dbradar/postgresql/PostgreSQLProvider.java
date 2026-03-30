package dbradar.postgresql;

import com.google.auto.service.AutoService;
import dbradar.DatabaseProvider;
import dbradar.GlobalState;
import dbradar.IgnoreMeException;
import dbradar.QueryProvider;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.SQLProviderAdapter;
import dbradar.StatementExecutor;
import dbradar.common.query.DBRadarResultSet;
import dbradar.common.query.QueryConfig;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AutoService(DatabaseProvider.class)
public class PostgreSQLProvider extends SQLProviderAdapter {

    /**
     * Generate only data types and expressions that are understood by PQS.
     */

    protected String extensionsList;

    public PostgreSQLProvider() {
        super(PostgreSQLGlobalState.class, PostgreSQLOptions.class);
    }

    protected PostgreSQLProvider(Class<PostgreSQLGlobalState> globalClass, Class<PostgreSQLOptions> optionClass) {
        super(globalClass, optionClass);
    }

    public enum PostgreSQLQueryProvider implements QueryProvider {
        CREATE_TABLE("create_table", true),
        CREATE_INDEX("create_index", true),
        CREATE_VIEW("create_view", true),
        SHOW_TABLES("show_tables", false),
        TRUNCATE_TABLE("truncate", true),
        ALTER_TABLE("alter_table", true),
        ALTER_TABLE_ADD_COLUMN("alter_table_add_column", true),
        ALTER_TABLE_DROP_COLUMN("alter_table_drop_column", true),
        ALTER_TABLE_ALTER_COLUMN_TYPE("alter_table_alter_column_type", true),
        ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT("alter_table_alter_column_drop_default", true),
        ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT("alter_table_alter_column_set_default", true),
        ALTER_TABLE_ALTER_COLUMN_SET_NOT_NULL("alter_table_alter_column_set_not_null", true),
        ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL("alter_table_alter_column_drop_not_null", true),
        ALTER_TABLE_SET_COLUMN("alter_table_set_column", true),
        ALTER_TABLE_RESET_COLUMN("alter_table_reset_column", true),
        ALTER_TABLE_ALTER_COLUMN_SET_STORAGE("alter_table_alter_column_set_storage", true),
        ALTER_TABLE_ADD_UNIQUE_KEY("alter_table_add_unique_key", true),
        ALTER_TABLE_ADD_PRIMARY_KEY("alter_table_add_primary_key", true),
        ALTER_TABLE_ADD_FOREIGN_KEY("alter_table_add_foreign_key", true),
        ALTER_TABLE_OPTION("alter_table_option", true),
        ALTER_TABLE_RENAME_TABLE("alter_table_rename_table", true),
        REINDEX("reindex", true),
        DROP_TABLE("drop_table", true),
        DROP_INDEX("drop_index", true),
        DROP_VIEW("drop_view", true),
        INSERT("insert", false),
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

        PostgreSQLQueryProvider(String queryRoot, boolean canAffectSchema) {
            this.queryConfig = new QueryConfig(queryRoot, canAffectSchema);
        }

        PostgreSQLQueryProvider(String queryRoot, boolean canAffectSchema, boolean canBeRetried) {
            this.queryConfig = new QueryConfig(queryRoot, canAffectSchema, canBeRetried);
        }

        @Override
        public SQLQueryAdapter getQuery(GlobalState globalState) {
            PostgreSQLGlobalState state = (PostgreSQLGlobalState) globalState;
            QueryGenerator queryGenerator = new QueryGenerator(state, state.getGrammar(), queryConfig.getQueryRoot(),
                    new PostgreSQLKeyFunctionManager(state));

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
        PostgreSQLGlobalState globalState = (PostgreSQLGlobalState) state;

        Map<QueryProvider, Integer> queryWeights = new HashMap<>();
        queryWeights.put(PostgreSQLQueryProvider.CREATE_TABLE, 0);
        queryWeights.put(PostgreSQLQueryProvider.CREATE_INDEX, 5);
        queryWeights.put(PostgreSQLQueryProvider.CREATE_VIEW, 5);
        queryWeights.put(PostgreSQLQueryProvider.SHOW_TABLES, 2);
        queryWeights.put(PostgreSQLQueryProvider.TRUNCATE_TABLE, 2);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE, 5);
        queryWeights.put(PostgreSQLQueryProvider.REINDEX, 5);
        queryWeights.put(PostgreSQLQueryProvider.DROP_INDEX, 5);
        queryWeights.put(PostgreSQLQueryProvider.DROP_VIEW, 5);
        queryWeights.put(PostgreSQLQueryProvider.INSERT, globalState.getOptions().getMaxNumberInserts());
        queryWeights.put(PostgreSQLQueryProvider.UPDATE, 20);
        queryWeights.put(PostgreSQLQueryProvider.DELETE, 5);
        queryWeights.put(PostgreSQLQueryProvider.SET_VARIABLE, 5);
        queryWeights.put(PostgreSQLQueryProvider.ANALYZE_TABLE, 1);
        queryWeights.put(PostgreSQLQueryProvider.RESET, 1);
        queryWeights.put(PostgreSQLQueryProvider.TRANSACTION_START, 0);
        queryWeights.put(PostgreSQLQueryProvider.TRANSACTION_COMMIT, 0);
        queryWeights.put(PostgreSQLQueryProvider.TRANSACTION_ROLLBACK, 0);
        queryWeights.put(PostgreSQLQueryProvider.TRANSACTION_ISOLATION, 0);

        return queryWeights;
    }

    public enum PostgreSQLDDLStmt {
        CREATE_TABLE(PostgreSQLQueryProvider.CREATE_TABLE),
        CREATE_INDEX(PostgreSQLQueryProvider.CREATE_INDEX),
        CREATE_VIEW(PostgreSQLQueryProvider.CREATE_VIEW),
        ALTER_TABLE_ADD_COLUMN(PostgreSQLQueryProvider.ALTER_TABLE_ADD_COLUMN),
        ALTER_TABLE_DROP_COLUMN(PostgreSQLQueryProvider.ALTER_TABLE_DROP_COLUMN),
        ALTER_TABLE_ALTER_COLUMN_TYPE(PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_TYPE),
        ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT(PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT),
        ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT(PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT),
        ALTER_TABLE_ALTER_COLUMN_SET_NOT_NULL(PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_NOT_NULL),
        ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL(PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL),
        ALTER_TABLE_SET_COLUMN(PostgreSQLQueryProvider.ALTER_TABLE_SET_COLUMN),
        ALTER_TABLE_RESET_COLUMN(PostgreSQLQueryProvider.ALTER_TABLE_RESET_COLUMN),
        ALTER_TABLE_ALTER_COLUMN_SET_STORAGE(PostgreSQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_STORAGE),
        ALTER_TABLE_ADD_UNIQUE_KEY(PostgreSQLQueryProvider.ALTER_TABLE_ADD_UNIQUE_KEY),
        ALTER_TABLE_ADD_PRIMARY_KEY(PostgreSQLQueryProvider.ALTER_TABLE_ADD_PRIMARY_KEY),
        ALTER_TABLE_ADD_FOREIGN_KEY(PostgreSQLQueryProvider.ALTER_TABLE_ADD_FOREIGN_KEY),
        ALTER_TABLE_OPTION(PostgreSQLQueryProvider.ALTER_TABLE_OPTION),
        ALTER_TABLE_RENAME_TABLE(PostgreSQLQueryProvider.ALTER_TABLE_RENAME_TABLE),
        REINDEX(PostgreSQLQueryProvider.REINDEX),
        TRUNCATE_TABLE(PostgreSQLQueryProvider.TRUNCATE_TABLE),
        DROP_TABLE(PostgreSQLQueryProvider.DROP_TABLE),
        DROP_INDEX(PostgreSQLQueryProvider.DROP_INDEX),
        DROP_VIEW(PostgreSQLQueryProvider.DROP_VIEW);

        private final PostgreSQLQueryProvider queryProvider;

        PostgreSQLDDLStmt(PostgreSQLQueryProvider queryProvider) {
            this.queryProvider = queryProvider;
        }

        public PostgreSQLQueryProvider getQueryProvider() {
            return queryProvider;
        }

        public static PostgreSQLQueryProvider getRandomDDL() {
            return Randomly.fromOptions(PostgreSQLDDLStmt.values()).getQueryProvider();
        }

    }

    public enum PostgreSQLDMLStmt {
        INSERT(PostgreSQLQueryProvider.INSERT, 10),
        UPDATE(PostgreSQLQueryProvider.UPDATE, 10),
        DELETE(PostgreSQLQueryProvider.DELETE, 1);

        private final PostgreSQLQueryProvider queryProvider;
        private final int weight;
        private static final List<PostgreSQLDMLStmt> weightedMap = new ArrayList<>();

        PostgreSQLDMLStmt(PostgreSQLQueryProvider queryProvider, int weight) {
            this.queryProvider = queryProvider;
            this.weight = weight;
        }

        private SQLQueryAdapter getQuery(PostgreSQLGlobalState state) {
            return queryProvider.getQuery(state);
        }

        public static SQLQueryAdapter getRandomDML(PostgreSQLGlobalState state) {
            if (weightedMap.isEmpty()) {
                for (PostgreSQLDMLStmt dmlStmt : PostgreSQLDMLStmt.values()) {
                    for (int i = 0; i < dmlStmt.weight; i++) {
                        weightedMap.add(dmlStmt);
                    }
                }
            }

            return Randomly.fromList(weightedMap).getQuery(state);
        }
    }

    public enum DatabaseInitStmt {
        CREATE_TABLE(PostgreSQLQueryProvider.CREATE_TABLE),
        CREATE_INDEX(PostgreSQLQueryProvider.CREATE_INDEX),
        INSERT(PostgreSQLQueryProvider.INSERT);

        private PostgreSQLQueryProvider queryProvider;

        DatabaseInitStmt(PostgreSQLQueryProvider queryProvider) {
            this.queryProvider = queryProvider;
        }

        private SQLQueryAdapter getQuery(PostgreSQLGlobalState state) {
            return queryProvider.getQuery(state);
        }
    }

    @Override
    public void generateDatabase(GlobalState state) throws Exception {
    }

    @Override
    public SQLConnection createDatabase(GlobalState state) throws SQLException {
        PostgreSQLGlobalState globalState = (PostgreSQLGlobalState) state;
        return globalState.createDatabase();
    }

    protected void readFunctions(PostgreSQLGlobalState globalState) throws SQLException {
        SQLQueryAdapter query = new SQLQueryAdapter("SELECT proname, provolatile FROM pg_proc;");
        try (DBRadarResultSet rs = query.executeAndGet(globalState)) {
            while (rs.next()) {
                String functionName = rs.getString(1);
                Character functionType = rs.getString(2).charAt(0);
                globalState.addFunctionAndType(functionName, functionType);
            }
        }
    }

    protected void createTables(PostgreSQLGlobalState globalState, int numTables) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < numTables) {
            try {
                SQLQueryAdapter createTable = PostgreSQLQueryProvider.CREATE_TABLE.getQuery(globalState);
                globalState.executeStatement(createTable);
            } catch (IgnoreMeException ignored) {
            }
        }
    }

    protected void prepareTables(PostgreSQLGlobalState globalState) throws Exception {
        // randomly generate DDL statements
        StatementExecutor se = new StatementExecutor(globalState, buildQueryWeights(globalState), (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();

        globalState.executeStatement(new SQLQueryAdapter("SET SESSION statement_timeout = 5000;"));
    }

    @Override
    public String getDBMSName() {
        return "postgresql";
    }

}
