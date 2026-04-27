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
import dbradar.common.query.generator.QueryGenerationException;

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
        CREATE_SEQUENCE("create_sequence", true),
        CREATE_INDEX("create_index", true),
        CREATE_VIEW("create_view", true),
        SHOW_TABLES("show_tables", false),
        TRUNCATE_TABLE("truncate", true),
        REFRESH_MATERIALIZED_VIEW("refresh_materialized_view", false),
        DROP_MATERIALIZED_VIEW("drop_materialized_view", true),
        ALTER_TABLE("alter_table", true),
        ALTER_TABLE_ATTACH_PARTITION("alter_table_attach_partition", true),
        ALTER_TABLE_ATTACH_PARTITION_FOR_VALUES("alter_table_attach_partition_for_values", true),
        ALTER_TABLE_DETACH_PARTITION("alter_table_detach_partition", true),
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
        ALTER_TABLE_RENAME_COLUMN("alter_table_rename_column", true),
        ALTER_TABLE_CHANGE_COLUMN("alter_table_change_column", true),
        ALTER_TABLE_MODIFY_COLUMN("alter_table_modify_column", true),
        ALTER_TABLE_ADD_INDEX("alter_table_add_index", true),
        ALTER_TABLE_DROP_INDEX("alter_table_drop_index", true),
        ALTER_TABLE_RENAME_INDEX("alter_table_rename_index", true),
        ALTER_TABLE_DROP_PRIMARY_KEY("alter_table_drop_primary_key", true),
        ALTER_TABLE_ADD_CHECK("alter_table_add_check", true),
        ALTER_INDEX("alter_index", true),
        ALTER_VIEW("alter_view", true),
        ALTER_SEQUENCE("alter_sequence", true),
        DROP_SEQUENCE("drop_sequence", true),
        COMMENT_ON_TABLE("comment_on_table", false),
        COMMENT_ON_COLUMN("comment_on_column", false),
        GRANT_TABLE("grant_table", false),
        REVOKE_TABLE("revoke_table", false),
        GRANT_SCHEMA("grant_schema", false),
        REVOKE_SCHEMA("revoke_schema", false),
        GRANT_SEQUENCE("grant_sequence", false),
        REVOKE_SEQUENCE("revoke_sequence", false),
        GRANT_FUNCTION("grant_function", false),
        REVOKE_FUNCTION("revoke_function", false),
        GRANT_PROCEDURE("grant_procedure", false),
        REVOKE_PROCEDURE("revoke_procedure", false),
        CREATE_FUNCTION("create_function", true),
        ALTER_FUNCTION("alter_function", true),
        DROP_FUNCTION("drop_function", true),
        CREATE_PROCEDURE("create_procedure", true),
        ALTER_PROCEDURE("alter_procedure", true),
        DROP_PROCEDURE("drop_procedure", true),
        CREATE_RULE("create_rule", true),
        DROP_RULE("drop_rule", true),
        CREATE_TRIGGER("create_trigger", true),
        DROP_TRIGGER("drop_trigger", true),
        REINDEX("reindex", true),
        DROP_TABLE("drop_table", true),
        DROP_INDEX("drop_index", true),
        DROP_VIEW("drop_view", true),
        INSERT("insert", false),
        INSERT_SELECT("insert_select", false),
        INSERT_ON_CONFLICT("insert_on_conflict", false),
        INSERT_RETURNING("insert_returning", false),
        UPDATE("update", false),
        UPDATE_MULTI_ROW("update_multi_row", false),
        UPDATE_FROM("update_from", false),
        UPDATE_CTE("update_cte", false),
        UPDATE_RETURNING("update_returning", false),
        DELETE("delete", false),
        DELETE_MULTI_ROW("delete_multi_row", false),
        DELETE_USING("delete_using", false),
        DELETE_RETURNING("delete_returning", false),
        MERGE("merge", false),
        LOCK_TABLE("lock_table", false),
        PREPARE_EXECUTE("prepare_execute", false),
        SAVEPOINT_RELEASE("savepoint_release", false),
        DECLARE_FETCH_CLOSE("declare_fetch_close", false),
        COPY_TO_STDOUT("copy_to_stdout", false),
        SET_VARIABLE("set_variable", false),
        ANALYZE_TABLE("analyze_table", false),
        VACUUM("vacuum", false),
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
            if (this == SELECT) {
                return getSafeSelectQuery(state);
            }

            QueryGenerator queryGenerator = new QueryGenerator(state, state.getGrammar(), queryConfig.getQueryRoot(),
                    new PostgreSQLKeyFunctionManager(state));
            SQLQueryAdapter query = new SQLQueryAdapter(queryGenerator.getRandomQuery());
            query.setCanAffectSchema(queryConfig.canAffectSchema());
            return query;
        }

        private SQLQueryAdapter getSafeSelectQuery(PostgreSQLGlobalState state) {
            for (int attempt = 0; attempt < 100; attempt++) {
                try {
                    SQLQueryAdapter query = new SQLQueryAdapter(PostgreSQLSelectQueryBuilder.generate(state));
                    PostgreSQLSelectQueryPolicy.validateGeneratedSelect(query.getQueryString());
                    query.setCanAffectSchema(false);
                    return query;
                } catch (QueryGenerationException | IgnoreMeException ignored) {
                }
            }
            throw new QueryGenerationException("Unable to generate a safe SELECT query");
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
        queryWeights.put(PostgreSQLQueryProvider.CREATE_SEQUENCE, 1);
        queryWeights.put(PostgreSQLQueryProvider.CREATE_INDEX, 5);
        queryWeights.put(PostgreSQLQueryProvider.CREATE_VIEW, 5);
        queryWeights.put(PostgreSQLQueryProvider.SHOW_TABLES, 2);
        queryWeights.put(PostgreSQLQueryProvider.TRUNCATE_TABLE, 2);
        queryWeights.put(PostgreSQLQueryProvider.REFRESH_MATERIALIZED_VIEW, 1);
        queryWeights.put(PostgreSQLQueryProvider.DROP_MATERIALIZED_VIEW, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE, 5);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE_ATTACH_PARTITION, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE_ATTACH_PARTITION_FOR_VALUES, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE_DETACH_PARTITION, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE_RENAME_COLUMN, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE_CHANGE_COLUMN, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE_MODIFY_COLUMN, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE_ADD_INDEX, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE_DROP_INDEX, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE_RENAME_INDEX, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE_DROP_PRIMARY_KEY, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_TABLE_ADD_CHECK, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_INDEX, 2);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_VIEW, 2);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_SEQUENCE, 2);
        queryWeights.put(PostgreSQLQueryProvider.DROP_SEQUENCE, 1);
        queryWeights.put(PostgreSQLQueryProvider.COMMENT_ON_TABLE, 1);
        queryWeights.put(PostgreSQLQueryProvider.COMMENT_ON_COLUMN, 1);
        queryWeights.put(PostgreSQLQueryProvider.GRANT_TABLE, 1);
        queryWeights.put(PostgreSQLQueryProvider.REVOKE_TABLE, 1);
        queryWeights.put(PostgreSQLQueryProvider.GRANT_SCHEMA, 1);
        queryWeights.put(PostgreSQLQueryProvider.REVOKE_SCHEMA, 1);
        queryWeights.put(PostgreSQLQueryProvider.GRANT_SEQUENCE, 1);
        queryWeights.put(PostgreSQLQueryProvider.REVOKE_SEQUENCE, 1);
        queryWeights.put(PostgreSQLQueryProvider.GRANT_FUNCTION, 1);
        queryWeights.put(PostgreSQLQueryProvider.REVOKE_FUNCTION, 1);
        queryWeights.put(PostgreSQLQueryProvider.GRANT_PROCEDURE, 1);
        queryWeights.put(PostgreSQLQueryProvider.REVOKE_PROCEDURE, 1);
        queryWeights.put(PostgreSQLQueryProvider.CREATE_FUNCTION, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_FUNCTION, 1);
        queryWeights.put(PostgreSQLQueryProvider.DROP_FUNCTION, 1);
        queryWeights.put(PostgreSQLQueryProvider.CREATE_PROCEDURE, 1);
        queryWeights.put(PostgreSQLQueryProvider.ALTER_PROCEDURE, 1);
        queryWeights.put(PostgreSQLQueryProvider.DROP_PROCEDURE, 1);
        queryWeights.put(PostgreSQLQueryProvider.CREATE_RULE, 1);
        queryWeights.put(PostgreSQLQueryProvider.DROP_RULE, 1);
        queryWeights.put(PostgreSQLQueryProvider.CREATE_TRIGGER, 1);
        queryWeights.put(PostgreSQLQueryProvider.DROP_TRIGGER, 1);
        queryWeights.put(PostgreSQLQueryProvider.REINDEX, 5);
        queryWeights.put(PostgreSQLQueryProvider.DROP_INDEX, 5);
        queryWeights.put(PostgreSQLQueryProvider.DROP_VIEW, 5);
        queryWeights.put(PostgreSQLQueryProvider.INSERT, globalState.getOptions().getMaxNumberInserts());
        queryWeights.put(PostgreSQLQueryProvider.INSERT_SELECT, 3);
        queryWeights.put(PostgreSQLQueryProvider.INSERT_ON_CONFLICT, 3);
        queryWeights.put(PostgreSQLQueryProvider.INSERT_RETURNING, 2);
        queryWeights.put(PostgreSQLQueryProvider.UPDATE, 20);
        queryWeights.put(PostgreSQLQueryProvider.UPDATE_MULTI_ROW, 5);
        queryWeights.put(PostgreSQLQueryProvider.UPDATE_FROM, 3);
        queryWeights.put(PostgreSQLQueryProvider.UPDATE_CTE, 3);
        queryWeights.put(PostgreSQLQueryProvider.UPDATE_RETURNING, 2);
        queryWeights.put(PostgreSQLQueryProvider.DELETE, 5);
        queryWeights.put(PostgreSQLQueryProvider.DELETE_MULTI_ROW, 2);
        queryWeights.put(PostgreSQLQueryProvider.DELETE_USING, 2);
        queryWeights.put(PostgreSQLQueryProvider.DELETE_RETURNING, 2);
        queryWeights.put(PostgreSQLQueryProvider.MERGE, 2);
        queryWeights.put(PostgreSQLQueryProvider.LOCK_TABLE, 1);
        queryWeights.put(PostgreSQLQueryProvider.PREPARE_EXECUTE, 1);
        queryWeights.put(PostgreSQLQueryProvider.SAVEPOINT_RELEASE, 1);
        queryWeights.put(PostgreSQLQueryProvider.DECLARE_FETCH_CLOSE, 1);
        queryWeights.put(PostgreSQLQueryProvider.COPY_TO_STDOUT, 1);
        queryWeights.put(PostgreSQLQueryProvider.SET_VARIABLE, 5);
        queryWeights.put(PostgreSQLQueryProvider.ANALYZE_TABLE, 1);
        queryWeights.put(PostgreSQLQueryProvider.VACUUM, 1);
        queryWeights.put(PostgreSQLQueryProvider.RESET, 1);
        queryWeights.put(PostgreSQLQueryProvider.TRANSACTION_START, 0);
        queryWeights.put(PostgreSQLQueryProvider.TRANSACTION_COMMIT, 0);
        queryWeights.put(PostgreSQLQueryProvider.TRANSACTION_ROLLBACK, 0);
        queryWeights.put(PostgreSQLQueryProvider.TRANSACTION_ISOLATION, 0);

        return queryWeights;
    }

    public enum PostgreSQLDDLStmt {
        CREATE_TABLE(PostgreSQLQueryProvider.CREATE_TABLE),
        CREATE_SEQUENCE(PostgreSQLQueryProvider.CREATE_SEQUENCE),
        CREATE_INDEX(PostgreSQLQueryProvider.CREATE_INDEX),
        CREATE_VIEW(PostgreSQLQueryProvider.CREATE_VIEW),
        ALTER_TABLE_ATTACH_PARTITION(PostgreSQLQueryProvider.ALTER_TABLE_ATTACH_PARTITION),
        ALTER_TABLE_ATTACH_PARTITION_FOR_VALUES(PostgreSQLQueryProvider.ALTER_TABLE_ATTACH_PARTITION_FOR_VALUES),
        ALTER_TABLE_DETACH_PARTITION(PostgreSQLQueryProvider.ALTER_TABLE_DETACH_PARTITION),
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
        ALTER_TABLE_RENAME_COLUMN(PostgreSQLQueryProvider.ALTER_TABLE_RENAME_COLUMN),
        ALTER_TABLE_CHANGE_COLUMN(PostgreSQLQueryProvider.ALTER_TABLE_CHANGE_COLUMN),
        ALTER_TABLE_MODIFY_COLUMN(PostgreSQLQueryProvider.ALTER_TABLE_MODIFY_COLUMN),
        ALTER_TABLE_ADD_INDEX(PostgreSQLQueryProvider.ALTER_TABLE_ADD_INDEX),
        ALTER_TABLE_DROP_INDEX(PostgreSQLQueryProvider.ALTER_TABLE_DROP_INDEX),
        ALTER_TABLE_RENAME_INDEX(PostgreSQLQueryProvider.ALTER_TABLE_RENAME_INDEX),
        ALTER_TABLE_DROP_PRIMARY_KEY(PostgreSQLQueryProvider.ALTER_TABLE_DROP_PRIMARY_KEY),
        ALTER_TABLE_ADD_CHECK(PostgreSQLQueryProvider.ALTER_TABLE_ADD_CHECK),
        ALTER_INDEX(PostgreSQLQueryProvider.ALTER_INDEX),
        ALTER_VIEW(PostgreSQLQueryProvider.ALTER_VIEW),
        ALTER_SEQUENCE(PostgreSQLQueryProvider.ALTER_SEQUENCE),
        DROP_SEQUENCE(PostgreSQLQueryProvider.DROP_SEQUENCE),
        COMMENT_ON_TABLE(PostgreSQLQueryProvider.COMMENT_ON_TABLE),
        COMMENT_ON_COLUMN(PostgreSQLQueryProvider.COMMENT_ON_COLUMN),
        GRANT_TABLE(PostgreSQLQueryProvider.GRANT_TABLE),
        REVOKE_TABLE(PostgreSQLQueryProvider.REVOKE_TABLE),
        GRANT_SCHEMA(PostgreSQLQueryProvider.GRANT_SCHEMA),
        REVOKE_SCHEMA(PostgreSQLQueryProvider.REVOKE_SCHEMA),
        GRANT_SEQUENCE(PostgreSQLQueryProvider.GRANT_SEQUENCE),
        REVOKE_SEQUENCE(PostgreSQLQueryProvider.REVOKE_SEQUENCE),
        GRANT_FUNCTION(PostgreSQLQueryProvider.GRANT_FUNCTION),
        REVOKE_FUNCTION(PostgreSQLQueryProvider.REVOKE_FUNCTION),
        GRANT_PROCEDURE(PostgreSQLQueryProvider.GRANT_PROCEDURE),
        REVOKE_PROCEDURE(PostgreSQLQueryProvider.REVOKE_PROCEDURE),
        CREATE_FUNCTION(PostgreSQLQueryProvider.CREATE_FUNCTION),
        ALTER_FUNCTION(PostgreSQLQueryProvider.ALTER_FUNCTION),
        DROP_FUNCTION(PostgreSQLQueryProvider.DROP_FUNCTION),
        CREATE_PROCEDURE(PostgreSQLQueryProvider.CREATE_PROCEDURE),
        ALTER_PROCEDURE(PostgreSQLQueryProvider.ALTER_PROCEDURE),
        DROP_PROCEDURE(PostgreSQLQueryProvider.DROP_PROCEDURE),
        CREATE_RULE(PostgreSQLQueryProvider.CREATE_RULE),
        DROP_RULE(PostgreSQLQueryProvider.DROP_RULE),
        CREATE_TRIGGER(PostgreSQLQueryProvider.CREATE_TRIGGER),
        DROP_TRIGGER(PostgreSQLQueryProvider.DROP_TRIGGER),
        REINDEX(PostgreSQLQueryProvider.REINDEX),
        VACUUM(PostgreSQLQueryProvider.VACUUM),
        REFRESH_MATERIALIZED_VIEW(PostgreSQLQueryProvider.REFRESH_MATERIALIZED_VIEW),
        DROP_MATERIALIZED_VIEW(PostgreSQLQueryProvider.DROP_MATERIALIZED_VIEW),
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
        INSERT_SELECT(PostgreSQLQueryProvider.INSERT_SELECT, 3),
        INSERT_ON_CONFLICT(PostgreSQLQueryProvider.INSERT_ON_CONFLICT, 3),
        INSERT_RETURNING(PostgreSQLQueryProvider.INSERT_RETURNING, 2),
        UPDATE(PostgreSQLQueryProvider.UPDATE, 10),
        UPDATE_MULTI_ROW(PostgreSQLQueryProvider.UPDATE_MULTI_ROW, 3),
        UPDATE_FROM(PostgreSQLQueryProvider.UPDATE_FROM, 3),
        UPDATE_CTE(PostgreSQLQueryProvider.UPDATE_CTE, 3),
        UPDATE_RETURNING(PostgreSQLQueryProvider.UPDATE_RETURNING, 2),
        DELETE(PostgreSQLQueryProvider.DELETE, 1),
        DELETE_MULTI_ROW(PostgreSQLQueryProvider.DELETE_MULTI_ROW, 1),
        DELETE_USING(PostgreSQLQueryProvider.DELETE_USING, 2),
        DELETE_RETURNING(PostgreSQLQueryProvider.DELETE_RETURNING, 2),
        MERGE(PostgreSQLQueryProvider.MERGE, 2),
        LOCK_TABLE(PostgreSQLQueryProvider.LOCK_TABLE, 1),
        PREPARE_EXECUTE(PostgreSQLQueryProvider.PREPARE_EXECUTE, 1),
        SAVEPOINT_RELEASE(PostgreSQLQueryProvider.SAVEPOINT_RELEASE, 1),
        DECLARE_FETCH_CLOSE(PostgreSQLQueryProvider.DECLARE_FETCH_CLOSE, 1),
        COPY_TO_STDOUT(PostgreSQLQueryProvider.COPY_TO_STDOUT, 1);

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

    @Override
    public SQLConnection createConnection(GlobalState state) throws Exception {
        PostgreSQLGlobalState globalState = (PostgreSQLGlobalState) state;
        return globalState.createConnection();
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
