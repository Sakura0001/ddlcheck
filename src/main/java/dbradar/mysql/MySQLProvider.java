package dbradar.mysql;

import com.google.auto.service.AutoService;
import dbradar.DatabaseConnection;
import dbradar.DatabaseProvider;
import dbradar.GlobalState;
import dbradar.QueryProvider;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.SQLProviderAdapter;
import dbradar.common.query.QueryConfig;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerator;
import dbradar.common.schema.AbstractTable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AutoService(DatabaseProvider.class)
public class MySQLProvider extends SQLProviderAdapter {

    public MySQLProvider() {
        super(MySQLGlobalState.class, MySQLOptions.class);
    }

    public enum MySQLQueryProvider implements QueryProvider {
        CREATE_TABLE("create_table", true),
        CREATE_INDEX("create_index", true),
        CREATE_VIEW("create_view", true),
        SHOW_TABLES("show_tables", false),
        CHECK_TABLE("check_table", false),
        TRUNCATE_TABLE("truncate_table", true),
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
        ALTER_VIEW("alter_view", true),
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

        MySQLQueryProvider(String queryRoot, boolean canAffectSchema) {
            this.queryConfig = new QueryConfig(queryRoot, canAffectSchema);
        }

        @Override
        public SQLQueryAdapter getQuery(GlobalState globalState) {
            MySQLGlobalState state = (MySQLGlobalState) globalState;
            QueryGenerator queryGenerator = new QueryGenerator(state, state.getGrammar(), queryConfig.getQueryRoot(),
                    new MySQLKeyFuncManager(state));

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
        MySQLGlobalState globalState = (MySQLGlobalState) state;

        Map<QueryProvider, Integer> queryWeights = new HashMap<>();
        queryWeights.put(MySQLQueryProvider.CREATE_TABLE, 0);
        queryWeights.put(MySQLQueryProvider.CREATE_INDEX, 5);
        queryWeights.put(MySQLQueryProvider.CREATE_VIEW, 5);
        queryWeights.put(MySQLQueryProvider.SHOW_TABLES, 2);
        queryWeights.put(MySQLQueryProvider.CHECK_TABLE, 5);
        queryWeights.put(MySQLQueryProvider.TRUNCATE_TABLE, 2);
        queryWeights.put(MySQLQueryProvider.ALTER_TABLE, 5);
        queryWeights.put(MySQLQueryProvider.ALTER_VIEW, 5);
        queryWeights.put(MySQLQueryProvider.RENAME_TABLE, 5);
        queryWeights.put(MySQLQueryProvider.DROP_INDEX, 5);
        queryWeights.put(MySQLQueryProvider.DROP_VIEW, 5);
        queryWeights.put(MySQLQueryProvider.INSERT, globalState.getOptions().getMaxNumberInserts());
        queryWeights.put(MySQLQueryProvider.UPDATE, 20);
        queryWeights.put(MySQLQueryProvider.DELETE, 5);
        queryWeights.put(MySQLQueryProvider.SET_VARIABLE, 5);
        queryWeights.put(MySQLQueryProvider.REPAIR, 3);
        queryWeights.put(MySQLQueryProvider.OPTIMIZE, 1);
        queryWeights.put(MySQLQueryProvider.CHECKSUM, 1);
        queryWeights.put(MySQLQueryProvider.ANALYZE_TABLE, 1);
        queryWeights.put(MySQLQueryProvider.FLUSH, 1);
        queryWeights.put(MySQLQueryProvider.RESET, 1);
        queryWeights.put(MySQLQueryProvider.TRANSACTION_START, 0);
        queryWeights.put(MySQLQueryProvider.TRANSACTION_COMMIT, 0);
        queryWeights.put(MySQLQueryProvider.TRANSACTION_ROLLBACK, 0);
        queryWeights.put(MySQLQueryProvider.TRANSACTION_ISOLATION, 0);

        return queryWeights;
    }

    public enum MySQLDDLStmt {

        CREATE_TABLE(MySQLQueryProvider.CREATE_TABLE),
        CREATE_INDEX(MySQLQueryProvider.CREATE_INDEX),
        CREATE_VIEW(MySQLQueryProvider.CREATE_VIEW),
        ALTER_TABLE_ADD_COLUMN(MySQLQueryProvider.ALTER_TABLE_ADD_COLUMN),
        ALTER_TABLE_DROP_COLUMN(MySQLQueryProvider.ALTER_TABLE_DROP_COLUMN),
        ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT(MySQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT),
        ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT(MySQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT),
        ALTER_TABLE_ALTER_COLUMN_SET_VISIBLE(MySQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_VISIBLE),
        ALTER_TABLE_ALTER_COLUMN_SET_INVISIBLE(MySQLQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_INVISIBLE),
        ALTER_TABLE_CHANGE_COLUMN(MySQLQueryProvider.ALTER_TABLE_CHANGE_COLUMN),
        ALTER_TABLE_MODIFY_COLUMN(MySQLQueryProvider.ALTER_TABLE_MODIFY_COLUMN),
        ALTER_TABLE_RENAME_COLUMN(MySQLQueryProvider.ALTER_TABLE_RENAME_COLUMN),
        ALTER_TABLE_ADD_INDEX(MySQLQueryProvider.ALTER_TABLE_ADD_INDEX),
        ALTER_TABLE_DROP_INDEX(MySQLQueryProvider.ALTER_TABLE_DROP_INDEX),
        ALTER_TABLE_RENAME_INDEX(MySQLQueryProvider.ALTER_TABLE_RENAME_INDEX),
        ALTER_TABLE_ADD_PRIMARY_KEY(MySQLQueryProvider.ALTER_TABLE_ADD_PRIMARY_KEY),
        ALTER_TABLE_DROP_PRIMARY_KEY(MySQLQueryProvider.ALTER_TABLE_DROP_PRIMARY_KEY),
        ALTER_TABLE_ADD_UNIQUE_KEY(MySQLQueryProvider.ALTER_TABLE_ADD_UNIQUE_KEY),
        ALTER_TABLE_ADD_FOREIGN_KEY(MySQLQueryProvider.ALTER_TABLE_ADD_FOREIGN_KEY),
        ALTER_TABLE_RENAME_TABLE(MySQLQueryProvider.ALTER_TABLE_RENAME_TABLE),
        ALTER_TABLE_OPTION(MySQLQueryProvider.ALTER_TABLE_OPTION),
        ALTER_VIEW(MySQLQueryProvider.ALTER_VIEW),
        RENAME_TABLE(MySQLQueryProvider.RENAME_TABLE),
        TRUNCATE_TABLE(MySQLQueryProvider.TRUNCATE_TABLE),
        DROP_TABLE(MySQLQueryProvider.DROP_TABLE),
        DROP_INDEX(MySQLQueryProvider.DROP_INDEX),
        DROP_VIEW(MySQLQueryProvider.DROP_VIEW);

        private final MySQLQueryProvider queryProvider;

        MySQLDDLStmt(MySQLQueryProvider queryProvider) {
            this.queryProvider = queryProvider;
        }

        public MySQLQueryProvider getQueryProvider() {
            return queryProvider;
        }

        public static MySQLQueryProvider getRandomDDL() {
            return Randomly.fromOptions(MySQLDDLStmt.values()).getQueryProvider();
        }

    }

    public enum MySQLDMLStmt {

        INSERT(MySQLQueryProvider.INSERT, 10),
        UPDATE(MySQLQueryProvider.UPDATE, 10),
        REPLACE(MySQLQueryProvider.REPLACE, 10),
        DELETE(MySQLQueryProvider.DELETE, 1);

        private final MySQLQueryProvider queryProvider;
        private final int weight;
        private static final List<MySQLDMLStmt> weightedMap = new ArrayList<>();

        MySQLDMLStmt(MySQLQueryProvider queryProvider, int weight) {
            this.queryProvider = queryProvider;
            this.weight = weight;
        }

        private SQLQueryAdapter getQuery(MySQLGlobalState state) {
            return queryProvider.getQuery(state);
        }

        public static SQLQueryAdapter getRandomDML(MySQLGlobalState state) {
            if (weightedMap.isEmpty()) {
                for (MySQLDMLStmt dmlStmt : MySQLDMLStmt.values()) {
                    for (int i = 0; i < dmlStmt.weight; i++) {
                        weightedMap.add(dmlStmt);
                    }
                }
            }

            return Randomly.fromList(weightedMap).getQuery(state);
        }
    }

    public enum DatabaseInitStmt {

        CREATE_TABLE(MySQLQueryProvider.CREATE_TABLE),
        CREATE_INDEX(MySQLQueryProvider.CREATE_INDEX),
        INSERT(MySQLQueryProvider.INSERT);

        private final MySQLQueryProvider sqlQueryProvider;

        DatabaseInitStmt(MySQLQueryProvider sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(MySQLGlobalState state) {
            return sqlQueryProvider.getQuery(state);
        }
    }


    @Override
    public void generateDatabase(GlobalState state) throws Exception {
    }

    @Override
    public SQLConnection createDatabase(GlobalState state) throws SQLException {
        MySQLGlobalState globalState = (MySQLGlobalState) state;
        return globalState.createDatabase();
    }

    @Override
    public String getDBMSName() {
        return "mysql";
    }

    @Override
    public DatabaseConnection createConnection(GlobalState globalState) throws Exception {
        MySQLGlobalState state = (MySQLGlobalState) globalState;
        return state.createConnection();
    }
}
