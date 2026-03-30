package dbradar.tidb;

import com.google.auto.service.AutoService;
import dbradar.DatabaseProvider;
import dbradar.GlobalState;
import dbradar.IgnoreMeException;
import dbradar.QueryProvider;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.SQLProviderAdapter;
import dbradar.StatementExecutor;
import dbradar.common.query.QueryConfig;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.common.query.generator.QueryGenerator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AutoService(DatabaseProvider.class)
public class TiDBProvider extends SQLProviderAdapter {

    public TiDBProvider() {
        super(TiDBGlobalState.class, TiDBOptions.class);
    }

    public enum TiDBQueryProvider implements QueryProvider {
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
        CHECKSUM("checksum", false),
        ANALYZE_TABLE("analyze_table", false),
        SELECT("select", false),
        TRANSACTION_START("begin", false),
        TRANSACTION_ROLLBACK("rollback", true),
        TRANSACTION_COMMIT("commit", true),
        TRANSACTION_ISOLATION("set_isolation", true);

        private final QueryConfig queryConfig;

        TiDBQueryProvider(String queryRoot, boolean canAffectSchema) {
            this.queryConfig = new QueryConfig(queryRoot, canAffectSchema);
        }

        @Override
        public SQLQueryAdapter getQuery(GlobalState globalState) {
            TiDBGlobalState state = (TiDBGlobalState) globalState;
            QueryGenerator queryGenerator = new QueryGenerator(state, state.getGrammar(), queryConfig.getQueryRoot(),
                    new TiDBKeyFuncManager(state));

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
        TiDBGlobalState globalState = (TiDBGlobalState) state;

        Map<QueryProvider, Integer> queryWeights = new HashMap<>();
        queryWeights.put(TiDBQueryProvider.CREATE_TABLE, 0);
        queryWeights.put(TiDBQueryProvider.CREATE_INDEX, 5);
        queryWeights.put(TiDBQueryProvider.CREATE_VIEW, 5);
        queryWeights.put(TiDBQueryProvider.SHOW_TABLES, 2);
        queryWeights.put(TiDBQueryProvider.CHECK_TABLE, 5);
        queryWeights.put(TiDBQueryProvider.TRUNCATE_TABLE, 2);
        queryWeights.put(TiDBQueryProvider.ALTER_TABLE, 5);
        queryWeights.put(TiDBQueryProvider.DROP_INDEX, 5);
        queryWeights.put(TiDBQueryProvider.DROP_VIEW, 5);
        queryWeights.put(TiDBQueryProvider.INSERT, globalState.getOptions().getMaxNumberInserts());
        queryWeights.put(TiDBQueryProvider.UPDATE, 20);
        queryWeights.put(TiDBQueryProvider.DELETE, 5);
        queryWeights.put(TiDBQueryProvider.SET_VARIABLE, 5);
        queryWeights.put(TiDBQueryProvider.CHECKSUM, 1);
        queryWeights.put(TiDBQueryProvider.ANALYZE_TABLE, 1);

        return queryWeights;
    }

    public enum TiDBDDLStmt {

        CREATE_TABLE(TiDBQueryProvider.CREATE_TABLE),
        CREATE_INDEX(TiDBQueryProvider.CREATE_INDEX),
        CREATE_VIEW(TiDBQueryProvider.CREATE_VIEW),
        ALTER_TABLE_ADD_COLUMN(TiDBQueryProvider.ALTER_TABLE_ADD_COLUMN),
        ALTER_TABLE_DROP_COLUMN(TiDBQueryProvider.ALTER_TABLE_DROP_COLUMN),
        ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT(TiDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT),
        ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT(TiDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT),
        ALTER_TABLE_ALTER_COLUMN_SET_VISIBLE(TiDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_VISIBLE),
        ALTER_TABLE_ALTER_COLUMN_SET_INVISIBLE(TiDBQueryProvider.ALTER_TABLE_ALTER_COLUMN_SET_INVISIBLE),
        ALTER_TABLE_CHANGE_COLUMN(TiDBQueryProvider.ALTER_TABLE_CHANGE_COLUMN),
        ALTER_TABLE_MODIFY_COLUMN(TiDBQueryProvider.ALTER_TABLE_MODIFY_COLUMN),
        ALTER_TABLE_RENAME_COLUMN(TiDBQueryProvider.ALTER_TABLE_RENAME_COLUMN),
        ALTER_TABLE_ADD_INDEX(TiDBQueryProvider.ALTER_TABLE_ADD_INDEX),
        ALTER_TABLE_DROP_INDEX(TiDBQueryProvider.ALTER_TABLE_DROP_INDEX),
        ALTER_TABLE_RENAME_INDEX(TiDBQueryProvider.ALTER_TABLE_RENAME_INDEX),
        ALTER_TABLE_ADD_PRIMARY_KEY(TiDBQueryProvider.ALTER_TABLE_ADD_PRIMARY_KEY),
        ALTER_TABLE_DROP_PRIMARY_KEY(TiDBQueryProvider.ALTER_TABLE_DROP_PRIMARY_KEY),
        ALTER_TABLE_ADD_UNIQUE_KEY(TiDBQueryProvider.ALTER_TABLE_ADD_UNIQUE_KEY),
        ALTER_TABLE_ADD_FOREIGN_KEY(TiDBQueryProvider.ALTER_TABLE_ADD_FOREIGN_KEY),
        ALTER_TABLE_OPTION(TiDBQueryProvider.ALTER_TABLE_OPTION),
        ALTER_TABLE_RENAME_TABLE(TiDBQueryProvider.ALTER_TABLE_RENAME_TABLE),
        RENAME_TABLE(TiDBQueryProvider.RENAME_TABLE),
        TRUNCATE_TABLE(TiDBQueryProvider.TRUNCATE_TABLE),
        DROP_TABLE(TiDBQueryProvider.DROP_TABLE),
        DROP_INDEX(TiDBQueryProvider.DROP_INDEX),
        DROP_VIEW(TiDBQueryProvider.DROP_VIEW);

        private final TiDBQueryProvider queryProvider;

        TiDBDDLStmt(TiDBQueryProvider queryProvider) {
            this.queryProvider = queryProvider;
        }

        public TiDBQueryProvider getQueryProvider() {
            return queryProvider;
        }

        public static TiDBQueryProvider getRandomDDL() {
            return Randomly.fromOptions(TiDBDDLStmt.values()).getQueryProvider();
        }

    }

    public enum TiDBDMLStmt {

        INSERT(TiDBQueryProvider.INSERT, 10),
        UPDATE(TiDBQueryProvider.UPDATE, 10),
        REPLACE(TiDBQueryProvider.REPLACE, 10),
        DELETE(TiDBQueryProvider.DELETE, 1);

        private final TiDBQueryProvider queryProvider;
        private final int weight;
        private static final List<TiDBDMLStmt> weightedMap = new ArrayList<>();

        TiDBDMLStmt(TiDBQueryProvider queryProvider, int weight) {
            this.queryProvider = queryProvider;
            this.weight = weight;
        }

        private SQLQueryAdapter getQuery(TiDBGlobalState state) {
            return queryProvider.getQuery(state);
        }

        public static SQLQueryAdapter getRandomDML(TiDBGlobalState state) {
            if (weightedMap.isEmpty()) {
                for (TiDBDMLStmt dmlStmt : TiDBDMLStmt.values()) {
                    for (int i = 0; i < dmlStmt.weight; i++) {
                        weightedMap.add(dmlStmt);
                    }
                }
            }

            return Randomly.fromList(weightedMap).getQuery(state);
        }
    }

    public enum DatabaseInitStmt {
        CREATE_TABLE(TiDBQueryProvider.CREATE_TABLE),
        CREATE_INDEX(TiDBQueryProvider.CREATE_INDEX),
        INSERT(TiDBQueryProvider.INSERT);

        private final TiDBQueryProvider sqlQueryProvider;

        DatabaseInitStmt(TiDBQueryProvider sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(TiDBGlobalState state) {
            return sqlQueryProvider.getQuery(state);
        }
    }


    @Override
    public void generateDatabase(GlobalState state) throws Exception {
    }

    @Override
    public SQLConnection createDatabase(GlobalState state) throws SQLException {
        TiDBGlobalState globalState = (TiDBGlobalState) state;
        return globalState.createDatabase();
    }

    @Override
    public String getDBMSName() {
        return "tidb";
    }
}
