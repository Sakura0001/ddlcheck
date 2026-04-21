package dbradar.sqlite3;

import com.google.auto.service.AutoService;
import dbradar.*;
import dbradar.common.query.ExpectedErrors;
import dbradar.common.query.QueryConfig;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerator;
import dbradar.sqlite3.schema.SQLite3Table;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AutoService(DatabaseProvider.class)
public class SQLite3Provider extends SQLProviderAdapter {

    // PRAGMAS to achieve good performance
    private static final List<String> DEFAULT_PRAGMAS = Arrays.asList("PRAGMA cache_size = 50000;",
            "PRAGMA temp_store=MEMORY;", "PRAGMA synchronous=off;");

    public SQLite3Provider() {
        super(SQLite3GlobalState.class, SQLite3Options.class);
    }

    public enum SQLite3QueryProvider implements QueryProvider {
        PRAGMA("pragma", false),
        CREATE_TABLE("create_table", true),
        CREATE_VIRTUAL_TABLE_FTS4("create_virtual_table_using_fts4", true),
        CREATE_VIRTUAL_TABLE_FTS5("create_virtual_table_using_fts5", true),
        CREATE_VIRTUAL_TABLE_RTREE("create_virtual_table_using_rtree", true),
        CREATE_INDEX("create_index", true),
        CREATE_VIEW("create_view", true),
        CREATE_TRIGGER("create_trigger", true),
        ALTER_TABLE("alter_table", true),
        ALTER_TABLE_RENAME_TABLE("alter_table_rename_table", true),
        ALTER_TABLE_RENAME_COLUMN("alter_table_rename_column", true),
        ALTER_TABLE_ADD_COLUMN("alter_table_add_column", true),
        ALTER_TABLE_DROP_COLUMN("alter_table_drop_column", true),
        DROP_TABLE("drop_table", true),
        DROP_INDEX("drop_index", true),
        DROP_VIEW("drop_view", true),
        INSERT("insert", false),
        REPLACE("replace", false),
        UPDATE("update", true),
        DELETE("delete", true),
        SELECT("select", false),
        VACUUM("vacuum", false),
        REINDEX("reindex", true),
        ANALYZE("analyze", false),
        EXPLAIN("explain", true),
        CHECK_RTREE_TABLE("check_rtree_table", false),
        INSERT_FTS4_COMMAND("insert_fts4_command", false),
        INSERT_FTS5_COMMAND("insert_fts5_command", false),
        MODIFY_STAT_TABLE("modify_sqlite_stat1", true),
        TRANSACTION_START("begin", false, false),
        TRANSACTION_ROLLBACK("rollback", true, false),
        TRANSACTION_COMMIT("commit", true, false);

        private final QueryConfig queryConfig;

        SQLite3QueryProvider(String queryRoot, boolean canAffectSchema) {
            this.queryConfig = new QueryConfig(queryRoot, canAffectSchema);
        }

        SQLite3QueryProvider(String queryRoot, boolean canAffectSchema, boolean canBeRetried) {
            this.queryConfig = new QueryConfig(queryRoot, canAffectSchema, canBeRetried);
        }

        @Override
        public SQLQueryAdapter getQuery(GlobalState globalState) {
            SQLite3GlobalState state = (SQLite3GlobalState) globalState;
            QueryGenerator queryGenerator = new QueryGenerator(state, state.getGrammar(), queryConfig.getQueryRoot(),
                    new SQLite3KeyFuncManager(state));

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
        SQLite3GlobalState globalState = (SQLite3GlobalState) state;

        Map<QueryProvider, Integer> queryWeights = new HashMap<>();
        queryWeights.put(SQLite3QueryProvider.PRAGMA, 10);
        queryWeights.put(SQLite3QueryProvider.CREATE_TABLE, 0);
        queryWeights.put(SQLite3QueryProvider.CREATE_VIRTUAL_TABLE_FTS4, 0);
        queryWeights.put(SQLite3QueryProvider.CREATE_VIRTUAL_TABLE_FTS5, 0);
        queryWeights.put(SQLite3QueryProvider.CREATE_VIRTUAL_TABLE_RTREE, 0);
        queryWeights.put(SQLite3QueryProvider.CREATE_INDEX, 5);
        queryWeights.put(SQLite3QueryProvider.CREATE_VIEW, 2);
        queryWeights.put(SQLite3QueryProvider.CREATE_TRIGGER, 0);
        queryWeights.put(SQLite3QueryProvider.ALTER_TABLE, 5);
        queryWeights.put(SQLite3QueryProvider.DROP_TABLE, 0);
        queryWeights.put(SQLite3QueryProvider.DROP_INDEX, 5);
        queryWeights.put(SQLite3QueryProvider.DROP_VIEW, 5);
        queryWeights.put(SQLite3QueryProvider.INSERT, globalState.getOptions().getMaxNumberInserts());
        queryWeights.put(SQLite3QueryProvider.UPDATE, 20);
        queryWeights.put(SQLite3QueryProvider.DELETE, 5);
        queryWeights.put(SQLite3QueryProvider.SELECT, 10);
        queryWeights.put(SQLite3QueryProvider.VACUUM, 3);
        queryWeights.put(SQLite3QueryProvider.REINDEX, 10);
        queryWeights.put(SQLite3QueryProvider.ANALYZE, 10);
        queryWeights.put(SQLite3QueryProvider.EXPLAIN, 1);
        queryWeights.put(SQLite3QueryProvider.CHECK_RTREE_TABLE, 3);
        queryWeights.put(SQLite3QueryProvider.INSERT_FTS4_COMMAND, 10);
        queryWeights.put(SQLite3QueryProvider.INSERT_FTS5_COMMAND, 10);
        queryWeights.put(SQLite3QueryProvider.MODIFY_STAT_TABLE, 5);
        queryWeights.put(SQLite3QueryProvider.TRANSACTION_START, 0);
        queryWeights.put(SQLite3QueryProvider.TRANSACTION_ROLLBACK, 0);
        queryWeights.put(SQLite3QueryProvider.TRANSACTION_COMMIT, 0);

        return queryWeights;
    }

    public enum SQLite3DDLStmt {
        CREATE_TABLE(SQLite3QueryProvider.CREATE_TABLE),
        CREATE_INDEX(SQLite3QueryProvider.CREATE_INDEX),
        CREATE_VIEW(SQLite3QueryProvider.CREATE_VIEW),
        ALTER_TABLE_RENAME_TABLE(SQLite3QueryProvider.ALTER_TABLE_RENAME_TABLE),
        ALTER_TABLE_RENAME_COLUMN(SQLite3QueryProvider.ALTER_TABLE_RENAME_COLUMN),
        ALTER_TABLE_ADD_COLUMN(SQLite3QueryProvider.ALTER_TABLE_ADD_COLUMN),
        ALTER_TABLE_DROP_COLUMN(SQLite3QueryProvider.ALTER_TABLE_DROP_COLUMN),
        REINDEX(SQLite3QueryProvider.REINDEX),
        DROP_TABLE(SQLite3QueryProvider.DROP_TABLE),
        DROP_INDEX(SQLite3QueryProvider.DROP_INDEX),
        DROP_VIEW(SQLite3QueryProvider.DROP_VIEW);

        private final SQLite3QueryProvider queryProvider;

        SQLite3DDLStmt(SQLite3QueryProvider queryProvider) {
            this.queryProvider = queryProvider;
        }

        public SQLite3QueryProvider getQueryProvider() {
            return queryProvider;
        }

        public static SQLite3QueryProvider getRandomDDL() {
            return Randomly.fromOptions(SQLite3DDLStmt.values()).getQueryProvider();
        }

    }

    public enum SQLite3DMLStmt {
        INSERT(SQLite3QueryProvider.INSERT, 10),
        REPLACE(SQLite3QueryProvider.REPLACE, 10),
        UPDATE(SQLite3QueryProvider.UPDATE, 10),
        DELETE(SQLite3QueryProvider.DELETE, 1);

        private final SQLite3QueryProvider queryProvider;
        private final int weight;
        private static final List<SQLite3DMLStmt> weightedMap = new ArrayList<>();

        SQLite3DMLStmt(SQLite3QueryProvider queryProvider, int weight) {
            this.queryProvider = queryProvider;
            this.weight = weight;
        }

        private SQLQueryAdapter getQuery(SQLite3GlobalState state) {
            return queryProvider.getQuery(state);
        }

        public static SQLQueryAdapter getRandomDML(SQLite3GlobalState state) {
            if (weightedMap.isEmpty()) {
                for (SQLite3DMLStmt dmlStmt : SQLite3DMLStmt.values()) {
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
        SQLite3GlobalState globalState = (SQLite3GlobalState) state;

        if (globalState.getDbmsSpecificOptions().useEquation()) {
            return;
        }

        Randomly r = new Randomly(SQLite3SpecialStringGenerator::generate);
        globalState.setRandomly(r);

        int nrTablesToCreate = Randomly.smallNumber() + 1;

        if (globalState.getDbmsSpecificOptions().testDBStats && Randomly.getBooleanWithSmallProbability()) {
            SQLQueryAdapter tableQuery = new SQLQueryAdapter(
                    "CREATE VIRTUAL TABLE IF NOT EXISTS stat USING dbstat(main)");
            globalState.executeStatement(tableQuery);
        }

        addSensiblePragmaDefaults(globalState);
        Map<SQLite3QueryProvider, Integer> tableWeights = new HashMap<>();
        tableWeights.put(SQLite3QueryProvider.CREATE_TABLE, 5);
        tableWeights.put(SQLite3QueryProvider.CREATE_VIRTUAL_TABLE_FTS4, 1);
        tableWeights.put(SQLite3QueryProvider.CREATE_VIRTUAL_TABLE_FTS5, 1);
        tableWeights.put(SQLite3QueryProvider.CREATE_VIRTUAL_TABLE_RTREE, 1);

        do {
            SQLQueryAdapter tableQuery = Randomly.fromWeights(tableWeights).getQuery(globalState);
            globalState.executeStatement(tableQuery);
        } while (globalState.getSchema().getDatabaseTables().size() < nrTablesToCreate);
        assert globalState.getSchema().getDatabaseTables().size() == nrTablesToCreate;

        StatementExecutor se = new StatementExecutor(globalState, buildQueryWeights(globalState), (q) -> {
            if (q.couldAffectSchema() && globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();

        checkTablesForGeneratedColumnLoops(globalState);
    }

    private void checkTablesForGeneratedColumnLoops(SQLite3GlobalState globalState) throws Exception {
        for (SQLite3Table table : globalState.getSchema().getDatabaseTables()) {
            SQLQueryAdapter q = new SQLQueryAdapter("SELECT * FROM " + table.getName(),
                    ExpectedErrors.from("needs an odd number of arguments", " requires an even number of arguments",
                            "generated column loop", "integer overflow", "malformed JSON",
                            "JSON cannot hold BLOB values", "JSON path error", "labels must be TEXT",
                            "table does not support scanning"));
            if (!q.execute(globalState)) {
                throw new IgnoreMeException();
            }
        }
    }

    private void addSensiblePragmaDefaults(SQLite3GlobalState globalState) throws Exception {
        List<String> pragmasToExecute = new ArrayList<>();
        if (!Randomly.getBooleanWithSmallProbability()) {
            pragmasToExecute.addAll(DEFAULT_PRAGMAS);
        }
        if (Randomly.getBoolean()) {
            // the PQS implementation currently assumes the default behavior of LIKE
            pragmasToExecute.add("PRAGMA case_sensitive_like=ON;");
        }
        if (Randomly.getBoolean()) {
            // the encoding has an influence how binary strings are cast
            pragmasToExecute.add(String.format("PRAGMA encoding = '%s';",
                    Randomly.fromOptions("UTF-8", "UTF-16", "UTF-16le", "UTF-16be")));
        }
        for (String s : pragmasToExecute) {
            globalState.executeStatement(new SQLQueryAdapter(s));
        }
    }

    @Override
    public SQLConnection createDatabase(GlobalState state) throws SQLException {
        SQLite3GlobalState globalState = (SQLite3GlobalState) state;
        return globalState.createDatabase();
    }

    @Override
    public String getDBMSName() {
        return "sqlite3";
    }

    @Override
    public DatabaseConnection createConnection(GlobalState globalState) throws Exception {
        return globalState.createNewConnection(globalState.getDatabaseName());
    }
}
