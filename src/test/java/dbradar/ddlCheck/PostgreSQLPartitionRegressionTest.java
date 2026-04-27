package dbradar.ddlCheck;

import dbradar.IgnoreMeException;
import dbradar.MainOptions;
import dbradar.Randomly;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.common.query.generator.QueryGenerator;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLKeyFunctionManager;
import dbradar.postgresql.PostgreSQLOptions;
import dbradar.postgresql.PostgreSQLSchema;
import dbradar.postgresql.oracle.PostgreSQLEDCOracle;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public final class PostgreSQLPartitionRegressionTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 5432;
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";

    private PostgreSQLPartitionRegressionTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyPartitionedTablesReplayIntoSemiState();
        verifyListPartitionedTablesReplayIntoSemiState();
        verifyHashPartitionedTablesReplayIntoSemiState();
        verifyMultiColumnRangeReplayIntoSemiState();
        verifyExpressionPartitionedTablesReplayIntoSemiState();
        verifyMixedModulusHashPartitionRouting();
        verifyTemporaryTableIndexesReplayIntoSemiState();
        verifyPartitionLocalForeignKeysReplayIntoSemiState();
        verifyParentOnlyPartitionedUniqueIndexesReplayAfterPartitions();
        verifyBootstrapDdlSequenceRecoversWhenLastTableWasDropped();
        verifyPartitionGrammarRootsGenerateExecutableStatements();
    }

    private static void verifyPartitionedTablesReplayIntoSemiState() throws Exception {
        PostgreSQLGlobalState state = createState("partition_replay_state");
        PostgreSQLGlobalState semiState = createState("partition_replay_state_semi");
        try {
            execute(state, "CREATE TABLE sales (id INT, sale_date INT NOT NULL, region TEXT) PARTITION BY RANGE (sale_date)");
            execute(state, "CREATE TABLE sales_staging (id INT, sale_date INT NOT NULL, region TEXT)");
            execute(state, "ALTER TABLE sales ATTACH PARTITION sales_staging DEFAULT");
            state.updateSchema();

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            List<SQLQueryAdapter> fetchedStatements = oracle.fetchCreateStmts(state);
            List<String> fetchedSql = fetchedStatements.stream().map(SQLQueryAdapter::getQueryString).toList();

            requireContains(fetchedSql, "PARTITION BY RANGE (sale_date)");
            requireContains(fetchedSql, "PARTITION OF sales DEFAULT");

            List<SQLQueryAdapter> replayStatements = new ArrayList<>(fetchedStatements);
            List<String> replayedSql = oracle.replayCreateStmts(semiState, replayStatements);
            requireContains(replayedSql, "PARTITION BY RANGE (sale_date)");

            execute(state, "INSERT INTO sales (id, sale_date, region) VALUES (1, 10, 'north'), (2, 150, 'south')");
            execute(semiState, "INSERT INTO sales (id, sale_date, region) VALUES (1, 10, 'north'), (2, 150, 'south')");

            requireSingleValue(state, "SELECT count(*) FROM sales_staging", 2);
            requireSingleValue(semiState, "SELECT count(*) FROM sales_staging", 2);
        } finally {
            closeQuietly(state, semiState);
        }
    }

    private static void verifyListPartitionedTablesReplayIntoSemiState() throws Exception {
        PostgreSQLGlobalState state = createState("partition_list_state");
        PostgreSQLGlobalState semiState = createState("partition_list_state_semi");
        try {
            execute(state, "CREATE TABLE region_sales (id INT, partition_key1 INT NOT NULL, payload TEXT) PARTITION BY LIST (partition_key1)");
            execute(state, "CREATE TABLE region_sales_p0 PARTITION OF region_sales FOR VALUES IN (1, 2, 3)");
            execute(state, "CREATE TABLE region_sales_default PARTITION OF region_sales DEFAULT");
            state.updateSchema();

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            List<SQLQueryAdapter> fetchedStatements = oracle.fetchCreateStmts(state);
            List<String> fetchedSql = fetchedStatements.stream().map(SQLQueryAdapter::getQueryString).toList();
            requireContains(fetchedSql, "PARTITION BY LIST (partition_key1)");
            requireContains(fetchedSql, "FOR VALUES IN (1, 2, 3)");

            List<String> replayedSql = oracle.replayCreateStmts(semiState, new ArrayList<>(fetchedStatements));
            requireContains(replayedSql, "PARTITION BY LIST (partition_key1)");

            execute(state, "INSERT INTO region_sales (id, partition_key1, payload) VALUES (1, 1, 'east'), (2, 99, 'other')");
            execute(semiState, "INSERT INTO region_sales (id, partition_key1, payload) VALUES (1, 1, 'east'), (2, 99, 'other')");

            requireSingleValue(state, "SELECT count(*) FROM region_sales_p0", 1);
            requireSingleValue(state, "SELECT count(*) FROM region_sales_default", 1);
            requireSingleValue(semiState, "SELECT count(*) FROM region_sales_p0", 1);
            requireSingleValue(semiState, "SELECT count(*) FROM region_sales_default", 1);
        } finally {
            closeQuietly(state, semiState);
        }
    }

    private static void verifyHashPartitionedTablesReplayIntoSemiState() throws Exception {
        PostgreSQLGlobalState state = createState("partition_hash_state");
        PostgreSQLGlobalState semiState = createState("partition_hash_state_semi");
        try {
            execute(state, "CREATE TABLE tenant_events (partition_key1 INT NOT NULL, partition_key2 INT NOT NULL, payload TEXT) PARTITION BY HASH (partition_key1, partition_key2)");
            execute(state, "CREATE TABLE tenant_events_p0 PARTITION OF tenant_events FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
            execute(state, "CREATE TABLE tenant_events_p1 PARTITION OF tenant_events FOR VALUES WITH (MODULUS 2, REMAINDER 1)");
            state.updateSchema();

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            List<SQLQueryAdapter> fetchedStatements = oracle.fetchCreateStmts(state);
            List<String> fetchedSql = fetchedStatements.stream().map(SQLQueryAdapter::getQueryString).toList();
            requireContains(fetchedSql, "PARTITION BY HASH (partition_key1, partition_key2)");
            requireContains(fetchedSql, "FOR VALUES WITH (modulus 2, remainder 0)");
            requireContains(fetchedSql, "FOR VALUES WITH (modulus 2, remainder 1)");

            List<String> replayedSql = oracle.replayCreateStmts(semiState, new ArrayList<>(fetchedStatements));
            requireContains(replayedSql, "PARTITION BY HASH (partition_key1, partition_key2)");

            execute(state, "INSERT INTO tenant_events (partition_key1, partition_key2, payload) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd')");
            execute(semiState, "INSERT INTO tenant_events (partition_key1, partition_key2, payload) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd')");

            requireSingleValue(state, "SELECT count(*) FROM tenant_events", 4);
            requireSingleValue(semiState, "SELECT count(*) FROM tenant_events", 4);
        } finally {
            closeQuietly(state, semiState);
        }
    }

    private static void verifyMultiColumnRangeReplayIntoSemiState() throws Exception {
        PostgreSQLGlobalState state = createState("partition_multi_range_state");
        PostgreSQLGlobalState semiState = createState("partition_multi_range_state_semi");
        try {
            execute(state, "CREATE TABLE audit_log (partition_key1 INT NOT NULL, partition_key2 INT NOT NULL, payload TEXT) PARTITION BY RANGE (partition_key1, partition_key2)");
            execute(state, "CREATE TABLE audit_log_p0 PARTITION OF audit_log FOR VALUES FROM (0, 0) TO (100, 100)");
            execute(state, "CREATE TABLE audit_log_default PARTITION OF audit_log DEFAULT");
            state.updateSchema();

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            List<SQLQueryAdapter> fetchedStatements = oracle.fetchCreateStmts(state);
            List<String> fetchedSql = fetchedStatements.stream().map(SQLQueryAdapter::getQueryString).toList();
            requireContains(fetchedSql, "PARTITION BY RANGE (partition_key1, partition_key2)");
            requireContains(fetchedSql, "FOR VALUES FROM (0, 0) TO (100, 100)");

            List<String> replayedSql = oracle.replayCreateStmts(semiState, new ArrayList<>(fetchedStatements));
            requireContains(replayedSql, "PARTITION BY RANGE (partition_key1, partition_key2)");

            execute(state, "INSERT INTO audit_log (partition_key1, partition_key2, payload) VALUES (10, 10, 'inside'), (150, 150, 'default')");
            execute(semiState, "INSERT INTO audit_log (partition_key1, partition_key2, payload) VALUES (10, 10, 'inside'), (150, 150, 'default')");

            requireSingleValue(state, "SELECT count(*) FROM audit_log_p0", 1);
            requireSingleValue(state, "SELECT count(*) FROM audit_log_default", 1);
            requireSingleValue(semiState, "SELECT count(*) FROM audit_log_p0", 1);
            requireSingleValue(semiState, "SELECT count(*) FROM audit_log_default", 1);
        } finally {
            closeQuietly(state, semiState);
        }
    }

    private static void verifyExpressionPartitionedTablesReplayIntoSemiState() throws Exception {
        PostgreSQLGlobalState state = createState("partition_expr_range_state");
        PostgreSQLGlobalState semiState = createState("partition_expr_range_state_semi");
        try {
            execute(state, "CREATE TABLE expr_sales (partition_key1 INT NOT NULL, partition_key2 INT NOT NULL, payload TEXT) PARTITION BY RANGE ((partition_key1 + partition_key2))");
            execute(state, "CREATE TABLE expr_sales_low PARTITION OF expr_sales FOR VALUES FROM (0) TO (100)");
            execute(state, "CREATE TABLE expr_sales_default PARTITION OF expr_sales DEFAULT");
            state.updateSchema();

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            List<SQLQueryAdapter> fetchedStatements = oracle.fetchCreateStmts(state);
            List<String> fetchedSql = fetchedStatements.stream().map(SQLQueryAdapter::getQueryString).toList();
            requireContains(fetchedSql, "PARTITION BY RANGE");
            requireContains(fetchedSql, "partition_key1 + partition_key2");
            requireContains(fetchedSql, "FOR VALUES FROM (0) TO (100)");

            List<String> replayedSql = oracle.replayCreateStmts(semiState, new ArrayList<>(fetchedStatements));
            requireContains(replayedSql, "partition_key1 + partition_key2");

            execute(state, "INSERT INTO expr_sales (partition_key1, partition_key2, payload) VALUES (10, 20, 'inside'), (100, 50, 'default')");
            execute(semiState, "INSERT INTO expr_sales (partition_key1, partition_key2, payload) VALUES (10, 20, 'inside'), (100, 50, 'default')");

            requireSingleValue(state, "SELECT count(*) FROM expr_sales_low", 1);
            requireSingleValue(state, "SELECT count(*) FROM expr_sales_default", 1);
            requireSingleValue(semiState, "SELECT count(*) FROM expr_sales_low", 1);
            requireSingleValue(semiState, "SELECT count(*) FROM expr_sales_default", 1);
        } finally {
            closeQuietly(state, semiState);
        }
    }

    private static void verifyMixedModulusHashPartitionRouting() throws Exception {
        PostgreSQLGlobalState state = createState("partition_mixed_hash_state");
        try {
            execute(state, "CREATE TABLE mixed_hash_events (partition_key1 INT NOT NULL, payload TEXT) PARTITION BY HASH (partition_key1)");
            execute(state, "CREATE TABLE mixed_hash_events_even PARTITION OF mixed_hash_events FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
            execute(state, "CREATE TABLE mixed_hash_events_one PARTITION OF mixed_hash_events FOR VALUES WITH (MODULUS 4, REMAINDER 1)");
            execute(state, "CREATE TABLE mixed_hash_events_three PARTITION OF mixed_hash_events FOR VALUES WITH (MODULUS 4, REMAINDER 3)");
            state.updateSchema();

            PostgreSQLSchema.PostgreSQLTable parent = findTable(state, "mixed_hash_events");
            state.getSchema().generatePartitionInsertValues(parent);

            execute(state, "INSERT INTO mixed_hash_events (partition_key1, payload) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
            requireSingleValue(state, "SELECT count(*) FROM mixed_hash_events", 4);
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyTemporaryTableIndexesReplayIntoSemiState() throws Exception {
        PostgreSQLGlobalState state = createState("temp_index_replay_state");
        PostgreSQLGlobalState semiState = createState("temp_index_replay_state_semi");
        try {
            execute(state, "CREATE TEMP TABLE temp_index_replay (a INT NOT NULL, b NUMERIC, c NUMERIC)");
            execute(state, "CREATE UNIQUE INDEX temp_index_replay_i1 ON temp_index_replay (b DESC, c NULLS LAST, a NULLS LAST)");
            state.updateSchema();

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            List<SQLQueryAdapter> fetchedStatements = oracle.fetchCreateStmts(state);
            List<String> fetchedSql = fetchedStatements.stream().map(SQLQueryAdapter::getQueryString).toList();
            requireContains(fetchedSql, "CREATE TEMPORARY TABLE temp_index_replay");
            requireContains(fetchedSql, "CREATE UNIQUE INDEX temp_index_replay_i1 ON pg_temp.temp_index_replay");

            List<String> replayedSql = oracle.replayCreateStmts(semiState, new ArrayList<>(fetchedStatements));
            requireContains(replayedSql, "CREATE UNIQUE INDEX temp_index_replay_i1 ON pg_temp.temp_index_replay");

            requireSingleValue(state,
                    "SELECT count(*) FROM pg_indexes WHERE tablename = 'temp_index_replay' AND indexname = 'temp_index_replay_i1'",
                    1);
            requireSingleValue(semiState,
                    "SELECT count(*) FROM pg_indexes WHERE tablename = 'temp_index_replay' AND indexname = 'temp_index_replay_i1'",
                    1);

            String insertRows = "INSERT INTO temp_index_replay (a, b, c) VALUES (12, 12.991, -6), (12, 12.991, -120)";
            execute(state, insertRows);
            execute(semiState, insertRows);

            String updateRows = "UPDATE temp_index_replay SET c = -7 WHERE TRUE";
            requireExecutionFailure(state, updateRows, "duplicate key value violates unique constraint");
            requireExecutionFailure(semiState, updateRows, "duplicate key value violates unique constraint");
        } finally {
            closeQuietly(state, semiState);
        }
    }

    private static void verifyPartitionLocalForeignKeysReplayIntoSemiState() throws Exception {
        PostgreSQLGlobalState state = createState("partition_child_fk_replay_state");
        PostgreSQLGlobalState semiState = createState("partition_child_fk_replay_state_semi");
        try {
            execute(state, "CREATE TABLE fk_ref (id INT PRIMARY KEY)");
            execute(state, "CREATE TABLE fk_parent (partition_key1 INT NOT NULL, payload INT) PARTITION BY RANGE (partition_key1)");
            execute(state, "CREATE TABLE fk_child_low PARTITION OF fk_parent FOR VALUES FROM (0) TO (100)");
            execute(state, "ALTER TABLE fk_child_low ADD FOREIGN KEY (payload) REFERENCES fk_ref(id)");
            state.updateSchema();

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            List<SQLQueryAdapter> fetchedStatements = oracle.fetchCreateStmts(state);
            List<String> fetchedSql = fetchedStatements.stream().map(SQLQueryAdapter::getQueryString).toList();
            requireContains(fetchedSql, "ALTER TABLE fk_child_low ADD FOREIGN KEY (payload) REFERENCES fk_ref(id)");

            List<String> replayedSql = oracle.replayCreateStmts(semiState, new ArrayList<>(fetchedStatements));
            requireContains(replayedSql, "ALTER TABLE fk_child_low ADD FOREIGN KEY (payload) REFERENCES fk_ref(id)");

            String insertViolatingChildFk = "INSERT INTO fk_parent (partition_key1, payload) VALUES (50, 999)";
            requireExecutionFailure(state, insertViolatingChildFk, "violates foreign key constraint");
            requireExecutionFailure(semiState, insertViolatingChildFk, "violates foreign key constraint");
        } finally {
            closeQuietly(state, semiState);
        }
    }

    private static void verifyParentOnlyPartitionedUniqueIndexesReplayAfterPartitions() throws Exception {
        PostgreSQLGlobalState state = createState("partition_parent_only_index_state");
        PostgreSQLGlobalState semiState = createState("partition_parent_only_index_state_semi");
        try {
            execute(state, "CREATE TABLE a_replay_parent (partition_key1 INT NOT NULL, payload INT) PARTITION BY RANGE (partition_key1)");
            execute(state, "CREATE TABLE z_replay_child PARTITION OF a_replay_parent FOR VALUES FROM (0) TO (100)");
            execute(state, "CREATE UNIQUE INDEX b_replay_parent_uq ON ONLY a_replay_parent (partition_key1)");
            state.updateSchema();

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            List<SQLQueryAdapter> fetchedStatements = oracle.fetchCreateStmts(state);
            List<String> replayedSql = oracle.replayCreateStmts(semiState, new ArrayList<>(fetchedStatements));
            requireContains(replayedSql, "CREATE UNIQUE INDEX b_replay_parent_uq ON ONLY public.a_replay_parent");

            String duplicateInsert = "INSERT INTO a_replay_parent (partition_key1, payload) VALUES (50, 1), (50, 2)";
            execute(state, duplicateInsert);
            execute(semiState, duplicateInsert);
            requireSingleValue(state, "SELECT count(*) FROM a_replay_parent", 2);
            requireSingleValue(semiState, "SELECT count(*) FROM a_replay_parent", 2);
        } finally {
            closeQuietly(state, semiState);
        }
    }

    private static void verifyBootstrapDdlSequenceRecoversWhenLastTableWasDropped() throws Exception {
        PostgreSQLGlobalState state = createState("bootstrap_empty_recovery_state");
        try {
            List<String> ddlSeq = new ArrayList<>();
            String createTable = "CREATE TABLE bootstrap_erased (c1 INT)";
            String dropTable = "DROP TABLE bootstrap_erased";
            execute(state, createTable);
            ddlSeq.add(createTable);
            execute(state, dropTable);
            ddlSeq.add(dropTable);
            state.updateSchema();

            if (!state.getSchema().getDatabaseTablesWithoutViews().isEmpty()) {
                throw new AssertionError("Expected bootstrap schema to have no base table before recovery");
            }

            PostgreSQLEDCOracle oracle = new PostgreSQLEDCOracle(state);
            oracle.getDDLSequence(ddlSeq, ddlSeq.size());

            if (state.getSchema().getDatabaseTablesWithoutViews().isEmpty()) {
                throw new AssertionError("Expected bootstrap recovery to add a base table");
            }
            if (ddlSeq.size() <= 2) {
                throw new AssertionError("Expected bootstrap recovery DDL to be appended");
            }
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyPartitionGrammarRootsGenerateExecutableStatements() throws Exception {
        PostgreSQLGlobalState coverageState = createState("partition_generator_state");
        PostgreSQLGlobalState mutationState = createState("partition_generator_mutation_state");
        try {
            verifyCreatePartitionedTableGrammarCoverage(coverageState);

            execute(mutationState, "CREATE TABLE generated_parent (partition_key1 INT NOT NULL, payload TEXT) PARTITION BY RANGE (partition_key1)");
            execute(mutationState, "CREATE TABLE generated_parent_staging (partition_key1 INT NOT NULL, payload TEXT)");
            mutationState.updateSchema();

            String createPartition = generateQuery(mutationState, "create_table_partition", 29L);
            if (!normalize(createPartition).contains("PARTITION OF")) {
                throw new AssertionError("Expected partition child table, got: " + createPartition);
            }
            execute(mutationState, createPartition);
            mutationState.updateSchema();

            String detachPartition = generateQuery(mutationState, "alter_table_detach_partition", 47L);
            if (!normalize(detachPartition).contains("DETACH PARTITION")) {
                throw new AssertionError("Expected detach partition statement, got: " + detachPartition);
            }
            execute(mutationState, detachPartition);
            mutationState.updateSchema();

            String attachPartition = generateQuery(mutationState, "alter_table_attach_partition", 53L);
            if (!normalize(attachPartition).contains("ATTACH PARTITION")) {
                throw new AssertionError("Expected attach partition statement, got: " + attachPartition);
            }
            execute(mutationState, attachPartition);
            mutationState.updateSchema();

            execute(mutationState, "CREATE TABLE generated_bound_parent (partition_key1 INT NOT NULL, payload TEXT) PARTITION BY RANGE (partition_key1)");
            execute(mutationState, "CREATE TABLE generated_bound_parent_staging (partition_key1 INT NOT NULL, payload TEXT)");
            mutationState.updateSchema();
            String attachPartitionWithBound = generateQuery(mutationState, "alter_table_attach_partition_for_values", 59L);
            if (!normalize(attachPartitionWithBound).contains("ATTACH PARTITION")
                    || !normalize(attachPartitionWithBound).contains("FOR VALUES")) {
                throw new AssertionError("Expected attach partition with explicit bound, got: " + attachPartitionWithBound);
            }
            execute(mutationState, attachPartitionWithBound);
        } finally {
            closeQuietly(coverageState, mutationState);
        }
    }

    private static void verifyCreatePartitionedTableGrammarCoverage(PostgreSQLGlobalState state) throws Exception {
        boolean sawList = false;
        boolean sawHash = false;
        boolean sawMultiColumnRange = false;
        boolean sawExpressionRange = false;

        for (long seed = 1; seed <= 200; seed++) {
            String createPartitionedTable;
            try {
                createPartitionedTable = generateQuery(state, "create_table", seed);
            } catch (IgnoreMeException | QueryGenerationException ignored) {
                continue;
            }
            String normalized = normalize(createPartitionedTable);
            if (normalized.contains("PARTITION BY LIST")) {
                sawList = true;
            }
            if (normalized.contains("PARTITION BY HASH")) {
                sawHash = true;
            }
            if (normalized.contains("PARTITION BY RANGE (PARTITION_KEY1, PARTITION_KEY2)")) {
                sawMultiColumnRange = true;
            }
            if (normalized.contains("PARTITION BY RANGE ((PARTITION_KEY1 + PARTITION_KEY2))")) {
                sawExpressionRange = true;
            }

            if ((sawList && normalized.contains("PARTITION BY LIST"))
                    || (sawHash && normalized.contains("PARTITION BY HASH"))
                    || (sawMultiColumnRange && normalized.contains("PARTITION BY RANGE (PARTITION_KEY1, PARTITION_KEY2)"))
                    || (sawExpressionRange && normalized.contains("PARTITION BY RANGE ((PARTITION_KEY1 + PARTITION_KEY2))"))) {
                execute(state, createPartitionedTable);
                state.updateSchema();
            }
            if (sawList && sawHash && sawMultiColumnRange && sawExpressionRange) {
                return;
            }
        }
        throw new AssertionError(String.format("Missing partition grammar coverage: list=%s hash=%s multiRange=%s exprRange=%s",
                sawList, sawHash, sawMultiColumnRange, sawExpressionRange));
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

    private static String generateQuery(PostgreSQLGlobalState state, String root, long seed) {
        state.setRandomly(new Randomly(seed));
        QueryGenerator generator = new QueryGenerator(state, state.getGrammar(), root, new PostgreSQLKeyFunctionManager(state));
        return generator.getRandomQuery().toQueryString();
    }

    private static void execute(PostgreSQLGlobalState state, String sql) throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
        }
    }

    private static void requireSingleValue(PostgreSQLGlobalState state, String sql, int expected) throws Exception {
        try (Statement statement = state.getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                throw new AssertionError("No result for query: " + sql);
            }
            int actual = resultSet.getInt(1);
            if (actual != expected) {
                throw new AssertionError(String.format("Expected %d for [%s] but got %d", expected, sql, actual));
            }
        }
    }

    private static void requireContains(List<String> statements, String expectedFragment) {
        String normalizedFragment = normalize(expectedFragment);
        for (String statement : statements) {
            if (normalize(statement).contains(normalizedFragment)) {
                return;
            }
        }
        throw new AssertionError("Expected fragment not found: " + expectedFragment + "\nStatements:\n" + String.join("\n", statements));
    }

    private static PostgreSQLSchema.PostgreSQLTable findTable(PostgreSQLGlobalState state, String tableName) {
        return state.getSchema().getDatabaseTablesWithoutViews().stream()
                .filter(table -> table.getName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected table not found: " + tableName));
    }

    private static void requireExecutionFailure(PostgreSQLGlobalState state, String sql, String expectedMessageFragment)
            throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
            throw new AssertionError("Expected statement to fail: " + sql);
        } catch (Exception exception) {
            String message = exception.getMessage();
            if (message == null || !message.contains(expectedMessageFragment)) {
                throw new AssertionError("Expected failure containing [" + expectedMessageFragment + "] for [" + sql
                        + "] but got: " + message, exception);
            }
        }
    }

    private static String normalize(String sql) {
        return sql.replaceAll("\\s+", " ").trim().toUpperCase();
    }

    private static void closeQuietly(PostgreSQLGlobalState... states) {
        for (PostgreSQLGlobalState state : states) {
            if (state == null || state.getConnection() == null) {
                continue;
            }
            try {
                state.getConnection().close();
            } catch (Exception ignored) {
            }
        }
    }
}
