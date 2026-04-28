package dbradar;

import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLProvider;
import dbradar.postgresql.PostgreSQLOptions;

import java.sql.Statement;
import java.util.Locale;

public final class PostgreSQLAdditionalGrammarCoverageTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;

    private PostgreSQLAdditionalGrammarCoverageTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyVacuumRootExecutes();
        verifyMaterializedViewRootsExecute();
        verifyCreateTableCopyRootsExecute();
        verifyDropSequenceRootExecutes();
        verifyCommentRootsExecute();
        verifyPrivilegeRootsExecute();
        verifyRoutineRuleAndTriggerRootsExecute();
        verifyInsertExtendedRootsExecute();
        verifyUpdateExtendedRootsExecute();
        verifyDeleteExtendedRootsExecute();
        verifyMergeRootExecutes();
        verifyUtilitySessionRootsExecute();
    }

    private static void verifyVacuumRootExecutes() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_vacuum_state");
        try {
            bootstrapBaseObjects(state);
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.VACUUM, state, "VACUUM ");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyMaterializedViewRootsExecute() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_matview_state");
        try {
            bootstrapBaseObjects(state);
            execute(state, "CREATE MATERIALIZED VIEW mv_base_a AS SELECT id, v FROM base_a");
            state.updateSchema();
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.REFRESH_MATERIALIZED_VIEW,
                    state, "REFRESH MATERIALIZED VIEW ");
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.DROP_MATERIALIZED_VIEW,
                    state, "DROP MATERIALIZED VIEW ");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyCreateTableCopyRootsExecute() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_create_table_copy_state");
        try {
            bootstrapBaseObjects(state);
            execute(state, "CREATE TABLE part_parent (partition_key1 INT NOT NULL, payload INT) "
                    + "PARTITION BY RANGE (partition_key1)");
            state.updateSchema();
            verifyCreateTableAlternativeExecutes(state, "LIKE ");
            verifyCreateTableAlternativeExecutes(state, " AS SELECT ");
            verifyCreateTableAlternativeExecutes(state, " PARTITION OF ");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyDropSequenceRootExecutes() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_drop_sequence_state");
        try {
            execute(state, "CREATE SEQUENCE seq_to_drop");
            state.updateSchema();
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.DROP_SEQUENCE,
                    state, "DROP SEQUENCE ");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyCommentRootsExecute() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_comment_state");
        try {
            bootstrapBaseObjects(state);
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.COMMENT_ON_TABLE,
                    state, "COMMENT ON TABLE ");
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.COMMENT_ON_COLUMN,
                    state, "COMMENT ON COLUMN ");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyPrivilegeRootsExecute() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_privilege_state");
        try {
            bootstrapBaseObjects(state);
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.GRANT_TABLE,
                    state, "GRANT ");
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.REVOKE_TABLE,
                    state, "REVOKE ");
            execute(state, "CREATE SEQUENCE seq_privilege_check");
            execute(state, "CREATE OR REPLACE FUNCTION public.fn_privilege_check(x int) "
                    + "RETURNS int LANGUAGE SQL AS 'SELECT x'");
            execute(state, "CREATE OR REPLACE PROCEDURE public.proc_privilege_check(IN x int) "
                    + "LANGUAGE plpgsql AS 'BEGIN PERFORM x; END'");
            state.updateSchema();
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.GRANT_SCHEMA,
                    state, "GRANT ");
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.REVOKE_SCHEMA,
                    state, "REVOKE ");
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.GRANT_SEQUENCE,
                    state, "GRANT ");
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.REVOKE_SEQUENCE,
                    state, "REVOKE ");
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.GRANT_FUNCTION,
                    state, "GRANT ");
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.REVOKE_FUNCTION,
                    state, "REVOKE ");
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.GRANT_PROCEDURE,
                    state, "GRANT ");
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.REVOKE_PROCEDURE,
                    state, "REVOKE ");
            execute(state, "SELECT * FROM base_a");
            execute(state, "SELECT nextval('seq_privilege_check')");
            execute(state, "SELECT public.fn_privilege_check(1)");
            execute(state, "CALL public.proc_privilege_check(1)");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyRoutineRuleAndTriggerRootsExecute() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_routine_rule_trigger_state");
        try {
            bootstrapBaseObjects(state);
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.CREATE_FUNCTION,
                    state, "CREATE OR REPLACE FUNCTION ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_FUNCTION,
                    state, "ALTER FUNCTION ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.DROP_FUNCTION,
                    state, "DROP FUNCTION ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.CREATE_PROCEDURE,
                    state, "CREATE OR REPLACE PROCEDURE ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.ALTER_PROCEDURE,
                    state, "ALTER PROCEDURE ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.DROP_PROCEDURE,
                    state, "DROP PROCEDURE ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.CREATE_RULE,
                    state, "CREATE OR REPLACE RULE ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.DROP_RULE,
                    state, "DROP RULE ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.CREATE_TRIGGER,
                    state, "CREATE TRIGGER ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.DROP_TRIGGER,
                    state, "DROP TRIGGER ");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyInsertExtendedRootsExecute() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_insert_extended_state");
        try {
            bootstrapBaseObjects(state);
            SQLQueryAdapter insertSelect = generateQuery(PostgreSQLProvider.PostgreSQLQueryProvider.INSERT_SELECT, state);
            String insertSelectSql = normalize(insertSelect.getQueryString());
            require(insertSelectSql.contains(" SELECT "),
                    "Expected INSERT_SELECT to generate INSERT ... SELECT: " + insertSelect.getQueryString());
            require(!insertSelectSql.contains(" LIMIT "),
                    "Expected INSERT_SELECT to avoid nondeterministic LIMIT scans: " + insertSelect.getQueryString());
            require(insertSelect.execute(state),
                    "Expected generated SQL to execute successfully: " + insertSelect.getQueryString());
            state.updateSchema();
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.INSERT_ON_CONFLICT,
                    state, " ON CONFLICT DO NOTHING");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.INSERT_RETURNING,
                    state, " RETURNING ");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyUpdateExtendedRootsExecute() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_update_extended_state");
        try {
            bootstrapBaseObjects(state);
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.UPDATE_FROM,
                    state, " FROM ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.UPDATE_CTE,
                    state, "WITH ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.UPDATE_RETURNING,
                    state, " RETURNING ");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyDeleteExtendedRootsExecute() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_delete_extended_state");
        try {
            bootstrapBaseObjects(state);
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.DELETE_USING,
                    state, " USING ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.DELETE_RETURNING,
                    state, " RETURNING ");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyMergeRootExecutes() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_merge_state");
        try {
            bootstrapBaseObjects(state);
            verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider.MERGE,
                    state, "MERGE INTO ");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyUtilitySessionRootsExecute() throws Exception {
        PostgreSQLGlobalState state = createState("additional_grammar_utility_session_state");
        try {
            bootstrapBaseObjects(state);
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.LOCK_TABLE,
                    state, "LOCK TABLE ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.PREPARE_EXECUTE,
                    state, "PREPARE ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.SAVEPOINT_RELEASE,
                    state, "SAVEPOINT ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.DECLARE_FETCH_CLOSE,
                    state, "DECLARE ");
            verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider.COPY_TO_STDOUT,
                    state, "COPY ");
        } finally {
            closeQuietly(state);
        }
    }

    private static void verifyExecutableRoot(PostgreSQLProvider.PostgreSQLQueryProvider provider,
                                             PostgreSQLGlobalState state,
                                             String expectedPrefix) throws Exception {
        SQLQueryAdapter query = generateQuery(provider, state);
        String sql = query.getQueryString();
        require(normalize(sql).startsWith(expectedPrefix),
                "Expected " + provider + " to generate SQL starting with " + expectedPrefix + ": " + sql);
        require(query.execute(state), "Expected generated SQL to execute successfully: " + sql);
        state.updateSchema();
    }

    private static void verifyExecutableRootContains(PostgreSQLProvider.PostgreSQLQueryProvider provider,
                                                     PostgreSQLGlobalState state,
                                                     String expectedToken) throws Exception {
        SQLQueryAdapter query = generateQuery(provider, state);
        String sql = query.getQueryString();
        require(normalize(sql).contains(expectedToken),
                "Expected " + provider + " to generate SQL containing " + expectedToken + ": " + sql);
        require(query.execute(state), "Expected generated SQL to execute successfully: " + sql);
        state.updateSchema();
    }

    private static void verifyCreateTableAlternativeExecutes(PostgreSQLGlobalState state,
                                                             String expectedToken) throws Exception {
        SQLQueryAdapter query = null;
        String sql = null;
        for (long seed = 1; seed <= 1000; seed++) {
            state.setRandomly(new Randomly(seed));
            try {
                SQLQueryAdapter candidate = PostgreSQLProvider.PostgreSQLQueryProvider.CREATE_TABLE.getQuery(state);
                String candidateSql = candidate.getQueryString();
                if (normalize(candidateSql).contains(expectedToken)) {
                    query = candidate;
                    sql = candidateSql;
                    break;
                }
            } catch (IgnoreMeException | QueryGenerationException ignored) {
            }
        }
        require(query != null, "Expected merged CREATE_TABLE root to generate variant containing "
                + expectedToken);
        require(query.execute(state), "Expected generated SQL to execute successfully: " + sql);
        state.updateSchema();
    }

    private static SQLQueryAdapter generateQuery(PostgreSQLProvider.PostgreSQLQueryProvider provider,
                                                 PostgreSQLGlobalState state) {
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
        return query;
    }

    private static void bootstrapBaseObjects(PostgreSQLGlobalState state) throws Exception {
        execute(state, "CREATE TABLE base_a (id INT PRIMARY KEY, v INT NOT NULL, t TEXT)");
        execute(state, "INSERT INTO base_a (id, v, t) VALUES (1, 10, 'a'), (2, 20, 'b')");
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
