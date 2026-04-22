package dbradar.postgresql.oracle;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.common.oracle.TestOracle;
import dbradar.common.query.DBRadarResultSet;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLGeneratedColumnSupport;
import dbradar.postgresql.PostgreSQLProvider.PostgreSQLDDLStmt;
import dbradar.postgresql.PostgreSQLProvider.PostgreSQLDMLStmt;
import dbradar.postgresql.PostgreSQLProvider.PostgreSQLQueryProvider;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class PostgreSQLStressOracle implements TestOracle {

    private static final int MAX_BOOTSTRAP_ATTEMPTS = 1000;
    private static final int MAX_GENERATION_ATTEMPTS = 100;
    private static final Set<String> BOOTSTRAPPED_SHARED_DATABASES = ConcurrentHashMap.newKeySet();
    private static final Map<String, Object> SHARED_BOOTSTRAP_LOCKS = new ConcurrentHashMap<>();
    private static final PostgreSQLDDLStmt[] SAFE_BOOTSTRAP_DDL = {
            PostgreSQLDDLStmt.CREATE_TABLE,
            PostgreSQLDDLStmt.CREATE_INDEX,
            PostgreSQLDDLStmt.CREATE_VIEW,
            PostgreSQLDDLStmt.ALTER_TABLE_ADD_COLUMN,
            PostgreSQLDDLStmt.ALTER_TABLE_DROP_COLUMN,
            PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_TYPE,
            PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT,
            PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT,
            PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_SET_NOT_NULL,
            PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL,
            PostgreSQLDDLStmt.ALTER_TABLE_SET_COLUMN,
            PostgreSQLDDLStmt.ALTER_TABLE_RESET_COLUMN,
            PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_SET_STORAGE,
            PostgreSQLDDLStmt.ALTER_TABLE_ADD_UNIQUE_KEY,
            PostgreSQLDDLStmt.ALTER_TABLE_ADD_PRIMARY_KEY,
            PostgreSQLDDLStmt.ALTER_TABLE_ADD_FOREIGN_KEY,
            PostgreSQLDDLStmt.ALTER_TABLE_OPTION,
            PostgreSQLDDLStmt.ALTER_TABLE_RENAME_TABLE,
            PostgreSQLDDLStmt.REINDEX,
            PostgreSQLDDLStmt.TRUNCATE_TABLE,
            PostgreSQLDDLStmt.DROP_INDEX,
            PostgreSQLDDLStmt.DROP_VIEW
    };

    private final PostgreSQLGlobalState state;
    private boolean initialized;
    private PostgreSQLGeneratedColumnSupport.GeneratedColumnScenario bootstrapGeneratedColumnScenario;
    private int nextStatementKindIndex = Randomly.getNotCachedInteger(0, StressStatementKind.values().length);

    public PostgreSQLStressOracle(PostgreSQLGlobalState state) {
        this.state = state;
    }

    @Override
    public void check() throws Exception {
        if (!initialized) {
            ensureBootstrapped();
            warmUpStatementKinds();
            initialized = true;
        }

        StressStatementKind statementKind = StressStatementKind.values()[nextStatementKindIndex];
        nextStatementKindIndex = (nextStatementKindIndex + 1) % StressStatementKind.values().length;
        switch (statementKind) {
            case DDL:
                executeDdl(false, false, false);
                break;
            case DML:
                executeDml(false);
                break;
            case DQL:
                executeDql(false);
                break;
            default:
                throw new AssertionError("Unhandled stress statement kind");
        }
    }

    private void bootstrapDdl() throws Exception {
        int successCount = 0;
        int targetRandomDdl = Math.max(state.getOptions().getDdlCount() - 1, 0);
        int attempts = 0;
        while (successCount < targetRandomDdl) {
            if (attempts++ >= MAX_BOOTSTRAP_ATTEMPTS) {
                throw new AssertionError("Unable to satisfy the requested successful DDL bootstrap count");
            }
            if (executeDdl(successCount == 0, true, true)) {
                successCount++;
            }
        }
        if (!executeGeneratedColumnBootstrapDdl()) {
            throw new AssertionError("Unable to create the mandatory generated-column table during stress bootstrap");
        }
    }

    private void bootstrapDml() throws Exception {
        if (!executeGeneratedColumnBootstrapDml()) {
            throw new AssertionError("Unable to insert into the mandatory generated-column table during stress bootstrap");
        }
        int successCount = 1;
        int maxAttempts = Math.max(MAX_BOOTSTRAP_ATTEMPTS,
                state.getOptions().getDmlCount() * state.getOptions().getNrStatementRetryCount());
        int attempts = 0;
        while (successCount < state.getOptions().getDmlCount()) {
            if (attempts++ >= maxAttempts) {
                throw new AssertionError("Unable to satisfy the requested successful DML bootstrap count");
            }
            if (executeDml(true)) {
                successCount++;
            }
        }
    }

    private boolean executeDdl(boolean forceCreateTable, boolean requireSuccess, boolean bootstrapPhase) throws Exception {
        PostgreSQLDDLStmt statementKind = forceCreateTable ? PostgreSQLDDLStmt.CREATE_TABLE
                : (bootstrapPhase ? chooseBootstrapDdlStmt() : Randomly.fromOptions(PostgreSQLDDLStmt.values()));
        SQLQueryAdapter query = generateDdlQuery(statementKind);
        if (query == null) {
            return false;
        }
        boolean success = executeStatement(query);
        return requireSuccess ? success : true;
    }

    private boolean executeDml(boolean requireSuccess) throws Exception {
        SQLQueryAdapter query = generateDmlQuery();
        if (query == null) {
            return false;
        }
        boolean success = executeStatement(query);
        return requireSuccess ? success : true;
    }

    private boolean executeDql(boolean requireSuccess) throws Exception {
        SQLQueryAdapter query = generateSelectQuery();
        if (query == null) {
            return false;
        }
        state.getLogger().writeCurrent(query.getQueryString());
        state.getState().logStatement(query);
        try (DBRadarResultSet ignored = query.executeAndGet(state.getConnection(), false)) {
            return requireSuccess ? ignored != null : true;
        }
    }

    private boolean executeStatement(SQLQueryAdapter query) throws Exception {
        state.getLogger().writeCurrent(query.getQueryString());
        state.getState().logStatement(query);
        boolean success = query.execute(state.getConnection(), false);
        if (success && query.couldAffectSchema()) {
            state.updateSchema();
        }
        return success;
    }

    private SQLQueryAdapter generateDdlQuery(PostgreSQLDDLStmt statementKind) {
        for (int attempt = 0; attempt < MAX_GENERATION_ATTEMPTS; attempt++) {
            try {
                return statementKind.getQueryProvider().getQuery(state);
            } catch (QueryGenerationException | IgnoreMeException ignored) {
            }
        }
        return null;
    }

    private SQLQueryAdapter generateDmlQuery() {
        for (int attempt = 0; attempt < MAX_GENERATION_ATTEMPTS; attempt++) {
            try {
                return PostgreSQLDMLStmt.getRandomDML(state);
            } catch (QueryGenerationException | IgnoreMeException ignored) {
            }
        }
        return null;
    }

    private SQLQueryAdapter generateSelectQuery() {
        for (int attempt = 0; attempt < MAX_GENERATION_ATTEMPTS; attempt++) {
            try {
                return PostgreSQLQueryProvider.SELECT.getQuery(state);
            } catch (QueryGenerationException | IgnoreMeException ignored) {
            }
        }
        return null;
    }

    @Override
    public String getOracleName() {
        return "Stress";
    }

    public static void resetSharedBootstrapTracking() {
        BOOTSTRAPPED_SHARED_DATABASES.clear();
        SHARED_BOOTSTRAP_LOCKS.clear();
    }

    private void ensureBootstrapped() throws Exception {
        if (!state.getDbmsSpecificOptions().useSharedStressTopology()) {
            bootstrapDdl();
            bootstrapDml();
            return;
        }

        String databaseName = state.getDatabaseName();
        Object lock = SHARED_BOOTSTRAP_LOCKS.computeIfAbsent(databaseName, key -> new Object());
        synchronized (lock) {
            if (!BOOTSTRAPPED_SHARED_DATABASES.contains(databaseName)) {
                bootstrapDdl();
                bootstrapDml();
                BOOTSTRAPPED_SHARED_DATABASES.add(databaseName);
            }
        }
        state.updateSchema();
    }

    private void warmUpStatementKinds() throws Exception {
        for (StressStatementKind statementKind : StressStatementKind.values()) {
            boolean attempted = false;
            for (int attempt = 0; attempt < MAX_BOOTSTRAP_ATTEMPTS && !attempted; attempt++) {
                switch (statementKind) {
                    case DDL:
                        attempted = executeDdl(false, false, true);
                        break;
                    case DML:
                        attempted = executeDml(false);
                        break;
                    case DQL:
                        attempted = executeDql(false);
                        break;
                    default:
                        throw new AssertionError("Unhandled stress statement kind");
                }
            }
            if (!attempted) {
                throw new AssertionError("Unable to emit a " + statementKind + " statement during stress warm-up");
            }
        }
    }

    private PostgreSQLDDLStmt chooseBootstrapDdlStmt() {
        if (state.getSchema().getDatabaseTablesWithoutViews().isEmpty()) {
            return PostgreSQLDDLStmt.CREATE_TABLE;
        }

        if (state.getSchema().getDatabaseTablesWithoutViews().size() <= 1) {
            return Randomly.fromOptions(SAFE_BOOTSTRAP_DDL);
        }

        return Randomly.fromOptions(PostgreSQLDDLStmt.values());
    }

    private boolean executeGeneratedColumnBootstrapDdl() throws Exception {
        bootstrapGeneratedColumnScenario = PostgreSQLGeneratedColumnSupport.createStoredGeneratedTable(state);
        return executeStatement(bootstrapGeneratedColumnScenario.getCreateTableQuery());
    }

    private boolean executeGeneratedColumnBootstrapDml() throws Exception {
        if (bootstrapGeneratedColumnScenario == null) {
            return false;
        }
        boolean success = executeStatement(bootstrapGeneratedColumnScenario.getInsertQuery());
        if (!success) {
            return false;
        }
        try (DBRadarResultSet ignored = bootstrapGeneratedColumnScenario.getValidationQuery()
                .executeAndGet(state.getConnection(), false)) {
            return ignored != null;
        }
    }

    private enum StressStatementKind {
        DDL,
        DML,
        DQL
    }
}
