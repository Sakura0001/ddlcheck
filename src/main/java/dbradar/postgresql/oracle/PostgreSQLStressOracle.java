package dbradar.postgresql.oracle;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.common.oracle.TestOracle;
import dbradar.common.query.DBRadarResultSet;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLProvider.PostgreSQLDDLStmt;
import dbradar.postgresql.PostgreSQLProvider.PostgreSQLDMLStmt;
import dbradar.postgresql.PostgreSQLProvider.PostgreSQLQueryProvider;

public final class PostgreSQLStressOracle implements TestOracle {

    private static final int MAX_BOOTSTRAP_ATTEMPTS = 1000;
    private static final int MAX_GENERATION_ATTEMPTS = 100;

    private final PostgreSQLGlobalState state;
    private boolean initialized;

    public PostgreSQLStressOracle(PostgreSQLGlobalState state) {
        this.state = state;
    }

    @Override
    public void check() throws Exception {
        if (!initialized) {
            bootstrapDdl();
            bootstrapDml();
            initialized = true;
        }

        switch (Randomly.fromOptions(StressStatementKind.values())) {
            case DDL:
                executeDdl(false);
                break;
            case DML:
                executeDml();
                break;
            case DQL:
                executeDql();
                break;
            default:
                throw new AssertionError("Unhandled stress statement kind");
        }
    }

    private void bootstrapDdl() throws Exception {
        int successCount = 0;
        int attempts = 0;
        while (successCount < state.getOptions().getDdlCount()) {
            if (attempts++ >= MAX_BOOTSTRAP_ATTEMPTS) {
                throw new AssertionError("Unable to satisfy the requested successful DDL bootstrap count");
            }
            if (executeDdl(successCount == 0)) {
                successCount++;
            }
        }
    }

    private void bootstrapDml() throws Exception {
        int successCount = 0;
        int attempts = 0;
        while (successCount < state.getOptions().getDmlCount()) {
            if (attempts++ >= MAX_BOOTSTRAP_ATTEMPTS) {
                throw new AssertionError("Unable to satisfy the requested successful DML bootstrap count");
            }
            if (executeDml()) {
                successCount++;
            }
        }
    }

    private boolean executeDdl(boolean forceCreateTable) throws Exception {
        PostgreSQLDDLStmt statementKind = forceCreateTable ? PostgreSQLDDLStmt.CREATE_TABLE
                : Randomly.fromOptions(PostgreSQLDDLStmt.values());
        SQLQueryAdapter query = generateDdlQuery(statementKind);
        return query != null && executeStatement(query);
    }

    private boolean executeDml() throws Exception {
        SQLQueryAdapter query = generateDmlQuery();
        return query != null && executeStatement(query);
    }

    private boolean executeDql() throws Exception {
        SQLQueryAdapter query = generateSelectQuery();
        if (query == null) {
            return false;
        }
        state.getLogger().writeCurrent(query.getQueryString());
        state.getState().logStatement(query);
        try (DBRadarResultSet ignored = query.executeAndGet(state.getConnection(), false)) {
            return ignored != null;
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

    private enum StressStatementKind {
        DDL,
        DML,
        DQL
    }
}
