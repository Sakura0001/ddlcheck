package dbradar.mysql.oracle;

import dbradar.Main;
import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.common.oracle.TestOracle;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.mysql.MySQLGlobalState;
import dbradar.mysql.MySQLOptions;
import dbradar.mysql.MySQLProvider.MySQLDDLStmt;
import dbradar.mysql.MySQLProvider.MySQLQueryProvider;
import dbradar.mysql.MySQLTablespaceGate;
import dbradar.mysql.schema.MySQLSchema.MySQLColumn;
import dbradar.mysql.schema.MySQLSchema.MySQLTable;

import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MySQLStressOracle implements TestOracle {

    private static final int MAX_QUERY_GEN_RETRIES = 80;
    private static final int MAX_EXEC_RETRIES = 5;
    private static final long GLOBAL_SUCCESS_RATE_LOG_INTERVAL_MILLIS = 5_000L;
    private static final DateTimeFormatter TS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final AtomicLong LAST_SUCCESS_RATE_LOG_AT_MILLIS = new AtomicLong(0L);

    private final MySQLGlobalState mainState;
    private volatile boolean initialized;
    private final AtomicInteger workerErrorCount = new AtomicInteger(0);

    public MySQLStressOracle(MySQLGlobalState state) {
        this.mainState = state;
    }

    @Override
    public void check() throws Exception {
        if (!initialized) {
            ensureAtLeastOneTable(mainState);
            initialized = true;
        }

        MySQLOptions options = mainState.getDbmsSpecificOptions();
        if (!options.useStress()) {
            return;
        }

        int threadsPerDb = options.getStressThreadsPerDb();
        if (threadsPerDb > 1) {
            runConcurrentRound(options, threadsPerDb);
        } else {
            runWorkerRound(mainState, options);
        }

        // verifySuccessRatio();
    }

    private void runConcurrentRound(MySQLOptions options, int workerCount) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < workerCount; i++) {
                final int workerId = i;
                tasks.add(() -> {
                    runSingleWorker(workerId, options);
                    return null;
                });
            }
            List<Future<Void>> futures = pool.invokeAll(tasks);
            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (CancellationException ignored) {
                    // Timed shutdown can cancel outstanding workers. Treat this as a graceful stop.
                }
            }
        } catch (InterruptedException e) {
            // Main.executeMain uses thread interruption to stop long-running stress work after timeout.
            // This should terminate the current round cleanly rather than be reported as a bug.
            Thread.currentThread().interrupt();
        } finally {
            pool.shutdownNow();
        }
    }

    private void runSingleWorker(int workerId, MySQLOptions options) {
        MySQLGlobalState workerState = null;
        try {
            workerState = createWorkerState(workerId);
            runWorkerRound(workerState, options);
        } catch (Exception e) {
            workerErrorCount.incrementAndGet();
            String dbName = mainState.getDatabaseName();
            String threadName = Thread.currentThread().getName();
            String msg = String.format("[worker=%d] [thread=%s] [db=%s] Worker failed: %s — reconnecting...",
                    workerId, threadName, dbName, e.getMessage() == null ? e.getClass().getName() : e.getMessage());
            System.err.println(msg);
            if (mainState.getLogger() != null) {
                mainState.getLogger().writeCurrent(msg);
            }

            closeQuietly(workerState);

            try {
                workerState = createWorkerState(workerId);
                ensureAtLeastOneTable(workerState);
                runWorkerRound(workerState, options);
            } catch (Exception retryEx) {
                String retryMsg = String.format("[worker=%d] [thread=%s] [db=%s] Retry also failed: %s",
                        workerId, threadName, dbName,
                        retryEx.getMessage() == null ? retryEx.getClass().getName() : retryEx.getMessage());
                System.err.println(retryMsg);
                if (mainState.getLogger() != null) {
                    mainState.getLogger().writeCurrent(retryMsg);
                }
            }
        } finally {
            closeQuietly(workerState);
        }
    }

    private void closeQuietly(MySQLGlobalState state) {
        if (state != null && state.getConnection() != null) {
            try {
                state.getConnection().close();
            } catch (Exception ignored) {
            }
        }
    }

    private MySQLGlobalState createWorkerState(int workerId) throws SQLException {
        MySQLGlobalState workerState = new MySQLGlobalState();
        workerState.setMainOptions(mainState.getOptions());
        workerState.setDbmsSpecificOptions(mainState.getDbmsSpecificOptions());
        workerState.setDatabaseName(mainState.getDatabaseName());
        workerState.setState(mainState.getState());
        workerState.setStateLogger(mainState.getLogger());
        workerState.setRandomly(new Randomly(System.nanoTime() + workerId));
        workerState.setConnection((SQLConnection) mainState.createConnection(
                mainState.getOptions().getHost() == null ? "localhost" : mainState.getOptions().getHost(),
                mainState.getOptions().getPort() == -1 ? 3306 : mainState.getOptions().getPort(),
                mainState.getOptions().getUserName(),
                mainState.getOptions().getPassword(),
                mainState.getDatabaseName()));
        return workerState;
    }

    private void runWorkerRound(MySQLGlobalState state, MySQLOptions options) throws Exception {
        ensureAtLeastOneTable(state);
        executeBatch(state, options.getStressDDLPerThread(), StatementKind.DDL);
        ensureAtLeastOneTable(state);
        executeBatch(state, options.getStressDMLPerThread(), StatementKind.DML);
        ensureAtLeastOneTable(state);
        executeBatch(state, options.getStressQueryPerThread(), StatementKind.QUERY);
    }

    private void executeBatch(MySQLGlobalState state, int count, StatementKind kind) throws Exception {
        if (kind == StatementKind.DML) {
            state.updateSchema();
        }
        for (int i = 0; i < count; i++) {
            if (kind == StatementKind.DDL) {
                state.updateSchema();
            }
            if ((kind == StatementKind.DML || kind == StatementKind.QUERY)
                    && state.getSchema().getDatabaseTables().isEmpty()) {
                ensureAtLeastOneTable(state);
            }
            SQLQueryAdapter query = generateQuery(state, kind);
            if (query == null) {
                if (state.getSchema().getDatabaseTables().isEmpty()) {
                    ensureAtLeastOneTable(state);
                }
                continue;
            }
            boolean refreshSchemaOnFailure = kind == StatementKind.DML && isInsertLike(query.getQueryString());
            String sql = query.getQueryString();
            MySQLTablespaceGate.GateLease lease = MySQLTablespaceGate.tryAcquire(sql);
            if (!lease.isAcquired()) {
                if (kind == StatementKind.DDL) {
                    SQLQueryAdapter fallback = generateQuery(state, kind, false);
                    if (fallback == null) {
                        continue;
                    }
                    sql = fallback.getQueryString();
                    refreshSchemaOnFailure = kind == StatementKind.DML && isInsertLike(sql);
                    lease = MySQLTablespaceGate.tryAcquire(sql);
                    if (!lease.isAcquired()) {
                        continue;
                    }
                } else {
                    continue;
                }
            }
            try (MySQLTablespaceGate.GateLease ignored = lease) {
                executeWithRetries(state, sql, kind, query.couldAffectSchema(), refreshSchemaOnFailure);
            }
        }
    }

    private SQLQueryAdapter generateQuery(MySQLGlobalState state, StatementKind kind) throws Exception {
        return generateQuery(state, kind, true);
    }

    private SQLQueryAdapter generateQuery(MySQLGlobalState state, StatementKind kind, boolean allowTablespace)
            throws Exception {
        if ((kind == StatementKind.DML || kind == StatementKind.QUERY)
                && state.getSchema().getDatabaseTables().isEmpty()) {
            return null;
        }
        for (int i = 0; i < MAX_QUERY_GEN_RETRIES; i++) {
            try {
                switch (kind) {
                    case DDL:
                        if (state.getSchema().getDatabaseTables().isEmpty()) {
                            return MySQLDDLStmt.CREATE_TABLE.getQueryProvider().getQuery(state);
                        }
                        return MySQLDDLStmt.getRandomDDL(allowTablespace).getQuery(state);
                    case DML:
                        return generateStressDML(state);
                    case QUERY:
                        return MySQLQueryProvider.SELECT.getQuery(state);
                    default:
                        throw new AssertionError(kind);
                }
            } catch (RuntimeException e) {
                if (state.getSchema().getDatabaseTables().isEmpty()) {
                    return null;
                }
            }
        }
        return null;
    }

    private SQLQueryAdapter generateStressDML(MySQLGlobalState state) {
        List<MySQLTable> tables = state.getSchema().getDatabaseTablesWithoutViews();
        if (tables.isEmpty()) {
            return null;
        }
        List<MySQLTable> writableTables = tables.stream()
                .filter(table -> !getWritableColumns(table).isEmpty())
                .collect(Collectors.toList());
        if (writableTables.isEmpty()) {
            return null;
        }

        List<MySQLTable> emptyWritableTables = new ArrayList<>();
        for (MySQLTable table : writableTables) {
            if (isEmptyTable(state, table)) {
                emptyWritableTables.add(table);
            }
        }
        if (!emptyWritableTables.isEmpty()) {
            MySQLTable table = Randomly.fromList(emptyWritableTables);
            return buildInsertLikeDml(state, table, getWritableColumns(table));
        }

        MySQLTable table = Randomly.fromList(writableTables);
        List<MySQLColumn> writableColumns = getWritableColumns(table);
        long rowCount = getRowCount(state, table);
        if (writableColumns.isEmpty()) {
            return null;
        }
        if (rowCount <= 0) {
            return buildInsertLikeDml(state, table, writableColumns);
        }
        if (rowCount == 1) {
            return Randomly.getBoolean()
                    ? buildInsertLikeDml(state, table, writableColumns)
                    : buildUpdateDml(state, table, writableColumns);
        }

        if (Randomly.getBoolean()) {
            SQLQueryAdapter deleteQuery = buildDeleteDml(state);
            if (deleteQuery != null) {
                return deleteQuery;
            }
        }
        if (Randomly.getBoolean()) {
            return buildInsertLikeDml(state, table, writableColumns);
        }
        return buildUpdateDml(state, table, writableColumns);
    }

    private List<MySQLColumn> getWritableColumns(MySQLTable table) {
        return table.getColumns().stream()
                .filter(col -> !col.isGenerated())
                .collect(Collectors.toList());
    }

    private boolean isEmptyTable(MySQLGlobalState state, MySQLTable table) {
        try {
            return table.getNrRows(state) == 0;
        } catch (IgnoreMeException ignored) {
            return false;
        }
    }

    private long getRowCount(MySQLGlobalState state, MySQLTable table) {
        try {
            return table.getNrRows(state);
        } catch (IgnoreMeException ignored) {
            return -1L;
        }
    }

    private SQLQueryAdapter buildInsertLikeDml(MySQLGlobalState state, MySQLTable table, List<MySQLColumn> writableColumns) {
        String keyword = isEmptyTable(state, table) || Randomly.getBoolean() ? "INSERT IGNORE INTO " : "REPLACE INTO ";
        StringBuilder sb = new StringBuilder(keyword);
        sb.append(table.getName());
        sb.append(" (");
        for (int i = 0; i < writableColumns.size(); i++) {
            sb.append(writableColumns.get(i).getName());
            if (i < writableColumns.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(") VALUES (");
        for (int i = 0; i < writableColumns.size(); i++) {
            sb.append(MySQLStressValueHelper.generateStressSafeValue(writableColumns.get(i), state));
            if (i < writableColumns.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return new SQLQueryAdapter(sb.toString());
    }

    private SQLQueryAdapter buildUpdateDml(MySQLGlobalState state, MySQLTable table, List<MySQLColumn> writableColumns) {
        MySQLColumn column = Randomly.fromList(writableColumns);
        return new SQLQueryAdapter("UPDATE IGNORE " + table.getName() + " SET " + column.getName() + " = "
                + MySQLStressValueHelper.generateStressSafeValue(column, state) + " LIMIT 1");
    }

    private SQLQueryAdapter buildDeleteDml(MySQLGlobalState state) {
        List<MySQLTable> deleteSafeTables = getDeleteSafeTables(state);
        if (deleteSafeTables.isEmpty()) {
            return null;
        }
        MySQLTable table = Randomly.fromList(deleteSafeTables);
        return new SQLQueryAdapter("DELETE FROM " + table.getName() + " LIMIT 1");
    }

    private List<MySQLTable> getDeleteSafeTables(MySQLGlobalState state) {
        Set<String> referencedTableNames = state.getSchema().getForeignKeys().stream()
                .map(foreignKey -> foreignKey.getReferencedTable().getName())
                .collect(Collectors.toSet());
        return state.getSchema().getDatabaseTablesWithoutViews().stream()
                .filter(table -> !referencedTableNames.contains(table.getName()))
                .filter(table -> getRowCount(state, table) > 0)
                .collect(Collectors.toList());
    }

    private void executeWithRetries(MySQLGlobalState state, String sql, StatementKind kind, boolean canAffectSchema,
                                    boolean refreshSchemaOnFailure) throws Exception {
        SQLException lastException = null;
        int attempts = 0;
        for (int retry = 0; retry < MAX_EXEC_RETRIES; retry++) {
            attempts++;
            try (Statement stmt = state.getConnection().createStatement()) {
                stmt.execute(sql);
                Main.nrQueries.incrementAndGet();
                Main.nrSuccessfulActions.incrementAndGet();
                logExecution(state, sql, kind, true, null, attempts);
                maybeLogGlobalSuccessRate(state);
                return;
            } catch (SQLException e) {
                lastException = e;
                if (isDropTableWithoutTables(sql, e)) {
                    Main.nrQueries.incrementAndGet();
                    Main.nrUnsuccessfulActions.incrementAndGet();
                    logExecution(state, sql, kind, false, e, 1);
                    refreshSchemaAfterFailure(state, canAffectSchema || refreshSchemaOnFailure);
                    maybeLogGlobalSuccessRate(state);
                    ensureAtLeastOneTable(state);
                    return;
                }
                int code = e.getErrorCode();
                if (code == 1146 || code == 1054 || code == 1064) {
                    Main.nrQueries.incrementAndGet();
                    Main.nrUnsuccessfulActions.incrementAndGet();
                    logExecution(state, sql, kind, false, e, 1);
                    refreshSchemaAfterFailure(state, canAffectSchema || refreshSchemaOnFailure);
                    maybeLogGlobalSuccessRate(state);
                    if (state.getSchema().getDatabaseTables().isEmpty()) {
                        ensureAtLeastOneTable(state);
                    }
                    return;
                }
                if (!shouldRetry(e) || retry == MAX_EXEC_RETRIES - 1) {
                    break;
                }
            }
        }
        if (lastException != null) {
            Main.nrQueries.incrementAndGet();
            Main.nrUnsuccessfulActions.incrementAndGet();
            logExecution(state, sql, kind, false, lastException, attempts);
            refreshSchemaAfterFailure(state, canAffectSchema || refreshSchemaOnFailure);
            maybeLogGlobalSuccessRate(state);
        }
    }

    public static void resetGlobalSuccessRateLogging() {
        LAST_SUCCESS_RATE_LOG_AT_MILLIS.set(0L);
    }

    private static void maybeLogGlobalSuccessRate(MySQLGlobalState state) {
        maybeLogGlobalSuccessRate(state, System.currentTimeMillis());
    }

    private static void maybeLogGlobalSuccessRate(MySQLGlobalState state, long nowMillis) {
        long success = Main.nrSuccessfulActions.get();
        long fail = Main.nrUnsuccessfulActions.get();
        long total = success + fail;
        if (total <= 0) {
            return;
        }

        while (true) {
            long lastLoggedAt = LAST_SUCCESS_RATE_LOG_AT_MILLIS.get();
            if (nowMillis - lastLoggedAt < GLOBAL_SUCCESS_RATE_LOG_INTERVAL_MILLIS) {
                return;
            }
            if (LAST_SUCCESS_RATE_LOG_AT_MILLIS.compareAndSet(lastLoggedAt, nowMillis)) {
                String message = formatGlobalSuccessRateMessage(success, fail, total);
                System.out.println(message);
                if (state != null && state.getLogger() != null && state.getOptions() != null
                        && state.getOptions().logEachSelect()) {
                    state.getLogger().writeCurrent(message);
                }
                return;
            }
        }
    }

    private static String formatGlobalSuccessRateMessage(long success, long fail, long total) {
        double ratio = total == 0 ? 0.0 : (success * 100.0) / total;
        return String.format("[MYSQL-STRESS] global SQL success rate: %.2f%% (success=%d, fail=%d, total=%d)",
                ratio, success, fail, total);
    }

    private void refreshSchemaAfterFailure(MySQLGlobalState state, boolean shouldRefreshSchema) throws Exception {
        if (!shouldRefreshSchema) {
            return;
        }
        state.updateSchema();
    }

    private static boolean shouldRetry(SQLException e) {
        if (e == null) {
            return false;
        }
        String sqlState = e.getSQLState();
        if (sqlState != null && (sqlState.startsWith("08") || sqlState.startsWith("40"))) {
            return true;
        }
        int errorCode = e.getErrorCode();
        return errorCode == 1205 || errorCode == 1213 || errorCode == 1317;
    }

    private static final String[] FALLBACK_CREATE_TABLES = {
            "CREATE TABLE t0 (c1 INT PRIMARY KEY, c2 VARCHAR(255), c3 INT, c4 DOUBLE, c5 DATE)",
            "CREATE TABLE t1 (c1 INT PRIMARY KEY AUTO_INCREMENT, c2 TEXT, c3 BIGINT, c4 DECIMAL(10,2), c5 TIMESTAMP NULL)",
            "CREATE TABLE t2 (c1 INT, c2 VARCHAR(100), c3 BOOLEAN, c4 FLOAT, c5 DATETIME, c6 INT, c7 DOUBLE)"
    };

    private void ensureAtLeastOneTable(MySQLGlobalState state) throws Exception {
        state.updateSchema();
        if (!state.getSchema().getDatabaseTables().isEmpty()) {
            return;
        }
        Exception lastException = null;
        int targetTables = 3;
        for (int t = 0; t < targetTables; t++) {
            for (int i = 0; i < MAX_QUERY_GEN_RETRIES; i++) {
                try {
                    SQLQueryAdapter create = MySQLDDLStmt.CREATE_TABLE.getQueryProvider().getQuery(state);
                    try (Statement stmt = state.getConnection().createStatement()) {
                        stmt.execute(create.getQueryString());
                        state.updateSchema();
                        if (!state.getSchema().getDatabaseTables().isEmpty() && t == 0) {
                            break;
                        }
                        if (state.getSchema().getDatabaseTables().size() > t) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    lastException = e;
                }
            }
        }
        if (state.getSchema().getDatabaseTables().isEmpty()) {
            System.err.println("[STRESS] All random CREATE TABLE attempts failed, trying fallback. Last error: "
                    + (lastException != null ? lastException.getMessage() : "unknown"));
            for (String fallbackSQL : FALLBACK_CREATE_TABLES) {
                try (Statement stmt = state.getConnection().createStatement()) {
                    stmt.execute(fallbackSQL);
                } catch (Exception e) {
                    System.err.println("[STRESS] Fallback CREATE TABLE failed: " + fallbackSQL + " | " + e.getMessage());
                }
            }
            state.updateSchema();
        }
        if (state.getSchema().getDatabaseTables().isEmpty()) {
            throw new RuntimeException("Failed to create initial table for stress run."
                    + (lastException != null ? " Last error: " + lastException.getMessage() : ""));
        }
    }

    private boolean isDropTableWithoutTables(String sql, SQLException e) {
        String normalized = sql == null ? "" : sql.trim().toUpperCase();
        boolean isDropOrTruncate = normalized.startsWith("DROP TABLE")
                || normalized.startsWith("TRUNCATE TABLE");
        if (!isDropOrTruncate) {
            return false;
        }
        int code = e.getErrorCode();
        return code == 1051 || code == 1146;
    }

    private boolean isInsertLike(String sql) {
        String normalized = sql == null ? "" : sql.trim().toUpperCase();
        return normalized.startsWith("INSERT ") || normalized.startsWith("REPLACE ");
    }

    private void logExecution(MySQLGlobalState state, String sql, StatementKind kind,
                              boolean success, SQLException e, int attempts) {
        if (!state.getOptions().logEachSelect()) {
            return;
        }
        String now = LocalDateTime.now().format(TS_FORMATTER);
        String threadName = Thread.currentThread().getName();
        String dbName = state.getDatabaseName();
        String status;
        if (success) {
            if (attempts > 1) {
                status = String.format("SUCCESS(retries=%d)", attempts - 1);
            } else {
                status = "SUCCESS";
            }
        } else {
            int errCode = e == null ? -1 : e.getErrorCode();
            String errMsg = e == null ? "" : sanitize(e.getMessage());
            if (attempts > 1) {
                status = String.format("FAIL(code=%d,retries=%d,msg=%s)", errCode, attempts, errMsg);
            } else {
                status = String.format("FAIL(code=%d,msg=%s)", errCode, errMsg);
            }
        }
        String line = String.format("[%s] [thread=%s] [db=%s] [kind=%s] %s | %s",
                now, threadName, dbName, kind.name(), status, sanitize(sql));
        state.getLogger().writeCurrent(line);
    }

    private String sanitize(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\n", " ").replace("\r", " ");
    }

    private void verifySuccessRatio() {
        long ok = Main.nrSuccessfulActions.get();
        long fail = Main.nrUnsuccessfulActions.get();
        long total = ok + fail;
        if (total < 50) {
            return;
        }
        long ratio = (ok * 100) / total;
        if (ratio < 70) {
            throw new AssertionError("Stress success ratio is below 70% (" + ratio
                    + "%). Please tune grammar/statement mix or inspect DB errors.");
        }
    }

    @Override
    public String getOracleName() {
        return "MySQLStress";
    }

    private enum StatementKind {
        DDL, DML, QUERY
    }
}
