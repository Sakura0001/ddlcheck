package dbradar.mysql;

import java.util.Locale;
import java.util.concurrent.Semaphore;

public final class MySQLTablespaceGate {

    private static final int MAX_CONCURRENCY = 30;
    private static final Semaphore EXECUTION_GATE = new Semaphore(MAX_CONCURRENCY, true);
    private static final GateLease NOOP_LEASE = new GateLease(false, false);

    private MySQLTablespaceGate() {
    }

    public static int getMaxConcurrency() {
        return MAX_CONCURRENCY;
    }

    public static boolean isTablespaceSensitive(String sql) {
        String normalized = normalize(sql);
        if (normalized.isEmpty()) {
            return false;
        }
        if (normalized.contains("INFORMATION_SCHEMA.DSTORE_TABLESPACES")) {
            return true;
        }
        if (normalized.startsWith("CREATE TABLESPACE ")
                || normalized.startsWith("ALTER TABLESPACE ")
                || normalized.startsWith("DROP TABLESPACE ")) {
            return true;
        }
        return normalized.startsWith("ALTER TABLE ") && normalized.contains(" TABLESPACE ");
    }

    public static GateLease tryAcquire(String sql) {
        if (!isTablespaceSensitive(sql)) {
            return NOOP_LEASE;
        }
        if (!EXECUTION_GATE.tryAcquire()) {
            return new GateLease(false, true);
        }
        return new GateLease(true, true);
    }

    public static void resetForTests() {
        EXECUTION_GATE.drainPermits();
        EXECUTION_GATE.release(MAX_CONCURRENCY);
    }

    private static String normalize(String sql) {
        return sql == null ? "" : sql.trim().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
    }

    public static final class GateLease implements AutoCloseable {
        private final boolean acquired;
        private final boolean sensitive;
        private boolean released;

        private GateLease(boolean acquired, boolean sensitive) {
            this.acquired = acquired;
            this.sensitive = sensitive;
        }

        public boolean isAcquired() {
            return acquired || !sensitive;
        }

        @Override
        public void close() {
            if (released || !acquired || !sensitive) {
                return;
            }
            released = true;
            EXECUTION_GATE.release();
        }
    }
}
