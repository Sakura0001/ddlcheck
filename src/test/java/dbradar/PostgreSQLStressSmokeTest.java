package dbradar;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

public final class PostgreSQLStressSmokeTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;
    private static final String ISOLATED_DATABASE_PREFIX = "task2_isolated_";
    private static final String SHARED_DATABASE_PREFIX = "task2_shared_";
    private static final String GROUPED_DATABASE_PREFIX = "task2_grouped_";
    private static final Path GLOBAL_EXECUTION_LOG = Path.of("logs", "postgresql", "global-execution.log");

    private PostgreSQLStressSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        verifiesIsolatedStressMode();
        verifiesSharedStressMode();
        verifiesGroupedStressMode();
    }

    private static void verifiesIsolatedStressMode() throws Exception {
        int exitCode = Main.executeMain(
                "--num-threads", "1",
                "--num-tries", "5",
                "--num-queries", "25",
                "--max-generated-databases", "1",
                "--print-progress-information", "false",
                "--database-prefix", ISOLATED_DATABASE_PREFIX,
                "--host", HOST,
                "--port", String.valueOf(PORT),
                "--username", USERNAME,
                "--password", PASSWORD,
                "--log-global-execution", "true",
                "postgresql", "--oracle", "stress", "--stress-topology", "isolated");
        require(exitCode == 0, "Expected Main.executeMain to succeed in isolated stress mode");

        Path logFile = Path.of("logs", "postgresql", ISOLATED_DATABASE_PREFIX + "1-cur.log");
        require(Files.exists(logFile), "Expected stress log file to exist: " + logFile);
        require(countCurrentLogs(ISOLATED_DATABASE_PREFIX) == 1,
                "Stress isolated mode should emit one log per thread, independent of --num-tries");

        assertStressLog(logFile);
        assertGlobalExecutionLog(List.of(ISOLATED_DATABASE_PREFIX + "0"), 1);
    }

    private static void verifiesSharedStressMode() throws Exception {
        int exitCode = Main.executeMain(
                "--num-threads", "2",
                "--num-tries", "7",
                "--num-queries", "30",
                "--max-generated-databases", "1",
                "--print-progress-information", "false",
                "--database-prefix", SHARED_DATABASE_PREFIX,
                "--host", HOST,
                "--port", String.valueOf(PORT),
                "--username", USERNAME,
                "--password", PASSWORD,
                "postgresql", "--oracle", "stress", "--stress-topology", "shared");
        require(exitCode == 0, "Expected Main.executeMain to succeed in shared stress mode");

        Path thread0Log = Path.of("logs", "postgresql", SHARED_DATABASE_PREFIX + "1-cur.log");
        Path thread1Log = Path.of("logs", "postgresql", SHARED_DATABASE_PREFIX + "2-cur.log");
        require(Files.exists(thread0Log), "Expected shared stress log file to exist: " + thread0Log);
        require(Files.exists(thread1Log), "Expected shared stress log file to exist: " + thread1Log);
        require(countCurrentLogs(SHARED_DATABASE_PREFIX) == 2,
                "Stress shared mode should emit one current log per thread, independent of --num-tries");

        assertStressLog(thread0Log);
        assertStressLog(thread1Log);
    }

    private static void verifiesGroupedStressMode() throws Exception {
        int exitCode = Main.executeMain(
                "--num-threads", "4",
                "--num-tries", "9",
                "--num-queries", "30",
                "--max-generated-databases", "1",
                "--print-progress-information", "false",
                "--database-prefix", GROUPED_DATABASE_PREFIX,
                "--host", HOST,
                "--port", String.valueOf(PORT),
                "--username", USERNAME,
                "--password", PASSWORD,
                "--log-global-execution", "true",
                "postgresql", "--oracle", "stress", "--stress-threads-per-db", "2");
        require(exitCode == 0, "Expected Main.executeMain to succeed in grouped stress mode");

        require(Files.exists(Path.of("logs", "postgresql", GROUPED_DATABASE_PREFIX + "1-cur.log")),
                "Expected grouped stress log for thread0");
        require(Files.exists(Path.of("logs", "postgresql", GROUPED_DATABASE_PREFIX + "2-cur.log")),
                "Expected grouped stress log for thread1");
        require(Files.exists(Path.of("logs", "postgresql", GROUPED_DATABASE_PREFIX + "3-cur.log")),
                "Expected grouped stress log for thread2");
        require(Files.exists(Path.of("logs", "postgresql", GROUPED_DATABASE_PREFIX + "4-cur.log")),
                "Expected grouped stress log for thread3");
        require(countCurrentLogs(GROUPED_DATABASE_PREFIX) == 4,
                "Grouped stress mode should emit one current log per thread");

        assertStressLog(Path.of("logs", "postgresql", GROUPED_DATABASE_PREFIX + "1-cur.log"));
        assertStressLog(Path.of("logs", "postgresql", GROUPED_DATABASE_PREFIX + "2-cur.log"));
        assertStressLog(Path.of("logs", "postgresql", GROUPED_DATABASE_PREFIX + "3-cur.log"));
        assertStressLog(Path.of("logs", "postgresql", GROUPED_DATABASE_PREFIX + "4-cur.log"));
        assertGlobalExecutionLog(List.of(GROUPED_DATABASE_PREFIX + "0_g0", GROUPED_DATABASE_PREFIX + "0_g1"), 4);
    }

    private static void assertStressLog(Path logFile) throws Exception {
        List<String> statements = Files.readAllLines(logFile).stream().map(String::trim).filter(line -> !line.isEmpty())
                .filter(line -> !line.startsWith("--")).toList();

        require(statements.stream().noneMatch(line -> line.contains("SemiState")),
                "Stress mode must not emit semi-state markers");
        require(containsDdl(statements), "Expected at least one DDL statement in the stress log");
        require(containsDml(statements), "Expected at least one DML statement in the stress log");
        require(containsDql(statements), "Expected at least one DQL statement in the stress log");
    }

    private static long countCurrentLogs(String prefix) throws Exception {
        try (var logFiles = Files.list(Path.of("logs", "postgresql"))) {
            return logFiles.filter(path -> path.getFileName().toString().startsWith(prefix))
                    .filter(path -> path.getFileName().toString().endsWith("-cur.log"))
                    .count();
        }
    }

    private static void assertGlobalExecutionLog(List<String> databaseNames, int expectedThreadCount) throws Exception {
        require(Files.exists(GLOBAL_EXECUTION_LOG), "Expected global execution log to exist: " + GLOBAL_EXECUTION_LOG);
        String content = Files.readString(GLOBAL_EXECUTION_LOG);
        for (int threadId = 1; threadId <= expectedThreadCount; threadId++) {
            require(content.contains("thread=" + threadId), "Expected global log to include thread=" + threadId);
        }
        for (String databaseName : databaseNames) {
            require(content.contains("db=" + databaseName),
                    "Expected global log to include actual database name " + databaseName);
        }
        require(content.contains("status=SUCCESS"), "Expected global log to include successful statements");
        require(content.contains("error=-"), "Expected global log to include sanitized success errors");
        require(content.contains("sql="), "Expected global log to include SQL text");
    }

    private static boolean containsDdl(List<String> statements) {
        return statements.stream().map(PostgreSQLStressSmokeTest::normalize).anyMatch(line ->
                line.startsWith("CREATE ")
                        || line.startsWith("ALTER ")
                        || line.startsWith("DROP ")
                        || line.startsWith("TRUNCATE ")
                        || line.startsWith("REINDEX "));
    }

    private static boolean containsDml(List<String> statements) {
        return statements.stream().map(PostgreSQLStressSmokeTest::normalize).anyMatch(line ->
                line.startsWith("INSERT ")
                        || line.startsWith("UPDATE ")
                        || line.startsWith("DELETE "));
    }

    private static boolean containsDql(List<String> statements) {
        return statements.stream().map(PostgreSQLStressSmokeTest::normalize)
                .anyMatch(line -> line.startsWith("SELECT "));
    }

    private static String normalize(String statement) {
        return statement.toUpperCase(Locale.ROOT);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
