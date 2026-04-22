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

    private PostgreSQLStressSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        verifiesIsolatedStressMode();
        verifiesSharedStressMode();
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
                "postgresql", "--oracle", "stress", "--stress-topology", "isolated");
        require(exitCode == 0, "Expected Main.executeMain to succeed in isolated stress mode");

        Path logFile = Path.of("logs", "postgresql", ISOLATED_DATABASE_PREFIX + "0-cur.log");
        require(Files.exists(logFile), "Expected stress log file to exist: " + logFile);
        require(countCurrentLogs(ISOLATED_DATABASE_PREFIX) == 1,
                "Stress isolated mode should emit one log per thread, independent of --num-tries");

        assertStressLog(logFile);
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

        Path thread0Log = Path.of("logs", "postgresql", SHARED_DATABASE_PREFIX + "0-thread0-cur.log");
        Path thread1Log = Path.of("logs", "postgresql", SHARED_DATABASE_PREFIX + "0-thread1-cur.log");
        require(Files.exists(thread0Log), "Expected shared stress log file to exist: " + thread0Log);
        require(Files.exists(thread1Log), "Expected shared stress log file to exist: " + thread1Log);
        require(countCurrentLogs(SHARED_DATABASE_PREFIX) == 2,
                "Stress shared mode should emit one current log per thread, independent of --num-tries");

        assertStressLog(thread0Log);
        assertStressLog(thread1Log);
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
