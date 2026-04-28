package dbradar.ddlCheck;

import dbradar.Main;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPostgreSQLStressOracle {

    private static final String DB_NAME = "postgresql";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 5432;
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final String DATABASE_PREFIX = "stress_startup_";
    private static final String THREADS_PER_DB_PREFIX = "stress_threads_per_db_";
    private static final Path STRESS_LOG = Path.of("logs", "postgresql", DATABASE_PREFIX + "1-cur.log");

    @Test
    public void testPostgreSQLStressOracleStarts() throws Exception {
        Files.deleteIfExists(STRESS_LOG);

        int exitCode = Main.executeMain(
                "--num-threads", "40",
                "--num-tries", "10000",
                "--num-queries", "300",
                "--max-generated-databases", "100000",
                "--random-seed", "20260428",
                "--ddl-count", "100",
                "--dml-count", "100",
                "--timeout-seconds", "3000000",
                "--print-progress-information", "false",
                "--database-prefix", DATABASE_PREFIX,
                "--host", HOST,
                "--port", String.valueOf(PORT),
                "--username", USERNAME,
                "--password", PASSWORD,
                DB_NAME, "--oracle", "stress", "--stress-topology", "isolated");

        assertEquals(0, exitCode);
        assertTrue(Files.exists(STRESS_LOG), "Expected stress current log to be created");

        List<String> statements = Files.readAllLines(STRESS_LOG).stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .filter(line -> !line.startsWith("--"))
                .toList();

        assertFalse(statements.isEmpty(), "Expected stress mode to emit SQL statements");
        assertFalse(statements.stream().anyMatch(line -> line.contains("SemiState")),
                "Stress mode should not use the EDC semi-state replay path");
        assertTrue(containsStatementStartingWith(statements, "CREATE", "ALTER", "DROP", "TRUNCATE", "REINDEX"),
                "Expected stress startup to emit DDL");
        assertTrue(containsStatementStartingWith(statements, "INSERT", "UPDATE", "DELETE", "MERGE"),
                "Expected stress startup to emit DML");
        assertTrue(containsStatementStartingWith(statements, "SELECT"),
                "Expected stress startup to emit DQL");
    }

    @Test
    public void testPostgreSQLStressOracleStartsWithCustomThreadsPerDatabase() throws Exception {
        List<Path> threadLogs = List.of(
                Path.of("logs", "postgresql", THREADS_PER_DB_PREFIX + "1-cur.log"),
                Path.of("logs", "postgresql", THREADS_PER_DB_PREFIX + "2-cur.log"),
                Path.of("logs", "postgresql", THREADS_PER_DB_PREFIX + "3-cur.log"),
                Path.of("logs", "postgresql", THREADS_PER_DB_PREFIX + "4-cur.log"));
        for (Path threadLog : threadLogs) {
            Files.deleteIfExists(threadLog);
        }

        int exitCode = Main.executeMain(
                "--num-threads", "4",
                "--num-tries", "1",
                "--num-queries", "3",
                "--max-generated-databases", "1",
                "--random-seed", "20260428",
                "--ddl-count", "4",
                "--dml-count", "3",
                "--timeout-seconds", "30",
                "--print-progress-information", "false",
                "--database-prefix", THREADS_PER_DB_PREFIX,
                "--host", HOST,
                "--port", String.valueOf(PORT),
                "--username", USERNAME,
                "--password", PASSWORD,
                DB_NAME, "--oracle", "stress", "--stress-threads-per-db", "2");

        assertEquals(0, exitCode);
        assertStressLog(threadLogs.get(0), THREADS_PER_DB_PREFIX + "0_g0");
        assertStressLog(threadLogs.get(1), THREADS_PER_DB_PREFIX + "0_g0");
        assertStressLog(threadLogs.get(2), THREADS_PER_DB_PREFIX + "0_g1");
        assertStressLog(threadLogs.get(3), THREADS_PER_DB_PREFIX + "0_g1");
    }

    private static void assertStressLog(Path logFile, String expectedDatabaseName) throws Exception {
        assertTrue(Files.exists(logFile), "Expected stress current log to be created: " + logFile);
        List<String> lines = Files.readAllLines(logFile);
        assertTrue(lines.stream().anyMatch(line -> line.equals("-- Database: " + expectedDatabaseName)),
                "Expected " + logFile + " to run against " + expectedDatabaseName);

        List<String> statements = lines.stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .filter(line -> !line.startsWith("--"))
                .toList();
        assertFalse(statements.isEmpty(), "Expected stress mode to emit SQL statements");
        assertFalse(statements.stream().anyMatch(line -> line.contains("SemiState")),
                "Stress mode should not use the EDC semi-state replay path");
    }

    private static boolean containsStatementStartingWith(List<String> statements, String... prefixes) {
        for (String statement : statements) {
            String normalized = statement.toUpperCase(Locale.ROOT);
            for (String prefix : prefixes) {
                if (normalized.startsWith(prefix + " ")) {
                    return true;
                }
            }
        }
        return false;
    }
}
