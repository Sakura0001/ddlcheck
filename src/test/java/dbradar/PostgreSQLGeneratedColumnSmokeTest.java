package dbradar;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PostgreSQLGeneratedColumnSmokeTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;
    private static final String EQUATION_PREFIX = "task4_generated_eq_";
    private static final String STRESS_PREFIX = "task4_generated_stress_";
    private static final Pattern GENERATED_TABLE_PATTERN = Pattern.compile(
            "CREATE TABLE(?: IF NOT EXISTS)? ([A-Za-z0-9_]+) \\(.*GENERATED ALWAYS AS \\(c1 \\+ c2\\) STORED.*");

    private PostgreSQLGeneratedColumnSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyEquationGeneratedColumnBootstrap();
        verifyStressGeneratedColumnBootstrap();
    }

    private static void verifyEquationGeneratedColumnBootstrap() throws Exception {
        int exitCode = Main.executeMain(
                "--num-threads", "1",
                "--num-tries", "1",
                "--num-queries", "1",
                "--max-generated-databases", "1",
                "--print-progress-information", "false",
                "--database-prefix", EQUATION_PREFIX,
                "--ddl-count", "4",
                "--dml-count", "3",
                "--host", HOST,
                "--port", String.valueOf(PORT),
                "--username", USERNAME,
                "--password", PASSWORD,
                "postgresql", "--oracle", "equation");
        require(exitCode == 0, "Expected generated-column equation run to succeed");

        Path logFile = Path.of("logs", "postgresql", EQUATION_PREFIX + "0-cur.log");
        require(Files.exists(logFile), "Expected generated-column equation log: " + logFile);
        List<String> statements = readStatements(logFile);
        String tableName = findGeneratedTableName(statements);
        require(tableName != null, "Expected equation log to contain a generated-column CREATE TABLE");
        require(containsGeneratedInsert(statements, tableName),
                "Expected equation log to contain a generated-column INSERT for " + tableName);

        try (Connection connection = createConnection(EQUATION_PREFIX + "0");
             Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(String.format(
                    "SELECT COUNT(*) FROM information_schema.columns "
                            + "WHERE table_schema = 'public' AND table_name = '%s' AND is_generated = 'ALWAYS'",
                    tableName))) {
                require(rs.next() && rs.getInt(1) == 1,
                        "Expected exactly one generated column in table " + tableName);
            }
            try (ResultSet rs = statement.executeQuery(
                    String.format("SELECT c1, c2, c8 FROM %s ORDER BY c1, c2 LIMIT 1", tableName))) {
                require(rs.next(), "Expected at least one row in generated-column table " + tableName);
                int c1 = rs.getInt(1);
                int c2 = rs.getInt(2);
                int c8 = rs.getInt(3);
                require(c8 == c1 + c2,
                        String.format("Expected generated column c8 to equal c1 + c2, observed %d, %d, %d", c1, c2,
                                c8));
            }
        }
    }

    private static void verifyStressGeneratedColumnBootstrap() throws Exception {
        int exitCode = Main.executeMain(
                "--num-threads", "2",
                "--num-tries", "4",
                "--num-queries", "12",
                "--max-generated-databases", "1",
                "--print-progress-information", "false",
                "--database-prefix", STRESS_PREFIX,
                "--ddl-count", "4",
                "--dml-count", "3",
                "--host", HOST,
                "--port", String.valueOf(PORT),
                "--username", USERNAME,
                "--password", PASSWORD,
                "postgresql", "--oracle", "stress", "--stress-topology", "shared");
        require(exitCode == 0, "Expected generated-column stress run to succeed");

        Path thread0Log = Path.of("logs", "postgresql", STRESS_PREFIX + "0-thread0-cur.log");
        Path thread1Log = Path.of("logs", "postgresql", STRESS_PREFIX + "0-thread1-cur.log");
        require(Files.exists(thread0Log), "Expected shared stress log: " + thread0Log);
        require(Files.exists(thread1Log), "Expected shared stress log: " + thread1Log);

        List<String> combinedStatements = readStatements(thread0Log);
        combinedStatements.addAll(readStatements(thread1Log));
        String tableName = findGeneratedTableName(combinedStatements);
        require(tableName != null, "Expected shared stress logs to contain a generated-column CREATE TABLE");
        require(containsGeneratedInsert(combinedStatements, tableName),
                "Expected shared stress logs to contain a generated-column INSERT for " + tableName);
    }

    private static Connection createConnection(String databaseName) throws Exception {
        return DriverManager.getConnection(
                String.format("jdbc:postgresql://%s:%d/%s", HOST, PORT, databaseName),
                USERNAME,
                PASSWORD);
    }

    private static List<String> readStatements(Path logFile) throws Exception {
        return Files.readAllLines(logFile).stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .filter(line -> !line.startsWith("--"))
                .collect(java.util.stream.Collectors.toList());
    }

    private static String findGeneratedTableName(List<String> statements) {
        for (String statement : statements) {
            Matcher matcher = GENERATED_TABLE_PATTERN.matcher(statement);
            if (matcher.matches()) {
                return matcher.group(1);
            }
        }
        return null;
    }

    private static boolean containsGeneratedInsert(List<String> statements, String tableName) {
        String insertPrefix = "INSERT INTO " + tableName + " (c1,c2,c3,c4,c5,c6,c7) VALUES";
        return statements.stream().anyMatch(statement -> statement.startsWith(insertPrefix));
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
