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

import dbradar.postgresql.PostgreSQLGeneratedColumnSupport;

public final class PostgreSQLGeneratedColumnSmokeTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;
    private static final String EQUATION_PREFIX = "task4_generated_eq_";
    private static final String STRESS_PREFIX = "task4_generated_stress_";
    private static final Pattern GENERATED_TABLE_PATTERN = Pattern.compile(
            "CREATE TABLE(?: IF NOT EXISTS)? ([A-Za-z0-9_]+) \\(.*GENERATED ALWAYS AS \\(c1 \\+ c2\\) (STORED|VIRTUAL).*");

    private PostgreSQLGeneratedColumnSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyEquationGeneratedColumnBootstrap();
        verifyStressGeneratedColumnBootstrap();
    }

    private static void verifyEquationGeneratedColumnBootstrap() throws Exception {
        boolean supportsVirtualGeneratedColumns = supportsVirtualGeneratedColumns();
        int exitCode = Main.executeMain(
                "--num-threads", "1",
                "--num-tries", "1",
                "--num-queries", "1",
                "--max-generated-databases", "1",
                "--print-progress-information", "false",
                "--database-prefix", EQUATION_PREFIX,
                "--ddl-count", supportsVirtualGeneratedColumns ? "5" : "4",
                "--dml-count", supportsVirtualGeneratedColumns ? "4" : "3",
                "--host", HOST,
                "--port", String.valueOf(PORT),
                "--username", USERNAME,
                "--password", PASSWORD,
                "postgresql", "--oracle", "equation");
        require(exitCode == 0, "Expected generated-column equation run to succeed");

        Path logFile = Path.of("logs", "postgresql", EQUATION_PREFIX + "0-cur.log");
        require(Files.exists(logFile), "Expected generated-column equation log: " + logFile);
        List<String> statements = readStatements(logFile);
        String storedTableName = findGeneratedTableName(statements, "STORED");
        require(storedTableName != null, "Expected equation log to contain a stored generated-column CREATE TABLE");
        require(containsGeneratedInsert(statements, storedTableName),
                "Expected equation log to contain a stored generated-column INSERT for " + storedTableName);

        try (Connection connection = createConnection(EQUATION_PREFIX + "0");
             Statement statement = connection.createStatement()) {
            assertGeneratedColumnMetadata(connection, storedTableName, "s");
            assertGeneratedColumnValue(connection, storedTableName);
            if (supportsVirtualGeneratedColumns) {
                String virtualTableName = findGeneratedTableName(statements, "VIRTUAL");
                require(virtualTableName != null,
                        "Expected equation log to contain a virtual generated-column CREATE TABLE");
                require(containsGeneratedInsert(statements, virtualTableName),
                        "Expected equation log to contain a virtual generated-column INSERT for " + virtualTableName);
                assertGeneratedColumnMetadata(connection, virtualTableName, "v");
                assertGeneratedColumnValue(connection, virtualTableName);
            }
        }
    }

    private static void verifyStressGeneratedColumnBootstrap() throws Exception {
        boolean supportsVirtualGeneratedColumns = supportsVirtualGeneratedColumns();
        int exitCode = Main.executeMain(
                "--num-threads", "4",
                "--num-tries", "4",
                "--num-queries", "12",
                "--max-generated-databases", "1",
                "--print-progress-information", "false",
                "--database-prefix", STRESS_PREFIX,
                "--ddl-count", supportsVirtualGeneratedColumns ? "5" : "4",
                "--dml-count", supportsVirtualGeneratedColumns ? "4" : "3",
                "--host", HOST,
                "--port", String.valueOf(PORT),
                "--username", USERNAME,
                "--password", PASSWORD,
                "postgresql", "--oracle", "stress", "--stress-threads-per-db", "2");
        require(exitCode == 0, "Expected generated-column stress run to succeed");

        Path group0Thread0Log = Path.of("logs", "postgresql", STRESS_PREFIX + "1-cur.log");
        Path group0Thread1Log = Path.of("logs", "postgresql", STRESS_PREFIX + "2-cur.log");
        Path group1Thread2Log = Path.of("logs", "postgresql", STRESS_PREFIX + "3-cur.log");
        Path group1Thread3Log = Path.of("logs", "postgresql", STRESS_PREFIX + "4-cur.log");
        require(Files.exists(group0Thread0Log), "Expected grouped stress log: " + group0Thread0Log);
        require(Files.exists(group0Thread1Log), "Expected grouped stress log: " + group0Thread1Log);
        require(Files.exists(group1Thread2Log), "Expected grouped stress log: " + group1Thread2Log);
        require(Files.exists(group1Thread3Log), "Expected grouped stress log: " + group1Thread3Log);

        List<String> group0Statements = readStatements(group0Thread0Log);
        group0Statements.addAll(readStatements(group0Thread1Log));
        String group0StoredTableName = findGeneratedTableName(group0Statements, "STORED");
        require(group0StoredTableName != null,
                "Expected grouped stress logs for group0 to contain a stored generated-column CREATE TABLE");
        require(containsGeneratedInsert(group0Statements, group0StoredTableName),
                "Expected grouped stress logs for group0 to contain a stored generated-column INSERT for "
                        + group0StoredTableName);

        List<String> group1Statements = readStatements(group1Thread2Log);
        group1Statements.addAll(readStatements(group1Thread3Log));
        String group1StoredTableName = findGeneratedTableName(group1Statements, "STORED");
        require(group1StoredTableName != null,
                "Expected grouped stress logs for group1 to contain a stored generated-column CREATE TABLE");
        require(containsGeneratedInsert(group1Statements, group1StoredTableName),
                "Expected grouped stress logs for group1 to contain a stored generated-column INSERT for "
                        + group1StoredTableName);

        try (Connection group0Connection = createConnection(STRESS_PREFIX + "0_g0");
             Connection group1Connection = createConnection(STRESS_PREFIX + "0_g1")) {
            assertGeneratedColumnMetadata(group0Connection, group0StoredTableName, "s");
            assertGeneratedColumnMetadata(group1Connection, group1StoredTableName, "s");
            if (supportsVirtualGeneratedColumns) {
                String group0VirtualTableName = findGeneratedTableName(group0Statements, "VIRTUAL");
                String group1VirtualTableName = findGeneratedTableName(group1Statements, "VIRTUAL");
                require(group0VirtualTableName != null,
                        "Expected grouped stress logs for group0 to contain a virtual generated-column CREATE TABLE");
                require(group1VirtualTableName != null,
                        "Expected grouped stress logs for group1 to contain a virtual generated-column CREATE TABLE");
                require(containsGeneratedInsert(group0Statements, group0VirtualTableName),
                        "Expected grouped stress logs for group0 to contain a virtual generated-column INSERT for "
                                + group0VirtualTableName);
                require(containsGeneratedInsert(group1Statements, group1VirtualTableName),
                        "Expected grouped stress logs for group1 to contain a virtual generated-column INSERT for "
                                + group1VirtualTableName);
                assertGeneratedColumnMetadata(group0Connection, group0VirtualTableName, "v");
                assertGeneratedColumnMetadata(group1Connection, group1VirtualTableName, "v");
            }
        }
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

    private static String findGeneratedTableName(List<String> statements, String storageKeyword) {
        for (String statement : statements) {
            Matcher matcher = GENERATED_TABLE_PATTERN.matcher(statement);
            if (matcher.matches() && storageKeyword.equalsIgnoreCase(matcher.group(2))) {
                return matcher.group(1);
            }
        }
        return null;
    }

    private static boolean supportsVirtualGeneratedColumns() throws Exception {
        try (Connection connection = createConnection("postgres");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SHOW server_version_num")) {
            require(rs.next(), "Expected SHOW server_version_num to return one row");
            return PostgreSQLGeneratedColumnSupport.supportsVirtualGeneratedColumns(rs.getInt(1));
        }
    }

    private static void assertGeneratedColumnMetadata(Connection connection, String tableName, String expectedKind)
            throws Exception {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(String.format(
                    "SELECT COUNT(*) FROM information_schema.columns "
                            + "WHERE table_schema = 'public' AND table_name = '%s' AND is_generated = 'ALWAYS'",
                    tableName))) {
                require(rs.next() && rs.getInt(1) == 1,
                        "Expected exactly one generated column in table " + tableName);
            }
            try (ResultSet rs = statement.executeQuery(String.format(
                    "SELECT a.attgenerated FROM pg_attribute a "
                            + "JOIN pg_class c ON c.oid = a.attrelid "
                            + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                            + "WHERE n.nspname = 'public' AND c.relname = '%s' AND a.attname = 'c8'",
                    tableName))) {
                require(rs.next(), "Expected attgenerated metadata for table " + tableName);
                require(expectedKind.equals(rs.getString(1)),
                        "Expected generated column kind " + expectedKind + " for table " + tableName);
            }
        }
    }

    private static void assertGeneratedColumnValue(Connection connection, String tableName) throws Exception {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(String.format("SELECT c1, c2, c8 FROM %s ORDER BY c1, c2 LIMIT 1",
                     tableName))) {
            require(rs.next(), "Expected at least one row in generated-column table " + tableName);
            int c1 = rs.getInt(1);
            int c2 = rs.getInt(2);
            int c8 = rs.getInt(3);
            require(c8 == c1 + c2,
                    String.format("Expected generated column c8 to equal c1 + c2, observed %d, %d, %d", c1, c2,
                            c8));
        }
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
