package dbradar;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class PostgreSQLTypeCoverageSmokeTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;
    private static final String EQUATION_PREFIX = "task5_typecov_eq_";
    private static final String STRESS_PREFIX = "task5_typecov_stress_";

    private PostgreSQLTypeCoverageSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyEquationTypeCoverageAndReplay();
        verifyStressTypeCoverage();
    }

    private static void verifyEquationTypeCoverageAndReplay() throws Exception {
        int exitCode = Main.executeMain(
                "--num-threads", "1",
                "--num-tries", "1",
                "--num-queries", "2",
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
        require(exitCode == 0, "Expected equation type coverage run to succeed");

        Path logFile = Path.of("logs", "postgresql", EQUATION_PREFIX + "0-cur.log");
        require(Files.exists(logFile), "Expected equation type coverage log: " + logFile);
        List<String> statements = readStatements(logFile);
        require(statements.stream().anyMatch(line -> line.startsWith("DO $$ BEGIN ")),
                "Expected equation log to contain the custom-type setup block");

        try (Connection stateConnection = createConnection(EQUATION_PREFIX + "0");
             Connection semiConnection = createConnection(EQUATION_PREFIX + "0_semi")) {
            String builtInTableName = findCoverageTableName(stateConnection, "typecov_builtin");
            String userTableName = findCoverageTableName(stateConnection, "typecov_user");
            Map<String, String> stateTypes = readColumnTypes(stateConnection, userTableName);
            Map<String, String> semiTypes = readColumnTypes(semiConnection, userTableName);
            require(!stateTypes.isEmpty(), "Expected the user-defined type coverage table to exist in the main database");
            require(stateTypes.equals(semiTypes),
                    "Expected semi-state replay to preserve exact type identities for the user-defined coverage table");
            require("int4range".equals(stateTypes.get("c1")), "Expected c1 to remain int4range after replay");
            require("int4multirange".equals(stateTypes.get("c2")), "Expected c2 to remain int4multirange after replay");
            require(stateTypes.get("c3").endsWith("coverage_enum"),
                    "Expected c3 to use the generated enum type but observed " + stateTypes.get("c3"));
            require(stateTypes.get("c4").endsWith("coverage_domain"),
                    "Expected c4 to use the generated domain type but observed " + stateTypes.get("c4"));
            require(stateTypes.get("c5").endsWith("coverage_composite"),
                    "Expected c5 to use the generated composite type but observed " + stateTypes.get("c5"));
            require("box".equals(stateTypes.get("c9")), "Expected c9 to remain box after replay");
            require("lseg".equals(stateTypes.get("c10")), "Expected c10 to remain lseg after replay");
            require("path".equals(stateTypes.get("c11")), "Expected c11 to remain path after replay");
            require("polygon".equals(stateTypes.get("c12")), "Expected c12 to remain polygon after replay");
            require("circle".equals(stateTypes.get("c13")), "Expected c13 to remain circle after replay");
            require("macaddr8".equals(stateTypes.get("c14")), "Expected c14 to remain macaddr8 after replay");
            require("tsquery".equals(stateTypes.get("c15")), "Expected c15 to remain tsquery after replay");
            require(readRowCount(stateConnection, builtInTableName) >= 1,
                    "Expected the built-in coverage table to contain at least one row");
            require(readRowCount(stateConnection, userTableName) >= 1,
                    "Expected the user-defined coverage table to contain at least one row");
        }
    }

    private static void verifyStressTypeCoverage() throws Exception {
        int exitCode = Main.executeMain(
                "--num-threads", "2",
                "--num-tries", "4",
                "--num-queries", "18",
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
        require(exitCode == 0, "Expected stress type coverage run to succeed");

        Path thread0Log = Path.of("logs", "postgresql", STRESS_PREFIX + "1-cur.log");
        Path thread1Log = Path.of("logs", "postgresql", STRESS_PREFIX + "2-cur.log");
        require(Files.exists(thread0Log), "Expected shared stress log: " + thread0Log);
        require(Files.exists(thread1Log), "Expected shared stress log: " + thread1Log);

        List<String> combinedStatements = readStatements(thread0Log);
        combinedStatements.addAll(readStatements(thread1Log));
        require(combinedStatements.stream().anyMatch(line -> line.startsWith("DO $$ BEGIN ")),
                "Expected stress logs to contain the custom-type setup block");
        require(combinedStatements.stream().anyMatch(line -> line.startsWith("CREATE TABLE ") && line.contains("typecov_builtin")),
                "Expected stress logs to create the built-in coverage table");
        require(combinedStatements.stream().anyMatch(line -> line.startsWith("CREATE TABLE ") && line.contains("typecov_user")),
                "Expected stress logs to create the user-defined coverage table");
        require(combinedStatements.stream().anyMatch(line -> line.startsWith("INSERT INTO ") && line.contains("typecov_builtin")),
                "Expected stress logs to insert into the built-in coverage table");
        require(combinedStatements.stream().anyMatch(line -> line.startsWith("INSERT INTO ") && line.contains("typecov_user")),
                "Expected stress logs to insert into the user-defined coverage table");

        try (Connection connection = createConnection(STRESS_PREFIX + "0")) {
            require(findCoverageTypeName(connection, "coverage_enum").endsWith("coverage_enum"),
                    "Expected the stress database to retain a generated enum type");
            require(findCoverageTypeName(connection, "coverage_domain").endsWith("coverage_domain"),
                    "Expected the stress database to retain a generated domain type");
            require(findCoverageTypeName(connection, "coverage_composite").endsWith("coverage_composite"),
                    "Expected the stress database to retain a generated composite type");
        }
    }

    private static Connection createConnection(String databaseName) throws Exception {
        return DriverManager.getConnection(
                String.format("jdbc:postgresql://%s:%d/%s", HOST, PORT, databaseName),
                USERNAME,
                PASSWORD);
    }

    private static Map<String, String> readColumnTypes(Connection connection, String tableName) throws Exception {
        Map<String, String> columnTypes = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(String.format(
                     "SELECT a.attname, format_type(a.atttypid, a.atttypmod) "
                             + "FROM pg_attribute a "
                             + "JOIN pg_class c ON c.oid = a.attrelid "
                             + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                             + "WHERE n.nspname = 'public' AND c.relname = '%s' "
                             + "AND a.attnum > 0 AND NOT a.attisdropped "
                             + "ORDER BY a.attnum",
                     tableName))) {
            while (rs.next()) {
                columnTypes.put(rs.getString(1), rs.getString(2));
            }
        }
        return columnTypes;
    }

    private static String findCoverageTableName(Connection connection, String suffix) throws Exception {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(String.format(
                     "SELECT relname FROM pg_class c "
                             + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                             + "WHERE n.nspname = 'public' AND c.relkind = 'r' "
                             + "AND relname LIKE '%%%s%%' ORDER BY relname LIMIT 1",
                     suffix))) {
            require(rs.next(), "Expected to find a coverage table containing suffix " + suffix);
            return rs.getString(1);
        }
    }

    private static String findCoverageTypeName(Connection connection, String suffix) throws Exception {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(String.format(
                     "SELECT typname FROM pg_type t "
                             + "JOIN pg_namespace n ON n.oid = t.typnamespace "
                             + "WHERE n.nspname = 'public' AND typname LIKE '%%%s%%' "
                             + "ORDER BY typname LIMIT 1",
                     suffix))) {
            require(rs.next(), "Expected to find a coverage type containing suffix " + suffix);
            return rs.getString(1);
        }
    }

    private static int readRowCount(Connection connection, String tableName) throws Exception {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
            require(rs.next(), "Expected COUNT(*) result for " + tableName);
            return rs.getInt(1);
        }
    }

    private static List<String> readStatements(Path logFile) throws Exception {
        return new ArrayList<>(Files.readAllLines(logFile).stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .filter(line -> !line.startsWith("--"))
                .toList());
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
