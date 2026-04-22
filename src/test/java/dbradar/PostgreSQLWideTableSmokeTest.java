package dbradar;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class PostgreSQLWideTableSmokeTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;
    private static final String DATABASE_PREFIX = "task3_columns_";

    private PostgreSQLWideTableSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        int exitCode = Main.executeMain(
                "--num-threads", "1",
                "--num-tries", "1",
                "--num-queries", "1",
                "--max-generated-databases", "1",
                "--print-progress-information", "false",
                "--database-prefix", DATABASE_PREFIX,
                "--ddl-count", "6",
                "--dml-count", "4",
                "--host", HOST,
                "--port", String.valueOf(PORT),
                "--username", USERNAME,
                "--password", PASSWORD,
                "postgresql", "--oracle", "equation");
        require(exitCode == 0, "Expected Main.executeMain to succeed for wide-table smoke");

        Path logFile = Path.of("logs", "postgresql", DATABASE_PREFIX + "0-cur.log");
        require(Files.exists(logFile), "Expected wide-table log file to exist: " + logFile);

        List<String> statements = Files.readAllLines(logFile).stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .filter(line -> !line.startsWith("--"))
                .toList();

        List<String> createTableStatements = new ArrayList<>();
        for (String statement : statements) {
            if (statement.startsWith("CREATE TABLE ")
                    || statement.startsWith("CREATE TEMP TABLE ")
                    || statement.startsWith("CREATE TEMPORARY TABLE ")
                    || statement.startsWith("CREATE UNLOGGED TABLE ")) {
                createTableStatements.add(statement);
            }
        }

        require(!createTableStatements.isEmpty(), "Expected at least one CREATE TABLE statement in the log");
        for (String statement : createTableStatements) {
            int columnCount = countColumnDefinitions(statement);
            require(columnCount >= 8 && columnCount <= 15,
                    "Expected CREATE TABLE statement to contain 8-15 column definitions but observed " + columnCount
                            + " in: " + statement);
        }
    }

    private static int countColumnDefinitions(String statement) {
        int openingParen = statement.indexOf('(');
        int closingParen = findMatchingParen(statement, openingParen);
        require(openingParen >= 0 && closingParen > openingParen, "Expected CREATE TABLE statement to contain columns");

        String columnSection = statement.substring(openingParen + 1, closingParen);
        List<String> topLevelItems = splitTopLevelItems(columnSection);
        int columnCount = 0;
        for (String item : topLevelItems) {
            String trimmed = item.trim();
            if (trimmed.matches("c\\d+\\s+.*")) {
                columnCount++;
            }
        }
        return columnCount;
    }

    private static int findMatchingParen(String text, int openingParen) {
        int depth = 0;
        for (int i = openingParen; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static List<String> splitTopLevelItems(String text) {
        List<String> items = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth--;
            } else if (ch == ',' && depth == 0) {
                items.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(ch);
        }
        if (current.length() > 0) {
            items.add(current.toString());
        }
        return items;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
