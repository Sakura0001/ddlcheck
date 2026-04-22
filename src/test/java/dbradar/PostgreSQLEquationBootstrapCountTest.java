package dbradar;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class PostgreSQLEquationBootstrapCountTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;
    private static final int DDL_COUNT = 4;
    private static final int DML_COUNT = 3;
    private static final String DATABASE_PREFIX = "task1_bootstrap_";

    private PostgreSQLEquationBootstrapCountTest() {
    }

    public static void main(String[] args) throws Exception {
        int exitCode = Main.executeMain(
                "--num-threads", "1",
                "--num-tries", "1",
                "--num-queries", "1",
                "--max-generated-databases", "1",
                "--print-progress-information", "false",
                "--database-prefix", DATABASE_PREFIX,
                "--ddl-count", String.valueOf(DDL_COUNT),
                "--dml-count", String.valueOf(DML_COUNT),
                "--host", HOST,
                "--port", String.valueOf(PORT),
                "--username", USERNAME,
                "--password", PASSWORD,
                "postgresql", "--oracle", "equation");
        require(exitCode == 0, "Expected Main.executeMain to succeed");

        Path logFile = Path.of("logs", "postgresql", DATABASE_PREFIX + "0-cur.log");
        require(Files.exists(logFile), "Expected log file to exist: " + logFile);

        List<String> statements = Files.readAllLines(logFile).stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .filter(line -> !line.startsWith("--"))
                .toList();

        int semiStartIndex = statements.indexOf("==== Start SemiState ====;");
        int semiEndIndex = statements.indexOf("==== End SemiState ====;");
        require(semiStartIndex > 0, "Expected a semi-state start marker");
        require(semiEndIndex > semiStartIndex, "Expected a semi-state end marker");

        int observedDdlCount = semiStartIndex;
        require(observedDdlCount == DDL_COUNT,
                String.format("Expected %d bootstrap DDL statements but observed %d", DDL_COUNT, observedDdlCount));

        int firstSelectIndex = firstSelectIndex(statements, semiEndIndex + 1);
        require(firstSelectIndex > semiEndIndex, "Expected a SELECT after bootstrap DML statements");

        int observedDmlCount = firstSelectIndex - semiEndIndex - 1;
        require(observedDmlCount == DML_COUNT,
                String.format("Expected %d bootstrap DML statements but observed %d", DML_COUNT, observedDmlCount));
    }

    private static int firstSelectIndex(List<String> statements, int startIndex) {
        for (int i = startIndex; i < statements.size(); i++) {
            if (statements.get(i).startsWith("SELECT")) {
                return i;
            }
        }
        return -1;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
