package dbradar.mysql.oracle;

import dbradar.Main;
import dbradar.mysql.MySQLGlobalState;
import dbradar.mysql.MySQLOptions;
import dbradar.mysql.schema.MySQLSchema.MySQLTable;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMySQLStressDMLIntegration {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 3306;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "Taurus_123";

    @Test
    public void testTinyStressRoundLeavesRowsAndLogsInsertLikeDml() throws Exception {
        String databasePrefix = "codex_mysql_stress_dml_it_" + System.nanoTime() + "_";
        String databaseName = databasePrefix + "0";

        int exitCode = Main.executeMain(buildArgs(databasePrefix));
        assertEquals(0, exitCode, "main run should complete cleanly");

        MySQLGlobalState state = new MySQLGlobalState();
        state.setDatabaseName(databaseName);
        state.setConnection(state.createConnection(HOST, PORT, USERNAME, PASSWORD, databaseName));
        try {
            state.updateSchema();
            Path logPath = Path.of("logs", "mysql", databaseName + "-cur.log");
            assertAll(
                    () -> assertTrue(hasRowsInAnyBaseTable(state),
                            "stress DML should leave at least one table with inserted data"),
                    () -> assertTrue(containsInsertLikeDml(logPath),
                            "stress log should record insert-like DML after the fix"));
        } finally {
            if (state.getConnection() != null) {
                state.getConnection().close();
            }
        }
    }

    private String[] buildArgs(String databasePrefix) {
        List<String> args = new ArrayList<>();
        args.add("--username");
        args.add(USERNAME);
        args.add("--password");
        args.add(PASSWORD);
        args.add("--host");
        args.add(HOST);
        args.add("--port");
        args.add(String.valueOf(PORT));
        args.add("--num-threads");
        args.add("1");
        args.add("--timeout-seconds");
        args.add("20");
        args.add("--log-each-select");
        args.add("true");
        args.add("--log-execution-time");
        args.add("false");
        args.add("--print-progress-information");
        args.add("false");
        args.add("--use-connection-test");
        args.add("false");
        args.add("--database-prefix");
        args.add(databasePrefix);
        args.add("mysql");
        args.add("--oracle");
        args.add("STRESS");
        args.add("--stress-threads-per-db");
        args.add("1");
        args.add("--stress-rounds-per-db");
        args.add("1");
        args.add("--stress-ddl-per-thread");
        args.add("0");
        args.add("--stress-dml-per-thread");
        args.add("3");
        args.add("--stress-query-per-thread");
        args.add("0");
        return args.toArray(String[]::new);
    }

    private boolean hasRowsInAnyBaseTable(MySQLGlobalState state) throws Exception {
        List<MySQLTable> tables = state.getSchema().getDatabaseTablesWithoutViews();
        try (Statement statement = state.getConnection().createStatement()) {
            for (MySQLTable table : tables) {
                try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM `" + table.getName() + "`")) {
                    if (rs.next() && rs.getLong(1) > 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean containsInsertLikeDml(Path logPath) throws Exception {
        try (var lines = Files.lines(logPath)) {
            return lines.anyMatch(line -> line.contains("| INSERT ") || line.contains("| REPLACE "));
        }
    }
}
