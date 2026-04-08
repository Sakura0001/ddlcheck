package dbradar.mysql;

import dbradar.ddlCheck.TestMySQLEDCOracle;
import dbradar.mysql.schema.MySQLSchema.MySQLColumn;
import dbradar.mysql.schema.MySQLSchema.MySQLTable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMySQLVirtualColumnCoverage {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 3306;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "Taurus_123";

    @Test
    public void testSchemaMarksAlterAddedGeneratedColumns() throws Exception {
        String databaseName = "codex_virtual_columns_" + System.nanoTime();
        MySQLGlobalState state = new MySQLGlobalState();
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase(HOST, PORT, USERNAME, PASSWORD, databaseName));
        try {
            try (Statement statement = state.getConnection().createStatement()) {
                statement.execute("CREATE TABLE t0 (c1 INT, c2 INT)");
                statement.execute("ALTER TABLE t0 ADD COLUMN c3 INT GENERATED ALWAYS AS ((c1 + c2)) VIRTUAL");
                statement.execute("ALTER TABLE t0 ADD COLUMN c4 INT GENERATED ALWAYS AS ((c1 * 2)) STORED");
            }

            state.updateSchema();

            MySQLTable table = state.getSchema().getDatabaseTables().stream()
                    .filter(t -> t.getName().equals("t0"))
                    .findFirst()
                    .orElseThrow();
            MySQLColumn c3 = table.getColumns().stream()
                    .filter(c -> c.getName().equals("c3"))
                    .findFirst()
                    .orElseThrow();
            MySQLColumn c4 = table.getColumns().stream()
                    .filter(c -> c.getName().equals("c4"))
                    .findFirst()
                    .orElseThrow();

            assertTrue(c3.isGenerated(), "ALTER-added virtual column should be marked generated");
            assertTrue(c4.isGenerated(), "ALTER-added stored column should be marked generated");

            try (Statement statement = state.getConnection().createStatement();
                 ResultSet rs = statement.executeQuery("SHOW CREATE TABLE t0")) {
                assertTrue(rs.next(), "SHOW CREATE TABLE should return the created table");
                String createTable = rs.getString("Create Table");
                assertNotNull(createTable, "SHOW CREATE TABLE should include the create statement");
                assertTrue(createTable.contains("GENERATED ALWAYS AS"),
                        "SHOW CREATE TABLE should preserve generated-column syntax");
            }
        } finally {
            if (state.getConnection() != null) {
                state.getConnection().close();
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReplayInsertHelperExcludesGeneratedColumns() throws Exception {
        String databaseName = "codex_virtual_replay_" + System.nanoTime();
        MySQLGlobalState state = new MySQLGlobalState();
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase(HOST, PORT, USERNAME, PASSWORD, databaseName));
        try {
            try (Statement statement = state.getConnection().createStatement()) {
                statement.execute("CREATE TABLE t0 (c1 INT, c2 INT, generated_total INT GENERATED ALWAYS AS ((c1 + c2)) VIRTUAL)");
                statement.execute("INSERT INTO t0 (c1, c2) VALUES (1, 2)");
            }

            Method fetchInsertStmts = TestMySQLEDCOracle.class.getDeclaredMethod("fetchInsertStmts", MySQLGlobalState.class, String.class);
            fetchInsertStmts.setAccessible(true);
            List<String> insertStmts = (List<String>) fetchInsertStmts.invoke(new TestMySQLEDCOracle(), state, "t0");

            assertNotNull(insertStmts, "helper should return replay INSERT statements");
            assertFalse(insertStmts.isEmpty(), "table with rows should produce replay INSERT statements");

            String insertSql = insertStmts.get(0);
            assertFalse(insertSql.contains("generated_total"),
                    "replay INSERTs must not target generated columns");
            assertTrue(insertSql.contains("(c1, c2)"),
                    "replay INSERTs should contain only writable base columns");
        } finally {
            if (state.getConnection() != null) {
                state.getConnection().close();
            }
        }
    }
}
