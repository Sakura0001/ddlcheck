package dbradar.mysql;

import dbradar.MainOptions;
import dbradar.SQLConnection;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.KeyFunc;
import dbradar.mysql.schema.MySQLSchema;
import dbradar.mysql.schema.MySQLSchema.MySQLColumn;
import dbradar.mysql.schema.MySQLSchema.MySQLForeignKey;
import dbradar.mysql.schema.MySQLSchema.MySQLTable;
import grammar.Token;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMySQLStressGenerationRegression {

    @Test
    public void testInsertSelectSameTableProjectsWritableColumns() {
        FakeMySQLState state = new FakeMySQLState();
        state.setMainOptions(new MainOptions());
        state.installSchema(createSchemaWithGeneratedColumn());

        MySQLKeyFuncManager keyFuncManager = new MySQLKeyFuncManager(state);
        KeyFunc keyFunc = keyFuncManager.getFuncByKey("_insert_select_same_table");
        ASTNode parent = new ASTNode(new Token(Token.TokenType.KEYWORD, "_insert_select_same_table"));

        keyFunc.generateAST(parent);

        String sql = parent.toQueryString();
        assertFalse(sql.contains("SELECT *"),
                "same-table insert-select must project the same writable columns instead of SELECT *: " + sql);
        assertTrue(sql.contains("SELECT c1 , c3 FROM t0") || sql.contains("SELECT c1, c3 FROM t0"),
                "same-table insert-select should project writable columns only: " + sql);
    }

    @Test
    public void testGrammarUsesExistingAliasKeyForGroupAndOrderBy() throws Exception {
        String content = java.nio.file.Files.readString(
                java.nio.file.Path.of("src/main/resources/dbradar/mysql/mysql.grammar.yy"));
        assertTrue(content.contains("group_by_expr:\n    _existing_column_alias"),
                "GROUP BY should use an existing emitted alias");
        assertTrue(content.contains("ordering_term:\n    _existing_column_alias"),
                "ORDER BY should use an existing emitted alias");
    }

    @Test
    public void testStressGrammarUsesDedicatedWritableDuplicateUpdateKey() throws Exception {
        String content = java.nio.file.Files.readString(
                java.nio.file.Path.of("src/main/resources/dbradar/mysql/mysql.grammar.yy"));
        assertTrue(content.contains("ON DUPLICATE KEY UPDATE _insert_duplicate_update_column = VALUES( _insert_duplicate_update_column )"),
                "duplicate-key update should target a writable insert column only");
    }

    @Test
    public void testStressOracleRetriesOnlyTransientErrors() throws Exception {
        Method shouldRetry = dbradar.mysql.oracle.MySQLStressOracle.class
                .getDeclaredMethod("shouldRetry", SQLException.class);
        shouldRetry.setAccessible(true);

        boolean unknownColumn = (boolean) shouldRetry.invoke(null,
                new SQLException("Unknown column", "42S22", 1054));
        boolean deadlock = (boolean) shouldRetry.invoke(null,
                new SQLException("Deadlock found", "40001", 1213));

        assertFalse(unknownColumn, "deterministic semantic errors must not be retried");
        assertTrue(deadlock, "deadlocks should still be retried");
    }

    @Test
    public void testFetchForeignKeysSkipsTransientMetadataRowsWithMissingReferencedTable() throws Exception {
        Method fetchForeignKeys = MySQLSchema.class.getDeclaredMethod("fetchForeignKeys",
                SQLConnection.class, String.class, List.class);
        fetchForeignKeys.setAccessible(true);

        SQLConnection connection = new SQLConnection(createConnectionProxy(List.of(Map.of(
                "CONSTRAINT_NAME", "fk_missing_ref",
                "TABLE_NAME", "t_child",
                "COLUMN_NAME", "c_ref",
                "REFERENCED_TABLE_NAME", "t_parent",
                "REFERENCED_COLUMN_NAME", "c_id"
        ))));

        List<MySQLTable> tables = List.of(createTable("t_child", "c_ref"));

        @SuppressWarnings("unchecked")
        List<MySQLForeignKey> foreignKeys = assertDoesNotThrow(() ->
                (List<MySQLForeignKey>) fetchForeignKeys.invoke(null, connection, "test_db", tables));

        assertEquals(0, foreignKeys.size(),
                "schema refresh should skip transient FK metadata when referenced tables disappear mid-read");
    }

    private MySQLSchema createSchemaWithGeneratedColumn() {
        MySQLColumn c1 = new MySQLColumn("c1", null, true, "int", 0, 10, 0, "");
        MySQLColumn c2 = new MySQLColumn("c2", null, true, "int", 0, 10, 0, "");
        c2.setGenerated(true);
        MySQLColumn c3 = new MySQLColumn("c3", null, true, "int", 0, 10, 0, "");
        MySQLTable table = new MySQLTable("t0", List.of(c1, c2, c3), List.of(),
                MySQLTable.MySQLEngine.INNO_DB, false);
        c1.setTable(table);
        c2.setTable(table);
        c3.setTable(table);
        return new MySQLSchema(List.of(table), List.of());
    }

    private MySQLTable createTable(String tableName, String... columnNames) {
        List<MySQLColumn> columns = new ArrayList<>();
        for (String columnName : columnNames) {
            columns.add(new MySQLColumn(columnName, null, true, "int", 0, 10, 0, ""));
        }
        MySQLTable table = new MySQLTable(tableName, columns, List.of(),
                MySQLTable.MySQLEngine.INNO_DB, false);
        for (MySQLColumn column : columns) {
            column.setTable(table);
        }
        return table;
    }

    private Connection createConnectionProxy(List<Map<String, String>> rows) {
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                new StatementHandler(rows));

        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "createStatement":
                    return statement;
                case "close":
                    return null;
                case "isClosed":
                    return false;
                case "unwrap":
                    return null;
                case "isWrapperFor":
                    return false;
                default:
                    throw new UnsupportedOperationException("Unsupported Connection method: " + method.getName());
            }
        };
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                handler);
    }

    private static final class StatementHandler implements InvocationHandler {
        private final List<Map<String, String>> rows;

        private StatementHandler(List<Map<String, String>> rows) {
            this.rows = rows;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "executeQuery":
                    return Proxy.newProxyInstance(
                            ResultSet.class.getClassLoader(),
                            new Class<?>[]{ResultSet.class},
                            new ResultSetHandler(rows));
                case "close":
                    return null;
                case "unwrap":
                    return null;
                case "isWrapperFor":
                    return false;
                default:
                    throw new UnsupportedOperationException("Unsupported Statement method: " + method.getName());
            }
        }
    }

    private static final class ResultSetHandler implements InvocationHandler {
        private final List<Map<String, String>> rows;
        private int index = -1;

        private ResultSetHandler(List<Map<String, String>> rows) {
            this.rows = rows;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "next":
                    index++;
                    return index < rows.size();
                case "getString":
                    return rows.get(index).get(String.valueOf(args[0]));
                case "close":
                    return null;
                case "wasNull":
                    return false;
                case "unwrap":
                    return null;
                case "isWrapperFor":
                    return false;
                default:
                    throw new UnsupportedOperationException("Unsupported ResultSet method: " + method.getName());
            }
        }
    }

    private static final class FakeMySQLState extends MySQLGlobalState {
        void installSchema(MySQLSchema schema) {
            setSchema(schema);
        }
    }
}
