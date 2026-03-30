package dbradar.mysql;

import dbradar.MainOptions;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.KeyFunc;
import dbradar.mysql.schema.MySQLSchema;
import dbradar.mysql.schema.MySQLSchema.MySQLColumn;
import dbradar.mysql.schema.MySQLSchema.MySQLTable;
import grammar.Token;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
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

    private static final class FakeMySQLState extends MySQLGlobalState {
        void installSchema(MySQLSchema schema) {
            setSchema(schema);
        }
    }
}
