package dbradar.mysql;

import dbradar.mysql.schema.MySQLSchema;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMySQLGrammarAndTypeSupport {

    @Test
    public void testGrammarContainsImportantMySQL80Features() throws Exception {
        Path grammar = Path.of("src/main/resources/dbradar/mysql/mysql.grammar.yy");
        String content = Files.readString(grammar);
        String luaConfig = Files.readString(Path.of("src/main/resources/dbradar/mysql/mysql.zz.lua"));
        String alterAddColumnSection = extractGrammarSection(content, "alter_table_add_column:", "algorithm:");

        assertTrue(content.contains("GENERATED ALWAYS AS"), "generated column syntax should exist");
        assertTrue(content.contains("column_definition:"),
                "shared column definition rule should exist");
        assertTrue(content.contains("_new_column_name type_name generated_column_definition"),
                "shared column definition should include generated column support");
        assertTrue(alterAddColumnSection.contains("column_definition"),
                "ALTER TABLE ADD COLUMN should route through the shared column definition rule");
        assertTrue(alterAddColumnSection.contains("ALTER TABLE _table ADD COLUMN? column_definition algorithm?"),
                "ALTER TABLE ADD COLUMN should still support the plain ADD COLUMN form");
        assertTrue(alterAddColumnSection.contains("ALTER TABLE _table ADD COLUMN? column_definition FIRST algorithm?"),
                "ALTER TABLE ADD COLUMN should still support the FIRST positional form");
        assertTrue(alterAddColumnSection.contains("ALTER TABLE _table ADD COLUMN? column_definition AFTER _column algorithm?"),
                "ALTER TABLE ADD COLUMN should still support the AFTER positional form");
        assertTrue(content.contains("ALTER TABLE _table ALTER COLUMN? _mutable_column SET DEFAULT NULL"),
                "ALTER COLUMN SET DEFAULT should avoid generated columns");
        assertTrue(content.contains("ALTER TABLE _table MODIFY COLUMN? _mutable_column type_name algorithm?"),
                "MODIFY COLUMN should avoid generated columns");
        assertTrue(!content.contains("generated_constraint"),
                "stale generated-constraint symbol references should be removed");
        assertTrue(content.contains("function_index_expr"), "functional index syntax should exist");
        assertTrue(!content.contains("| JSON"), "MySQL grammar should not expose JSON as a type");
        assertTrue(!content.contains("json_func:"), "MySQL grammar should not expose JSON functions");
        assertTrue(!luaConfig.contains("JSON ="), "MySQL Lua generator config should not expose JSON values");
        assertTrue(!content.contains("SLEEP("), "slow sleep function should be removed from query grammar");
        assertTrue(!content.contains("table_option* partition_option?"),
                "create table should not append generic partition clauses that frequently generate invalid MySQL 8.0.41 DDL");
        assertTrue(!content.contains("PARTITION BY partition_strategy PARTITIONS"),
                "MySQL 8.0.41 compatibility mode should not use generic PARTITIONS with every partition strategy");
        assertTrue(!content.contains("PARTITION BY partition_strategy subpartition_clause?"),
                "MySQL 8.0.41 compatibility mode should not allow arbitrary subpartition combinations");
        assertTrue(!content.contains("ALTER TABLE _table ADD PARTITION"),
                "stress grammar should not emit partition-only ALTER TABLE variants for arbitrary tables");
        assertTrue(!content.contains("INSERT_METHOD = insert_method_option"),
                "MySQL 8.0.41 compatibility mode should avoid engine-specific CREATE TABLE options");
        assertTrue(!content.contains("| FEDERATED"),
                "MySQL 8.0.41 compatibility mode should avoid optional storage engines that are often unavailable");
    }

    @Test
    public void testGrammarContainsApprovedInnoDBAndDstoreExtensions() throws Exception {
        String content = Files.readString(Path.of("src/main/resources/dbradar/mysql/mysql.grammar.yy"));

        assertTrue(content.contains("alter_table_multi_add_column:"),
                "MySQL grammar should support multi-column ALTER TABLE ADD COLUMN forms");
        assertTrue(content.contains("alter_table_multi_drop_column:"),
                "MySQL grammar should support multi-column ALTER TABLE DROP COLUMN forms");
        assertTrue(content.contains("lock_clause:"),
                "MySQL grammar should expose ALTER TABLE LOCK clauses");
        assertTrue(content.contains("row_format_clause:"),
                "MySQL grammar should expose ALTER TABLE ROW_FORMAT clauses");
        assertTrue(content.contains(", ALGORITHM = INPLACE"),
                "MySQL grammar should support ALGORITHM=INPLACE");
        assertTrue(content.contains(", ALGORITHM = INSTANT"),
                "MySQL grammar should support ALGORITHM=INSTANT");
        assertTrue(content.contains(", ALGORITHM = COPY"),
                "MySQL grammar should support ALGORITHM=COPY");
        assertTrue(content.contains(", ALGORITHM = DEFAULT"),
                "MySQL grammar should support ALGORITHM=DEFAULT");
        assertTrue(content.contains("for_lock_clause:"),
                "MySQL grammar should support locking read clauses");
        assertTrue(content.contains("FOR SHARE lock_wait_option?"),
                "MySQL grammar should support FOR SHARE");
        assertTrue(content.contains("NOWAIT"),
                "MySQL grammar should support NOWAIT");
        assertTrue(content.contains("SKIP LOCKED"),
                "MySQL grammar should support SKIP LOCKED");
        assertTrue(content.contains("SELECT all_columns FROM system_information"),
                "ALTER query root should allow dstore system table inspection through SELECT *");
        assertTrue(content.contains("system_information:"),
                "MySQL grammar should define dstore system-table targets");
        assertTrue(content.contains("INFORMATION_SCHEMA.DSTORE_BUFFER_POOL_STATS LIMIT 250"),
                "MySQL grammar should include dstore buffer pool stats");
        assertTrue(content.contains("INFORMATION_SCHEMA.DSTORE_INDEXES LIMIT 250"),
                "MySQL grammar should include dstore indexes metadata");
        assertTrue(content.contains("INFORMATION_SCHEMA.DSTORE_LOCKS LIMIT 250"),
                "MySQL grammar should include dstore lock metadata");
        assertTrue(content.contains("INFORMATION_SCHEMA.DSTORE_MEM LIMIT 250"),
                "MySQL grammar should include dstore memory metadata");
        assertTrue(content.contains("INFORMATION_SCHEMA.DSTORE_TRX LIMIT 250"),
                "MySQL grammar should include dstore transaction metadata");
        assertTrue(content.contains("INFORMATION_SCHEMA.DSTORE_UNDO LIMIT 250"),
                "MySQL grammar should include dstore undo metadata");
        assertTrue(content.contains("INFORMATION_SCHEMA.DSTORE_SEGMENT LIMIT 250"),
                "MySQL grammar should include dstore segment metadata");
    }

    @Test
    public void testGrammarContainsAdditionalApprovedMySQL80Syntax() throws Exception {
        String content = Files.readString(Path.of("src/main/resources/dbradar/mysql/mysql.grammar.yy"));

        assertTrue(content.contains("COALESCE( _column, 0 )"),
                "generated column expressions should allow COALESCE over existing columns");
        assertTrue(content.contains("IFNULL( _column, 0 )"),
                "generated column expressions should allow IFNULL over existing columns");
        assertTrue(content.contains("ABS( _column )"),
                "generated column expressions should allow ABS over existing columns");
        assertTrue(content.contains("CONCAT( _column, '_suffix' )"),
                "generated column expressions should allow CONCAT over existing columns");
        assertTrue(content.contains("UPPER( _column )"),
                "generated column expressions should allow UPPER over existing columns");
        assertTrue(content.contains("LOWER( _column )"),
                "generated column expressions should allow LOWER over existing columns");
        assertTrue(content.contains("| (function_index_expr) primary_key_order?"),
                "indexed columns should allow functional key parts in composite indexes");
        assertTrue(content.contains("LEFT(_column,"),
                "functional indexes should support LEFT");
        assertTrue(content.contains("RIGHT(_column,"),
                "functional indexes should support RIGHT");
        assertTrue(content.contains("SUBSTRING(_column,"),
                "functional indexes should support SUBSTRING");
        assertTrue(content.contains("TRIM(_column)"),
                "functional indexes should support TRIM");
        assertTrue(content.contains("REVERSE(_column)"),
                "functional indexes should support REVERSE");
        assertTrue(content.contains("CHAR_LENGTH(_column)"),
                "functional indexes should support CHAR_LENGTH");
        assertTrue(content.contains("YEAR(_column)"),
                "functional indexes should support YEAR");
        assertTrue(content.contains("DATE_FORMAT(_column,"),
                "functional indexes should support DATE_FORMAT");
        assertTrue(content.contains("| HIGH_PRIORITY"),
                "SELECT should support HIGH_PRIORITY");
        assertTrue(content.contains("| SQL_BUFFER_RESULT"),
                "SELECT should support SQL_BUFFER_RESULT");
        assertTrue(content.contains("| SQL_CALC_FOUND_ROWS"),
                "SELECT should support SQL_CALC_FOUND_ROWS");
        assertTrue(content.contains("| join_table RIGHT JOIN join_table on_or_use_clause"),
                "join grammar should support RIGHT JOIN");
        assertTrue(content.contains("ORDER BY _insert_columns"),
                "DML grammar should support ORDER BY for single-table UPDATE/DELETE");
        assertTrue(content.contains("LIMIT _int4_unsigned"),
                "DML grammar should support LIMIT for single-table UPDATE/DELETE");
    }

    @Test
    public void testGrammarContainsTablespaceSupport() throws Exception {
        String content = Files.readString(Path.of("src/main/resources/dbradar/mysql/mysql.grammar.yy"));

        assertTrue(content.contains("alter_tablespace:"),
                "MySQL grammar should expose a tablespace query root");
        assertTrue(content.contains("ALTER TABLESPACE _exist_tablespace RENAME TO _tablespace_name"),
                "MySQL grammar should support ALTER TABLESPACE ... RENAME TO");
        assertTrue(content.contains("ALTER TABLE _table TABLESPACE _exist_tablespace"),
                "MySQL grammar should support moving a table into an existing tablespace");
        assertTrue(content.contains("DROP TABLESPACE _exist_tablespace"),
                "MySQL grammar should support DROP TABLESPACE");
        assertTrue(content.contains("CREATE TABLESPACE _tablespace_name"),
                "MySQL grammar should support CREATE TABLESPACE");

        assertEquals(MySQLProvider.MySQLQueryProvider.ALTER_TABLESPACE,
                MySQLProvider.MySQLQueryProvider.valueOf("ALTER_TABLESPACE"));
        assertEquals(MySQLProvider.MySQLDDLStmt.ALTER_TABLESPACE,
                MySQLProvider.MySQLDDLStmt.valueOf("ALTER_TABLESPACE"));
    }

    private String extractGrammarSection(String content, String startMarker, String endMarker) {
        int start = content.indexOf(startMarker);
        int end = content.indexOf(endMarker, start);
        assertTrue(start >= 0, "grammar should contain section: " + startMarker);
        assertTrue(end > start, "grammar should contain end marker after section: " + endMarker);
        return content.substring(start, end);
    }

    @Test
    public void testExtendedDataTypeMapping() throws Exception {
        Method m = MySQLSchema.class.getDeclaredMethod("getMySQLDataType", String.class);
        m.setAccessible(true);

        assertEquals(MySQLSchema.MySQLDataType.DATE, m.invoke(null, "date"));
        assertEquals(MySQLSchema.MySQLDataType.TIME, m.invoke(null, "time"));
        assertEquals(MySQLSchema.MySQLDataType.DATETIME, m.invoke(null, "datetime"));
        assertEquals(MySQLSchema.MySQLDataType.DATETIME, m.invoke(null, "timestamp"));
        assertEquals(MySQLSchema.MySQLDataType.BOOLEAN, m.invoke(null, "boolean"));
        assertEquals(MySQLSchema.MySQLDataType.TEXT, m.invoke(null, "json"));
        assertEquals(MySQLSchema.MySQLDataType.CHAR, m.invoke(null, "enum"));
        assertEquals(MySQLSchema.MySQLDataType.CHAR, m.invoke(null, "set"));
    }

}
