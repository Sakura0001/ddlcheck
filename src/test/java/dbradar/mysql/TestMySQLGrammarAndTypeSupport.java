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
