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
        assertTrue(content.contains("GENERATED ALWAYS AS"), "generated column syntax should exist");
        assertTrue(content.contains("function_index_expr"), "functional index syntax should exist");
        assertTrue(content.contains("JSON"), "JSON data type should exist");
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
    public void testExtendedDataTypeMapping() throws Exception {
        Method m = MySQLSchema.class.getDeclaredMethod("getMySQLDataType", String.class);
        m.setAccessible(true);

        assertEquals(MySQLSchema.MySQLDataType.DATE, m.invoke(null, "date"));
        assertEquals(MySQLSchema.MySQLDataType.TIME, m.invoke(null, "time"));
        assertEquals(MySQLSchema.MySQLDataType.DATETIME, m.invoke(null, "datetime"));
        assertEquals(MySQLSchema.MySQLDataType.DATETIME, m.invoke(null, "timestamp"));
        assertEquals(MySQLSchema.MySQLDataType.BOOLEAN, m.invoke(null, "boolean"));
        assertEquals(MySQLSchema.MySQLDataType.JSON, m.invoke(null, "json"));
    }
}
