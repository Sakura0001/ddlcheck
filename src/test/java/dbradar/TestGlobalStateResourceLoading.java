package dbradar;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestGlobalStateResourceLoading {

    @Test
    public void testReadClasspathResourceLoadsMysqlGrammarText() {
        String grammar = GlobalState.readClasspathResource("dbradar/mysql/mysql.grammar.yy");

        assertFalse(grammar.isEmpty(), "grammar resource should not be empty");
        assertTrue(grammar.contains("create_table:"),
                "mysql grammar resource should contain the create_table production");
        assertTrue(grammar.contains("engine_option:"),
                "mysql grammar resource should contain engine_option production");
    }
}
