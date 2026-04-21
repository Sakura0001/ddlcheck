package dbradar;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestGlobalStateResourceLoading {

    @Test
    public void testReadClasspathResourceLoadsPostgreSQLGrammarText() {
        String grammar = GlobalState.readClasspathResource("dbradar/postgresql/postgresql.grammar.yy");

        assertFalse(grammar.isEmpty(), "grammar resource should not be empty");
        assertTrue(grammar.contains("create_table:"),
                "postgresql grammar resource should contain the create_table production");
        assertTrue(grammar.contains("alter_table:"),
                "postgresql grammar resource should contain the alter_table production");
    }
}
