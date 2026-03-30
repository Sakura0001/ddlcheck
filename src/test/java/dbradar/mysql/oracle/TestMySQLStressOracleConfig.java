package dbradar.mysql.oracle;

import dbradar.mysql.MySQLOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMySQLStressOracleConfig {

    @Test
    public void testStressDefaultsAreSafe() {
        MySQLOptions options = new MySQLOptions();
        assertTrue(options.useStress());
        assertTrue(options.getStressThreadsPerDb() >= 1);
        assertTrue(options.getStressSchemaRefreshInterval() >= 1);
        assertTrue(options.getStressDMLPerThread() >= 0);
        assertTrue(options.getStressDDLPerThread() >= 0);
        assertTrue(options.getStressQueryPerThread() >= 0);
    }

    @Test
    public void testSchemaRefreshController() throws Exception {
        MySQLStressOracle.SchemaRefreshController controller = new MySQLStressOracle.SchemaRefreshController(3);
        FakeState state = new FakeState();
        controller.onStatementSuccess(state, false);
        controller.onStatementSuccess(state, false);
        assertEquals(0, state.refreshCount);
        controller.onStatementSuccess(state, false);
        assertEquals(1, state.refreshCount);
        controller.onStatementSuccess(state, false);
        controller.onStatementSuccess(state, false);
        controller.onStatementSuccess(state, false);
        assertEquals(2, state.refreshCount);
        controller.forceRefresh(state);
        assertEquals(3, state.refreshCount);
    }

    @Test
    public void testDeterministicSqlErrorsAreNotRetried() throws Exception {
        Method shouldRetry = MySQLStressOracle.class.getDeclaredMethod("shouldRetry", SQLException.class);
        shouldRetry.setAccessible(true);
        boolean retry = (boolean) shouldRetry.invoke(null, new SQLException("Unknown column", "42S22", 1054));
        assertFalse(retry);
    }

    private static class FakeState extends dbradar.mysql.MySQLGlobalState {
        int refreshCount;

        @Override
        public void updateSchema() {
            refreshCount++;
        }
    }
}
