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
        assertTrue(options.getStressRoundsPerDb() >= 1);
        assertTrue(options.getStressDMLPerThread() >= 0);
        assertTrue(options.getStressDDLPerThread() >= 0);
        assertTrue(options.getStressQueryPerThread() >= 0);
    }

    @Test
    public void testDeterministicSqlErrorsAreNotRetried() throws Exception {
        Method shouldRetry = MySQLStressOracle.class.getDeclaredMethod("shouldRetry", SQLException.class);
        shouldRetry.setAccessible(true);
        boolean retry = (boolean) shouldRetry.invoke(null, new SQLException("Unknown column", "42S22", 1054));
        assertFalse(retry);
    }
}
