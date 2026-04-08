package dbradar.mysql.oracle;

import dbradar.Main;
import dbradar.MainOptions;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.StateLogger;
import dbradar.mysql.MySQLGlobalState;
import dbradar.mysql.MySQLOptions;
import dbradar.mysql.MySQLProvider;
import dbradar.mysql.schema.MySQLSchema;
import dbradar.mysql.schema.MySQLSchema.MySQLColumn;
import dbradar.mysql.schema.MySQLSchema.MySQLTable;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMySQLStressExecutionBehavior {

    @Test
    public void testGlobalSuccessRateLoggingIsThrottledAcrossCalls() throws Exception {
        Main.nrSuccessfulActions.set(3);
        Main.nrUnsuccessfulActions.set(1);
        resetSuccessRateLogClock();

        Method maybeLogGlobalSuccessRate = MySQLStressOracle.class
                .getDeclaredMethod("maybeLogGlobalSuccessRate", MySQLGlobalState.class, long.class);
        maybeLogGlobalSuccessRate.setAccessible(true);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        try (PrintStream capture = new PrintStream(out, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            maybeLogGlobalSuccessRate.invoke(null, null, 5_000L);
            maybeLogGlobalSuccessRate.invoke(null, null, 7_000L);
            maybeLogGlobalSuccessRate.invoke(null, null, 10_000L);
        } finally {
            System.setOut(originalOut);
        }

        String output = out.toString(StandardCharsets.UTF_8);
        assertEquals(2, countOccurrences(output, "global SQL success rate"),
                "global success rate should only be printed once per logging interval");
        assertTrue(output.contains("75.00%"),
                "global success rate output should include the current success percentage, but was: " + output);
        assertTrue(output.contains("success=3") && output.contains("fail=1"),
                "global success rate output should include aggregated counts, but was: " + output);
    }

    @Test
    public void testDmlBatchRefreshesSchemaOnlyOnceWhenStatementsSucceed() throws Exception {
        new Randomly(0);
        TrackingMySQLState state = createTrackingState(createWritableSchemaWithRowCount(0), sqlConnectionThatExecutes(
                true, true, true));

        invokeExecuteBatch(new MySQLStressOracle(state), state, 3, "DML");

        assertEquals(1, state.getUpdateSchemaCount(),
                "DML batch should refresh schema once at phase start instead of before every statement");
    }

    @Test
    public void testDmlBatchRefreshesSchemaAgainAfterInsertFailure() throws Exception {
        new Randomly(0);
        TrackingMySQLState state = createTrackingState(createWritableSchemaWithRowCount(0), sqlConnectionThatExecutes(
                new SQLException("Unknown column", "42S22", 1054), true, true));

        invokeExecuteBatch(new MySQLStressOracle(state), state, 3, "DML");

        assertEquals(2, state.getUpdateSchemaCount(),
                "DML batch should refresh schema once at phase start and once more after an insert-like failure");
    }

    private TrackingMySQLState createTrackingState(MySQLSchema schema, SQLConnection connection) {
        TrackingMySQLState state = new TrackingMySQLState();
        state.installSchema(schema);
        state.setConnection(connection);
        state.setDatabaseName("mysql_stress_test");
        state.setMainOptions(new MainOptions());
        state.setDbmsSpecificOptions(new MySQLOptions());
        state.setStateLogger(new StateLogger("mysql_stress_test", new MySQLProvider(), new MainOptions()));
        return state;
    }

    private MySQLSchema createWritableSchemaWithRowCount(long rowCount) {
        MySQLColumn column = new MySQLColumn("c1", null, false, "int", 0L, 10, 0, "");
        MySQLTable table = new MySQLTable("t0", List.of(column), List.of(), MySQLTable.MySQLEngine.INNO_DB, false) {
            @Override
            public long getNrRows(dbradar.GlobalState globalState) {
                return rowCount;
            }
        };
        column.setTable(table);
        return new MySQLSchema(List.of(table), List.of());
    }

    private SQLConnection sqlConnectionThatExecutes(Object... executeResults) {
        AtomicInteger executeIndex = new AtomicInteger();
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class[]{Statement.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("execute".equals(name)) {
                        int index = executeIndex.getAndIncrement();
                        Object result = executeResults[Math.min(index, executeResults.length - 1)];
                        if (result instanceof SQLException) {
                            throw (SQLException) result;
                        }
                        return result;
                    }
                    if ("close".equals(name)) {
                        return null;
                    }
                    if ("isClosed".equals(name)) {
                        return false;
                    }
                    return defaultValue(method.getReturnType());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class[]{Connection.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("createStatement".equals(name)) {
                        return statement;
                    }
                    if ("close".equals(name)) {
                        return null;
                    }
                    if ("isClosed".equals(name)) {
                        return false;
                    }
                    return defaultValue(method.getReturnType());
                });
        return new SQLConnection(connection);
    }

    private Object defaultValue(Class<?> returnType) {
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == byte.class) {
            return (byte) 0;
        }
        if (returnType == short.class) {
            return (short) 0;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0f;
        }
        if (returnType == double.class) {
            return 0d;
        }
        if (returnType == char.class) {
            return '\0';
        }
        return null;
    }

    private void invokeExecuteBatch(MySQLStressOracle oracle, MySQLGlobalState state, int count, String kindName)
            throws Exception {
        Method executeBatch = MySQLStressOracle.class.getDeclaredMethod(
                "executeBatch",
                MySQLGlobalState.class,
                int.class,
                Class.forName("dbradar.mysql.oracle.MySQLStressOracle$StatementKind"));
        executeBatch.setAccessible(true);
        @SuppressWarnings("unchecked")
        Class<? extends Enum> kindClass = (Class<? extends Enum>) Class
                .forName("dbradar.mysql.oracle.MySQLStressOracle$StatementKind");
        Object kind = Enum.valueOf(kindClass, kindName);
        executeBatch.invoke(oracle, state, count, kind);
    }

    private void resetSuccessRateLogClock() throws Exception {
        Field lastLoggedAt = MySQLStressOracle.class.getDeclaredField("LAST_SUCCESS_RATE_LOG_AT_MILLIS");
        lastLoggedAt.setAccessible(true);
        ((AtomicLong) lastLoggedAt.get(null)).set(0L);
    }

    private int countOccurrences(String text, String pattern) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(pattern, index)) >= 0) {
            count++;
            index += pattern.length();
        }
        return count;
    }

    private static final class TrackingMySQLState extends MySQLGlobalState {
        private MySQLSchema schema;
        private int updateSchemaCount;

        void installSchema(MySQLSchema schema) {
            this.schema = schema;
            setSchema(schema);
        }

        int getUpdateSchemaCount() {
            return updateSchemaCount;
        }

        @Override
        public void updateSchema() {
            updateSchemaCount++;
            setSchema(schema);
        }
    }
}
