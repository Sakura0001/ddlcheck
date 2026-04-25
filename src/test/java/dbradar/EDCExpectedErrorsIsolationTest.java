package dbradar;

import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.query.ExpectedErrors;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.oracle.PostgreSQLEDCOracle;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public final class EDCExpectedErrorsIsolationTest {

    private EDCExpectedErrorsIsolationTest() {
    }

    public static void main(String[] args) throws Exception {
        verifiesExpectedErrorsAreNotStatic();
        verifiesPostgreSQLOraclesDoNotShareExpectedErrors();
    }

    private static void verifiesExpectedErrorsAreNotStatic() {
        for (Field field : EDCBase.class.getDeclaredFields()) {
            if (field.getType().equals(ExpectedErrors.class) && Modifier.isStatic(field.getModifiers())) {
                throw new AssertionError("EDC expected errors must be instance-scoped, not static: "
                        + field.getName());
            }
        }
    }

    private static void verifiesPostgreSQLOraclesDoNotShareExpectedErrors() throws Exception {
        PostgreSQLEDCOracle firstOracle = new PostgreSQLEDCOracle(createState("edc_errors_first"));
        PostgreSQLEDCOracle secondOracle = new PostgreSQLEDCOracle(createState("edc_errors_second"));
        Field expectedErrors = EDCBase.class.getDeclaredField("expectedQueryErrors");
        expectedErrors.setAccessible(true);
        Object firstExpectedErrors = expectedErrors.get(firstOracle);
        Object secondExpectedErrors = expectedErrors.get(secondOracle);
        if (firstExpectedErrors == secondExpectedErrors) {
            throw new AssertionError("PostgreSQL EDC oracle instances must not share expected query errors");
        }
    }

    private static PostgreSQLGlobalState createState(String databaseName) {
        PostgreSQLGlobalState state = new PostgreSQLGlobalState();
        state.setDatabaseName(databaseName);
        return state;
    }
}
