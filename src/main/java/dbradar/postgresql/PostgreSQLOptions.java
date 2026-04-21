package dbradar.postgresql;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import dbradar.DBMSSpecificOptions;
import dbradar.GlobalState;
import dbradar.OracleFactory;
import dbradar.common.oracle.TestOracle;
import dbradar.postgresql.oracle.PostgreSQLEDCOracle;

@Parameters(separators = "=", commandDescription = "PostgreSQL (default port: " + PostgreSQLOptions.DEFAULT_PORT
        + ", default host: " + PostgreSQLOptions.DEFAULT_HOST + ")")
public class PostgreSQLOptions implements DBMSSpecificOptions {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 5432;

    @Parameter(names = "--test-init", description = "Whether to initialize the database state transition graph")
    private boolean testInit = false;

    public boolean isTestInit() {
        return testInit;
    }

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for PostgreSQL")
    public List<PostgreSQLOptions.PostgreSQLOracleFactory> oracle = Arrays.asList(PostgreSQLOptions.PostgreSQLOracleFactory.EQUATION);

    @Parameter(names = "--test-collations", description = "Specifies whether to test different collations", arity = 1)
    public boolean testCollations = true;

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the PostgreSQL server", arity = 1)
    public String connectionURL = String.format("postgresql://%s:%d/test", PostgreSQLOptions.DEFAULT_HOST,
            PostgreSQLOptions.DEFAULT_PORT);

    @Parameter(names = "--extensions", description = "Specifies a comma-separated list of extension names to be created in each test database", arity = 1)
    public String extensions = "";

    public enum PostgreSQLOracleFactory implements OracleFactory {
        EQUATION {
            @Override
            public TestOracle create(GlobalState globalState) throws SQLException {
                PostgreSQLGlobalState state = (PostgreSQLGlobalState) globalState;
                return new PostgreSQLEDCOracle(state);
            }
        }

    }

    public boolean useEquation() {
        return oracle.get(0) == PostgreSQLOracleFactory.EQUATION;
    }

    @Override
    public List<PostgreSQLOptions.PostgreSQLOracleFactory> getTestOracleFactory() {
        return oracle;
    }
}
