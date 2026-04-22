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
import dbradar.postgresql.oracle.PostgreSQLStressOracle;

@Parameters(separators = "=", commandDescription = "PostgreSQL (default port: " + PostgreSQLOptions.DEFAULT_PORT
        + ", default host: " + PostgreSQLOptions.DEFAULT_HOST + ")")
public class PostgreSQLOptions implements DBMSSpecificOptions {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 5432;

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for PostgreSQL")
    public List<PostgreSQLOptions.PostgreSQLOracleFactory> oracle = Arrays.asList(PostgreSQLOptions.PostgreSQLOracleFactory.EQUATION);

    @Parameter(names = "--stress-topology", description = "Specifies how PostgreSQL stress mode maps threads to databases")
    public PostgreSQLOptions.PostgreSQLStressTopology stressTopology = PostgreSQLOptions.PostgreSQLStressTopology.ISOLATED;

    @Parameter(names = "--stress-threads-per-db",
            description = "Specifies how many PostgreSQL stress threads should share one database")
    public int stressThreadsPerDb = -1;

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
        },
        STRESS {
            @Override
            public TestOracle create(GlobalState globalState) throws SQLException {
                PostgreSQLGlobalState state = (PostgreSQLGlobalState) globalState;
                return new PostgreSQLStressOracle(state);
            }
        }

    }

    public enum PostgreSQLStressTopology {
        ISOLATED,
        SHARED
    }

    public boolean useEquation() {
        return oracle.get(0) == PostgreSQLOracleFactory.EQUATION;
    }

    public boolean useStress() {
        return oracle.get(0) == PostgreSQLOracleFactory.STRESS;
    }

    public boolean useSharedStressTopology() {
        return useStress() && (stressThreadsPerDb > 1 || stressTopology == PostgreSQLStressTopology.SHARED);
    }

    public PostgreSQLStressTopology getStressTopology() {
        return stressTopology;
    }

    public int getStressThreadsPerDb() {
        return stressThreadsPerDb;
    }

    public int getEffectiveStressThreadsPerDb(int concurrentThreads) {
        if (concurrentThreads <= 1) {
            return 1;
        }
        if (stressThreadsPerDb > 0) {
            return Math.min(stressThreadsPerDb, concurrentThreads);
        }
        if (stressTopology == PostgreSQLStressTopology.SHARED) {
            return concurrentThreads;
        }
        return 1;
    }

    @Override
    public List<PostgreSQLOptions.PostgreSQLOracleFactory> getTestOracleFactory() {
        return oracle;
    }
}
