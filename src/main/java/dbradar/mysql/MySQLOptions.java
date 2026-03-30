package dbradar.mysql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import dbradar.DBMSSpecificOptions;
import dbradar.GlobalState;
import dbradar.OracleFactory;
import dbradar.common.oracle.TestOracle;
import dbradar.mysql.oracle.MySQLEDCOracle;
import dbradar.mysql.oracle.MySQLStressOracle;

@Parameters(separators = "=", commandDescription = "MySQL (default port: " + MySQLOptions.DEFAULT_PORT
        + ", default host: " + MySQLOptions.DEFAULT_HOST + ")")
public class MySQLOptions implements DBMSSpecificOptions {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--test-init", description = "Whether to initialize the database state transition graph")
    private boolean testInit = false;

    public boolean isTestInit() {
        return testInit;
    }

    @Parameter(names = "--oracle")
    public List<MySQLOracleFactory> oracles = new ArrayList<>(Arrays.asList(MySQLOracleFactory.STRESS));

    @Parameter(names = "--stress-enable", description = "Enable MySQL stress mode (single database benchmarking)", arity = 1)
    private boolean stressEnable = true;

    @Parameter(names = "--stress-threads-per-db", description = "How many worker threads test each database concurrently. " +
            "1=single thread per db (default). 4 with --num-threads=30 means 30 databases each tested by 4 threads = 120 effective workers.")
    private int stressThreadsPerDb = 1;

    @Parameter(names = "--stress-ddl-per-thread", description = "How many DDL statements each worker executes in one round")
    private int stressDDLPerThread = 4;

    @Parameter(names = "--stress-dml-per-thread", description = "How many DML statements each worker executes in one round")
    private int stressDMLPerThread = 20;

    @Parameter(names = "--stress-query-per-thread", description = "How many query statements each worker executes in one round")
    private int stressQueryPerThread = 20;

    @Parameter(names = "--stress-schema-refresh-interval", description = "Refresh schema metadata every N successful statements")
    private int stressSchemaRefreshInterval = 20;

    @Parameter(names = "--stress-log-each-sql", description = "Log each SQL execution with timestamp/thread/db/result", arity = 1)
    private boolean stressLogEachSQL = true;

    @Parameter(names = "--stress-warn-error-code", description = "If this SQL error code is hit, print an explicit warning")
    private int stressWarnErrorCode = 168;

    public enum MySQLOracleFactory implements OracleFactory {
        STRESS {
            @Override
            public TestOracle create(GlobalState globalState) throws Exception {
                return new MySQLStressOracle((MySQLGlobalState) globalState);
            }
        },

        EQUATION {
            @Override
            public TestOracle create(GlobalState globalState) throws Exception {
                return new MySQLEDCOracle((MySQLGlobalState) globalState);
            }
        }
    }

    private MySQLOracleFactory getPrimaryOracle() {
        if (oracles == null || oracles.isEmpty()) {
            return MySQLOracleFactory.STRESS;
        }
        return oracles.get(oracles.size() - 1);
    }

    public boolean useEquation() {
        return getPrimaryOracle() == MySQLOracleFactory.EQUATION;
    }

    public boolean useStress() {
        return stressEnable && getPrimaryOracle() == MySQLOracleFactory.STRESS;
    }

    public int getStressThreadsPerDb() {
        return Math.max(1, stressThreadsPerDb);
    }

    public int getStressDDLPerThread() {
        return Math.max(0, stressDDLPerThread);
    }

    public int getStressDMLPerThread() {
        return Math.max(0, stressDMLPerThread);
    }

    public int getStressQueryPerThread() {
        return Math.max(0, stressQueryPerThread);
    }

    public int getStressSchemaRefreshInterval() {
        return Math.max(1, stressSchemaRefreshInterval);
    }

    public boolean isStressLogEachSQL() {
        return stressLogEachSQL;
    }

    public int getStressWarnErrorCode() {
        return stressWarnErrorCode;
    }

    @Override
    public List<MySQLOracleFactory> getTestOracleFactory() {
        if (oracles == null || oracles.isEmpty()) {
            return List.of(MySQLOracleFactory.STRESS);
        }
        MySQLOracleFactory primary = oracles.get(oracles.size() - 1);
        return List.of(primary);
    }

}
