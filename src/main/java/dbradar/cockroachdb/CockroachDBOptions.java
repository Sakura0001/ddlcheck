package dbradar.cockroachdb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import dbradar.DBMSSpecificOptions;
import dbradar.GlobalState;
import dbradar.OracleFactory;
import dbradar.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import dbradar.cockroachdb.oracle.CockroachDBEDCOracle;
import dbradar.common.oracle.TestOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "CockroachDB (default port: " + CockroachDBOptions.DEFAULT_PORT
        + " default host: " + CockroachDBOptions.DEFAULT_HOST + ")")
public class CockroachDBOptions implements DBMSSpecificOptions {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 26257;

    @Parameter(names = "--test-init", description = "Whether to initialize the database state transition graph")
    private boolean testInit = false;

    public boolean isTestInit() {
        return testInit;
    }

    @Parameter(names = "--oracle")
    public CockroachDBOracleFactory oracle = CockroachDBOracleFactory.EQUATION;

    public enum CockroachDBOracleFactory implements OracleFactory {
        EQUATION {
            @Override
            public TestOracle create(GlobalState globalState) throws SQLException {
                CockroachDBGlobalState state = (CockroachDBGlobalState) globalState;
                return new CockroachDBEDCOracle(state);
            }
        }
    }

    @Parameter(names = {
            "--test-hash-indexes"}, description = "Test the USING HASH WITH BUCKET_COUNT=n_buckets option in CREATE INDEX")
    public boolean testHashIndexes = true;

    @Parameter(names = {"--test-temp-tables"}, description = "Test TEMPORARY tables")
    public boolean testTempTables; // default: false https://github.com/cockroachdb/cockroach/issues/85388

    @Parameter(names = {"--max-num-tables"}, description = "The maximum number of tables that can be created")
    public int maxNumTables = 10;

    @Parameter(names = {"--max-num-indexes"}, description = "The maximum number of indexes that can be created")
    public int maxNumIndexes = 20;

    @Override
    public List<CockroachDBOracleFactory> getTestOracleFactory() {
        return Arrays.asList(oracle);
    }

    public boolean useEquation() {
        return oracle == CockroachDBOracleFactory.EQUATION;
    }
}
