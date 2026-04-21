package dbradar.tidb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import dbradar.common.oracle.TestOracle;
import dbradar.DBMSSpecificOptions;
import dbradar.GlobalState;
import dbradar.OracleFactory;
import dbradar.tidb.oracle.TiDBEDCOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "TiDB (default port: " + TiDBOptions.DEFAULT_PORT
        + ", default host: " + TiDBOptions.DEFAULT_HOST + ")")
public class TiDBOptions implements DBMSSpecificOptions {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 4000;

    @Parameter(names = "--test-init", description = "Whether to initialize the database state transition graph")
    private boolean testInit = false;

    public boolean isTestInit() {
        return testInit;
    }

    @Parameter(names = "--oracle")
    public List<TiDBOracleFactory> oracle = Arrays.asList(TiDBOracleFactory.EQUATION);

    public enum TiDBOracleFactory implements OracleFactory {
        EQUATION {
            @Override
            public TestOracle create(GlobalState globalState) throws SQLException {
                TiDBGlobalState state = (TiDBGlobalState) globalState;
                return new TiDBEDCOracle(state);
            }
        }
    }

    public boolean useEquation() {
        return oracle.get(0) == TiDBOracleFactory.EQUATION;
    }

    @Override
    public List<TiDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }
}
