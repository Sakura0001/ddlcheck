package dbradar.mysql;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import dbradar.DBMSSpecificOptions;
import dbradar.GlobalState;
import dbradar.OracleFactory;
import dbradar.common.oracle.TestOracle;
import dbradar.mysql.oracle.MySQLEDCOracle;

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
    public List<MySQLOracleFactory> oracles = Arrays.asList(MySQLOracleFactory.EQUATION);

    public enum MySQLOracleFactory implements OracleFactory {

        EQUATION {
            @Override
            public TestOracle create(GlobalState globalState) throws Exception {
                return new MySQLEDCOracle((MySQLGlobalState) globalState);
            }
        }
    }

    public boolean useEquation() {
        return oracles.get(0) == MySQLOracleFactory.EQUATION;
    }

    @Override
    public List<MySQLOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
