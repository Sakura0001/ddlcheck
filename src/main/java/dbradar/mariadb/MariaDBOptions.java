package dbradar.mariadb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import dbradar.DBMSSpecificOptions;
import dbradar.GlobalState;
import dbradar.OracleFactory;
import dbradar.common.oracle.TestOracle;
import dbradar.mariadb.MariaDBProvider.MariaDBGlobalState;
import dbradar.mariadb.oracle.MariaDBEDCOracle;

import java.sql.SQLException;
import java.util.List;

@Parameters(separators = "=", commandDescription = "MariaDB (default port: " + MariaDBOptions.DEFAULT_PORT
        + ", default host: " + MariaDBOptions.DEFAULT_HOST + ")")
public class MariaDBOptions implements DBMSSpecificOptions {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--test-init", description = "Whether to initialize the database state transition graph")
    private boolean testInit = false;

    public boolean isTestInit() {
        return testInit;
    }

    @Parameter(names = "--oracle")
    public List<MariaDBOracleFactory> oracles = List.of(MariaDBOracleFactory.EQUATION);

    public enum MariaDBOracleFactory implements OracleFactory {

        EQUATION {
            @Override
            public TestOracle create(GlobalState globalState) throws SQLException {
                MariaDBProvider.MariaDBGlobalState state = (MariaDBProvider.MariaDBGlobalState) globalState;
                return new MariaDBEDCOracle(state);
            }

        }
    }

    public boolean useEquation() {
        return oracles.get(0) == MariaDBOracleFactory.EQUATION;
    }

    @Override
    public List<MariaDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
