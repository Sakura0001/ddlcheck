package dbradar.tidb;

import dbradar.MainOptions;
import dbradar.SQLConnection;
import dbradar.SQLGlobalState;
import dbradar.tidb.schema.TiDBSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class TiDBGlobalState extends SQLGlobalState {

    private static final String DEFAULT_GENERATOR_CONFIG_PATH = "dbradar/tidb/tidb.zz.lua";

    private static final String DEFAULT_GRAMMAR_PATH = "dbradar/tidb/tidb.grammar.yy";

    public TiDBGlobalState() {
        setGrammarPath(DEFAULT_GRAMMAR_PATH);
        // todo add error pattern using regex
        setRegexErrorTypes(new HashMap<>());
    }

    @Override
    protected TiDBSchema readSchema() throws SQLException {
        return TiDBSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public TiDBSchema getSchema() {
        return (TiDBSchema) super.getSchema();
    }

    public SQLConnection createConnection() throws SQLException {
        String username = getOptions().getUserName();
        String password = getOptions().getPassword();
        String host = getOptions().getHost();
        int port = getOptions().getPort();
        if (host == null) {
            host = TiDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = TiDBOptions.DEFAULT_PORT;
        }
        String databaseName = getDatabaseName();
        String url = String.format("jdbc:mysql://%s:%d/%s",
                host, port, databaseName);
        Connection con = DriverManager.getConnection(url, username, password);
        return new SQLConnection(con);
    }

    @Override
    public String getGeneratorConfigPath() {
        String generatorConfigPath = null;
        if (getOptions() != null) {
            generatorConfigPath = getOptions().getGeneratorConfigPath();
        }
        if (generatorConfigPath == null) { // load default generator config path
            generatorConfigPath = DEFAULT_GENERATOR_CONFIG_PATH;
        }

        return generatorConfigPath;
    }

    @Override
    public SQLConnection createDatabase() throws SQLException {
        String username = getOptions().getUserName();
        String password = getOptions().getPassword();
        String host = getOptions().getHost();
        int port = getOptions().getPort();
        if (host == null) {
            host = TiDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = TiDBOptions.DEFAULT_PORT;
        }
        String databaseName = getDatabaseName();

        if (!getDbmsSpecificOptions().useEquation()) { // format log in equation
            getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
            getState().logStatement("CREATE DATABASE " + databaseName);
            getState().logStatement("USE " + databaseName);
        }

        return createDatabase(host, port, username, password, databaseName);
    }

    public SQLConnection createDatabase(String host, int port, String username, String password, String databaseName) throws SQLException {
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                host, port);
        Connection conn = DriverManager.getConnection(url, username, password);
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + databaseName);
            statement.execute("CREATE DATABASE " + databaseName);
            statement.execute("USE " + databaseName);
        }
        return new SQLConnection(conn);
    }


    public TiDBOptions getDbmsSpecificOptions() {
        return (TiDBOptions) super.getDbmsSpecificOptions();
    }
}
