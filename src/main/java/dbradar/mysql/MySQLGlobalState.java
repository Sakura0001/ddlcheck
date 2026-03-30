
package dbradar.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import dbradar.MainOptions;
import dbradar.mysql.schema.MySQLSchema;
import dbradar.SQLConnection;
import dbradar.SQLGlobalState;

public class MySQLGlobalState extends SQLGlobalState {

    private static final String DEFAULT_GENERATOR_CONFIG_PATH = "dbradar/mysql/mysql.zz.lua";

    private static final String DEFAULT_GRAMMAR_PATH = "dbradar/mysql/mysql.grammar.yy";


    public MySQLGlobalState() {
        setGrammarPath(DEFAULT_GRAMMAR_PATH);
        // todo add error pattern using regex
        setRegexErrorTypes(new HashMap<>());
    }

    @Override
    protected MySQLSchema readSchema() throws SQLException {
        return MySQLSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public MySQLSchema getSchema() {
        return (MySQLSchema) super.getSchema();
    }

    /**
     * Create a connection to an existing database, and do not delete it
     */
    public SQLConnection createConnection() throws SQLException {
        String username = getOptions().getUserName();
        String password = getOptions().getPassword();
        String host = getOptions().getHost();
        int port = getOptions().getPort();
        if (host == null) {
            host = MySQLOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = MySQLOptions.DEFAULT_PORT;
        }
        String databaseName = getDatabaseName();
        return createConnection(host, port, username, password, databaseName);
    }

    /**
     * Create a connection to an existing database, and do not delete it
     */
    public SQLConnection createConnection(String host, int port, String username, String password, String databaseName) throws SQLException {
        String url = String.format("jdbc:mysql://%s:%d/%s", host, port, databaseName);
        Connection conn = DriverManager.getConnection(url, username, password);
        return new SQLConnection(conn);
    }

    @Override
    public SQLConnection createDatabase() throws SQLException {
        String username = getOptions().getUserName();
        String password = getOptions().getPassword();
        String host = getOptions().getHost();
        int port = getOptions().getPort();
        if (host == null) {
            host = MySQLOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = MySQLOptions.DEFAULT_PORT;
        }
        String databaseName = getDatabaseName();

        if (!getDbmsSpecificOptions().useEquation()) { // format log in equation oracle
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

    public MySQLOptions getDbmsSpecificOptions() {
        return (MySQLOptions) super.getDbmsSpecificOptions();
    }

}
