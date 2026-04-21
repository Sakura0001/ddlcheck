package dbradar.sqlite3;

import dbradar.SQLConnection;
import dbradar.SQLGlobalState;
import dbradar.sqlite3.schema.SQLite3Schema;

import java.io.File;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SQLite3GlobalState extends SQLGlobalState {

    private static final String DEFAULT_GENERATOR_CONFIG_PATH = "dbradar/sqlite3/sqlite3.zz.lua";
    private static final String DEFAULT_GRAMMAR_PATH = "dbradar/sqlite3/sqlite3.grammar.yy";

    public SQLite3GlobalState() {
        setGrammarPath(DEFAULT_GRAMMAR_PATH);
        Map<String, String> regexErrorTypes = new HashMap<>();
        regexErrorTypes.put("near \".*\": syntax error", "Syntax Error");
        regexErrorTypes.put("callback requested query abort", "User Abort");
        setRegexErrorTypes(regexErrorTypes);
    }

    @Override
    protected SQLite3Schema readSchema() throws Exception {
        return SQLite3Schema.fromConnection(this);
    }

    public SQLite3Schema getSchema() {
        return (SQLite3Schema) super.getSchema();
    }

    /**
     * Build a new connection and delete the exist database
     */
    public SQLConnection createConnection() throws SQLException {
        return createConnection(getDatabaseName());
    }

    public SQLConnection createConnection(String databaseName) throws SQLException {
        File dir = new File("." + File.separator + "databases");
        if (!dir.exists()) {
            dir.mkdir();
        }
        File dataBase = new File(dir, databaseName + ".db");
        String url = "jdbc:sqlite:" + dataBase.getAbsolutePath();
        return new SQLConnection(DriverManager.getConnection(url));
    }

    @Override
    public SQLConnection createDatabase() throws SQLException {
        return createDatabase(getDatabaseName());
    }

    public SQLConnection createDatabase(String databaseName) throws SQLException {
        File dir = new File("." + File.separator + "databases");
        if (!dir.exists()) {
            dir.mkdir();
        }
        File dataBase = new File(dir, databaseName + ".db");
        if (dataBase.exists()) {
            dataBase.delete();
        }
        String url = "jdbc:sqlite:" + dataBase.getAbsolutePath();
        return new SQLConnection(DriverManager.getConnection(url));
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

    public SQLite3Options getDbmsSpecificOptions() {
        return (SQLite3Options) super.getDbmsSpecificOptions();
    }

    @Override
    public SQLConnection createNewConnection(String databaseName) {
        File dir = new File("." + File.separator + "databases");
        if (!dir.exists()) {
            dir.mkdir();
        }
        File dataBase = new File(dir, databaseName + ".db");
        if (dataBase.exists()) {
            dataBase.delete();
        }
        try {
            String url = "jdbc:sqlite:" + dataBase.getAbsolutePath();
            return new SQLConnection(DriverManager.getConnection(url));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
