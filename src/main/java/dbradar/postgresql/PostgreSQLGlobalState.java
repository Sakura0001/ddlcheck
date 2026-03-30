package dbradar.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dbradar.DatabaseConnection;
import dbradar.MainOptions;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.SQLGlobalState;

public class PostgreSQLGlobalState extends SQLGlobalState {

    private static final String DEFAULT_GENERATOR_CONFIG_PATH = "dbradar/postgresql/postgresql.zz.lua";
    private static final String DEFAULT_GRAMMAR_PATH = "dbradar/postgresql/postgresql.grammar.yy";

    public static final char IMMUTABLE = 'i';
    public static final char STABLE = 's';
    public static final char VOLATILE = 'v';

    private List<String> operators = Collections.emptyList();
    private List<String> collates = Collections.emptyList();
    private List<String> opClasses = Collections.emptyList();
    private List<String> tableAccessMethods = Collections.emptyList();
    // store and allow filtering by function volatility classifications
    private final Map<String, Character> functionsAndTypes = new HashMap<>();
    private List<Character> allowedFunctionTypes = Arrays.asList(IMMUTABLE, STABLE, VOLATILE);

    public PostgreSQLGlobalState() {
        setGrammarPath(DEFAULT_GRAMMAR_PATH);
        // todo add error pattern using regex
        setRegexErrorTypes(new HashMap<>());
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
            host = PostgreSQLOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = PostgreSQLOptions.DEFAULT_PORT;
        }
        String databaseName = getDatabaseName();
        return createConnection(host, port, username, password, databaseName);
    }

    /**
     * Create a connection to an existing database, and do not delete it
     */
    public SQLConnection createConnection(String host, int port, String username, String password, String databaseName) throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName);
        Connection conn = DriverManager.getConnection(url, username, password);
        return new SQLConnection(conn);
    }

    @Override
    public void setConnection(DatabaseConnection con) {
        super.setConnection(con);
        try {
            this.opClasses = getOpclasses(getConnection());
            this.operators = getOperators(getConnection());
            this.collates = getCollnames(getConnection());
            this.tableAccessMethods = getTableAccessMethods(getConnection());
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    private List<String> getOpclasses(SQLConnection con) throws SQLException {
        List<String> opClasses = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select opcname FROM pg_opclass;")) {
                while (rs.next()) {
                    opClasses.add(rs.getString(1));
                }
            }
        }
        return opClasses;
    }

    private List<String> getOperators(SQLConnection con) throws SQLException {
        List<String> operators = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT oprname FROM pg_operator;")) {
                while (rs.next()) {
                    operators.add(rs.getString(1));
                }
            }
        }
        return operators;
    }

    private List<String> getCollnames(SQLConnection con) throws SQLException {
        List<String> collNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("SELECT collname FROM pg_collation WHERE collname LIKE '%utf8' or collname = 'C';")) {
                while (rs.next()) {
                    collNames.add(rs.getString(1));
                }
            }
        }
        return collNames;
    }

    private List<String> getTableAccessMethods(SQLConnection con) throws SQLException {
        List<String> tableAccessMethods = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            /*
             * pg_am includes both index and table access methods, so we need to filter with amtype = 't'
             */
            try (ResultSet rs = s.executeQuery("SELECT amname FROM pg_am WHERE amtype = 't';")) {
                while (rs.next()) {
                    tableAccessMethods.add(rs.getString(1));
                }
            }
        }
        return tableAccessMethods;
    }

    public List<String> getOperators() {
        return operators;
    }

    public String getRandomOperator() {
        return Randomly.fromList(operators);
    }

    public List<String> getCollates() {
        return collates;
    }

    public String getRandomCollate() {
        return Randomly.fromList(collates);
    }

    public List<String> getOpClasses() {
        return opClasses;
    }

    public String getRandomOpclass() {
        return Randomly.fromList(opClasses);
    }

    public List<String> getTableAccessMethods() {
        return tableAccessMethods;
    }

    public String getRandomTableAccessMethod() {
        return Randomly.fromList(tableAccessMethods);
    }

    public void addFunctionAndType(String functionName, Character functionType) {
        this.functionsAndTypes.put(functionName, functionType);
    }

    public Map<String, Character> getFunctionsAndTypes() {
        return this.functionsAndTypes;
    }

    public void setAllowedFunctionTypes(List<Character> types) {
        this.allowedFunctionTypes = types;
    }

    public void setDefaultAllowedFunctionTypes() {
        this.allowedFunctionTypes = Arrays.asList(IMMUTABLE, STABLE, VOLATILE);
    }

    public List<Character> getAllowedFunctionTypes() {
        return this.allowedFunctionTypes;
    }


    public PostgreSQLOptions getDbmsSpecificOptions() {
        return (PostgreSQLOptions) super.getDbmsSpecificOptions();
    }

    public PostgreSQLSchema getSchema() {
        return (PostgreSQLSchema) super.getSchema();
    }


    @Override
    protected PostgreSQLSchema readSchema() throws Exception {
        return PostgreSQLSchema.fromConnection(getConnection(), getDatabaseName());
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
        String databaseName = getDatabaseName();

        String createDatabaseCommand = getCreateDatabaseCommand(databaseName);

        return createDatabase(host, port, username, password, databaseName, createDatabaseCommand);
    }


    public SQLConnection createDatabase(String host, int port, String username, String password, String databaseName) throws SQLException {
        String createDatabaseCommand = "CREATE DATABASE " + databaseName;
        return createDatabase(host, port, username, password, databaseName, createDatabaseCommand);
    }

    public SQLConnection createDatabase(String host, int port, String username, String password, String databaseName, String createDatabaseCommand) throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/postgres", host, port);
        try (Connection conn = DriverManager.getConnection(url, username, password); Statement statement = conn.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + databaseName);
            statement.execute(createDatabaseCommand);
        }

        url = String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName);
        Connection conn = DriverManager.getConnection(url, username, password);

        return new SQLConnection(conn);
    }

    private String getCreateDatabaseCommand(String databaseName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATABASE " + databaseName + " ");
        if (((PostgreSQLOptions) getDbmsSpecificOptions()).testCollations) {
            if (Randomly.getBoolean()) {
                if (Randomly.getBoolean()) {
                    sb.append("WITH ENCODING '");
                    sb.append(Randomly.fromOptions("utf8"));
                    sb.append("' ");
                }
                for (String lc : Arrays.asList("LC_COLLATE", "LC_CTYPE")) {
                    if (!getCollates().isEmpty() && Randomly.getBoolean()) {
                        sb.append(String.format(" %s = '%s'", lc, Randomly.fromList(getCollates())));
                    }
                }
                sb.append(" TEMPLATE template0");
            }
        } else {
            sb.append("WITH ENCODING 'UTF8' TEMPLATE template0");
        }
        return sb.toString();
    }

}
