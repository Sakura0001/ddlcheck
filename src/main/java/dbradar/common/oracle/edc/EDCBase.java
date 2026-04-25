package dbradar.common.oracle.edc;

import dbradar.ComparatorHelper;
import dbradar.IgnoreMeException;
import dbradar.Main;
import dbradar.Randomly;
import dbradar.SQLGlobalState;
import dbradar.common.oracle.TestOracle;
import dbradar.common.query.ExpectedErrors;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.QueryGenerationException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class EDCBase<S extends SQLGlobalState> implements TestOracle {

    private static final int MIN_BOOTSTRAP_ATTEMPTS = 100;
    private static final String[] DML_TARGET_TABLE_NODE_NAMES = {
            "_insert_target_table", "_updatable_table", "_table"
    };

    protected final S genState; // generated DDL sequence
    protected S synState = null; // synthesized DDL sequence
    protected static String databaseName;
    protected boolean init = true;
    protected static int maxLength = 10;

    protected final ExpectedErrors expectedQueryErrors = new ExpectedErrors();
    protected final ExpectedErrors unexpectedDdlErrors = new ExpectedErrors();

    public EDCBase(S state) {
        this.genState = state;
        databaseName = state.getDatabaseName();
    }

    @Override
    public void check() throws Exception {
        if (init) {
            List<String> ddlSeq = new ArrayList<>();
            boolean foundBug = false;
            String errorMessage = null;
            try {
                generateState(ddlSeq, genState.getOptions().getDdlCount());
            } catch (SQLException e) {
                foundBug = true;
                errorMessage = e.getMessage();
            }
            for (String ddlStatement : ddlSeq) {
                genState.getState().logStatement(ddlStatement); // log statement
                genState.getLogger().writeCurrent(ddlStatement);
            }

            if (foundBug) {
                throw new AssertionError(errorMessage);
            }

            synthesizeState(); // check the correctness of DDL
            populateBootstrapDml();

            init = false;
        }

        String queryString = null;
        for (int i = 0; i < 100; i++) {
            try {
                queryString = generateSelectStmt(genState);
                if (queryString != null) break;
            } catch (QueryGenerationException | IgnoreMeException ignored) {
            }
        }

        if (queryString != null) {
            genState.getLogger().writeCurrent(queryString);
            SQLQueryAdapter query = new SQLQueryAdapter(queryString, expectedQueryErrors);
            try {
                // check the correctness of DQL
                checkDQLStmt(query);
            } catch (IgnoreMeException ignoreMeException) {
            } catch (AssertionError error) {
                genState.getState().logStatement(queryString);
                genState.getState().logStatement("-- Plan in state:");
                genState.getState().logStatement(checkQueryPlan(queryString, genState));
                genState.getState().logStatement("-- Plan in semiState:");
                genState.getState().logStatement(checkQueryPlan(queryString, synState));
                throw error;
            }
        }

        if (Randomly.getBooleanWithRatherLowProbability()) {
            checkDMLStmt();
        }
    }

    protected void checkDQLStmt(SQLQueryAdapter query) throws SQLException {
        checkDQLStmt(query, query, genState, synState);
    }

    public static void checkDQLStmt(SQLQueryAdapter queryOnState, SQLQueryAdapter queryOnSemiState, SQLGlobalState state, SQLGlobalState semiState) throws SQLException {
        List<String> manualResult = getQueryResult(queryOnState, state);
        List<String> semiResult = getQueryResult(queryOnSemiState, semiState);
        ComparatorHelper.assumeResultSetsAreEqual(manualResult, semiResult, queryOnState.getQueryString(), List.of(queryOnSemiState.getQueryString()), state);
    }

    private void populateBootstrapDml() throws SQLException {
        int targetSuccessfulDml = genState.getOptions().getDmlCount();
        int maxAttempts = Math.max(MIN_BOOTSTRAP_ATTEMPTS, targetSuccessfulDml * genState.getOptions().getNrStatementRetryCount());
        int successfulStatements = populateRequiredBootstrapDml();

        for (int attempts = 0; attempts < maxAttempts && successfulStatements < targetSuccessfulDml; attempts++) {
            try {
                if (checkDMLStmt(false)) {
                    successfulStatements++;
                }
            } catch (IgnoreMeException ignored) {
                // Keep retrying within the bootstrap budget until the requested number of DML statements succeeds.
            }
        }

        if (successfulStatements < targetSuccessfulDml) {
            throw new AssertionError(String.format(
                    "Expected %d successful bootstrap DML statements but observed %d",
                    targetSuccessfulDml, successfulStatements));
        }
    }

    protected int populateRequiredBootstrapDml() throws SQLException {
        return 0;
    }

    protected boolean checkDMLStmt() throws SQLException {
        return checkDMLStmt(true);
    }

    protected boolean checkDMLStmt(boolean logExecutionAttempt) throws SQLException {
        SQLQueryAdapter query = generateDMLStmt(genState);
        if (query != null) { // may face generation error
            boolean success = checkStmt(query.getQueryString(), logExecutionAttempt);
            if (success) {
                // validate the table data
                String tableName = getDmlTargetTableName(query);
                if (tableName != null) {
                    String checkTableContent = String.format("SELECT * FROM %s;", tableName);
                    checkDQLStmt(new SQLQueryAdapter(checkTableContent));
                }
            }
            return success;
        }
        return false;
    }

    private String getDmlTargetTableName(SQLQueryAdapter query) {
        ASTNode queryAST = query.getQueryAST();
        if (queryAST == null) {
            return null;
        }
        for (String nodeName : DML_TARGET_TABLE_NODE_NAMES) {
            ASTNode table = queryAST.getChildByName(nodeName);
            if (table == null) {
                continue;
            }
            String tableName = table.toQueryString();
            if (tableName != null && !tableName.isBlank()) {
                return tableName;
            }
        }
        return null;
    }

    protected boolean checkStmt(String stmt) {
        return checkStmt(stmt, true);
    }

    protected boolean checkStmt(String stmt, boolean logExecutionAttempt) {
        if (stmt == null) return false;
        if (logExecutionAttempt) {
            genState.getLogger().writeCurrent(stmt);
        }
        try {
            if (checkStmt(stmt, genState, synState)) {
                if (!logExecutionAttempt) {
                    genState.getLogger().writeCurrent(stmt);
                }
                genState.getState().logStatement(stmt);
                Main.nrSuccessfulActions.addAndGet(1);
                return true;
            } else {
                Main.nrUnsuccessfulActions.addAndGet(1);
            }
        } catch (AssertionError error) {
            genState.getState().logStatement(stmt);
            throw error;
        }
        return false;
    }

    public boolean checkStmt(String stmt, S state1, S state2) {
        String manualResult = getExecutionResult(stmt, state1);
        String semiResult = getExecutionResult(stmt, state2);
        if (!Objects.equals(manualResult, semiResult)) {
            throw new AssertionError(String.format("%s\n" +
                    "State1: %s\n" +
                    "State2: %s\n", stmt, manualResult, semiResult));
        }
        // when no exception happens
        if (manualResult != null && state1.getState() != null) {
            state1.getState().logStatement(stmt);
        }
        return manualResult == null;
    }

    public void closeConnection() {
        try {
            genState.getConnection().close();
            if (synState != null && synState.getConnection() != null) {
                synState.getConnection().close();
            }
        } catch (SQLException ignored) {
        }
    }

    public void generateState(List<String> ddlSequence, int targetDdlCount) throws Exception {
        throw new RuntimeException("Not implemented yet");
    }

    private void synthesizeState() throws Exception {
        assert synState != null; // should be first initialized in the constructor

        genState.getLogger().writeCurrent("==== Start SemiState ====");
        genState.getState().logStatement("==== Start SemiState ====");

        // build connection, create semiDB and connect semiDB
        synState.setMainOptions(genState.getOptions());
        synState.setDbmsSpecificOptions(genState.getDbmsSpecificOptions());
        synState.setState(genState.getState());
        synState.setStateLogger(genState.getLogger());
        String semiDB = genState.getDatabaseName() + "_semi";
        synState.setDatabaseName(semiDB);
        synState.setConnection(synState.createDatabase());

        // replay create table statement
        genState.updateSchema();
        List<SQLQueryAdapter> createStmts = fetchCreateStmts(genState); // a set of create tables
        List<String> orderedStmts = replayCreateStmts(synState, createStmts);
        for (String stmt : orderedStmts) { // log statement
            genState.getState().logStatement(stmt);
            genState.getLogger().writeCurrent(stmt);
        }

        genState.getLogger().writeCurrent("==== End SemiState ====");
        genState.getState().logStatement("==== End SemiState ====");
    }

    public List<String> replayCreateStmts(S state, List<SQLQueryAdapter> createStmts) throws SQLException {
        int errorCount = 0;
        List<String> orderedStmts = new ArrayList<>();
        try (Statement statement = state.getConnection().createStatement()) {
            for (int i = 0; !createStmts.isEmpty(); i++) {
                i = i % createStmts.size(); // valid range
                String query = createStmts.get(i).getQueryString();
                try {
                    statement.execute(query);
                    orderedStmts.add(query);
                    createStmts.remove(i);
                } catch (SQLException e) {
                    errorCount++;
                    if (errorCount > 100) {
                        break; // tolerate invalid view
                    }
                }
            }
        }

        return orderedStmts;
    }

    public abstract void cleanDatabase();

    public abstract List<SQLQueryAdapter> fetchCreateStmts(S state) throws SQLException;

    public abstract String generateSelectStmt(S state);

    public abstract SQLQueryAdapter generateDMLStmt(S state);

    public abstract String checkQueryPlan(String query, S state);

    public abstract String getExecutionResult(String query, S state);

    public static List<String> getQueryResult(SQLQueryAdapter query, SQLGlobalState state) throws SQLException {
        List<String> resultSet = new ArrayList<>();
        ResultSet result = null;
        try (Statement statement = state.getConnection().createStatement()) {
            result = statement.executeQuery(query.getQueryString());
            Main.nrSuccessfulActions.addAndGet(1);
            if (result == null) {
                throw new IgnoreMeException(); // avoid too many false positives
            }
            ResultSetMetaData metaData = result.getMetaData();
            int columns = metaData.getColumnCount();
            while (result.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columns; i++) {
                    String resultTemp = result.getString(i);
                    if (resultTemp != null) {
                        resultTemp = resultTemp.replaceAll("[\\.]0+$", ""); // Remove the trailing zeros as many DBMS treat it as non-bugs
                    }
                    row.append(resultTemp).append(",");
                }
                resultSet.add(row.toString());
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw e;
            }
            if (e.getMessage() == null) {
                throw new AssertionError(query.getQueryString(), e);
            }
            if (query.getExpectedErrors().errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
            Main.nrUnsuccessfulActions.addAndGet(1);
            throw new AssertionError(query.getQueryString(), e);
        } finally {
            if (result != null && !result.isClosed()) {
                result.close();
            }
        }

        return resultSet;
    }

    @Override
    public String getOracleName() {
        return "Equation";
    }
}
