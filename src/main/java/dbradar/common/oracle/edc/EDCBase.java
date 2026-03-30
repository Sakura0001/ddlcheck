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

    protected final S genState; // generated DDL sequence
    protected S synState = null; // synthesized DDL sequence
    protected static String databaseName;
    protected boolean init = true;
    protected static int maxLength = 10;
    protected static int totalSequences = 0;
    protected static int uniqueSequences = 0;

    protected static final ExpectedErrors EXPECTED_QUERY_ERRORS = new ExpectedErrors();
    protected static final ExpectedErrors UNEXPECTED_DDL_ERRORS = new ExpectedErrors();

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
                generateState(ddlSeq);
                int emptyRetries = 0;
                while (genState.getSchema().getDatabaseTables().isEmpty()) {
                    if (++emptyRetries > 200) {
                        throw new SQLException("Could not create any table after 200 retries.");
                    }
                    generateState(ddlSeq);
                }
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

            String countNum = String.format("We generate %d sequences and %d are unique", totalSequences, uniqueSequences);
            genState.getState().logStatement(countNum);
            genState.getLogger().writeCurrent(countNum);

            synthesizeState(); // check the correctness of DDL

            for (int i = 0; i < 10; i++) {
                checkDMLStmt(); // add initial data
            }

            init = false;
        }

        String queryString = null;
        for (int i = 0; i < 100; i++) {
            try {
                queryString = generateSelectStmt(genState);
                if (queryString != null) break;
            } catch (QueryGenerationException ignored) {
            }
        }

        if (queryString != null) {
            genState.getLogger().writeCurrent(queryString);
            SQLQueryAdapter query = new SQLQueryAdapter(queryString, EXPECTED_QUERY_ERRORS);
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

    protected void checkDMLStmt() throws SQLException {
        SQLQueryAdapter query = generateDMLStmt(genState);
        if (query != null) { // may face generation error
            boolean success = checkStmt(query.getQueryString());
            if (success) {
                // validate the table data
                ASTNode table = query.getQueryAST().getChildByName("_table");
                if (table != null) {
                    String tableName = table.getToken().getValue();
                    String checkTableContent = String.format("SELECT * FROM %s;", tableName);
                    checkDQLStmt(new SQLQueryAdapter(checkTableContent));
                }
            }
        }
    }

    protected boolean checkStmt(String stmt) {
        if (stmt == null) return false;
        genState.getLogger().writeCurrent(stmt);
        try {
            if (checkStmt(stmt, genState, synState)) {
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

    public void generateState(List<String> ddlSequence) throws Exception {
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
//            throw new AssertionError(query.getQueryString(), e);
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
