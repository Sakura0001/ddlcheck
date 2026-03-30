package dbradar.common.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mysql.cj.jdbc.exceptions.CommunicationsException;

import dbradar.GlobalState;
import dbradar.Main;
import dbradar.SQLConnection;
import dbradar.common.query.generator.ASTNode;

public class SQLQueryAdapter extends QueryAdapter {

    private ExpectedErrors expectedErrors;
    private boolean couldAffectSchema;

    public SQLQueryAdapter(ASTNode rootNode) {
        this(rootNode, new ExpectedErrors());
    }

    public SQLQueryAdapter(ASTNode rootNode, ExpectedErrors expectedErrors) {
        this(rootNode, expectedErrors, guessAffectSchemaFromQuery(rootNode));
    }

    public SQLQueryAdapter(ASTNode rootNode, ExpectedErrors expectedErrors, boolean couldAffectSchema) {
        this(rootNode, expectedErrors, couldAffectSchema, true);
    }

    public SQLQueryAdapter(ASTNode rootNode, ExpectedErrors expectedErrors, boolean couldAffectSchema,
            boolean canonicalizeString) {
        super(rootNode, canonicalizeString);
        this.expectedErrors = expectedErrors;
        this.couldAffectSchema = couldAffectSchema;
        checkQueryString();
    }

    public SQLQueryAdapter(String query) {
        this(query, new ExpectedErrors());
    }

    public SQLQueryAdapter(String query, boolean couldAffectSchema) {
        this(query, new ExpectedErrors(), couldAffectSchema);
    }

    public SQLQueryAdapter(String query, ExpectedErrors expectedErrors) {
        this(query, expectedErrors, guessAffectSchemaFromQuery(query));
    }

    private static boolean guessAffectSchemaFromQuery(String query) {
        return query.contains("CREATE TABLE") && !query.startsWith("EXPLAIN");
    }

    private static boolean guessAffectSchemaFromQuery(ASTNode astNode) {
        String query = astNode.toQueryString();
        return query.contains("CREATE TABLE") && !query.startsWith("EXPLAIN");
    }

    public SQLQueryAdapter(String query, ExpectedErrors expectedErrors, boolean couldAffectSchema) {
        this(query, expectedErrors, couldAffectSchema, true);
    }

    public SQLQueryAdapter(String query, ExpectedErrors expectedErrors, boolean couldAffectSchema,
            boolean canonicalizeString) {
        super(query, canonicalizeString);
        this.expectedErrors = expectedErrors;
        this.couldAffectSchema = couldAffectSchema;
        checkQueryString();
    }

    private void checkQueryString() {
        if (!couldAffectSchema && guessAffectSchemaFromQuery(query)) {
            throw new AssertionError("CREATE TABLE statements should set couldAffectSchema to true");
        }
    }

    public void setExpectedErrors(ExpectedErrors expectedErrors) {
        this.expectedErrors = expectedErrors;
    }

    public void setCanAffectSchema(boolean couldAffectSchema) {
        this.couldAffectSchema = couldAffectSchema;
    }

    @Override
    public String getUnterminatedQueryString() {
        String result;
        if (query.endsWith(";")) {
            result = query.substring(0, query.length() - 1);
        } else {
            result = query;
        }
        assert !result.endsWith(";");
        return result;
    }

    @Override
    public <G extends GlobalState> boolean execute(G globalState, String... fills) throws SQLException {
        Statement s;
        if (fills.length > 0) {
            s = ((SQLConnection) globalState.getConnection()).prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = ((SQLConnection) globalState.getConnection()).createStatement();
        }
        try {
            if (fills.length > 0) {
                ((PreparedStatement) s).execute();
            } else {
                s.execute(query);
            }
            Main.nrSuccessfulActions.addAndGet(1);
            return true;
        } catch (Exception e) {
            Main.nrUnsuccessfulActions.addAndGet(1);
            checkException(e, globalState);
            return false;
        } finally {
            s.close();
        }
    }

    public <G extends GlobalState> boolean execute(SQLConnection connection, boolean reportException, String... fills)
            throws SQLException {
        Statement s;
        if (fills.length > 0) {
            s = connection.prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = connection.createStatement();
        }
        try {
            if (fills.length > 0) {
                ((PreparedStatement) s).execute();
            } else {
                s.execute(query);
            }
            Main.nrSuccessfulActions.addAndGet(1);
            return true;
        } catch (Exception e) {
            Main.nrUnsuccessfulActions.addAndGet(1);
            checkException(e);
            if (reportException) {
                throw e;
            } else {
                return false;
            }
        } finally {
            s.close();
        }
    }

    /**
     * TODO Review go-randgen, sqlsmith, squirrel, and find error checking
     * conditions.
     */
    public <G extends GlobalState> void checkException(Exception e, G globalState) throws AssertionError {
        String errorMessage = e.getMessage();
        for (String regex : globalState.getRegexErrorTypes().keySet()) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(errorMessage);
            if (matcher.find()) {
                String errorType = globalState.getRegexErrorTypes().get(regex);
//                throw new AssertionError("\t**** " + errorType + ": " + e.getMessage());
            }
        }
        if (e instanceof CommunicationsException) {
            throw new AssertionError("\t****  Communication Error: " + e.getMessage());
        }
    }

    public void checkException(Exception e) throws AssertionError {
        Throwable ex = e;

        while (ex != null) {
            if (expectedErrors.errorIsExpected(ex.getMessage())) {
                return;
            } else {
                ex = ex.getCause();
            }
        }

    }

    @Override
    public <G extends GlobalState> DBRadarResultSet executeAndGet(G globalState, String... fills) throws SQLException {
        Statement s;
        if (fills.length > 0) {
            s = ((SQLConnection) globalState.getConnection()).prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = ((SQLConnection) globalState.getConnection()).createStatement();
        }
        ResultSet result;
        try {
            if (fills.length > 0) {
                result = ((PreparedStatement) s).executeQuery();
            } else {
                result = s.executeQuery(query);
            }
            Main.nrSuccessfulActions.addAndGet(1);
            if (result == null) {
                return null;
            }
            return new DBRadarResultSet(result);
        } catch (Exception e) {
            s.close();
            Main.nrUnsuccessfulActions.addAndGet(1);
            checkException(e, globalState);
        }
        return null;
    }

    public <G extends GlobalState> DBRadarResultSet executeAndGet(SQLConnection connection, boolean reportException,
            String... fills) throws SQLException {
        Statement s;
        if (fills.length > 0) {
            s = connection.prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = connection.createStatement();
        }
        ResultSet result;
        try {
            if (fills.length > 0) {
                result = ((PreparedStatement) s).executeQuery();
            } else {
                result = s.executeQuery(query);
            }
            Main.nrSuccessfulActions.addAndGet(1);
            if (result == null) {
                return null;
            }
            return new DBRadarResultSet(result);
        } catch (Exception e) {
            s.close();
            Main.nrUnsuccessfulActions.addAndGet(1);
            checkException(e);
            if (reportException) {
                throw e;
            } else {
                return null;
            }
        }
    }

    @Override
    public boolean couldAffectSchema() {
        return couldAffectSchema;
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        return expectedErrors;
    }

    @Override
    public String getLogString() {
        return getQueryString();
    }
}
