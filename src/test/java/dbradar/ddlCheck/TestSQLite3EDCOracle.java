package dbradar.ddlCheck;

import dbradar.ComparatorHelper;
import dbradar.Main;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.sqlite3.SQLite3GlobalState;
import dbradar.sqlite3.oracle.SQLite3EDCOracle;
import dbradar.sqlite3.schema.SQLite3Table;
import org.junit.jupiter.api.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSQLite3EDCOracle extends TestEDCOracleBase<SQLite3GlobalState, SQLite3EDCOracle> {

    String dbName = "sqlite3";

    @Test
    public void testSQLite3EquationOracle() {
        assertEquals(0, Main.executeMain("--num-threads", "1",
                "--num-tries", "100000", "--num-queries", "5000", "--max-generated-databases", "1",
                dbName, "--oracle", "equation"));
    }

    String folderPath = "folderPath";

    @Test
    public void filterOutSQLite3Bugs() throws IOException {
        checkExistenceOfBugs(folderPath);
    }

    String databaseName = null;
    String reportPath = "reportPath";
    List<String> witnessQueries = List.of(
            "SELECT1",
            "SELECT2"
    );

    @Test
    public void reproduceSQLite3Bug() throws SQLException, IOException {
        BugReport report = new BugReport(reportPath);
        reproduceBug(report, report.knownToReproduce, witnessQueries);
    }

    @Test
    public void reduceSQLite3BugByStatement() throws Exception {
        BugReport report = new BugReport(reportPath);
        report.knownToReproduce = reduceByStatement(report, report.knownToReproduce, (report1, knownToReproduce) -> reproduceBug(report1, knownToReproduce, witnessQueries));
        report.witnessQueries = witnessQueries;
        String intoFile = report.reportFile.getParent() + File.separator + databaseName + "-statement.txt";
        flushIntoFile(report.toString(), intoFile);
    }

    @Test
    public void reduceSQLite3BugByState() throws Exception {
        SQLite3GlobalState state = getState();
        BugReport report = new BugReport(reportPath);
        Statement statement = state.getConnection().createStatement();
        for (String stmt : report.initDBStmts) {
            statement.execute(stmt);
        }
        for (String stmt : report.knownToReproduce) {
            statement.execute(stmt);
        }
        Map<String, List<String>> tblInsertStmts = new HashMap<>();
        for (SQLite3Table table : state.getSchema().getDatabaseTables()) {
            String tableName = table.getName();
            List<String> insertStmts = fetchInsertStmts(state, tableName);
            if (insertStmts != null) {
                tblInsertStmts.put(tableName, insertStmts);
            }
        }

        SQLite3EDCOracle oracle = getOracle(state);
        List<SQLQueryAdapter> createStmts = oracle.fetchCreateStmts(state);

        statement.close();
        state.getConnection().close();

        state.setConnection(state.createDatabase(databaseName));
        List<String> orderedStmts = oracle.replayCreateStmts(state, createStmts);
        List<String> knownToReproduce = new ArrayList<>(orderedStmts);
        statement = state.getConnection().createStatement();
        Pattern pattern = Pattern.compile("CREATE(?: TEMPORARY)? TABLE (\\w+)");
        for (String stmt : orderedStmts) {
            Matcher matcher = pattern.matcher(stmt);
            if (matcher.find()) {
                String tableName = matcher.group(1);
                if (tblInsertStmts.containsKey(tableName)) {
                    for (String insertStmt : tblInsertStmts.get(tableName)) {
                        statement.execute(insertStmt);
                    }
                    knownToReproduce.addAll(tblInsertStmts.get(tableName));
                }
            }
        }
        statement.close();

        boolean canBeReproduced = false;
        List<String> result0 = EDCBase.getQueryResult(new SQLQueryAdapter(witnessQueries.get(0)), state);
        List<String> result1 = EDCBase.getQueryResult(new SQLQueryAdapter(witnessQueries.get(1)), state);
        try {
            ComparatorHelper.assumeResultSetsAreEqual(result0, result1, witnessQueries.get(0), List.of(witnessQueries.get(1)), state);
        } catch (AssertionError ignored) {
            canBeReproduced = true;
        }

        state.getConnection().close();

        assert canBeReproduced;

        knownToReproduce = reduceByStatement(null, knownToReproduce, new Reproducer() {
            @Override
            public void bugStillTrigger(BugReport report, List<String> knownToReproduce) throws SQLException {
                SQLite3GlobalState state = getState();
                try (Statement statement1 = state.getConnection().createStatement()) {
                    for (String stmt : knownToReproduce) {
                        try {
                            statement1.execute(stmt);
                        } catch (SQLException ignored) {
                        }
                    }

                    try {
                        EDCBase.checkDQLStmt(new SQLQueryAdapter(witnessQueries.get(0)), new SQLQueryAdapter(witnessQueries.get(1)), state, state);
                    } finally {
                        state.getConnection().close();
                    }
                }
            }
        });

        // reformat bug report
        report.reformat(knownToReproduce, witnessQueries);

        String intoFile = report.reportFile.getParent() + File.separator + databaseName + "-state.txt";
        flushIntoFile(report.toString(), intoFile);
    }


    @Override
    protected SQLite3GlobalState getState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        if (databaseName == null) {
            databaseName = "test";
        }
        SQLite3GlobalState state = new SQLite3GlobalState();
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase(databaseName));

        return state;
    }

    @Override
    protected SQLite3GlobalState getSemiState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        assert databaseName != null;
        SQLite3GlobalState semiState = new SQLite3GlobalState();
        String semiDB = databaseName + "_semi";
        semiState.setDatabaseName(semiDB);
        semiState.setConnection(semiState.createDatabase(semiDB));

        return semiState;
    }

    @Override
    protected SQLite3EDCOracle getOracle(SQLite3GlobalState state) {
        return new SQLite3EDCOracle(state);
    }

    private List<String> fetchInsertStmts(SQLite3GlobalState state, String tableName) {
        if (!tableName.startsWith("t")) return null; // only fetch insert statements for tables without views
        List<String> insertStmts = new ArrayList<>();
        try (Statement statement = state.getConnection().createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            while (rs.next()) {
                StringBuilder insertStmt = new StringBuilder("INSERT INTO " + tableName + "(");

                // Append column name
                for (int i = 1; i <= columnCount; i++) {
                    insertStmt.append(rsmd.getColumnName(i));
                    if (i < columnCount) {
                        insertStmt.append(", ");
                    }
                }

                insertStmt.append(") VALUES (");

                // Append column values based on their types
                for (int i = 1; i <= columnCount; i++) {
                    int columnType = rsmd.getColumnType(i);
                    if (rs.getObject(i) == null) {
                        insertStmt.append("NULL");
                    } else {
                        switch (columnType) {
                            case Types.INTEGER:
                            case Types.BIGINT:
                            case Types.SMALLINT:
                            case Types.TINYINT:
                                insertStmt.append(rs.getInt(i));
                                break;
                            case Types.FLOAT:
                            case Types.REAL:
                            case Types.DOUBLE:
                                insertStmt.append(rs.getDouble(i));
                                break;
                            case Types.DECIMAL:
                            case Types.NUMERIC:
                                insertStmt.append(rs.getBigDecimal(i));
                                break;
                            case Types.DATE:
                                insertStmt.append("'").append(rs.getDate(i)).append("'");
                                break;
                            case Types.TIME:
                                insertStmt.append("'").append(rs.getTime(i)).append("'");
                                break;
                            case Types.TIMESTAMP:
                                insertStmt.append("'").append(rs.getTimestamp(i)).append("'");
                                break;
                            case Types.BIT:
                                insertStmt.append("0x").append(DatatypeConverter.printHexBinary(rs.getBytes(i)));
                                break;
                            default:
                                insertStmt.append("'").append(rs.getString(i)).append("'");
                        }
                    }

                    if (i < columnCount) {
                        insertStmt.append(", ");
                    }
                }

                insertStmt.append(");");

                insertStmts.add(insertStmt.toString());
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return insertStmts;
    }

}
