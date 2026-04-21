package dbradar.ddlCheck;

import dbradar.ComparatorHelper;
import dbradar.Main;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.mariadb.MariaDBProvider.MariaDBGlobalState;
import dbradar.mariadb.MariaDBSchema;
import dbradar.mariadb.MariaDBSchema.MariaDBTable;
import dbradar.mariadb.oracle.MariaDBEDCOracle;
import dbradar.mariadb.oracle.MariaDBEDCOracle.MariaDBQueryPlan;
import org.junit.jupiter.api.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMariaDBEDCOracle extends TestEDCOracleBase<MariaDBGlobalState, MariaDBEDCOracle> {

    String dbName = "mariadb";
    String host = "127.0.0.1";
    int port = 3306;
    String username = "root";
    String password = "root";

    @Test
    public void testMariaDBEquationOracle() {
        assertEquals(0, Main.executeMain("--num-threads", "1",
                "--num-tries", "100000", "--num-queries", "5000", "--max-generated-databases", "1",
                "--host", host, "--port", String.valueOf(port), "--username", username, "--password", password,
                dbName, "--oracle", "equation"));
    }

    String folderPath = "folderPath";

    @Test
    public void filterOutMariaDBBugs() throws IOException {
        checkExistenceOfBugs(folderPath);
    }

    String databaseName = null;
    String reportPath = "reportPath";
    List<String> witnessQueries = List.of(
            "SELECT1",
            "SELECT2"
    );

    @Test
    public void reproduceMariaDBBug() throws SQLException, IOException {
        BugReport report = new BugReport(reportPath);
        reproduceBug(report, report.knownToReproduce, witnessQueries);
    }

    @Test
    public void reduceMariaDBBugByStatement() throws Exception {
        BugReport report = new BugReport(reportPath);
        report.knownToReproduce = reduceByStatement(report, report.knownToReproduce, (report1, knownToReproduce) -> reproduceBug(report1, knownToReproduce, witnessQueries));
        report.witnessQueries = witnessQueries;
        String intoFile = report.reportFile.getParent() + File.separator + databaseName + "-statement.txt";
        flushIntoFile(report.toString(), intoFile);
    }

    @Test
    public void reduceMariaDBBugByState() throws Exception {
        MariaDBGlobalState state = getState();
        BugReport report = new BugReport(reportPath);
        Statement statement = state.getConnection().createStatement();
        for (String stmt : report.initDBStmts) {
            statement.execute(stmt);
        }
        for (String stmt : report.knownToReproduce) {
            statement.execute(stmt);
        }

        MariaDBQueryPlan plan = MariaDBEDCOracle.getMariaDBQueryPlan(witnessQueries.get(0), state);
        List<String> tableNames = plan.getTableNames();
        List<MariaDBTable> tables = state.getSchema().getDatabaseTables();
        int errorCount = 0;
        for (int i = 0; tables.size() > tableNames.size(); i++) {
            i = i % tables.size(); // valid range
            MariaDBTable table = tables.get(i);
            String tableName = table.getName();
            if (!tableNames.contains(tableName)) { // drop unused tables
                try {
                    String dropStmt;
                    if (table.isView()) {
                        dropStmt = "DROP VIEW " + tableName;
                    } else {
                        dropStmt = "DROP TABLE " + tableName;
                    }
                    statement.execute(dropStmt);
                    tables.remove(i);
                } catch (SQLException e) {
                    errorCount++;
                    if (errorCount > 100) {
                        throw new AssertionError(e.getMessage());
                    }
                }
            }
        }
        state.updateSchema();

        MariaDBEDCOracle oracle = getOracle(state);
        List<SQLQueryAdapter> createStmts = oracle.fetchCreateStmts(state);
        Map<String, List<String>> tblInsertStmts = new HashMap<>();
        for (String tableName : tableNames) {
            List<String> insertStmts = fetchInsertStmts(state, tableName);
            if (insertStmts != null) {
                tblInsertStmts.put(tableName, insertStmts);
            }
        }

        statement.close();
        state.getConnection().close();

        state.setConnection(state.createDatabase(host, port, username, password, databaseName));
        List<String> orderedStmts = oracle.replayCreateStmts(state, createStmts);
        List<String> knownToReproduce = new ArrayList<>(orderedStmts);
        statement = state.getConnection().createStatement();
        Pattern pattern = Pattern.compile("CREATE(?: TEMPORARY)? TABLE `(\\w+)`");
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
                MariaDBGlobalState state = getState();
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

    @Test
    public void testMariaDBSchema() throws Exception {
        MariaDBGlobalState state = getState();
        try (Statement createTable1 = state.getConnection().createStatement();
             Statement createTable2 = state.getConnection().createStatement();) {
            createTable1.execute("CREATE TABLE `t1` ( `c1` int PRIMARY KEY, UNIQUE KEY `idx_1` (c1))");
            createTable2.execute("CREATE TABLE `t2` ( `c1` int DEFAULT NULL, FOREIGN KEY (`c1`) REFERENCES `t1` (`c1`))");
        }

        MariaDBSchema schema1 = state.getSchema();

        try (Statement dropTable1 = state.getConnection().createStatement();
             Statement dropTable2 = state.getConnection().createStatement();) {
            dropTable1.execute("DROP TABLE t2;");
            dropTable2.execute("DROP TABLE t1;");
        }
        try (Statement createTable1 = state.getConnection().createStatement();
             Statement createTable2 = state.getConnection().createStatement();) {
            createTable1.execute("CREATE TABLE `t3` ( `c1` int UNIQUE, PRIMARY KEY(c1))");
            createTable2.execute("CREATE TABLE `t4` ( `c1` int DEFAULT NULL, FOREIGN KEY (`c1`) REFERENCES `t3` (`c1`))");
        }
        state.updateSchema();
        MariaDBSchema schema2 = state.getSchema();
        MariaDBEDCOracle oracle = getOracle(state);
        assertTrue(oracle.isEquivalentMariaDBSchema(schema1, schema2));
    }

    @Override
    protected MariaDBGlobalState getState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        if (databaseName == null) {
            databaseName = "test";
        }
        MariaDBGlobalState state = new MariaDBGlobalState();
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase(host, port, username, password, databaseName));

        return state;
    }

    @Override
    protected MariaDBGlobalState getSemiState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        assert databaseName != null;
        MariaDBGlobalState semiState = new MariaDBGlobalState();
        String semiDB = databaseName + "_semi";
        semiState.setDatabaseName(semiDB);
        semiState.setConnection(semiState.createDatabase(host, port, username, password, semiDB));

        return semiState;
    }

    @Override
    protected MariaDBEDCOracle getOracle(MariaDBGlobalState state) {
        return new MariaDBEDCOracle(state);
    }

    private List<String> fetchInsertStmts(MariaDBGlobalState state, String tableName) {
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
                                insertStmt.append("b'").append(rs.getString(i)).append("'");
                                break;
                            case Types.VARBINARY:
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
