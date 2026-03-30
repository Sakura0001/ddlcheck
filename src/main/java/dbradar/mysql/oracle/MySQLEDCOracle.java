package dbradar.mysql.oracle;

import dbradar.Randomly;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.mysql.MySQLGlobalState;
import dbradar.mysql.MySQLProvider.MySQLQueryProvider;
import dbradar.mysql.MySQLProvider.MySQLDDLStmt;
import dbradar.mysql.MySQLProvider.MySQLDMLStmt;
import dbradar.mysql.schema.MySQLSchema.MySQLTable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MySQLEDCOracle extends EDCBase<MySQLGlobalState> {

    public MySQLEDCOracle(MySQLGlobalState state) {
        super(state);
        synState = new MySQLGlobalState();
        EXPECTED_QUERY_ERRORS.add("Subquery returns more than 1 row");
        EXPECTED_QUERY_ERRORS.add("Invalid data type for JSON data in argument 2 to function member of");
        UNEXPECTED_DDL_ERRORS.add("innodb");
    }

    @Override
    public void generateState(List<String> ddlSeq) throws Exception {
        ddlSeq.clear();
        getDDLSequence(ddlSeq);
        totalSequences++;
        uniqueSequences++;
    }

    public void getDDLSequence(List<String> ddlSeq) throws SQLException {
        int bootstrapTableCount = Randomly.getNotCachedInteger(3, 6);
        for (int t = 0; t < bootstrapTableCount; t++) {
            boolean created = false;
            for (int retry = 0; retry < 200 && !created; retry++) {
                try {
                    String createTable = MySQLDDLStmt.CREATE_TABLE.getQueryProvider().getQuery(genState).getQueryString();
                    try (Statement stmt = genState.getConnection().createStatement()) {
                        stmt.execute(createTable);
                        genState.updateSchema();
                        ddlSeq.add(createTable);
                        created = true;
                    }
                } catch (SQLException e) {
                    if (UNEXPECTED_DDL_ERRORS.errorIsExpected(e.getMessage())) {
                        throw new RuntimeException(e.getMessage());
                    }
                } catch (Exception ignored) {
                }
            }
        }
        if (ddlSeq.isEmpty()) {
            throw new SQLException("Failed to bootstrap initial table for DDL sequence.");
        }

        int schemaRefreshCounter = 0;
        int currentLength = Randomly.getNotCachedInteger(5, maxLength + 10);
        for (int i = 0; i < currentLength; i++) {
            MySQLDDLStmt ddlStmt;
            if (genState.getSchema().getDatabaseTables().isEmpty()) {
                ddlStmt = MySQLDDLStmt.CREATE_TABLE;
            } else {
                ddlStmt = Randomly.fromOptions(MySQLDDLStmt.values());
            }
            SQLQueryAdapter ddlQuery = null;
            for (int j = 0; j < 100; j++) {
                try {
                    ddlQuery = ddlStmt.getQueryProvider().getQuery(genState);
                    break;
                } catch (QueryGenerationException ignored) {
                }
            }
            if (ddlQuery == null) continue;

            try (Statement stmt = genState.getConnection().createStatement()) {
                stmt.execute(ddlQuery.getQueryString());
                ddlSeq.add(ddlQuery.getQueryString());
                schemaRefreshCounter++;
                if (ddlStmt == MySQLDDLStmt.CREATE_TABLE || ddlStmt == MySQLDDLStmt.DROP_TABLE
                        || ddlStmt == MySQLDDLStmt.RENAME_TABLE || ddlStmt == MySQLDDLStmt.ALTER_TABLE_RENAME_TABLE
                        || schemaRefreshCounter >= 5) {
                    genState.updateSchema();
                    schemaRefreshCounter = 0;
                }
            } catch (SQLException e) {
                if (UNEXPECTED_DDL_ERRORS.errorIsExpected(e.getMessage())) {
                    throw e;
                }
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public void cleanDatabase() {
        try (Statement disableFKChecks = genState.getConnection().createStatement();
             Statement showTables = genState.getConnection().createStatement();
             Statement dropTable = genState.getConnection().createStatement();
             Statement enableFKChecks = genState.getConnection().createStatement()) {

            // Disable foreign key checks
            disableFKChecks.execute("SET foreign_key_checks = 0");

            // Get the list of all tables
            ResultSet resultSet = showTables.executeQuery("SHOW FULL TABLES");
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                String tableType = resultSet.getString(2);
                // Drop each table
                if ("BASE TABLE".equals(tableType)) {
                    // Drop tables
                    dropTable.execute("DROP TABLE IF EXISTS " + tableName);
                } else if ("VIEW".equals(tableType)) {
                    // Drop views
                    dropTable.execute("DROP VIEW IF EXISTS " + tableName);
                }
            }

            // Enable foreign key checks
            enableFKChecks.execute("SET foreign_key_checks = 1");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public List<SQLQueryAdapter> fetchCreateStmts(MySQLGlobalState state) {
        List<SQLQueryAdapter> createStmts = new ArrayList<>(); // a set of create tables

        for (MySQLTable table : state.getSchema().getDatabaseTables()) {
            String tableName = table.getName();
            try (Statement statement = state.getConnection().createStatement()) {
                ResultSet resultSet = statement.executeQuery("SHOW CREATE TABLE " + state.getDatabaseName() + "." + tableName);
                String createTable = null;
                if (resultSet.next()) {
                    if (table.isView()) {
                        createTable = resultSet.getString("Create View");
                        if (createTable == null) {
                            throw new AssertionError("fetchCreateStmts: " + tableName);
                        }
                        createTable = createTable.replaceAll(state.getDatabaseName(), state.getDatabaseName() + "_semi");
                    } else {
                        createTable = resultSet.getString("Create Table");
                        if (createTable == null) {
                            throw new AssertionError("fetchCreateStmts: " + tableName);
                        }
                        if (createTable.contains("FOREIGN KEY")) { // do not create implicit indexes
                            String[] createElements = createTable.split("\\r?\\n");
                            StringBuilder newCreateTable = new StringBuilder();
                            for (String element : createElements) {
                                if (element.contains("KEY") && !element.contains("UNIQUE KEY") && !element.contains("FOREIGN KEY") && !element.contains("PRIMARY KEY")) {
                                    if (!element.matches("\\s+KEY `c\\d` \\(`c\\d`\\),")) {
                                        newCreateTable.append(element);
                                    }
                                } else if (element.contains("FOREIGN KEY")) { // trim the automatic generated foreign key name
                                    int foreignIndex = element.indexOf("FOREIGN");
                                    element = element.substring(foreignIndex);
                                    newCreateTable.append(element);
                                } else {
                                    newCreateTable.append(element);
                                }
                            }
                            createTable = newCreateTable.toString();
                        } else {
                            createTable = createTable.replaceAll("\\r?\\n", ""); // format show create table
                        }
                    }
                }
                createStmts.add(new SQLQueryAdapter(createTable)); // log create table statements
            } catch (SQLException ignored) {
            }
        }

        Collections.reverse(createStmts); // revise the order, since we store them in a revise order
        return createStmts;
    }

    @Override
    public String generateSelectStmt(MySQLGlobalState state) {
        return MySQLQueryProvider.SELECT.getQuery(state).getQueryString();
    }

    @Override
    public SQLQueryAdapter generateDMLStmt(MySQLGlobalState state) {
        for (int i = 0; i < 100; i++) {
            try {
                return MySQLDMLStmt.getRandomDML(state);
            } catch (QueryGenerationException ignored) {
            }
        }

        return null;
    }

    @Override
    public String checkQueryPlan(String query, MySQLGlobalState state) {
        MySQLQueryPlan plan = getMySQLQueryPlan(query, state);
        return plan.toString();
    }

    @Override
    public String getExecutionResult(String query, MySQLGlobalState state) {
        String errorMessage = null;
        boolean gotException = false;
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(query);
        } catch (SQLException ignored) {
            gotException = true;
        }

        if (gotException) {
            try (Statement statement = state.getConnection().createStatement()) {
                ResultSet resultSet = statement.executeQuery("SHOW WARNINGS;");
                while (resultSet.next()) {
                    errorMessage = resultSet.getString("Level");
                    if (errorMessage.equals("Error")) { // one Error should also happen
                        break;
                    }
                }
            } catch (SQLException ignored) {
            }
        }

        return errorMessage;
    }

    public static MySQLQueryPlan getMySQLQueryPlan(String query, MySQLGlobalState state) {
        MySQLQueryPlan plan = new MySQLQueryPlan();
        String checkQueryPlan = String.format("EXPLAIN %s", query);
        try (Statement statement = state.getConnection().createStatement()) {
            ResultSet planRes = statement.executeQuery(checkQueryPlan);
            while (planRes.next()) {
                plan.id.add(planRes.getString("id"));
                plan.selectType.add(planRes.getString("select_type"));
                plan.table.add(planRes.getString("table"));
                plan.partitions.add(planRes.getString("partitions"));
                plan.type.add(planRes.getString("type"));
                plan.possibleKeys.add(planRes.getString("possible_keys"));
                plan.key.add(planRes.getString("key"));
                plan.keyLen.add(planRes.getString("key_len"));
                plan.ref.add(planRes.getString("ref"));
                plan.rows.add(planRes.getString("rows"));
                plan.filtered.add(planRes.getString("filtered"));
                plan.extra.add(planRes.getString("Extra"));
            }
            planRes.close();
        } catch (SQLException e) {
            plan.exception = e.getMessage();
        }

        return plan;
    }

    public static class MySQLQueryPlan {
        List<String> id = new ArrayList<>();
        List<String> selectType = new ArrayList<>();
        List<String> table = new ArrayList<>();
        List<String> partitions = new ArrayList<>();
        List<String> type = new ArrayList<>();
        List<String> possibleKeys = new ArrayList<>();
        List<String> key = new ArrayList<>();
        List<String> keyLen = new ArrayList<>();
        List<String> ref = new ArrayList<>();
        List<String> rows = new ArrayList<>();
        List<String> filtered = new ArrayList<>();
        List<String> extra = new ArrayList<>();
        String exception = null;

        public List<String> getTableNames() {
            List<String> tblNames = new ArrayList<>();
            for (String tblName : table) {
                if (tblName != null && !tblName.startsWith("<") && !tblNames.contains(tblName)) { // we do not retrieve derived table, e.g, <derived2>
                    tblNames.add(tblName);
                }
            }

            return tblNames;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (exception != null) {
                sb.append("Exception occurred: ").append(exception);
            } else {
                sb.append("Query Plan:\n");
                sb.append(String.format("%-5s %-15s %-15s %-15s %-10s %-15s %-15s %-10s %-10s %-10s %-10s %-20s\n",
                        "id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra"));
                for (int i = 0; i < id.size(); i++) {
                    sb.append(String.format("%-5s %-15s %-15s %-15s %-10s %-15s %-15s %-10s %-10s %-10s %-10s %-20s\n",
                            id.get(i), selectType.get(i), table.get(i), partitions.get(i), type.get(i),
                            possibleKeys.get(i), key.get(i), keyLen.get(i), ref.get(i), rows.get(i),
                            filtered.get(i), extra.get(i)));
                }
            }
            return sb.toString();
        }
    }


}
