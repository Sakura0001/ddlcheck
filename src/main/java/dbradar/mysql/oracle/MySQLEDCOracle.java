package dbradar.mysql.oracle;

import dbradar.Randomly;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.oracle.edc.SchemaGraph;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.mysql.MySQLGlobalState;
import dbradar.mysql.MySQLProvider.MySQLQueryProvider;
import dbradar.mysql.MySQLProvider.MySQLDDLStmt;
import dbradar.mysql.MySQLProvider.MySQLDMLStmt;
import dbradar.mysql.schema.MySQLSchema;
import dbradar.mysql.schema.MySQLSchema.MySQLTable;
import dbradar.mysql.schema.MySQLSchema.MySQLColumn;
import dbradar.mysql.schema.MySQLSchema.MySQLIndex;
import dbradar.mysql.schema.MySQLSchema.MySQLForeignKey;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class MySQLEDCOracle extends EDCBase<MySQLGlobalState> {

    private static final List<SchemaGraph<MySQLTable>> schemaGraphList = new ArrayList<>();

    public MySQLEDCOracle(MySQLGlobalState state) {
        super(state);
        synState = new MySQLGlobalState();
        EXPECTED_QUERY_ERRORS.add("Subquery returns more than 1 row");
        EXPECTED_QUERY_ERRORS.add("Invalid data type for JSON data in argument 2 to function member of");
        UNEXPECTED_DDL_ERRORS.add("innodb");
    }

    @Override
    public void generateState(List<String> ddlSeq) throws Exception {
        while (true) {
            ddlSeq.clear();
            SchemaGraph<MySQLTable> schemaGraph = new SchemaGraph<>();
            getDDLSequence(schemaGraph, ddlSeq);
            boolean isUnique = true;
            for (SchemaGraph<MySQLTable> graph : schemaGraphList) {
                if (isEquivalentGraph(schemaGraph, graph)) {
                    isUnique = false;
                    break;
                }
            }
            totalSequences++;
            if (isUnique) {
                uniqueSequences++;
                schemaGraphList.add(schemaGraph);
                break;
            } else {
                cleanDatabase();
                genState.updateSchema();
            }
        }
    }

    public boolean isEquivalentGraph(SchemaGraph<MySQLTable> graph1, SchemaGraph<MySQLTable> graph2) {
        if (graph1.getVertices().size() != graph2.getVertices().size()) return false;
        if (graph1.getAdjacencyList().size() != graph2.getAdjacencyList().size()) return false;

        List<SchemaGraph.Vertex<MySQLTable>> tables1 = new ArrayList<>(graph1.getLeafVertices());
        List<SchemaGraph.Vertex<MySQLTable>> tables2 = new ArrayList<>(graph2.getLeafVertices());
        for (int i = 0; i < tables1.size(); i++) {
            for (int j = 0; j < tables2.size(); j++) {
                SchemaGraph.Vertex<MySQLTable> table1 = tables1.get(i);
                SchemaGraph.Vertex<MySQLTable> table2 = tables2.get(j);
                if (isEquivalentVertex(table1, table2, graph1, graph2)) {
                    tables1.remove(i);
                    i--;
                    tables2.remove(j);
                    break;
                }
            }
        }
        if (!tables1.isEmpty()) {
            return false;
        }

        return true;
    }

    public boolean isEquivalentVertex(SchemaGraph.Vertex<MySQLTable> table1, SchemaGraph.Vertex<MySQLTable> table2, SchemaGraph<MySQLTable> graph1, SchemaGraph<MySQLTable> graph2) {
        if (table1 == null || table2 == null) return false;
        if (!isEquivalentMySQLTable(table1.getTable(), table2.getTable())) return false;
        List<SchemaGraph.Edge<MySQLTable>> edges1 = new ArrayList<>(graph1.getAdjacentEdges(table1));
        List<SchemaGraph.Edge<MySQLTable>> edges2 = new ArrayList<>(graph2.getAdjacentEdges(table2));
        if (edges1.size() != edges2.size()) return false;
        for (int i = 0; i < edges1.size(); i++) {
            SchemaGraph.Edge<MySQLTable> edge1 = edges1.get(i);
            SchemaGraph.Edge<MySQLTable> edge2 = edges2.get(i);
            if (!isEquivalentEdge(edge1, edge2, graph1, graph2)) {
                return false;
            }
        }

        return true;
    }

    public boolean isEquivalentEdge(SchemaGraph.Edge<MySQLTable> edge1, SchemaGraph.Edge<MySQLTable> edge2, SchemaGraph<MySQLTable> graph1, SchemaGraph<MySQLTable> graph2) {
        if (!edge1.getEdgeType().equals(edge2.getEdgeType())) return false;
        SchemaGraph.Vertex<MySQLTable> table1 = edge1.getSource();
        SchemaGraph.Vertex<MySQLTable> table2 = edge2.getSource();
        if (!isEquivalentVertex(table1, table2, graph1, graph2)) return false;

        return true;
    }


    public void getDDLSequence(SchemaGraph<MySQLTable> schemaGraph, List<String> ddlSeq) throws SQLException {
        while (ddlSeq.isEmpty()) {
            String createTable = MySQLDDLStmt.CREATE_TABLE.getQueryProvider().getQuery(genState).getQueryString();
            try (Statement stmt = genState.getConnection().createStatement()) {
                stmt.execute(createTable);
                genState.updateSchema();
                ddlSeq.add(createTable);
                MySQLTable curTable = genState.getSchema().getDatabaseTables().get(0);
                schemaGraph.addVertex(curTable);
            } catch (SQLException e) {
                if (UNEXPECTED_DDL_ERRORS.errorIsExpected(e.getMessage())) {
                    throw new RuntimeException(e.getMessage());
                }
            } catch (Exception ignored) {
            }
        }

        int currentLength = Randomly.getNotCachedInteger(2, maxLength);
        for (int i = 0; i < currentLength; i++) {
            MySQLDDLStmt ddlStmt = Randomly.fromOptions(MySQLDDLStmt.values());
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
                genState.updateSchema();
                ddlSeq.add(ddlQuery.getQueryString());

                String curTableName;
                MySQLTable curTable = null;
                SchemaGraph.Vertex<MySQLTable> srcTable = null;
                switch (ddlStmt) {
                    case CREATE_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (MySQLTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                curTable = table;
                                break;
                            }
                        }
                        srcTable = schemaGraph.addVertex(curTable);

                        // check foreign key constraints
                        List<ASTNode> tableConstraints = ddlQuery.getQueryAST().getChildrenByName("table_constraint");
                        if (!tableConstraints.isEmpty()) {
                            for (ASTNode tableConstraint : tableConstraints) {
                                ASTNode foreignKey = tableConstraint.getChildByName("foreign_key_table_constraint");
                                if (foreignKey != null) {
                                    String referTableName = foreignKey.getChildByName("foreign_key_clause").getChildByName("_reference_table").getChildren().get(0).getToken().toString();
                                    SchemaGraph.Vertex<MySQLTable> referTable = null;
                                    for (SchemaGraph.Vertex<MySQLTable> v : schemaGraph.getVertices().values()) { // existing table
                                        if (v.isLeaf() && v.getTable().getName().equals(referTableName)) {
                                            referTable = v;
                                            break;
                                        }
                                    }
                                    if (referTable != null) {
                                        schemaGraph.addEdge(srcTable, referTable, "FK");
                                    }
                                }
                            }
                        }
                        break;
                    case CREATE_INDEX:
                    case ALTER_TABLE_ADD_COLUMN:
                    case ALTER_TABLE_DROP_COLUMN:
                    case ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT:
                    case ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT:
                    case ALTER_TABLE_ALTER_COLUMN_SET_VISIBLE:
                    case ALTER_TABLE_ALTER_COLUMN_SET_INVISIBLE:
                    case ALTER_TABLE_CHANGE_COLUMN:
                    case ALTER_TABLE_MODIFY_COLUMN:
                    case ALTER_TABLE_RENAME_COLUMN:
                    case ALTER_TABLE_ADD_INDEX:
                    case ALTER_TABLE_DROP_INDEX:
                    case ALTER_TABLE_RENAME_INDEX:
                    case ALTER_TABLE_ADD_PRIMARY_KEY:
                    case ALTER_TABLE_DROP_PRIMARY_KEY:
                    case ALTER_TABLE_ADD_UNIQUE_KEY:
                    case ALTER_TABLE_OPTION:
                    case TRUNCATE_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (MySQLTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<MySQLTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (MySQLTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case DROP_INDEX:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_drop_index").getChildren().get(2).getToken().toString();
                        for (MySQLTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<MySQLTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (MySQLTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case ALTER_TABLE_RENAME_TABLE:
                    case RENAME_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (MySQLTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<MySQLTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        String newTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (MySQLTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(newTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case DROP_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (MySQLTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<MySQLTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                v.setLeaf(false);
                                break;
                            }
                        }
                        break;
                    case ALTER_TABLE_ADD_FOREIGN_KEY:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (MySQLTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<MySQLTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (MySQLTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());

                        String referTableName = ddlQuery.getQueryAST().getChildByName("foreign_key_clause").getChildByName("_reference_table").getChildren().get(0).getToken().toString();
                        SchemaGraph.Vertex<MySQLTable> referTable = null;
                        for (SchemaGraph.Vertex<MySQLTable> v : schemaGraph.getVertices().values()) { // existing table
                            if (v.isLeaf() && v.getTable().getName().equals(referTableName)) {
                                referTable = v;
                                break;
                            }
                        }
                        if (referTable != null) {
                            schemaGraph.addEdge(srcTable, referTable, "FK");
                        }
                        break;
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

    public boolean isEquivalentMySQLSchema(MySQLSchema schema1, MySQLSchema schema2) {
        if (schema1.getDatabaseTables().size() != schema2.getDatabaseTables().size()) {
            return false;
        }
        if (schema1.getForeignKeys().size() != schema2.getForeignKeys().size()) {
            return false;
        }
        List<MySQLTable> tables1 = new ArrayList<>(schema1.getDatabaseTables());
        List<MySQLTable> tables2 = new ArrayList<>(schema2.getDatabaseTables());
        for (int i = 0; i < tables1.size(); i++) {
            for (int j = 0; j < tables2.size(); j++) {
                if (isEquivalentMySQLTable(tables1.get(i), tables2.get(j))) {
                    tables1.remove(i);
                    i--;
                    tables2.remove(j);
                    break;
                }
            }
        }
        if (!tables1.isEmpty()) {
            return false;
        }
        List<MySQLForeignKey> fks1 = new ArrayList<>(schema1.getForeignKeys());
        List<MySQLForeignKey> fks2 = new ArrayList<>(schema2.getForeignKeys());
        for (int i = 0; i < fks1.size(); i++) {
            for (int j = 0; j < fks2.size(); j++) {
                if (isEquivalentMySQLForeignKey(fks1.get(i), fks2.get(j))) {
                    fks1.remove(i);
                    i--;
                    fks2.remove(j);
                    break;
                }
            }
        }
        if (!fks1.isEmpty()) {
            return false;
        }

        return true;
    }

    private boolean isEquivalentMySQLForeignKey(MySQLForeignKey fk1, MySQLForeignKey fk2) {
        if (!isEquivalentMySQLTable(fk1.getTable(), fk2.getTable())) {
            return false;
        }
        if (fk1.getColumns().size() != fk2.getColumns().size()) {
            return false;
        }
        if (!isEquivalentMySQLTable(fk1.getReferencedTable(), fk2.getReferencedTable())) {
            return false;
        }
        if (fk1.getReferencedColumns().size() != fk2.getReferencedColumns().size()) {
            return false;
        }
        List<MySQLColumn> columns1 = new ArrayList<>(fk1.getColumns());
        List<MySQLColumn> columns2 = new ArrayList<>(fk2.getColumns());
        for (int i = 0; i < columns1.size(); i++) {
            if (!isEquivalentMySQLColumn(columns1.get(i), columns2.get(i))) {
                return false;
            }
        }

        columns1 = new ArrayList<>(fk1.getReferencedColumns());
        columns2 = new ArrayList<>(fk2.getReferencedColumns());
        for (int i = 0; i < columns1.size(); i++) {
            if (!isEquivalentMySQLColumn(columns1.get(i), columns2.get(i))) {
                return false;
            }
        }

        return true;
    }

    private boolean isEquivalentMySQLTable(MySQLTable table1, MySQLTable table2) {
        if (table1.getColumns().size() != table2.getColumns().size()) {
            return false;
        }
        if (table1.getIndexes().size() != table2.getIndexes().size()) {
            return false;
        }
        List<MySQLColumn> columns1 = new ArrayList<>(table1.getColumns());
        List<MySQLColumn> columns2 = new ArrayList<>(table2.getColumns());
        for (int i = 0; i < columns1.size(); i++) {
            for (int j = 0; j < columns2.size(); j++) {
                if (isEquivalentMySQLColumn(columns1.get(i), columns2.get(j))) {
                    columns1.remove(i);
                    i--;
                    columns2.remove(j);
                    break;
                }
            }
        }
        if (!columns1.isEmpty()) {
            return false;
        }
        List<MySQLIndex> indexes1 = new ArrayList<>(table1.getIndexes());
        List<MySQLIndex> indexes2 = new ArrayList<>(table2.getIndexes());
        for (int i = 0; i < indexes1.size(); i++) {
            for (int j = 0; j < indexes2.size(); j++) {
                if (isEquivalentMySQLIndex(indexes1.get(i), indexes2.get(j))) {
                    indexes1.remove(i);
                    i--;
                    indexes2.remove(j);
                    break;
                }
            }
        }
        if (!indexes1.isEmpty()) {
            return false;
        }

        return true;
    }

    private boolean isEquivalentMySQLIndex(MySQLIndex index1, MySQLIndex index2) {
        if (index1.getNonUnique() != index2.getNonUnique()) {
            return false;
        }
        if (index1.isNullable() != index2.isNullable()) {
            return false;
        }
        if (!Objects.equals(index1.getIndexType(), index2.getIndexType())) {
            return false;
        }
        if (index1.getColumns().size() != index2.getColumns().size()) {
            return false;
        }
        for (int i = 0; i < index1.getColumns().size(); i++) {
            MySQLColumn column1 = index1.getColumns().get(i);
            MySQLColumn column2 = index2.getColumns().get(i);
            if (!isEquivalentMySQLColumn(column1, column2)) {
                return false;
            }
        }

        return true;
    }

    private boolean isEquivalentMySQLColumn(MySQLColumn column1, MySQLColumn column2) {
        return Objects.equals(column1.getColumnDefault(), column2.getColumnDefault()) &&
                column1.isNullable() == column2.isNullable() &&
                Objects.equals(column1.getDataType(), column2.getDataType()) &&
                Objects.equals(column1.getCharacterMaximumLength(), column2.getCharacterMaximumLength()) &&
                Objects.equals(column1.getNumericPrecision(), column2.getNumericPrecision()) &&
                Objects.equals(column1.getNumericScale(), column2.getNumericScale()) &&
                Objects.equals(column1.getColumnKey(), column2.getColumnKey());
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
