package dbradar.mariadb.oracle;

import dbradar.Randomly;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.oracle.edc.SchemaGraph;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.mariadb.MariaDBProvider.MariaDBQueryProvider;
import dbradar.mariadb.MariaDBProvider.MariaDBDDLStmt;
import dbradar.mariadb.MariaDBProvider.MariaDBDMLStmt;
import dbradar.mariadb.MariaDBProvider.MariaDBGlobalState;
import dbradar.mariadb.MariaDBSchema;
import dbradar.mariadb.MariaDBSchema.MariaDBTable;
import dbradar.mariadb.MariaDBSchema.MariaDBColumn;
import dbradar.mariadb.MariaDBSchema.MariaDBIndex;
import dbradar.mariadb.MariaDBSchema.MariaDBForeignKey;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MariaDBEDCOracle extends EDCBase<MariaDBGlobalState> {

    private static final List<SchemaGraph<MariaDBTable>> schemaGraphList = new ArrayList<>();

    public MariaDBEDCOracle(MariaDBGlobalState state) {
        super(state);
        synState = new MariaDBGlobalState();
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute("SET GLOBAL optimizer_switch='condition_pushdown_from_having=off'"); // avoid too many crashes
            statement.execute("SET GLOBAL 'IGNORE_SPACE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"); // only full group by
        } catch (SQLException ignored) {
        }
        EXPECTED_QUERY_ERRORS.add("Subquery returns more than 1 row");
        UNEXPECTED_DDL_ERRORS.add("innodb");
    }

    @Override
    public void generateState(List<String> ddlSeq) throws Exception {
        while (true) {
            ddlSeq.clear();
            SchemaGraph<MariaDBTable> schemaGraph = new SchemaGraph<>();
            getDDLSequence(schemaGraph, ddlSeq);
            boolean isUnique = true;
            for (SchemaGraph<MariaDBTable> graph : schemaGraphList) {
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

    public boolean isEquivalentGraph(SchemaGraph<MariaDBTable> graph1, SchemaGraph<MariaDBTable> graph2) {
        if (graph1.getVertices().size() != graph2.getVertices().size()) return false;
        if (graph1.getAdjacencyList().size() != graph2.getAdjacencyList().size()) return false;

        List<SchemaGraph.Vertex<MariaDBTable>> tables1 = new ArrayList<>(graph1.getLeafVertices());
        List<SchemaGraph.Vertex<MariaDBTable>> tables2 = new ArrayList<>(graph2.getLeafVertices());
        for (int i = 0; i < tables1.size(); i++) {
            for (int j = 0; j < tables2.size(); j++) {
                SchemaGraph.Vertex<MariaDBTable> table1 = tables1.get(i);
                SchemaGraph.Vertex<MariaDBTable> table2 = tables2.get(j);
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

    public boolean isEquivalentVertex(SchemaGraph.Vertex<MariaDBTable> table1, SchemaGraph.Vertex<MariaDBTable> table2, SchemaGraph<MariaDBTable> graph1, SchemaGraph<MariaDBTable> graph2) {
        if (table1 == null || table2 == null) return false;
        if (!isEquivalentMariaDBTable(table1.getTable(), table2.getTable())) return false;
        List<SchemaGraph.Edge<MariaDBTable>> edges1 = new ArrayList<>(graph1.getAdjacentEdges(table1));
        List<SchemaGraph.Edge<MariaDBTable>> edges2 = new ArrayList<>(graph2.getAdjacentEdges(table2));
        if (edges1.size() != edges2.size()) return false;
        for (int i = 0; i < edges1.size(); i++) {
            SchemaGraph.Edge<MariaDBTable> edge1 = edges1.get(i);
            SchemaGraph.Edge<MariaDBTable> edge2 = edges2.get(i);
            if (!isEquivalentEdge(edge1, edge2, graph1, graph2)) {
                return false;
            }
        }

        return true;
    }

    public boolean isEquivalentEdge(SchemaGraph.Edge<MariaDBTable> edge1, SchemaGraph.Edge<MariaDBTable> edge2, SchemaGraph<MariaDBTable> graph1, SchemaGraph<MariaDBTable> graph2) {
        if (!edge1.getEdgeType().equals(edge2.getEdgeType())) return false;
        SchemaGraph.Vertex<MariaDBTable> table1 = edge1.getSource();
        SchemaGraph.Vertex<MariaDBTable> table2 = edge2.getSource();
        if (!isEquivalentVertex(table1, table2, graph1, graph2)) return false;

        return true;
    }

    public void getDDLSequence(SchemaGraph<MariaDBTable> schemaGraph, List<String> ddlSeq) throws SQLException {
        while (ddlSeq.isEmpty()) {
            String createTable = MariaDBDDLStmt.CREATE_TABLE.getQueryProvider().getQuery(genState).getQueryString();
            try (Statement stmt = genState.getConnection().createStatement()) {
                stmt.execute(createTable);
                genState.updateSchema();
                ddlSeq.add(createTable);
                MariaDBTable curTable = genState.getSchema().getDatabaseTables().get(0);
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
            MariaDBDDLStmt ddlStmt = Randomly.fromOptions(MariaDBDDLStmt.values());
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
                MariaDBTable curTable = null;
                SchemaGraph.Vertex<MariaDBTable> srcTable = null;
                switch (ddlStmt) {
                    case CREATE_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (MariaDBTable table : genState.getSchema().getDatabaseTables()) { // new table
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
                                    SchemaGraph.Vertex<MariaDBTable> referTable = null;
                                    for (SchemaGraph.Vertex<MariaDBTable> v : schemaGraph.getVertices().values()) { // existing table
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
                        for (MariaDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<MariaDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (MariaDBTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case DROP_INDEX:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_drop_index").getChildren().get(2).getToken().toString();
                        for (MariaDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<MariaDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (MariaDBTable table : genState.getSchema().getDatabaseTables()) { // new table
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
                        for (MariaDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<MariaDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        String newTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (MariaDBTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(newTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case DROP_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (MariaDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<MariaDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                v.setLeaf(false);
                                break;
                            }
                        }
                        break;
                    case ALTER_TABLE_ADD_FOREIGN_KEY:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (MariaDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<MariaDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (MariaDBTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());

                        String referTableName = ddlQuery.getQueryAST().getChildByName("foreign_key_clause").getChildByName("_reference_table").getChildren().get(0).getToken().toString();
                        SchemaGraph.Vertex<MariaDBTable> referTable = null;
                        for (SchemaGraph.Vertex<MariaDBTable> v : schemaGraph.getVertices().values()) { // existing table
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
            ResultSet resultSet = showTables.executeQuery("SHOW TABLES");
            while (resultSet.next()) {
                // Drop each table
                dropTable.execute("DROP TABLE IF EXISTS " + resultSet.getString(1));
            }

            // Enable foreign key checks
            enableFKChecks.execute("SET foreign_key_checks = 1");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public List<SQLQueryAdapter> fetchCreateStmts(MariaDBGlobalState state) {
        List<SQLQueryAdapter> createStmts = new ArrayList<>(); // a set of create tables
        for (MariaDBTable table : state.getSchema().getDatabaseTables()) {
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
        return createStmts;
    }

    public boolean isEquivalentMariaDBSchema(MariaDBSchema schema1, MariaDBSchema schema2) {
        if (schema1.getDatabaseTables().size() != schema2.getDatabaseTables().size()) {
            return false;
        }
        if (schema1.getForeignKeyList().size() != schema2.getForeignKeyList().size()) {
            return false;
        }
        List<MariaDBTable> tables1 = new ArrayList<>(schema1.getDatabaseTables());
        List<MariaDBTable> tables2 = new ArrayList<>(schema2.getDatabaseTables());
        for (int i = 0; i < tables1.size(); i++) {
            for (int j = 0; j < tables2.size(); j++) {
                if (isEquivalentMariaDBTable(tables1.get(i), tables2.get(j))) {
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
        List<MariaDBForeignKey> fks1 = new ArrayList<>(schema1.getForeignKeyList());
        List<MariaDBForeignKey> fks2 = new ArrayList<>(schema2.getForeignKeyList());
        for (int i = 0; i < fks1.size(); i++) {
            for (int j = 0; j < fks2.size(); j++) {
                if (isEquivalentMariaDBForeignKey(fks1.get(i), fks2.get(j))) {
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

    private boolean isEquivalentMariaDBForeignKey(MariaDBForeignKey fk1, MariaDBForeignKey fk2) {
        if (!isEquivalentMariaDBTable(fk1.getTable(), fk2.getTable())) {
            return false;
        }
        if (fk1.getColumns().size() != fk2.getColumns().size()) {
            return false;
        }
        if (!isEquivalentMariaDBTable(fk1.getReferencedTable(), fk2.getReferencedTable())) {
            return false;
        }
        if (fk1.getReferencedColumns().size() != fk2.getReferencedColumns().size()) {
            return false;
        }
        List<MariaDBColumn> columns1 = new ArrayList<>(fk1.getColumns());
        List<MariaDBColumn> columns2 = new ArrayList<>(fk2.getColumns());
        for (int i = 0; i < columns1.size(); i++) {
            if (!isEquivalentMariaDBColumn(columns1.get(i), columns2.get(i))) {
                return false;
            }
        }

        columns1 = new ArrayList<>(fk1.getReferencedColumns());
        columns2 = new ArrayList<>(fk2.getReferencedColumns());
        for (int i = 0; i < columns1.size(); i++) {
            if (!isEquivalentMariaDBColumn(columns1.get(i), columns2.get(i))) {
                return false;
            }
        }

        return true;
    }

    private boolean isEquivalentMariaDBTable(MariaDBTable table1, MariaDBTable table2) {
        if (table1.getColumns().size() != table2.getColumns().size()) {
            return false;
        }
        if (table1.getIndexes().size() != table2.getIndexes().size()) {
            return false;
        }
        List<MariaDBColumn> columns1 = new ArrayList<>(table1.getColumns());
        List<MariaDBColumn> columns2 = new ArrayList<>(table2.getColumns());
        for (int i = 0; i < columns1.size(); i++) {
            for (int j = 0; j < columns2.size(); j++) {
                if (isEquivalentMariaDBColumn(columns1.get(i), columns2.get(j))) {
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
        List<MariaDBIndex> indexes1 = new ArrayList<>(table1.getIndexes());
        List<MariaDBIndex> indexes2 = new ArrayList<>(table2.getIndexes());
        for (int i = 0; i < indexes1.size(); i++) {
            for (int j = 0; j < indexes2.size(); j++) {
                if (isEquivalentMariaDBIndex(indexes1.get(i), indexes2.get(j))) {
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

    private boolean isEquivalentMariaDBIndex(MariaDBIndex index1, MariaDBIndex index2) {
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
            MariaDBColumn column1 = index1.getColumns().get(i);
            MariaDBColumn column2 = index2.getColumns().get(i);
            if (!isEquivalentMariaDBColumn(column1, column2)) {
                return false;
            }
        }

        return true;
    }

    private boolean isEquivalentMariaDBColumn(MariaDBColumn column1, MariaDBColumn column2) {
        return Objects.equals(column1.getColumnDefault(), column2.getColumnDefault()) &&
                column1.isNullable() == column2.isNullable() &&
                Objects.equals(column1.getDataType(), column2.getDataType()) &&
                Objects.equals(column1.getCharacterMaximumLength(), column2.getCharacterMaximumLength()) &&
                Objects.equals(column1.getNumericPrecision(), column2.getNumericPrecision()) &&
                Objects.equals(column1.getNumericScale(), column2.getNumericScale()) &&
                Objects.equals(column1.getColumnKey(), column2.getColumnKey());
    }


    @Override
    public String generateSelectStmt(MariaDBGlobalState state) {
        return MariaDBQueryProvider.SELECT.getQuery(state).getQueryString();
    }

    @Override
    public SQLQueryAdapter generateDMLStmt(MariaDBGlobalState state) {
        for (int i = 0; i < 10; i++) {
            try {
                return MariaDBDMLStmt.getRandomDML(state);
            } catch (QueryGenerationException ignored) {
            }
        }

        return null;
    }

    @Override
    public String checkQueryPlan(String query, MariaDBGlobalState state) {
        MariaDBQueryPlan plan = getMariaDBQueryPlan(query, state);
        return plan.toString();
    }

    @Override
    public String getExecutionResult(String query, MariaDBGlobalState state) {
        String errorMessage = null;
        boolean gotException = false;
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(query);
        } catch (SQLException e) {
            if (!EXPECTED_QUERY_ERRORS.errorIsExpected(e.getMessage())) {
                gotException = true;
            }
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

    public static MariaDBQueryPlan getMariaDBQueryPlan(String query, MariaDBGlobalState state) {
        MariaDBQueryPlan plan = new MariaDBQueryPlan();
        String checkQueryPlan = String.format("EXPLAIN %s", query);
        try (Statement statement = state.getConnection().createStatement()) {
            ResultSet planRes = statement.executeQuery(checkQueryPlan);
            while (planRes.next()) {
                plan.id.add(planRes.getString("id"));
                plan.selectType.add(planRes.getString("select_type"));
                plan.table.add(planRes.getString("table"));
                plan.type.add(planRes.getString("type"));
                plan.possibleKeys.add(planRes.getString("possible_keys"));
                plan.key.add(planRes.getString("key"));
                plan.keyLen.add(planRes.getString("key_len"));
                plan.ref.add(planRes.getString("ref"));
                plan.rows.add(planRes.getString("rows"));
                plan.extra.add(planRes.getString("Extra"));
            }
        } catch (SQLException e) {
            plan.exception = e.getMessage();
        }

        return plan;
    }

    public static class MariaDBQueryPlan {
        List<String> id = new ArrayList<>();
        List<String> selectType = new ArrayList<>();
        List<String> table = new ArrayList<>();
        List<String> type = new ArrayList<>();
        List<String> possibleKeys = new ArrayList<>();
        List<String> key = new ArrayList<>();
        List<String> keyLen = new ArrayList<>();
        List<String> ref = new ArrayList<>();
        List<String> rows = new ArrayList<>();
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
                sb.append(String.format("%-5s %-15s %-15s %-10s %-15s %-15s %-10s %-10s %-10s %-20s\n",
                        "id", "select_type", "table", "type", "possible_keys", "key", "key_len", "ref", "rows", "Extra"));
                for (int i = 0; i < id.size(); i++) {
                    sb.append(String.format("%-5s %-15s %-15s %-10s %-15s %-15s %-10s %-10s %-10s %-20s\n",
                            id.get(i), selectType.get(i), table.get(i), type.get(i), possibleKeys.get(i),
                            key.get(i), keyLen.get(i), ref.get(i), rows.get(i), extra.get(i)));
                }
            }
            return sb.toString();
        }

    }

}
