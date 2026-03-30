package dbradar.tidb.oracle;

import dbradar.Randomly;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.oracle.edc.SchemaGraph;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.tidb.TiDBGlobalState;
import dbradar.tidb.TiDBProvider.TiDBQueryProvider;
import dbradar.tidb.TiDBProvider.TiDBDDLStmt;
import dbradar.tidb.TiDBProvider.TiDBDMLStmt;
import dbradar.tidb.schema.TiDBSchema;
import dbradar.tidb.schema.TiDBSchema.TiDBColumn;
import dbradar.tidb.schema.TiDBSchema.TiDBIndex;
import dbradar.tidb.schema.TiDBSchema.TiDBTable;
import dbradar.tidb.schema.TiDBSchema.TiDBForeignKey;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TiDBEDCOracle extends EDCBase<TiDBGlobalState> {

    private static final List<SchemaGraph<TiDBTable>> schemaGraphList = new ArrayList<>();

    public TiDBEDCOracle(TiDBGlobalState state) {
        super(state);
        synState = new TiDBGlobalState();
        EXPECTED_QUERY_ERRORS.add("Subquery returns more than 1 row");
    }

    @Override
    public void generateState(List<String> ddlSeq) throws Exception {
        while (true) {
            ddlSeq.clear();
            SchemaGraph<TiDBTable> schemaGraph = new SchemaGraph<>();
            getDDLSequence(schemaGraph, ddlSeq);
            boolean isUnique = true;
            for (SchemaGraph<TiDBTable> graph : schemaGraphList) {
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

    public boolean isEquivalentGraph(SchemaGraph<TiDBTable> graph1, SchemaGraph<TiDBTable> graph2) {
        if (graph1.getVertices().size() != graph2.getVertices().size()) return false;
        if (graph1.getAdjacencyList().size() != graph2.getAdjacencyList().size()) return false;

        List<SchemaGraph.Vertex<TiDBTable>> tables1 = new ArrayList<>(graph1.getLeafVertices());
        List<SchemaGraph.Vertex<TiDBTable>> tables2 = new ArrayList<>(graph2.getLeafVertices());
        for (int i = 0; i < tables1.size(); i++) {
            for (int j = 0; j < tables2.size(); j++) {
                SchemaGraph.Vertex<TiDBTable> table1 = tables1.get(i);
                SchemaGraph.Vertex<TiDBTable> table2 = tables2.get(j);
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

    public boolean isEquivalentVertex(SchemaGraph.Vertex<TiDBTable> table1, SchemaGraph.Vertex<TiDBTable> table2, SchemaGraph<TiDBTable> graph1, SchemaGraph<TiDBTable> graph2) {
        if (table1 == null || table2 == null) return false;
        if (!isEquivalentTiDBTable(table1.getTable(), table2.getTable())) return false;
        List<SchemaGraph.Edge<TiDBTable>> edges1 = new ArrayList<>(graph1.getAdjacentEdges(table1));
        List<SchemaGraph.Edge<TiDBTable>> edges2 = new ArrayList<>(graph2.getAdjacentEdges(table2));
        if (edges1.size() != edges2.size()) return false;
        for (int i = 0; i < edges1.size(); i++) {
            SchemaGraph.Edge<TiDBTable> edge1 = edges1.get(i);
            SchemaGraph.Edge<TiDBTable> edge2 = edges2.get(i);
            if (!isEquivalentEdge(edge1, edge2, graph1, graph2)) {
                return false;
            }
        }

        return true;
    }

    public boolean isEquivalentEdge(SchemaGraph.Edge<TiDBTable> edge1, SchemaGraph.Edge<TiDBTable> edge2, SchemaGraph<TiDBTable> graph1, SchemaGraph<TiDBTable> graph2) {
        if (!edge1.getEdgeType().equals(edge2.getEdgeType())) return false;
        SchemaGraph.Vertex<TiDBTable> table1 = edge1.getSource();
        SchemaGraph.Vertex<TiDBTable> table2 = edge2.getSource();
        if (!isEquivalentVertex(table1, table2, graph1, graph2)) return false;

        return true;
    }


    public void getDDLSequence(SchemaGraph<TiDBTable> schemaGraph, List<String> ddlSeq) {
        while (ddlSeq.isEmpty()) {
            String createTable = TiDBDDLStmt.CREATE_TABLE.getQueryProvider().getQuery(genState).getQueryString();
            try (Statement stmt = genState.getConnection().createStatement()) {
                stmt.execute(createTable);
                genState.updateSchema();
                ddlSeq.add(createTable);
                TiDBTable curTable = genState.getSchema().getDatabaseTables().get(0);
                schemaGraph.addVertex(curTable);
            } catch (Exception ignored) {
            }
        }

        int currentLength = Randomly.getNotCachedInteger(2, maxLength);
        for (int i = 0; i < currentLength; i++) {
            TiDBDDLStmt ddlStmt = Randomly.fromOptions(TiDBDDLStmt.values());
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
                TiDBTable curTable = null;
                SchemaGraph.Vertex<TiDBTable> srcTable = null;
                switch (ddlStmt) {
                    case CREATE_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (TiDBTable table : genState.getSchema().getDatabaseTables()) { // new table
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
                                    SchemaGraph.Vertex<TiDBTable> referTable = null;
                                    for (SchemaGraph.Vertex<TiDBTable> v : schemaGraph.getVertices().values()) { // existing table
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
                        for (TiDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<TiDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (TiDBTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case DROP_INDEX:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_drop_index").getChildren().get(2).getToken().toString();
                        for (TiDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<TiDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (TiDBTable table : genState.getSchema().getDatabaseTables()) { // new table
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
                        for (TiDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<TiDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        String newTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (TiDBTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(newTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case DROP_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (TiDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<TiDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                v.setLeaf(false);
                                break;
                            }
                        }
                        break;
                    case ALTER_TABLE_ADD_FOREIGN_KEY:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (TiDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<TiDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (TiDBTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());

                        String referTableName = ddlQuery.getQueryAST().getChildByName("foreign_key_clause").getChildByName("_reference_table").getChildren().get(0).getToken().toString();
                        SchemaGraph.Vertex<TiDBTable> referTable = null;
                        for (SchemaGraph.Vertex<TiDBTable> v : schemaGraph.getVertices().values()) { // existing table
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
            } catch (Exception ignored) {
            }
        }
    }

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
    public List<SQLQueryAdapter> fetchCreateStmts(TiDBGlobalState state) {
        List<SQLQueryAdapter> createStmts = new ArrayList<>(); // a set of create tables

        for (TiDBSchema.TiDBTable table : state.getSchema().getDatabaseTables()) {
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
                    createTable = createTable.replaceAll(state.getDatabaseName(), state.getDatabaseName() + "_semi"); // format
                }
                createStmts.add(new SQLQueryAdapter(createTable)); // log create table statements
            } catch (SQLException ignored) {
            }
        }

        Collections.reverse(createStmts); // revise the order, since we store them in a revise order
        return createStmts;
    }


    public boolean isEquivalentTiDBSchema(TiDBSchema schema1, TiDBSchema schema2) {
        if (schema1.getDatabaseTables().size() != schema2.getDatabaseTables().size()) {
            return false;
        }
        if (schema1.getForeignKeyList().size() != schema2.getForeignKeyList().size()) {
            return false;
        }
        List<TiDBSchema.TiDBTable> tables1 = new ArrayList<>(schema1.getDatabaseTables());
        List<TiDBSchema.TiDBTable> tables2 = new ArrayList<>(schema2.getDatabaseTables());
        for (int i = 0; i < tables1.size(); i++) {
            for (int j = 0; j < tables2.size(); j++) {
                if (isEquivalentTiDBTable(tables1.get(i), tables2.get(j))) {
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
        List<TiDBForeignKey> fks1 = new ArrayList<>(schema1.getForeignKeyList());
        List<TiDBForeignKey> fks2 = new ArrayList<>(schema2.getForeignKeyList());
        for (int i = 0; i < fks1.size(); i++) {
            for (int j = 0; j < fks2.size(); j++) {
                if (isEquivalentTiDBForeignKey(fks1.get(i), fks2.get(j))) {
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

    private boolean isEquivalentTiDBForeignKey(TiDBForeignKey fk1, TiDBForeignKey fk2) {
        if (!isEquivalentTiDBTable(fk1.getTable(), fk2.getTable())) {
            return false;
        }
        if (fk1.getColumns().size() != fk2.getColumns().size()) {
            return false;
        }
        if (!isEquivalentTiDBTable(fk1.getReferencedTable(), fk2.getReferencedTable())) {
            return false;
        }
        if (fk1.getReferencedColumns().size() != fk2.getReferencedColumns().size()) {
            return false;
        }
        List<TiDBColumn> columns1 = new ArrayList<>(fk1.getColumns());
        List<TiDBColumn> columns2 = new ArrayList<>(fk2.getColumns());
        for (int i = 0; i < columns1.size(); i++) {
            if (!isEquivalentTiDBColumn(columns1.get(i), columns2.get(i))) {
                return false;
            }
        }

        columns1 = new ArrayList<>(fk1.getReferencedColumns());
        columns2 = new ArrayList<>(fk2.getReferencedColumns());
        for (int i = 0; i < columns1.size(); i++) {
            if (!isEquivalentTiDBColumn(columns1.get(i), columns2.get(i))) {
                return false;
            }
        }

        return true;
    }

    private boolean isEquivalentTiDBTable(TiDBTable table1, TiDBTable table2) {
        if (table1.getColumns().size() != table2.getColumns().size()) {
            return false;
        }
        if (table1.getIndexes().size() != table2.getIndexes().size()) {
            return false;
        }
        List<TiDBColumn> columns1 = new ArrayList<>(table1.getColumns());
        List<TiDBColumn> columns2 = new ArrayList<>(table2.getColumns());
        for (int i = 0; i < columns1.size(); i++) {
            for (int j = 0; j < columns2.size(); j++) {
                if (isEquivalentTiDBColumn(columns1.get(i), columns2.get(j))) {
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
        List<TiDBIndex> indexes1 = new ArrayList<>(table1.getIndexes());
        List<TiDBIndex> indexes2 = new ArrayList<>(table2.getIndexes());
        for (int i = 0; i < indexes1.size(); i++) {
            for (int j = 0; j < indexes2.size(); j++) {
                if (isEquivalentTiDBIndex(indexes1.get(i), indexes2.get(j))) {
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

    private boolean isEquivalentTiDBIndex(TiDBIndex index1, TiDBIndex index2) {
        if (index1.getNonUnique() != index2.getNonUnique()) {
            return false;
        }
        if (index1.isNullable() != index2.isNullable()) {
            return false;
        }
//        if (!Objects.equals(index1.getIndexType(), index2.getIndexType())) {
//            return false;
//        }
        if (index1.getColumns().size() != index2.getColumns().size()) {
            return false;
        }
        for (int i = 0; i < index1.getColumns().size(); i++) {
            TiDBColumn column1 = index1.getColumns().get(i);
            TiDBColumn column2 = index2.getColumns().get(i);
            if (!isEquivalentTiDBColumn(column1, column2)) {
                return false;
            }
        }

        return true;
    }

    private boolean isEquivalentTiDBColumn(TiDBColumn column1, TiDBColumn column2) {
        return Objects.equals(column1.getColumnDefault(), column2.getColumnDefault()) &&
                column1.isNullable() == column2.isNullable() &&
                Objects.equals(column1.getDataType(), column2.getDataType()) &&
                Objects.equals(column1.getCharacterMaximumLength(), column2.getCharacterMaximumLength()) &&
                Objects.equals(column1.getNumericPrecision(), column2.getNumericPrecision()) &&
                Objects.equals(column1.getNumericScale(), column2.getNumericScale()) &&
                Objects.equals(column1.getColumnKey(), column2.getColumnKey());
    }


    @Override
    public String generateSelectStmt(TiDBGlobalState state) {
        return TiDBQueryProvider.SELECT.getQuery(state).getQueryString();
    }

    @Override
    public SQLQueryAdapter generateDMLStmt(TiDBGlobalState state) {
        for (int i = 0; i < 10; i++) {
            try {
                return TiDBDMLStmt.getRandomDML(state);
            } catch (QueryGenerationException ignored) {
            }
        }

        return null;
    }

    @Override
    public String checkQueryPlan(String query, TiDBGlobalState state) {
        TiDBQueryPlan plan = getTiDBQueryPlan(query, state);
        return plan.toString();
    }

    @Override
    public String getExecutionResult(String query, TiDBGlobalState state) {
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

    private TiDBQueryPlan getTiDBQueryPlan(String query, TiDBGlobalState state) {
        TiDBQueryPlan plan = new TiDBQueryPlan();
        String checkQueryPlan = String.format("EXPLAIN FORMAT=brief %s", query);
        try (Statement statement = state.getConnection().createStatement()) {
            ResultSet planRes = statement.executeQuery(checkQueryPlan);
            while (planRes.next()) {
                plan.id.add(planRes.getString("id"));
                plan.estRows.add(planRes.getString("estRows"));
                plan.task.add(planRes.getString("task"));
                plan.accessObject.add(planRes.getString("access object"));
                String operatorInfo = planRes.getString("operator info");
                if (!state.getDatabaseName().equals(databaseName)) {
                    operatorInfo = operatorInfo.replaceAll(state.getDatabaseName(), databaseName);
                }
                plan.operatorInfo.add(operatorInfo);
            }
            planRes.close();
        } catch (SQLException e) {
            plan.exception = e.getMessage();
        }

        return plan;
    }

    static class TiDBQueryPlan {
        List<String> id = new ArrayList<>();
        List<String> estRows = new ArrayList<>();
        List<String> task = new ArrayList<>();
        List<String> accessObject = new ArrayList<>();
        List<String> operatorInfo = new ArrayList<>();
        String exception = null;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (exception != null) {
                sb.append("Exception occurred: ").append(exception);
            } else {
                sb.append("Query Plan:\n");
                sb.append(String.format("%-5s %-10s %-20s %-20s %-30s\n", "id", "estRows", "task", "access object", "operator info"));
                for (int i = 0; i < id.size(); i++) {
                    sb.append(String.format("%-5s %-10s %-20s %-20s %-30s\n", id.get(i), estRows.get(i), task.get(i), accessObject.get(i), operatorInfo.get(i)));
                }
            }
            return sb.toString();
        }

    }

}
