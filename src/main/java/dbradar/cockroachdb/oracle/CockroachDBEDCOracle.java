package dbradar.cockroachdb.oracle;

import dbradar.Randomly;
import dbradar.cockroachdb.CockroachDBProvider.CockroachDBQueryProvider;
import dbradar.cockroachdb.CockroachDBProvider.CockroachDBDMLStmt;
import dbradar.cockroachdb.CockroachDBProvider.CockroachDBDDLStmt;
import dbradar.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import dbradar.cockroachdb.CockroachDBSchema;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.oracle.edc.SchemaGraph;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.QueryGenerationException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CockroachDBEDCOracle extends EDCBase<CockroachDBGlobalState> {

    private static final List<SchemaGraph<CockroachDBSchema.CockroachDBTable>> schemaGraphList = new ArrayList<>();

    public CockroachDBEDCOracle(CockroachDBGlobalState state) {
        super(state);
        synState = new CockroachDBGlobalState();
    }

    @Override
    public void generateState(List<String> ddlSeq) throws Exception {
        while (true) {
            ddlSeq.clear();
            SchemaGraph<CockroachDBSchema.CockroachDBTable> schemaGraph = new SchemaGraph<>();
            getDDLSequence(schemaGraph, ddlSeq);
            boolean isUnique = true;
            for (SchemaGraph<CockroachDBSchema.CockroachDBTable> graph : schemaGraphList) {
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

    public boolean isEquivalentGraph(SchemaGraph<CockroachDBSchema.CockroachDBTable> graph1, SchemaGraph<CockroachDBSchema.CockroachDBTable> graph2) {
        if (graph1.getVertices().size() != graph2.getVertices().size()) return false;
        if (graph1.getAdjacencyList().size() != graph2.getAdjacencyList().size()) return false;

        List<SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable>> tables1 = new ArrayList<>(graph1.getLeafVertices());
        List<SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable>> tables2 = new ArrayList<>(graph2.getLeafVertices());
        for (int i = 0; i < tables1.size(); i++) {
            for (int j = 0; j < tables2.size(); j++) {
                SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> table1 = tables1.get(i);
                SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> table2 = tables2.get(j);
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

    public boolean isEquivalentVertex(SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> table1, SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> table2, SchemaGraph<CockroachDBSchema.CockroachDBTable> graph1, SchemaGraph<CockroachDBSchema.CockroachDBTable> graph2) {
        if (table1 == null || table2 == null) return false;
        if (!isEquivalentCockroachDBTable(table1.getTable(), table2.getTable())) return false;
        List<SchemaGraph.Edge<CockroachDBSchema.CockroachDBTable>> edges1 = new ArrayList<>(graph1.getAdjacentEdges(table1));
        List<SchemaGraph.Edge<CockroachDBSchema.CockroachDBTable>> edges2 = new ArrayList<>(graph2.getAdjacentEdges(table2));
        if (edges1.size() != edges2.size()) return false;
        for (int i = 0; i < edges1.size(); i++) {
            SchemaGraph.Edge<CockroachDBSchema.CockroachDBTable> edge1 = edges1.get(i);
            SchemaGraph.Edge<CockroachDBSchema.CockroachDBTable> edge2 = edges2.get(i);
            if (!isEquivalentEdge(edge1, edge2, graph1, graph2)) {
                return false;
            }
        }

        return true;
    }

    public boolean isEquivalentEdge(SchemaGraph.Edge<CockroachDBSchema.CockroachDBTable> edge1, SchemaGraph.Edge<CockroachDBSchema.CockroachDBTable> edge2, SchemaGraph<CockroachDBSchema.CockroachDBTable> graph1, SchemaGraph<CockroachDBSchema.CockroachDBTable> graph2) {
        if (!edge1.getEdgeType().equals(edge2.getEdgeType())) return false;
        SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> table1 = edge1.getSource();
        SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> table2 = edge2.getSource();
        if (!isEquivalentVertex(table1, table2, graph1, graph2)) return false;

        return true;
    }


    public void getDDLSequence(SchemaGraph<CockroachDBSchema.CockroachDBTable> schemaGraph, List<String> ddlSeq) {
        while (ddlSeq.isEmpty()) {
            String createTable = CockroachDBDDLStmt.CREATE_TABLE.getQueryProvider().getQuery(genState).getQueryString();
            try (Statement stmt = genState.getConnection().createStatement()) {
                stmt.execute(createTable);
                genState.updateSchema();
                ddlSeq.add(createTable);
                CockroachDBSchema.CockroachDBTable curTable = genState.getSchema().getDatabaseTables().get(0);
                schemaGraph.addVertex(curTable);
            } catch (Exception ignored) {
            }
        }

        int currentLength = Randomly.getNotCachedInteger(2, maxLength);
        for (int i = 0; i < currentLength; i++) {
            CockroachDBDDLStmt ddlStmt = Randomly.fromOptions(CockroachDBDDLStmt.values());
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
                CockroachDBSchema.CockroachDBTable curTable = null;
                SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> srcTable = null;
                switch (ddlStmt) {
                    case CREATE_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (CockroachDBSchema.CockroachDBTable table : genState.getSchema().getDatabaseTables()) { // new table
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
                                    SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> referTable = null;
                                    for (SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> v : schemaGraph.getVertices().values()) { // existing table
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
                    case ALTER_TABLE_ADD_PRIMARY_KEY:
                    case ALTER_TABLE_ADD_UNIQUE_KEY:
                    case TRUNCATE_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (CockroachDBSchema.CockroachDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (CockroachDBSchema.CockroachDBTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case ALTER_TABLE_RENAME_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (CockroachDBSchema.CockroachDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        String newTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (CockroachDBSchema.CockroachDBTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(newTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case DROP_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (CockroachDBSchema.CockroachDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                v.setLeaf(false);
                                break;
                            }
                        }
                        break;
                    case ALTER_TABLE_ADD_FOREIGN_KEY:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (CockroachDBSchema.CockroachDBTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (CockroachDBSchema.CockroachDBTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());

                        String referTableName = ddlQuery.getQueryAST().getChildByName("foreign_key_clause").getChildByName("_reference_table").getChildren().get(0).getToken().toString();
                        SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> referTable = null;
                        for (SchemaGraph.Vertex<CockroachDBSchema.CockroachDBTable> v : schemaGraph.getVertices().values()) { // existing table
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

    @Override
    public void cleanDatabase() {
        try (Statement showTables = genState.getConnection().createStatement();
             Statement dropTable = genState.getConnection().createStatement()) {

            // Get the list of all tables
            ResultSet resultSet = showTables.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';");
            while (resultSet.next()) {
                // Drop each table
                dropTable.execute("DROP TABLE IF EXISTS " + resultSet.getString(1) + " CASCADE");
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public List<SQLQueryAdapter> fetchCreateStmts(CockroachDBGlobalState state) {
        List<SQLQueryAdapter> createStmts = new ArrayList<>(); // a set of create tables
        for (CockroachDBSchema.CockroachDBTable table : state.getSchema().getDatabaseTables()) {
            String tableName = table.getName();
            try (Statement statement = state.getConnection().createStatement()) {
                ResultSet resultSet = statement.executeQuery("SHOW CREATE TABLE " + state.getDatabaseName() + ".public." + tableName);
                String createTable = null;
                if (resultSet.next()) {
                    createTable = resultSet.getString("create_statement");
                }
                if (createTable == null) {
                    throw new AssertionError("fetchCreateStmts: " + tableName);
                }
                createTable = createTable.replaceAll("\\r?\\n", ""); // format show create table
                createTable = createTable.replaceAll("\\t", " "); // format show create table
                createTable = createTable.replaceAll(databaseName, databaseName + "_semi"); // format show create table
                if (table.isMatView() && createTable.contains("rowid")) {
                    // https://github.com/cockroachdb/cockroach/issues/85132
                    createTable = createTable.replaceFirst(", rowid", "");
                }
                createStmts.add(new SQLQueryAdapter(createTable)); // log create table statements
            } catch (SQLException ignored) {
            }
        }
        return createStmts;
    }

    public boolean isEquivalentCockroachDBSchema(CockroachDBSchema schema1, CockroachDBSchema schema2) {
        if (schema1.getDatabaseTables().size() != schema2.getDatabaseTables().size()) {
            return false;
        }
        if (schema1.getForeignKeys().size() != schema2.getForeignKeys().size()) {
            return false;
        }
        List<CockroachDBSchema.CockroachDBTable> tables1 = new ArrayList<>(schema1.getDatabaseTables());
        List<CockroachDBSchema.CockroachDBTable> tables2 = new ArrayList<>(schema2.getDatabaseTables());
        for (int i = 0; i < tables1.size(); i++) {
            for (int j = 0; j < tables2.size(); j++) {
                if (isEquivalentCockroachDBTable(tables1.get(i), tables2.get(j))) {
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
        List<CockroachDBSchema.CockroachDBForeignKey> fks1 = new ArrayList<>(schema1.getForeignKeys());
        List<CockroachDBSchema.CockroachDBForeignKey> fks2 = new ArrayList<>(schema2.getForeignKeys());
        for (int i = 0; i < fks1.size(); i++) {
            for (int j = 0; j < fks2.size(); j++) {
                if (isEquivalentCockroachDBForeignKey(fks1.get(i), fks2.get(j))) {
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

    private boolean isEquivalentCockroachDBForeignKey(CockroachDBSchema.CockroachDBForeignKey fk1, CockroachDBSchema.CockroachDBForeignKey fk2) {
        if (!isEquivalentCockroachDBTable(fk1.getTable(), fk2.getTable())) {
            return false;
        }
        if (fk1.getColumns().size() != fk2.getColumns().size()) {
            return false;
        }
        if (!isEquivalentCockroachDBTable(fk1.getReferencedTable(), fk2.getReferencedTable())) {
            return false;
        }
        if (fk1.getReferencedColumns().size() != fk2.getReferencedColumns().size()) {
            return false;
        }
        List<CockroachDBSchema.CockroachDBColumn> columns1 = new ArrayList<>(fk1.getColumns());
        List<CockroachDBSchema.CockroachDBColumn> columns2 = new ArrayList<>(fk2.getColumns());
        for (int i = 0; i < columns1.size(); i++) {
            if (!isEquivalentCockroachDBColumn(columns1.get(i), columns2.get(i))) {
                return false;
            }
        }

        columns1 = new ArrayList<>(fk1.getReferencedColumns());
        columns2 = new ArrayList<>(fk2.getReferencedColumns());
        for (int i = 0; i < columns1.size(); i++) {
            if (!isEquivalentCockroachDBColumn(columns1.get(i), columns2.get(i))) {
                return false;
            }
        }

        return true;
    }

    private boolean isEquivalentCockroachDBTable(CockroachDBSchema.CockroachDBTable table1, CockroachDBSchema.CockroachDBTable table2) {
        if (table1.getColumns().size() != table2.getColumns().size()) {
            return false;
        }
        if (table1.getIndexes().size() != table2.getIndexes().size()) {
            return false;
        }
        List<CockroachDBSchema.CockroachDBColumn> columns1 = new ArrayList<>(table1.getColumns());
        List<CockroachDBSchema.CockroachDBColumn> columns2 = new ArrayList<>(table2.getColumns());
        for (int i = 0; i < columns1.size(); i++) {
            for (int j = 0; j < columns2.size(); j++) {
                if (isEquivalentCockroachDBColumn(columns1.get(i), columns2.get(j))) {
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
        List<CockroachDBSchema.CockroachDBIndex> indexes1 = new ArrayList<>(table1.getIndexes());
        List<CockroachDBSchema.CockroachDBIndex> indexes2 = new ArrayList<>(table2.getIndexes());
        for (int i = 0; i < indexes1.size(); i++) {
            for (int j = 0; j < indexes2.size(); j++) {
                if (isEquivalentCockroachDBIndex(indexes1.get(i), indexes2.get(j))) {
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

    private boolean isEquivalentCockroachDBIndex(CockroachDBSchema.CockroachDBIndex index1, CockroachDBSchema.CockroachDBIndex index2) {
        if (index1.isNonUnique() != index2.isNonUnique()) {
            return false;
        }
        if (index1.isPrimaryKey() != index2.isPrimaryKey()) {
            return false;
        }
        if (index1.getColumns().size() != index2.getColumns().size()) {
            return false;
        }
        for (int i = 0; i < index1.getColumns().size(); i++) {
            CockroachDBSchema.CockroachDBColumn column1 = index1.getColumns().get(i);
            CockroachDBSchema.CockroachDBColumn column2 = index2.getColumns().get(i);
            if (!isEquivalentCockroachDBColumn(column1, column2)) {
                return false;
            }
        }

        return true;
    }

    private boolean isEquivalentCockroachDBColumn(CockroachDBSchema.CockroachDBColumn column1, CockroachDBSchema.CockroachDBColumn column2) {
        return Objects.equals(column1.getDataType(), column2.getDataType()) &&
                column1.isPrimaryKey() == column2.isPrimaryKey() &&
                column1.isNullable() == column2.isNullable() &&
                Objects.equals(column1.getDefaultValue(), column2.getDefaultValue());
    }


    @Override
    public String generateSelectStmt(CockroachDBGlobalState state) {
        return CockroachDBQueryProvider.SELECT.getQuery(state).getQueryString();
    }

    @Override
    public SQLQueryAdapter generateDMLStmt(CockroachDBGlobalState state) {
        for (int i = 0; i < 10; i++) {
            try {
                return CockroachDBDMLStmt.getRandomDML(state);
            } catch (Exception ignored) {
            }
        }

        return null;
    }

    @Override
    public String checkQueryPlan(String query, CockroachDBGlobalState state) {
        CockroachDBQueryPlan plan = getCockroachDBQueryPlan(query, state);
        return plan.toString();
    }

    @Override
    public String getExecutionResult(String query, CockroachDBGlobalState state) {
        String errorMessage = null;
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(query);
        } catch (SQLException ignored) {
            errorMessage = "ERROR"; // a temporary mitigation for multiple constraint violations
        }
        return errorMessage;
    }

    private CockroachDBQueryPlan getCockroachDBQueryPlan(String query, CockroachDBGlobalState state) {
        CockroachDBQueryPlan plan = new CockroachDBQueryPlan();
        String checkQueryPlan = String.format("EXPLAIN (OPT) %s", query);
        try (Statement statement = state.getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery(checkQueryPlan);
            while (resultSet.next()) {
                plan.info.add(resultSet.getString("info"));
            }
        } catch (SQLException e) {
            plan.exception = e.getMessage();
        }

        return plan;
    }

    static class CockroachDBQueryPlan {
        List<String> info = new ArrayList<>();
        String exception = null;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (exception != null) {
                sb.append("Exception occurred: ").append(exception);
            } else {
                sb.append("Query Plan:\n");
                for (String planInfo : info) {
                    sb.append(planInfo).append("\n");
                }
            }
            return sb.toString();
        }
    }

}
