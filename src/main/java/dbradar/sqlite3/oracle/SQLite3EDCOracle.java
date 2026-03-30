package dbradar.sqlite3.oracle;

import dbradar.Randomly;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.oracle.edc.SchemaGraph;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.sqlite3.SQLite3GlobalState;
import dbradar.sqlite3.SQLite3Provider.SQLite3DDLStmt;
import dbradar.sqlite3.SQLite3Provider.SQLite3QueryProvider;
import dbradar.sqlite3.schema.SQLite3Column;
import dbradar.sqlite3.schema.SQLite3Index;
import dbradar.sqlite3.schema.SQLite3Table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SQLite3EDCOracle extends EDCBase<SQLite3GlobalState> {

    private static final List<SchemaGraph<SQLite3Table>> schemaGraphList = new ArrayList<>();

    public SQLite3EDCOracle(SQLite3GlobalState state) {
        super(state);
        synState = new SQLite3GlobalState();
        EXPECTED_QUERY_ERRORS.add("json_object() labels must be TEXT");
        EXPECTED_QUERY_ERRORS.add("malformed JSON");
        EXPECTED_QUERY_ERRORS.add("JSON cannot hold BLOB values");
    }

    @Override
    public void generateState(List<String> ddlSeq) throws Exception {
        while (true) {
            ddlSeq.clear();
            SchemaGraph<SQLite3Table> schemaGraph = new SchemaGraph<>();
            getDDLSequence(schemaGraph, ddlSeq);
            boolean isUnique = true;
            for (SchemaGraph<SQLite3Table> graph : schemaGraphList) {
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

    public boolean isEquivalentGraph(SchemaGraph<SQLite3Table> graph1, SchemaGraph<SQLite3Table> graph2) {
        if (graph1.getVertices().size() != graph2.getVertices().size()) return false;
        if (graph1.getAdjacencyList().size() != graph2.getAdjacencyList().size()) return false;

        List<SchemaGraph.Vertex<SQLite3Table>> tables1 = new ArrayList<>(graph1.getLeafVertices());
        List<SchemaGraph.Vertex<SQLite3Table>> tables2 = new ArrayList<>(graph2.getLeafVertices());
        for (int i = 0; i < tables1.size(); i++) {
            for (int j = 0; j < tables2.size(); j++) {
                SchemaGraph.Vertex<SQLite3Table> table1 = tables1.get(i);
                SchemaGraph.Vertex<SQLite3Table> table2 = tables2.get(j);
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

    public boolean isEquivalentVertex(SchemaGraph.Vertex<SQLite3Table> table1, SchemaGraph.Vertex<SQLite3Table> table2, SchemaGraph<SQLite3Table> graph1, SchemaGraph<SQLite3Table> graph2) {
        if (table1 == null || table2 == null) return false;
        if (!isEquivalentSQLite3Table(table1.getTable(), table2.getTable())) return false;
        List<SchemaGraph.Edge<SQLite3Table>> edges1 = new ArrayList<>(graph1.getAdjacentEdges(table1));
        List<SchemaGraph.Edge<SQLite3Table>> edges2 = new ArrayList<>(graph2.getAdjacentEdges(table2));
        if (edges1.size() != edges2.size()) return false;
        for (int i = 0; i < edges1.size(); i++) {
            SchemaGraph.Edge<SQLite3Table> edge1 = edges1.get(i);
            SchemaGraph.Edge<SQLite3Table> edge2 = edges2.get(i);
            if (!isEquivalentEdge(edge1, edge2, graph1, graph2)) {
                return false;
            }
        }

        return true;
    }

    public boolean isEquivalentEdge(SchemaGraph.Edge<SQLite3Table> edge1, SchemaGraph.Edge<SQLite3Table> edge2, SchemaGraph<SQLite3Table> graph1, SchemaGraph<SQLite3Table> graph2) {
        if (!edge1.getEdgeType().equals(edge2.getEdgeType())) return false;
        SchemaGraph.Vertex<SQLite3Table> table1 = edge1.getSource();
        SchemaGraph.Vertex<SQLite3Table> table2 = edge2.getSource();
        if (!isEquivalentVertex(table1, table2, graph1, graph2)) return false;

        return true;
    }


    public void getDDLSequence(SchemaGraph<SQLite3Table> schemaGraph, List<String> ddlSeq) {
        while (ddlSeq.isEmpty()) {
            String createTable = SQLite3DDLStmt.CREATE_TABLE.getQueryProvider().getQuery(genState).getQueryString();
            try (Statement stmt = genState.getConnection().createStatement()) {
                stmt.execute(createTable);
                genState.updateSchema();
                ddlSeq.add(createTable);
                SQLite3Table curTable = genState.getSchema().getDatabaseTables().get(0);
                schemaGraph.addVertex(curTable);
            } catch (Exception ignored) {
            }
        }

        int currentLength = Randomly.getNotCachedInteger(2, maxLength);
        for (int i = 0; i < currentLength; i++) {
            SQLite3DDLStmt ddlStmt = Randomly.fromOptions(SQLite3DDLStmt.values());
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
                SQLite3Table curTable = null;
                SchemaGraph.Vertex<SQLite3Table> srcTable = null;
                switch (ddlStmt) {
                    case CREATE_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (SQLite3Table table : genState.getSchema().getDatabaseTables()) { // new table
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
                                    SchemaGraph.Vertex<SQLite3Table> referTable = null;
                                    for (SchemaGraph.Vertex<SQLite3Table> v : schemaGraph.getVertices().values()) { // existing table
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
                    case ALTER_TABLE_RENAME_COLUMN:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (SQLite3Table t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<SQLite3Table> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (SQLite3Table table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case DROP_INDEX:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_drop_index").getChildren().get(2).getToken().toString();
                        for (SQLite3Table t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<SQLite3Table> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (SQLite3Table table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case ALTER_TABLE_RENAME_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (SQLite3Table t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<SQLite3Table> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        String newTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (SQLite3Table table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(newTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case DROP_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (SQLite3Table t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<SQLite3Table> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                v.setLeaf(false);
                                break;
                            }
                        }
                        break;
                }
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public void cleanDatabase() {
        try (Statement statement = genState.getConnection().createStatement()) {

            // Get the list of all tables
            ResultSet resultSet = statement.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';");
            while (resultSet.next()) {
                // Drop each table
                statement.execute("DROP TABLE IF EXISTS " + resultSet.getString(1));
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public List<SQLQueryAdapter> fetchCreateStmts(SQLite3GlobalState state) throws SQLException {
        List<SQLQueryAdapter> createStmts = new ArrayList<>(); // a set of create tables
        Statement statement = state.getConnection().createStatement();

        try {
            ResultSet resultSet = statement.executeQuery("SELECT sql FROM main.sqlite_master where type in ('table') AND name NOT like 'sqlite_%%'");
            while (resultSet.next()) {
                String createTable = resultSet.getString("sql");
                createStmts.add(new SQLQueryAdapter(createTable));
            }
        } catch (SQLException e) {
            throw new AssertionError("Failed to obtain CREATE TABLE statements: " + e.getMessage());
        }

        // obtain temporary CREATE TABLE statements
        try {
            ResultSet resultSet = statement.executeQuery("SELECT sql FROM temp.sqlite_master where type in ('table') AND name NOT like 'sqlite_%%'");
            while (resultSet.next()) {
                String createTempTable = resultSet.getString("sql");
                createTempTable = createTempTable.replaceFirst("CREATE TABLE", "CREATE TEMP TABLE");
                createStmts.add(new SQLQueryAdapter(createTempTable));
            }
        } catch (SQLException e) {
            throw new AssertionError("Failed to obtain temporary CREATE TABLE statements: " + e.getMessage());
        }

        // obtain CREATE VIEW statements
        try {
            ResultSet resultSet = statement.executeQuery("SELECT sql FROM main.sqlite_master where type in ('view') AND name NOT like 'sqlite_%%'");
            while (resultSet.next()) {
                String createView = resultSet.getString("sql");
                createStmts.add(new SQLQueryAdapter(createView));
            }
        } catch (SQLException e) {
            throw new AssertionError("Failed to obtain CREATE VIEW statements: " + e.getMessage());
        }

        // obtain temporary CREATE VIEW statements
        try {
            ResultSet resultSet = statement.executeQuery("SELECT sql FROM temp.sqlite_master where type in ('view') AND name NOT like 'sqlite_%%'");
            while (resultSet.next()) {
                String createTempView = resultSet.getString("sql");
                createTempView = createTempView.replaceFirst("CREATE VIEW", "CREATE TEMP VIEW");
                createStmts.add(new SQLQueryAdapter(createTempView));
            }
        } catch (SQLException e) {
            throw new AssertionError("Failed to obtain temporary CREATE VIEW statements: " + e.getMessage());
        }

        // obtain CREATE INDEX statements
        try {
            ResultSet resultSet = statement.executeQuery("SELECT sql FROM main.sqlite_master where type in ('index') AND name NOT like 'sqlite_%%'");
            while (resultSet.next()) {
                String createIndex = resultSet.getString("sql");
                createStmts.add(new SQLQueryAdapter(createIndex));
            }
        } catch (SQLException e) {
            throw new AssertionError("Failed to obtain CREATE INDEX statements: " + e.getMessage());
        }

        // obtain temporary CREATE INDEX statements
        try {
            ResultSet resultSet = statement.executeQuery("SELECT sql FROM temp.sqlite_master where type in ('index') AND name NOT like 'sqlite_%%'");
            while (resultSet.next()) {
                String createIndex = resultSet.getString("sql");
                createStmts.add(new SQLQueryAdapter(createIndex));
            }
        } catch (SQLException e) {
            throw new AssertionError("Failed to obtain temporary CREATE INDEX statements: " + e.getMessage());
        }

        statement.close();

        return createStmts;
    }

    private boolean isEquivalentSQLite3Table(SQLite3Table table1, SQLite3Table table2) {
        if (table1.getColumns().size() != table2.getColumns().size()) {
            return false;
        }
        if (table1.getIndexes().size() != table2.getIndexes().size()) {
            return false;
        }
        List<SQLite3Column> columns1 = new ArrayList<>(table1.getColumns());
        List<SQLite3Column> columns2 = new ArrayList<>(table2.getColumns());
        for (int i = 0; i < columns1.size(); i++) {
            for (int j = 0; j < columns2.size(); j++) {
                if (isEquivalentSQLite3Column(columns1.get(i), columns2.get(j))) {
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
        List<SQLite3Index> indexes1 = new ArrayList<>(table1.getIndexes());
        List<SQLite3Index> indexes2 = new ArrayList<>(table2.getIndexes());
        for (int i = 0; i < indexes1.size(); i++) {
            for (int j = 0; j < indexes2.size(); j++) {
                if (isEquivalentSQLite3Index(indexes1.get(i), indexes2.get(j))) {
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

    private boolean isEquivalentSQLite3Index(SQLite3Index index1, SQLite3Index index2) {
        if (index1.isTemporary() != index2.isTemporary()) {
            return false;
        }
        if (index1.getColumns().size() != index2.getColumns().size()) {
            return false;
        }
        for (int i = 0; i < index1.getColumns().size(); i++) {
            SQLite3Column column1 = index1.getColumns().get(i);
            SQLite3Column column2 = index2.getColumns().get(i);
            if (!isEquivalentSQLite3Column(column1, column2)) {
                return false;
            }
        }

        return true;
    }

    private boolean isEquivalentSQLite3Column(SQLite3Column column1, SQLite3Column column2) {
        return column1.isPrimaryKey() == column2.isPrimaryKey() &&
                column1.isGenerated() == column2.isGenerated() &&
                column1.isNotNull() == column2.isNotNull() &&
                column1.isIntegerPrimaryKey() == column2.isIntegerPrimaryKey() &&
                Objects.equals(column1.getType(), column2.getType()) &&
                Objects.equals(column1.getCollateSequence(), column2.getCollateSequence());
    }

    @Override
    public String generateSelectStmt(SQLite3GlobalState state) {
        return SQLite3QueryProvider.SELECT.getQuery(state).getQueryString();
    }

    @Override
    public SQLQueryAdapter generateDMLStmt(SQLite3GlobalState state) {
        return SQLite3QueryProvider.SELECT.getQuery(state); // too slow for DML
    }

    @Override
    public String checkQueryPlan(String query, SQLite3GlobalState state) {
        SQLite3QueryPlan plan = getSQLite3QueryPlan(query, state);
        return plan.toString();
    }

    @Override
    public String getExecutionResult(String query, SQLite3GlobalState state) {
        String errorMessage = null;
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(query);
        } catch (SQLException ignored) {
            errorMessage = "ERROR"; // a temporary mitigation for multiple constraint violations
        }
        return errorMessage;
    }

    private SQLite3QueryPlan getSQLite3QueryPlan(String query, SQLite3GlobalState state) {
        SQLite3QueryPlan plan = new SQLite3QueryPlan();
        String checkQueryPlan = "EXPLAIN QUERY PLAN " + query;
        try (Statement statement = state.getConnection().createStatement()) {
            ResultSet planRes = statement.executeQuery(checkQueryPlan);
            while (planRes.next()) {
                plan.queryPlan.add(planRes.getString(4));
            }
            planRes.close();
        } catch (SQLException e) {
            plan.exception = e.getMessage();
        }

        return plan;
    }

    static class SQLite3QueryPlan {
        List<String> queryPlan = new ArrayList<>();
        String exception = null;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (exception != null) {
                sb.append("Exception occurred: ").append(exception);
            } else {
                sb.append("Query Plan:\n");
                for (String planInfo : queryPlan) {
                    sb.append(planInfo).append("\n");
                }
            }
            return sb.toString();
        }

    }

}
