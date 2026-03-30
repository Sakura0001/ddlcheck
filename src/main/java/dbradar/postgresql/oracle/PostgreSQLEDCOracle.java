package dbradar.postgresql.oracle;

import com.beust.jcommander.Strings;
import dbradar.Randomly;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.oracle.edc.SchemaGraph;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLProvider.PostgreSQLQueryProvider;
import dbradar.postgresql.PostgreSQLProvider.PostgreSQLDDLStmt;
import dbradar.postgresql.PostgreSQLProvider.PostgreSQLDMLStmt;
import dbradar.postgresql.PostgreSQLSchema;
import dbradar.postgresql.PostgresCommon;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class PostgreSQLEDCOracle extends EDCBase<PostgreSQLGlobalState> {

    private static final List<SchemaGraph<PostgreSQLSchema.PostgreSQLTable>> schemaGraphList = new ArrayList<>();


    public PostgreSQLEDCOracle(PostgreSQLGlobalState state) {
        super(state);
        synState = new PostgreSQLGlobalState();
        PostgresCommon.addCommonExpressionErrors(EXPECTED_QUERY_ERRORS);
        PostgresCommon.addCommonFetchErrors(EXPECTED_QUERY_ERRORS);
    }

    @Override
    public void generateState(List<String> ddlSeq) throws Exception {
        while (true) {
            ddlSeq.clear();
            SchemaGraph<PostgreSQLSchema.PostgreSQLTable> schemaGraph = new SchemaGraph<>();
            getDDLSequence(schemaGraph, ddlSeq);
            boolean isUnique = true;
            for (SchemaGraph<PostgreSQLSchema.PostgreSQLTable> graph : schemaGraphList) {
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

    public boolean isEquivalentGraph(SchemaGraph<PostgreSQLSchema.PostgreSQLTable> graph1, SchemaGraph<PostgreSQLSchema.PostgreSQLTable> graph2) {
        if (graph1.getVertices().size() != graph2.getVertices().size()) return false;
        if (graph1.getAdjacencyList().size() != graph2.getAdjacencyList().size()) return false;

        List<SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable>> tables1 = new ArrayList<>(graph1.getLeafVertices());
        List<SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable>> tables2 = new ArrayList<>(graph2.getLeafVertices());
        for (int i = 0; i < tables1.size(); i++) {
            for (int j = 0; j < tables2.size(); j++) {
                SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> table1 = tables1.get(i);
                SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> table2 = tables2.get(j);
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

    public boolean isEquivalentVertex(SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> table1, SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> table2, SchemaGraph<PostgreSQLSchema.PostgreSQLTable> graph1, SchemaGraph<PostgreSQLSchema.PostgreSQLTable> graph2) {
        if (table1 == null || table2 == null) return false;
        if (!isEquivalentPostgresTable(table1.getTable(), table2.getTable())) return false;
        List<SchemaGraph.Edge<PostgreSQLSchema.PostgreSQLTable>> edges1 = new ArrayList<>(graph1.getAdjacentEdges(table1));
        List<SchemaGraph.Edge<PostgreSQLSchema.PostgreSQLTable>> edges2 = new ArrayList<>(graph2.getAdjacentEdges(table2));
        if (edges1.size() != edges2.size()) return false;
        for (int i = 0; i < edges1.size(); i++) {
            SchemaGraph.Edge<PostgreSQLSchema.PostgreSQLTable> edge1 = edges1.get(i);
            SchemaGraph.Edge<PostgreSQLSchema.PostgreSQLTable> edge2 = edges2.get(i);
            if (!isEquivalentEdge(edge1, edge2, graph1, graph2)) {
                return false;
            }
        }

        return true;
    }

    public boolean isEquivalentEdge(SchemaGraph.Edge<PostgreSQLSchema.PostgreSQLTable> edge1, SchemaGraph.Edge<PostgreSQLSchema.PostgreSQLTable> edge2, SchemaGraph<PostgreSQLSchema.PostgreSQLTable> graph1, SchemaGraph<PostgreSQLSchema.PostgreSQLTable> graph2) {
        if (!edge1.getEdgeType().equals(edge2.getEdgeType())) return false;
        SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> table1 = edge1.getSource();
        SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> table2 = edge2.getSource();
        if (!isEquivalentVertex(table1, table2, graph1, graph2)) return false;

        return true;
    }


    public void getDDLSequence(SchemaGraph<PostgreSQLSchema.PostgreSQLTable> schemaGraph, List<String> ddlSeq) {
        while (ddlSeq.isEmpty()) {
            String createTable = PostgreSQLDDLStmt.CREATE_TABLE.getQueryProvider().getQuery(genState).getQueryString();
            try (Statement stmt = genState.getConnection().createStatement()) {
                stmt.execute(createTable);
                genState.updateSchema();
                ddlSeq.add(createTable);
                PostgreSQLSchema.PostgreSQLTable curTable = genState.getSchema().getDatabaseTables().get(0);
                schemaGraph.addVertex(curTable);
            } catch (Exception ignored) {
            }
        }

        int currentLength = Randomly.getNotCachedInteger(2, maxLength);
        for (int i = 0; i < currentLength; i++) {
            PostgreSQLDDLStmt ddlStmt = Randomly.fromOptions(PostgreSQLDDLStmt.values());
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
                PostgreSQLSchema.PostgreSQLTable curTable = null;
                SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> srcTable = null;
                switch (ddlStmt) {
                    case CREATE_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (PostgreSQLSchema.PostgreSQLTable table : genState.getSchema().getDatabaseTables()) { // new table
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
                                    SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> referTable = null;
                                    for (SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> v : schemaGraph.getVertices().values()) { // existing table
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
                    case ALTER_TABLE_ALTER_COLUMN_TYPE:
                    case ALTER_TABLE_ALTER_COLUMN_SET_NOT_NULL:
                    case ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL:
                    case ALTER_TABLE_SET_COLUMN:
                    case ALTER_TABLE_RESET_COLUMN:
                    case ALTER_TABLE_ALTER_COLUMN_SET_STORAGE:
                    case ALTER_TABLE_ADD_PRIMARY_KEY:
                    case ALTER_TABLE_ADD_UNIQUE_KEY:
                    case ALTER_TABLE_OPTION:
                    case TRUNCATE_TABLE:
                    case REINDEX:
                    case DROP_INDEX:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (PostgreSQLSchema.PostgreSQLTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (PostgreSQLSchema.PostgreSQLTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case ALTER_TABLE_RENAME_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (PostgreSQLSchema.PostgreSQLTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        String newTableName = ddlQuery.getQueryAST().getChildByName("_new_table_name").getChildren().get(0).getToken().toString();
                        for (PostgreSQLSchema.PostgreSQLTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(newTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());
                        break;
                    case DROP_TABLE:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (PostgreSQLSchema.PostgreSQLTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                v.setLeaf(false);
                                break;
                            }
                        }
                        break;
                    case ALTER_TABLE_ADD_FOREIGN_KEY:
                        curTableName = ddlQuery.getQueryAST().getChildByName("_table").getChildren().get(0).getToken().toString();
                        for (PostgreSQLSchema.PostgreSQLTable t : schemaGraph.getVertices().keySet()) { // existing table
                            SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> v = schemaGraph.getVertices().get(t);
                            if (v.isLeaf() && v.getTable().getName().equals(curTableName)) {
                                curTable = t;
                                break;
                            }
                        }
                        for (PostgreSQLSchema.PostgreSQLTable table : genState.getSchema().getDatabaseTables()) { // new table
                            if (table.getName().equals(curTableName)) {
                                srcTable = schemaGraph.addVertex(table);
                                break;
                            }
                        }
                        schemaGraph.addEdge(schemaGraph.getVertices().get(curTable), srcTable, ddlStmt.name());

                        String referTableName = ddlQuery.getQueryAST().getChildByName("foreign_key_clause").getChildByName("_reference_table").getChildren().get(0).getToken().toString();
                        SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> referTable = null;
                        for (SchemaGraph.Vertex<PostgreSQLSchema.PostgreSQLTable> v : schemaGraph.getVertices().values()) { // existing table
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
        try (Statement disableFKChecks = genState.getConnection().createStatement();
             Statement showTables = genState.getConnection().createStatement();
             Statement dropTable = genState.getConnection().createStatement();
             Statement enableFKChecks = genState.getConnection().createStatement()) {

            // Disable foreign key checks
            disableFKChecks.execute("SET session_replication_role = 'replica'");

            // Get the list of all tables
            ResultSet resultSet = showTables.executeQuery("SELECT tablename FROM pg_tables WHERE schemaname = 'public';");
            while (resultSet.next()) {
                // Drop each table
                dropTable.execute("DROP TABLE IF EXISTS " + resultSet.getString(1) + " CASCADE");
            }

            // Enable foreign key checks
            enableFKChecks.execute("SET session_replication_role = 'origin'");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public List<String> replayCreateStmts(PostgreSQLGlobalState state, List<SQLQueryAdapter> createStmts) throws SQLException {
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
                    if (e.getMessage().contains("already exists")) {
                        createStmts.remove(i);
                    }
                    if (errorCount > 100) {
                        throw new AssertionError(e.getMessage());
                    }
                }
            }
        }
        return orderedStmts;
    }

    @Override
    public List<SQLQueryAdapter> fetchCreateStmts(PostgreSQLGlobalState state) throws SQLException {
        List<SQLQueryAdapter> createStmts = new ArrayList<>();

        Statement statement = state.getConnection().createStatement();
        for (PostgreSQLSchema.PostgreSQLTable table : state.getSchema().getDatabaseTables()) {
            String tableName = table.getName();
            try {
                if (table.isView()) {
                    String fetchCreateView = String.format("select view_definition, check_option from information_schema.views where table_name = '%s';", tableName);
                    ResultSet viewRes = statement.executeQuery(fetchCreateView);
                    String viewDefinition = null;
                    String checkOption = "NONE"; // default value
                    if (viewRes.next()) {
                        viewDefinition = viewRes.getString("view_definition");
                        checkOption = viewRes.getString("check_option");
                    }
                    viewRes.close();
                    if (viewDefinition == null) {
                        throw new AssertionError("buildSemiState: " + tableName);
                    }
                    viewDefinition = viewDefinition.replaceAll("\\r?\\n", "");
                    if (viewDefinition.endsWith(";")) {
                        viewDefinition = viewDefinition.substring(0, viewDefinition.length() - 1);
                    }
                    StringBuilder createView = new StringBuilder("CREATE");
                    if (table.isTemporary()) {
                        createView.append(" TEMPORARY");
                    }
                    createView.append(" VIEW ").append(tableName).append(" AS (").append(viewDefinition).append(")");
                    if (!checkOption.equals("NONE")) {
                        createView.append(" WITH ").append(checkOption).append(" CHECK OPTION");
                    }
                    createStmts.add(new SQLQueryAdapter(createView.toString()));
                } else {
                    String fetchColumnInfo = String.format("SELECT column_name, data_type, collation_name, character_maximum_length, column_default, is_nullable, is_generated, generation_expression, identity_generation FROM information_schema.columns WHERE table_name = '%s' ORDER BY column_name", tableName);
                    ResultSet columnRes = statement.executeQuery(fetchColumnInfo);
                    List<String> columns = new ArrayList<>();
                    while (columnRes.next()) {
                        String columnName = columnRes.getString("column_name");
                        String dataType = columnRes.getString("data_type");
                        String collation = columnRes.getString("collation_name");
                        String dataLength = columnRes.getString("character_maximum_length"); // for a character or bit string
                        boolean isNullable = columnRes.getBoolean("is_nullable");
                        String hasDefault = columnRes.getString("column_default");
                        String isGenerated = columnRes.getString("is_generated");
                        String generatedExpression = columnRes.getString("generation_expression");
                        String identityGeneration = columnRes.getString("identity_generation");

                        StringBuilder column = new StringBuilder(columnName);
                        column.append(" ").append(dataType);
                        if (dataLength != null) {
                            column.append("(").append(dataLength).append(")");
                        }
                        if (collation != null) {
                            column.append(" COLLATE \"").append(collation).append("\"");
                        }
                        if (!isNullable) {
                            column.append(" NOT NULL");
                        }
                        if (hasDefault != null) {
                            column.append(" ").append("DEFAULT ").append(hasDefault);
                        }
                        if (isGenerated.equals("ALWAYS")) {
                            column.append(" ").append("GENERATED ALWAYS AS (").append(generatedExpression).append(") STORED");
                        }
                        if (identityGeneration != null) {
                            column.append(" ").append("GENERATED ").append(identityGeneration).append(" AS IDENTITY");
                        }
                        columns.add(column.toString());
                    }
                    columnRes.close();

                    List<String> constraints = new ArrayList<>();
                    List<String> constraintNames = new ArrayList<>();
                    String fetchConstraintName = String.format("SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_NAME = '%s'", tableName);
                    ResultSet constraintNameRes = statement.executeQuery(fetchConstraintName);
                    while (constraintNameRes.next()) {
                        String constraintName = constraintNameRes.getString("CONSTRAINT_NAME");
                        constraintNames.add(constraintName);
                    }
                    constraintNameRes.close();
                    for (String constraintName : constraintNames) {
                        String fetchConstraint = String.format("SELECT pg_get_constraintdef(oid) as constraint FROM pg_constraint WHERE conname='%s';", constraintName);
                        ResultSet constraintRes = statement.executeQuery(fetchConstraint);
                        if (constraintRes.next()) {
                            String constraint = constraintRes.getString("constraint");
                            constraints.add(constraint);
                        }
                        constraintRes.close();
                    }

                    // obtain the parent of the table specified by inherits
                    List<String> parents = new ArrayList<>();
                    String fetchItsParent = String.format("SELECT relname FROM pg_class WHERE oid IN (SELECT inhparent FROM pg_inherits WHERE inhrelid=(SELECT oid FROM pg_class WHERE relname='%s'))", tableName);
                    ResultSet parentRes = statement.executeQuery(fetchItsParent);
                    while (parentRes.next()) {
                        String parent = parentRes.getString("relname");
                        parents.add(parent);
                    }
                    Collections.reverse(parents);
                    parentRes.close();

                    // obtain the table options
                    String tableOptions = null;
                    String fetchTableOptions = String.format("SELECT reloptions FROM pg_class WHERE relname = '%s';", tableName);
                    ResultSet optionRes = statement.executeQuery(fetchTableOptions);
                    if (optionRes.next()) {
                        tableOptions = optionRes.getString("reloptions");
                        if (tableOptions != null) {
                            tableOptions = tableOptions.replace("{", "(").replace("}", ")");
                        }
                    }
                    optionRes.close();

                    // obtain the persistence of a table
                    boolean isUnlogged = false;
                    String obtainUnlogged = String.format("SELECT relpersistence FROM pg_class WHERE relname = '%s';", tableName);
                    ResultSet unloggedRes = statement.executeQuery(obtainUnlogged);
                    if (unloggedRes.next()) {
                        isUnlogged = unloggedRes.getString("relpersistence").equals("u");
                    }
                    unloggedRes.close();

                    StringBuilder createTable = new StringBuilder("CREATE");

                    if (table.isTemporary()) {
                        createTable.append(" TEMPORARY");
                    } else if (isUnlogged) {
                        createTable.append(" UNLOGGED");
                    }

                    createTable.append(" TABLE ");
                    createTable.append(tableName).append(" (");
                    String columnDef = Strings.join(", ", columns);
                    createTable.append(columnDef);
                    if (!constraints.isEmpty()) {
                        createTable.append(", ");
                        String constraintDef = Strings.join(",", constraints);
                        createTable.append(constraintDef);
                    }
                    createTable.append(")");

                    if (!parents.isEmpty()) {
                        createTable.append(" INHERITS (").append(Strings.join(", ", parents)).append(")");
                    }

                    if (tableOptions != null) {
                        createTable.append(" WITH").append(tableOptions);
                    }

                    createStmts.add(new SQLQueryAdapter(createTable.toString()));

                    // obtain create index on table
                    String fetchIndexInfo = String.format("SELECT indexdef FROM pg_indexes WHERE tablename='%s'", tableName);
                    ResultSet indexRes = statement.executeQuery(fetchIndexInfo);
                    while (indexRes.next()) {
                        String indexInfo = indexRes.getString("indexdef");
                        createStmts.add(new SQLQueryAdapter(indexInfo));
                    }
                    indexRes.close();
                }
            } catch (SQLException ignored) {
            }
        }

        // obtain materialized views
        String fetchMatView = "SELECT matviewname, definition FROM pg_matviews WHERE schemaname = 'public';";
        ResultSet matViewRes = statement.executeQuery(fetchMatView);
        while (matViewRes.next()) {
            String matViewName = matViewRes.getString("matviewname");
            String matViewDef = matViewRes.getString("definition");
            if (matViewDef == null) {
                throw new AssertionError("buildSemiState: " + matViewName);
            }
            matViewDef = matViewDef.replaceAll("\\r?\\n", "");
            if (matViewDef.endsWith(";")) {
                matViewDef = matViewDef.substring(0, matViewDef.length() - 1);
            }
            String createMatView = String.format("CREATE MATERIALIZED VIEW %s AS (%s)", matViewName, matViewDef);
            createStmts.add(new SQLQueryAdapter(createMatView));
        }
        matViewRes.close();
        statement.close();

        return createStmts;
    }

    private boolean isEquivalentPostgresTable(PostgreSQLSchema.PostgreSQLTable table1, PostgreSQLSchema.PostgreSQLTable table2) {
        if (table1.getColumns().size() != table2.getColumns().size()) {
            return false;
        }
        if (table1.getIndexes().size() != table2.getIndexes().size()) {
            return false;
        }
        List<PostgreSQLSchema.PostgreSQLColumn> columns1 = new ArrayList<>(table1.getColumns());
        List<PostgreSQLSchema.PostgreSQLColumn> columns2 = new ArrayList<>(table2.getColumns());
        for (int i = 0; i < columns1.size(); i++) {
            for (int j = 0; j < columns2.size(); j++) {
                if (isEquivalentPostgreSQLColumn(columns1.get(i), columns2.get(j))) {
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
        List<PostgreSQLSchema.PostgreSQLIndex> indexes1 = new ArrayList<>(table1.getIndexes());
        List<PostgreSQLSchema.PostgreSQLIndex> indexes2 = new ArrayList<>(table2.getIndexes());
        for (int i = 0; i < indexes1.size(); i++) {
            for (int j = 0; j < indexes2.size(); j++) {
                if (isEquivalentPostgreSQLIndex(indexes1.get(i), indexes2.get(j))) {
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

    private boolean isEquivalentPostgreSQLIndex(PostgreSQLSchema.PostgreSQLIndex index1, PostgreSQLSchema.PostgreSQLIndex index2) {
        if (index1.isUnique() != index2.isUnique()) {
            return false;
        }
        if (index1.isPrimaryKey() != index2.isPrimaryKey()) {
            return false;
        }
        if (index1.getColumns().size() != index2.getColumns().size()) {
            return false;
        }
        for (int i = 0; i < index1.getColumns().size(); i++) {
            PostgreSQLSchema.PostgreSQLColumn column1 = index1.getColumns().get(i);
            PostgreSQLSchema.PostgreSQLColumn column2 = index2.getColumns().get(i);
            if (!isEquivalentPostgreSQLColumn(column1, column2)) {
                return false;
            }
        }

        return true;
    }

    private boolean isEquivalentPostgreSQLColumn(PostgreSQLSchema.PostgreSQLColumn column1, PostgreSQLSchema.PostgreSQLColumn column2) {
        return Objects.equals(column1.getDataType(), column2.getDataType()) &&
                column1.isPrimaryKey() == column2.isPrimaryKey() &&
                column1.isNullable() == column2.isNullable() &&
                Objects.equals(column1.getColumnDefault(), column2.getColumnDefault());
    }


    @Override
    public String generateSelectStmt(PostgreSQLGlobalState state) {
        return PostgreSQLQueryProvider.SELECT.getQuery(state).getQueryString();
    }

    @Override
    public SQLQueryAdapter generateDMLStmt(PostgreSQLGlobalState state) {
        for (int i = 0; i < 10; i++) {
            try {
                return PostgreSQLDMLStmt.getRandomDML(state);
            } catch (QueryGenerationException ignored) {
            }
        }

        return null;
    }

    @Override
    public String checkQueryPlan(String query, PostgreSQLGlobalState state) {
        PostgreSQLQueryPlan plan = getPostgreSQLQueryPlan(query, state);
        return plan.toString();
    }

    @Override
    public String getExecutionResult(String query, PostgreSQLGlobalState state) {
        String errorMessage = null;
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(query);
        } catch (SQLException e) {
            errorMessage = e.getMessage(); // a temporary mitigation for multiple constraint violations
        }
        return errorMessage;
    }

    @Override
    public boolean checkStmt(String stmt, PostgreSQLGlobalState state1, PostgreSQLGlobalState state2) {
        String manualResult = getExecutionResult(stmt, state1);
        String semiResult = getExecutionResult(stmt, state2);
        if ((manualResult == null && semiResult != null) || (manualResult != null && semiResult == null)) {
            throw new AssertionError(String.format("%s\n" +
                    "ManualState: %s\n" +
                    "SemiState: %s\n", stmt, manualResult, semiResult));
        }
        // when no exception happens
        if (manualResult != null) {
            state1.getState().logStatement(stmt);
        }
        return manualResult == null;
    }

    private PostgreSQLQueryPlan getPostgreSQLQueryPlan(String query, PostgreSQLGlobalState state) {
        PostgreSQLQueryPlan plan = new PostgreSQLQueryPlan();
        String checkQueryPlan = String.format("EXPLAIN (COSTS FALSE) %s", query);
        try (Statement statement = state.getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery(checkQueryPlan);
            while (resultSet.next()) {
                plan.queryPlan.add(resultSet.getString("QUERY PLAN"));
            }
        } catch (SQLException e) {
            plan.exception = e.getMessage();
        }

        return plan;
    }

    static class PostgreSQLQueryPlan {
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
