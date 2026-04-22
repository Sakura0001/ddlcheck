package dbradar.postgresql.oracle;

import com.beust.jcommander.Strings;
import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLGeneratedColumnSupport;
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

public class PostgreSQLEDCOracle extends EDCBase<PostgreSQLGlobalState> {

    private static final int MAX_BOOTSTRAP_GENERATION_ATTEMPTS = 1000;
    private PostgreSQLGeneratedColumnSupport.GeneratedColumnScenario bootstrapGeneratedColumnScenario;

    public PostgreSQLEDCOracle(PostgreSQLGlobalState state) {
        super(state);
        synState = new PostgreSQLGlobalState();
        PostgresCommon.addCommonExpressionErrors(EXPECTED_QUERY_ERRORS);
        PostgresCommon.addCommonFetchErrors(EXPECTED_QUERY_ERRORS);
    }

    @Override
    public void generateState(List<String> ddlSeq, int targetDdlCount) throws Exception {
        ddlSeq.clear();
        int randomDdlTarget = Math.max(targetDdlCount - 1, 0);
        if (randomDdlTarget > 0) {
            getDDLSequence(ddlSeq, randomDdlTarget);
        }
        appendGeneratedColumnBootstrapTable(ddlSeq);
    }

    @Override
    protected int populateRequiredBootstrapDml() throws SQLException {
        if (bootstrapGeneratedColumnScenario == null) {
            return 0;
        }

        String insert = bootstrapGeneratedColumnScenario.getInsertQuery().getQueryString();
        if (!checkStmt(insert, false)) {
            throw new AssertionError("Unable to insert into the bootstrap generated-column table: "
                    + bootstrapGeneratedColumnScenario.getTableName());
        }
        checkDQLStmt(bootstrapGeneratedColumnScenario.getValidationQuery());
        return 1;
    }


    public void getDDLSequence(List<String> ddlSeq, int targetDdlCount) {
        int attempts = 0;

        while (ddlSeq.size() < targetDdlCount) {
            if (attempts++ > MAX_BOOTSTRAP_GENERATION_ATTEMPTS) {
                throw new AssertionError(String.format(
                        "Unable to generate %d successful bootstrap DDL statements",
                        targetDdlCount));
            }

            PostgreSQLDDLStmt ddlStmt = chooseBootstrapDdlStmt(ddlSeq.size(), targetDdlCount);
            SQLQueryAdapter ddlQuery = null;
            for (int j = 0; j < 100; j++) {
                try {
                    ddlQuery = ddlStmt.getQueryProvider().getQuery(genState);
                    break;
                } catch (QueryGenerationException | IgnoreMeException ignored) {
                }
            }
            if (ddlQuery == null) continue;

            try (Statement stmt = genState.getConnection().createStatement()) {
                stmt.execute(ddlQuery.getQueryString());
                genState.updateSchema();
                ddlSeq.add(ddlQuery.getQueryString());
            } catch (Exception ignored) {
            }
        }

        if (genState.getSchema().getDatabaseTablesWithoutViews().isEmpty()) {
            throw new AssertionError("Bootstrap DDL sequence ended without a base table");
        }
    }

    private PostgreSQLDDLStmt chooseBootstrapDdlStmt(int currentSuccessfulCount, int targetDdlCount) {
        if (currentSuccessfulCount == 0 || genState.getSchema().getDatabaseTablesWithoutViews().isEmpty()) {
            return PostgreSQLDDLStmt.CREATE_TABLE;
        }

        int remainingStatements = targetDdlCount - currentSuccessfulCount;
        if (remainingStatements == 1 && genState.getSchema().getDatabaseTablesWithoutViews().size() <= 1) {
            return Randomly.fromOptions(
                    PostgreSQLDDLStmt.CREATE_TABLE,
                    PostgreSQLDDLStmt.CREATE_INDEX,
                    PostgreSQLDDLStmt.CREATE_VIEW,
                    PostgreSQLDDLStmt.ALTER_TABLE_ADD_COLUMN,
                    PostgreSQLDDLStmt.ALTER_TABLE_DROP_COLUMN,
                    PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_TYPE,
                    PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_DROP_DEFAULT,
                    PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_SET_DEFAULT,
                    PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_SET_NOT_NULL,
                    PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL,
                    PostgreSQLDDLStmt.ALTER_TABLE_SET_COLUMN,
                    PostgreSQLDDLStmt.ALTER_TABLE_RESET_COLUMN,
                    PostgreSQLDDLStmt.ALTER_TABLE_ALTER_COLUMN_SET_STORAGE,
                    PostgreSQLDDLStmt.ALTER_TABLE_ADD_UNIQUE_KEY,
                    PostgreSQLDDLStmt.ALTER_TABLE_ADD_PRIMARY_KEY,
                    PostgreSQLDDLStmt.ALTER_TABLE_ADD_FOREIGN_KEY,
                    PostgreSQLDDLStmt.ALTER_TABLE_OPTION,
                    PostgreSQLDDLStmt.ALTER_TABLE_RENAME_TABLE,
                    PostgreSQLDDLStmt.REINDEX,
                    PostgreSQLDDLStmt.TRUNCATE_TABLE,
                    PostgreSQLDDLStmt.DROP_INDEX,
                    PostgreSQLDDLStmt.DROP_VIEW);
        }
        return Randomly.fromOptions(PostgreSQLDDLStmt.values());
    }

    private void appendGeneratedColumnBootstrapTable(List<String> ddlSeq) throws Exception {
        bootstrapGeneratedColumnScenario = PostgreSQLGeneratedColumnSupport.createStoredGeneratedTable(genState);
        try (Statement stmt = genState.getConnection().createStatement()) {
            stmt.execute(bootstrapGeneratedColumnScenario.getCreateTableQuery().getQueryString());
            genState.updateSchema();
            ddlSeq.add(bootstrapGeneratedColumnScenario.getCreateTableQuery().getQueryString());
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
                    String fetchColumnInfo = String.format("SELECT column_name, data_type, collation_name, character_maximum_length, column_default, is_nullable, is_generated, generation_expression, identity_generation FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position", tableName);
                    ResultSet columnRes = statement.executeQuery(fetchColumnInfo);
                    List<String> columns = new ArrayList<>();
                    while (columnRes.next()) {
                        String columnName = columnRes.getString("column_name");
                        String dataType = columnRes.getString("data_type");
                        String collation = columnRes.getString("collation_name");
                        String dataLength = columnRes.getString("character_maximum_length"); // for a character or bit string
                        boolean isNullable = "YES".equals(columnRes.getString("is_nullable"));
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

    @Override
    public String generateSelectStmt(PostgreSQLGlobalState state) {
        return PostgreSQLQueryProvider.SELECT.getQuery(state).getQueryString();
    }

    @Override
    public SQLQueryAdapter generateDMLStmt(PostgreSQLGlobalState state) {
        for (int i = 0; i < 10; i++) {
            try {
                return PostgreSQLDMLStmt.getRandomDML(state);
            } catch (QueryGenerationException | IgnoreMeException ignored) {
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
