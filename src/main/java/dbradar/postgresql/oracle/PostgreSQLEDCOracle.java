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
import dbradar.postgresql.PostgreSQLTypeCoverageSupport;
import dbradar.postgresql.PostgresCommon;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PostgreSQLEDCOracle extends EDCBase<PostgreSQLGlobalState> {

    private static final int MAX_BOOTSTRAP_GENERATION_ATTEMPTS = 1000;
    private List<PostgreSQLGeneratedColumnSupport.GeneratedColumnScenario> bootstrapGeneratedColumnScenarios = List.of();
    private PostgreSQLTypeCoverageSupport.CoveragePlan typeCoveragePlan;
    private boolean builtInCoverageTableCreated;
    private boolean userDefinedCoverageTableCreated;

    public PostgreSQLEDCOracle(PostgreSQLGlobalState state) {
        super(state);
        synState = new PostgreSQLGlobalState();
        PostgresCommon.addCommonExpressionErrors(expectedQueryErrors);
        PostgresCommon.addCommonFetchErrors(expectedQueryErrors);
    }

    @Override
    public void generateState(List<String> ddlSeq, int targetDdlCount) throws Exception {
        ddlSeq.clear();
        int randomDdlTarget = Math.max(targetDdlCount - getMandatoryBootstrapDdlCount(targetDdlCount), 0);
        if (randomDdlTarget > 0) {
            getDDLSequence(ddlSeq, randomDdlTarget);
        }
        appendGeneratedColumnBootstrapTables(ddlSeq);
        appendTypeCoverageBootstrapObjects(ddlSeq, targetDdlCount);
    }

    @Override
    protected int populateRequiredBootstrapDml() throws SQLException {
        int targetSuccessfulDml = genState.getOptions().getDmlCount();
        if (targetSuccessfulDml <= 0) {
            return 0;
        }
        int successfulStatements = 0;
        for (PostgreSQLGeneratedColumnSupport.GeneratedColumnScenario scenario : bootstrapGeneratedColumnScenarios) {
            if (successfulStatements >= targetSuccessfulDml) {
                break;
            }
            successfulStatements += executeBootstrapScenario(scenario);
        }
        if (successfulStatements < targetSuccessfulDml && builtInCoverageTableCreated) {
            successfulStatements += executeBootstrapScenario(typeCoveragePlan.getBuiltInScenario());
        }
        if (successfulStatements < targetSuccessfulDml && userDefinedCoverageTableCreated) {
            successfulStatements += executeBootstrapScenario(typeCoveragePlan.getUserDefinedScenario());
        }
        return successfulStatements;
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
            SQLQueryAdapter ddlQuery = generateBootstrapDdlQuery(ddlStmt);
            if (ddlQuery == null) continue;

            try (Statement stmt = genState.getConnection().createStatement()) {
                stmt.execute(ddlQuery.getQueryString());
                genState.updateSchema();
                ddlSeq.add(ddlQuery.getQueryString());
            } catch (Exception ignored) {
            }
        }

        ensureBootstrapBaseTable(ddlSeq);
    }

    private SQLQueryAdapter generateBootstrapDdlQuery(PostgreSQLDDLStmt ddlStmt) {
        for (int j = 0; j < 100; j++) {
            try {
                return ddlStmt.getQueryProvider().getQuery(genState);
            } catch (QueryGenerationException | IgnoreMeException ignored) {
            }
        }
        return null;
    }

    private void ensureBootstrapBaseTable(List<String> ddlSeq) {
        if (!genState.getSchema().getDatabaseTablesWithoutViews().isEmpty()) {
            return;
        }
        for (int attempts = 0; attempts < MAX_BOOTSTRAP_GENERATION_ATTEMPTS; attempts++) {
            SQLQueryAdapter createTable = generateBootstrapDdlQuery(PostgreSQLDDLStmt.CREATE_TABLE);
            if (createTable == null) {
                continue;
            }
            try (Statement stmt = genState.getConnection().createStatement()) {
                stmt.execute(createTable.getQueryString());
                genState.updateSchema();
                ddlSeq.add(createTable.getQueryString());
                if (!genState.getSchema().getDatabaseTablesWithoutViews().isEmpty()) {
                    return;
                }
            } catch (Exception ignored) {
            }
        }
        throw new AssertionError("Unable to generate a bootstrap base table");
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

    private void appendGeneratedColumnBootstrapTables(List<String> ddlSeq) throws Exception {
        bootstrapGeneratedColumnScenarios = PostgreSQLGeneratedColumnSupport.createBootstrapScenarios(genState);
        for (PostgreSQLGeneratedColumnSupport.GeneratedColumnScenario scenario : bootstrapGeneratedColumnScenarios) {
            try (Statement stmt = genState.getConnection().createStatement()) {
                stmt.execute(scenario.getCreateTableQuery().getQueryString());
                genState.updateSchema();
                ddlSeq.add(scenario.getCreateTableQuery().getQueryString());
            }
        }
    }

    private void appendTypeCoverageBootstrapObjects(List<String> ddlSeq, int targetDdlCount) throws Exception {
        builtInCoverageTableCreated = false;
        userDefinedCoverageTableCreated = false;
        typeCoveragePlan = PostgreSQLTypeCoverageSupport.createCoveragePlan(genState);
        int remainingBudget = targetDdlCount - bootstrapGeneratedColumnScenarios.size();

        if (remainingBudget < 1) {
            return;
        }
        executeBootstrapDdl(typeCoveragePlan.getTypeSetupQuery(), ddlSeq);

        if (remainingBudget < 2) {
            return;
        }
        executeBootstrapDdl(typeCoveragePlan.getBuiltInScenario().getCreateTableQuery(), ddlSeq);
        builtInCoverageTableCreated = true;

        if (remainingBudget < 3) {
            return;
        }
        executeBootstrapDdl(typeCoveragePlan.getUserDefinedScenario().getCreateTableQuery(), ddlSeq);
        userDefinedCoverageTableCreated = true;
    }

    private void executeBootstrapDdl(SQLQueryAdapter query, List<String> ddlSeq) throws Exception {
        try (Statement stmt = genState.getConnection().createStatement()) {
            stmt.execute(query.getQueryString());
            genState.updateSchema();
            ddlSeq.add(query.getQueryString());
        }
    }

    private int executeBootstrapScenario(PostgreSQLGeneratedColumnSupport.GeneratedColumnScenario scenario) throws SQLException {
        if (scenario == null) {
            return 0;
        }
        String insert = scenario.getInsertQuery().getQueryString();
        if (!checkStmt(insert, false)) {
            throw new AssertionError("Unable to insert into the bootstrap generated-column table: "
                    + scenario.getTableName());
        }
        checkDQLStmt(scenario.getValidationQuery());
        return 1;
    }

    private int executeBootstrapScenario(PostgreSQLTypeCoverageSupport.BootstrapScenario scenario) throws SQLException {
        if (scenario == null) {
            return 0;
        }
        String insert = scenario.getInsertQuery().getQueryString();
        if (!checkStmt(insert, false)) {
            throw new AssertionError("Unable to insert into the bootstrap type-coverage table: "
                    + scenario.getTableName());
        }
        checkDQLStmt(scenario.getValidationQuery());
        return 1;
    }

    private int getMandatoryBootstrapDdlCount(int targetDdlCount) {
        int generatedColumnCount = PostgreSQLGeneratedColumnSupport
                .getBootstrapGeneratedColumnKinds(genState.getServerVersionNum(), targetDdlCount).size();
        int typeCoverageCount = Math.min(Math.max(targetDdlCount - generatedColumnCount, 0), 3);
        return generatedColumnCount + typeCoverageCount;
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
                    i--;
                } catch (SQLException e) {
                    errorCount++;
                    if (e.getMessage().contains("already exists")) {
                        createStmts.remove(i);
                        i--;
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
        List<SQLQueryAdapter> indexStmts = new ArrayList<>();
        List<SQLQueryAdapter> postCreateStmts = new ArrayList<>();

        Statement statement = state.getConnection().createStatement();
        createStmts.addAll(fetchCustomTypeCreateStmts(statement));
        for (PostgreSQLSchema.PostgreSQLTable table : state.getSchema().getDatabaseTables()) {
            String tableName = table.getName();
            String schemaMatcher = table.isTemporary() ? "LIKE 'pg_temp_%'" : "= 'public'";
            try {
                if (table.isView()) {
                    String fetchCreateView = String.format(
                            "select view_definition, check_option from information_schema.views where table_name = '%s' and table_schema %s;",
                            tableName, schemaMatcher);
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
                    String fetchColumnInfo = String.format(
                            "SELECT c.column_name, "
                                    + "pg_catalog.format_type(a.atttypid, a.atttypmod) AS formatted_type, "
                                    + "c.collation_name, c.column_default, c.is_nullable, c.is_generated, "
                                    + "c.generation_expression, c.identity_generation, "
                                    + "CASE a.attgenerated WHEN 'v' THEN 'VIRTUAL' WHEN 's' THEN 'STORED' ELSE NULL END AS generated_kind "
                                    + "FROM information_schema.columns c "
                                    + "JOIN pg_class pc ON pc.relname = c.table_name "
                                    + "JOIN pg_namespace pn ON pn.oid = pc.relnamespace AND pn.nspname = c.table_schema "
                                    + "JOIN pg_attribute a ON a.attrelid = pc.oid AND a.attname = c.column_name "
                                    + "WHERE c.table_schema %s AND c.table_name = '%s' "
                                    + "AND a.attnum > 0 AND NOT a.attisdropped "
                                    + "ORDER BY c.ordinal_position",
                            schemaMatcher, tableName);
                    ResultSet columnRes = statement.executeQuery(fetchColumnInfo);
                    List<String> columns = new ArrayList<>();
                    while (columnRes.next()) {
                        String columnName = columnRes.getString("column_name");
                        String dataType = columnRes.getString("formatted_type");
                        String collation = columnRes.getString("collation_name");
                        boolean isNullable = "YES".equals(columnRes.getString("is_nullable"));
                        String hasDefault = columnRes.getString("column_default");
                        String isGenerated = columnRes.getString("is_generated");
                        String generatedExpression = columnRes.getString("generation_expression");
                        String identityGeneration = columnRes.getString("identity_generation");
                        String generatedKind = columnRes.getString("generated_kind");

                        StringBuilder column = new StringBuilder(columnName);
                        column.append(" ").append(dataType);
                        if (collation != null) {
                            column.append(" COLLATE \"").append(collation).append("\"");
                        }
                        if (!isNullable) {
                            column.append(" NOT NULL");
                        }
                        if (hasDefault != null) {
                            column.append(" ").append("DEFAULT ").append(hasDefault);
                        }
                        if ("ALWAYS".equals(isGenerated)) {
                            PostgreSQLGeneratedColumnSupport.GeneratedColumnKind kind =
                                    "VIRTUAL".equals(generatedKind)
                                            ? PostgreSQLGeneratedColumnSupport.GeneratedColumnKind.VIRTUAL
                                            : PostgreSQLGeneratedColumnSupport.GeneratedColumnKind.STORED;
                            column.append(" ").append(
                                    PostgreSQLGeneratedColumnSupport.renderGeneratedColumnClause(generatedExpression,
                                            kind));
                        }
                        if (identityGeneration != null) {
                            column.append(" ").append("GENERATED ").append(identityGeneration).append(" AS IDENTITY");
                        }
                        columns.add(column.toString());
                    }
                    columnRes.close();

                    List<String> constraints = fetchTableConstraintDefs(statement, tableName, schemaMatcher);
                    List<String> foreignKeys = fetchForeignKeyConstraintDefs(statement, tableName, schemaMatcher);

                    // obtain the parent of the table specified by inherits
                    List<String> parents = new ArrayList<>();
                    if (!table.isPartition()) {
                        String fetchItsParent = String.format(
                                "SELECT parent.relname "
                                        + "FROM pg_class child "
                                        + "JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace "
                                        + "JOIN pg_inherits i ON i.inhrelid = child.oid "
                                        + "JOIN pg_class parent ON parent.oid = i.inhparent "
                                        + "WHERE child.relname='%s' AND child_ns.nspname %s",
                                tableName, schemaMatcher);
                        ResultSet parentRes = statement.executeQuery(fetchItsParent);
                        while (parentRes.next()) {
                            String parent = parentRes.getString("relname");
                            parents.add(parent);
                        }
                        Collections.reverse(parents);
                        parentRes.close();
                    }

                    // obtain the table options
                    String tableOptions = null;
                    String fetchTableOptions = String.format(
                            "SELECT c.reloptions "
                                    + "FROM pg_class c "
                                    + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                                    + "WHERE c.relname = '%s' AND n.nspname %s;",
                            tableName, schemaMatcher);
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
                    String obtainUnlogged = String.format(
                            "SELECT c.relpersistence "
                                    + "FROM pg_class c "
                                    + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                                    + "WHERE c.relname = '%s' AND n.nspname %s;",
                            tableName, schemaMatcher);
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

                    createTable.append(" TABLE ").append(tableName);
                    if (table.isPartition()) {
                        createTable.append(" PARTITION OF ")
                                .append(table.getPartitionParentName())
                                .append(" ")
                                .append(table.getPartitionBound());
                    } else {
                        createTable.append(" (");
                        String columnDef = Strings.join(", ", columns);
                        createTable.append(columnDef);
                        if (!constraints.isEmpty()) {
                            createTable.append(", ");
                            String constraintDef = Strings.join(",", constraints);
                            createTable.append(constraintDef);
                        }
                        createTable.append(")");

                        if (table.isPartitionedTable()) {
                            createTable.append(" PARTITION BY ").append(table.getPartitionKeyDefinition());
                        } else if (!parents.isEmpty()) {
                            createTable.append(" INHERITS (").append(Strings.join(", ", parents)).append(")");
                        }
                    }

                    if (tableOptions != null) {
                        createTable.append(" WITH").append(tableOptions);
                    }

                    createStmts.add(new SQLQueryAdapter(createTable.toString()));
                    for (String foreignKey : foreignKeys) {
                        postCreateStmts.add(new SQLQueryAdapter("ALTER TABLE " + tableName + " ADD " + foreignKey));
                    }

                    // Rebuild standalone indexes for both public and temp tables.
                    String fetchIndexInfo = String.format(
                            "SELECT indexdef FROM pg_indexes WHERE schemaname %s AND tablename='%s'",
                            schemaMatcher, tableName);
                    ResultSet indexRes = statement.executeQuery(fetchIndexInfo);
                    while (indexRes.next()) {
                        String indexInfo = indexRes.getString("indexdef");
                        indexStmts.add(new SQLQueryAdapter(indexInfo));
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
        createStmts.addAll(indexStmts);
        createStmts.addAll(postCreateStmts);
        statement.close();

        return createStmts;
    }

    private List<String> fetchTableConstraintDefs(Statement statement, String tableName, String schemaMatcher)
            throws SQLException {
        List<String> constraints = new ArrayList<>();
        String fetchConstraints = String.format(
                "SELECT pg_get_constraintdef(c.oid, true) AS constraint_def "
                        + "FROM pg_constraint c "
                        + "JOIN pg_class r ON r.oid = c.conrelid "
                        + "JOIN pg_namespace n ON n.oid = r.relnamespace "
                        + "WHERE r.relname = %s AND n.nspname %s "
                        + "AND c.contype <> 'f' "
                        + "ORDER BY c.conname",
                quoteLiteral(tableName), schemaMatcher);
        try (ResultSet constraintRes = statement.executeQuery(fetchConstraints)) {
            while (constraintRes.next()) {
                constraints.add(constraintRes.getString("constraint_def"));
            }
        }
        return constraints;
    }

    private List<String> fetchForeignKeyConstraintDefs(Statement statement, String tableName, String schemaMatcher)
            throws SQLException {
        List<String> foreignKeys = new ArrayList<>();
        String fetchForeignKeys = String.format(
                "SELECT pg_get_constraintdef(c.oid, true) AS constraint_def "
                        + "FROM pg_constraint c "
                        + "JOIN pg_class r ON r.oid = c.conrelid "
                        + "JOIN pg_namespace n ON n.oid = r.relnamespace "
                        + "WHERE r.relname = %s AND n.nspname %s "
                        + "AND c.contype = 'f' AND c.conislocal "
                        + "ORDER BY c.conname",
                quoteLiteral(tableName), schemaMatcher);
        try (ResultSet foreignKeyRes = statement.executeQuery(fetchForeignKeys)) {
            while (foreignKeyRes.next()) {
                foreignKeys.add(foreignKeyRes.getString("constraint_def"));
            }
        }
        return foreignKeys;
    }

    private List<SQLQueryAdapter> fetchCustomTypeCreateStmts(Statement statement) throws SQLException {
        List<SQLQueryAdapter> createStmts = new ArrayList<>();
        createStmts.addAll(fetchEnumTypeCreateStmts(statement));
        createStmts.addAll(fetchDomainCreateStmts(statement));
        createStmts.addAll(fetchCompositeTypeCreateStmts(statement));
        return createStmts;
    }

    private List<SQLQueryAdapter> fetchEnumTypeCreateStmts(Statement statement) throws SQLException {
        Map<String, List<String>> enumLabels = new LinkedHashMap<>();
        String fetchEnums = "SELECT t.typname, e.enumlabel "
                + "FROM pg_type t "
                + "JOIN pg_namespace n ON n.oid = t.typnamespace "
                + "JOIN pg_enum e ON e.enumtypid = t.oid "
                + "WHERE n.nspname = 'public' AND t.typtype = 'e' "
                + "ORDER BY t.typname, e.enumsortorder";
        try (ResultSet enumRes = statement.executeQuery(fetchEnums)) {
            while (enumRes.next()) {
                enumLabels.computeIfAbsent(enumRes.getString("typname"), key -> new ArrayList<>())
                        .add(enumRes.getString("enumlabel"));
            }
        }

        List<SQLQueryAdapter> createStmts = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : enumLabels.entrySet()) {
            List<String> escapedLabels = entry.getValue().stream()
                    .map(PostgreSQLEDCOracle::quoteLiteral)
                    .toList();
            String createEnum = String.format("CREATE TYPE %s AS ENUM (%s)",
                    entry.getKey(), Strings.join(", ", escapedLabels));
            createStmts.add(new SQLQueryAdapter(createEnum));
        }
        return createStmts;
    }

    private List<SQLQueryAdapter> fetchDomainCreateStmts(Statement statement) throws SQLException {
        Map<String, DomainDefinition> domains = new LinkedHashMap<>();
        String fetchDomains = "SELECT t.typname, "
                + "pg_catalog.format_type(t.typbasetype, t.typtypmod) AS base_type, "
                + "t.typnotnull, "
                + "pg_get_expr(t.typdefaultbin, 0, true) AS default_expr, "
                + "pg_get_constraintdef(c.oid, true) AS constraint_def "
                + "FROM pg_type t "
                + "JOIN pg_namespace n ON n.oid = t.typnamespace "
                + "LEFT JOIN pg_constraint c ON c.contypid = t.oid "
                + "WHERE n.nspname = 'public' AND t.typtype = 'd' "
                + "ORDER BY t.typname, c.conname";
        try (ResultSet domainRes = statement.executeQuery(fetchDomains)) {
            while (domainRes.next()) {
                String typeName = domainRes.getString("typname");
                String baseType = domainRes.getString("base_type");
                boolean notNull = domainRes.getBoolean("typnotnull");
                String defaultExpression = domainRes.getString("default_expr");
                DomainDefinition definition = domains.computeIfAbsent(typeName, key -> new DomainDefinition(
                        typeName, baseType, notNull, defaultExpression));
                String constraint = domainRes.getString("constraint_def");
                if (constraint != null) {
                    definition.constraints.add(constraint);
                }
            }
        }

        List<SQLQueryAdapter> createStmts = new ArrayList<>();
        for (DomainDefinition definition : domains.values()) {
            StringBuilder createDomain = new StringBuilder(
                    String.format("CREATE DOMAIN %s AS %s", definition.name, definition.baseType));
            if (definition.defaultExpression != null) {
                createDomain.append(" DEFAULT ").append(definition.defaultExpression);
            }
            if (definition.notNull) {
                createDomain.append(" NOT NULL");
            }
            for (String constraint : definition.constraints) {
                createDomain.append(" ").append(constraint);
            }
            createStmts.add(new SQLQueryAdapter(createDomain.toString()));
        }
        return createStmts;
    }

    private List<SQLQueryAdapter> fetchCompositeTypeCreateStmts(Statement statement) throws SQLException {
        Map<String, List<String>> compositeAttributes = new LinkedHashMap<>();
        String fetchCompositeTypes = "SELECT t.typname, a.attname, "
                + "pg_catalog.format_type(a.atttypid, a.atttypmod) AS attribute_type "
                + "FROM pg_type t "
                + "JOIN pg_namespace n ON n.oid = t.typnamespace "
                + "JOIN pg_class c ON c.oid = t.typrelid AND c.relkind = 'c' "
                + "JOIN pg_attribute a ON a.attrelid = c.oid "
                + "WHERE n.nspname = 'public' AND t.typtype = 'c' "
                + "AND a.attnum > 0 AND NOT a.attisdropped "
                + "ORDER BY t.typname, a.attnum";
        try (ResultSet compositeRes = statement.executeQuery(fetchCompositeTypes)) {
            while (compositeRes.next()) {
                compositeAttributes.computeIfAbsent(compositeRes.getString("typname"), key -> new ArrayList<>())
                        .add(compositeRes.getString("attname") + " " + compositeRes.getString("attribute_type"));
            }
        }

        List<SQLQueryAdapter> createStmts = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : compositeAttributes.entrySet()) {
            String createComposite = String.format("CREATE TYPE %s AS (%s)",
                    entry.getKey(), Strings.join(", ", entry.getValue()));
            createStmts.add(new SQLQueryAdapter(createComposite));
        }
        return createStmts;
    }

    private static String quoteLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static final class DomainDefinition {
        private final String name;
        private final String baseType;
        private final boolean notNull;
        private final String defaultExpression;
        private final List<String> constraints = new ArrayList<>();

        private DomainDefinition(String name, String baseType, boolean notNull, String defaultExpression) {
            this.name = name;
            this.baseType = baseType;
            this.notNull = notNull;
            this.defaultExpression = defaultExpression;
        }
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
