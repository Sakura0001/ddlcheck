package dbradar.postgresql;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.postgresql.PostgreSQLSchema.PostgreSQLColumn;
import dbradar.postgresql.PostgreSQLSchema.PostgreSQLDataType;
import dbradar.postgresql.PostgreSQLSchema.PostgreSQLTable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class PostgreSQLSelectQueryBuilder {

    private static final int MAX_LIMIT = 100;
    private final PostgreSQLGlobalState state;

    private PostgreSQLSelectQueryBuilder(PostgreSQLGlobalState state) {
        this.state = state;
    }

    public static String generate(PostgreSQLGlobalState state) {
        return new PostgreSQLSelectQueryBuilder(state).generateSelect();
    }

    private String generateSelect() {
        List<PostgreSQLTable> tables = candidateTables();
        if (tables.isEmpty()) {
            throw new IgnoreMeException("There are no tables available for SELECT generation.");
        }

        for (SelectKind kind : Randomly.nonEmptySubset(SelectKind.SIMPLE, SelectKind.JOIN,
                SelectKind.AGGREGATE, SelectKind.COMPOUND, SelectKind.CTE)) {
            String query = tryGenerate(kind, tables);
            if (query != null) {
                return query;
            }
        }
        return generateSimpleSelect(Randomly.fromList(tables));
    }

    private String tryGenerate(SelectKind kind, List<PostgreSQLTable> tables) {
        switch (kind) {
            case SIMPLE:
                return generateSimpleSelect(Randomly.fromList(tables));
            case JOIN:
                return generateJoinSelect(tables);
            case AGGREGATE:
                return generateAggregateSelect(tables);
            case COMPOUND:
                return generateCompoundSelect(tables);
            case CTE:
                return generateCteSelect(tables);
            default:
                throw new AssertionError("Unhandled SELECT kind: " + kind);
        }
    }

    private String generateSimpleSelect(PostgreSQLTable table) {
        List<PostgreSQLColumn> columns = selectableColumns(table);
        if (columns.isEmpty()) {
            return "SELECT COUNT(*) AS ca1 FROM " + quoteIdentifier(table.getName()) + " AS t0"
                    + deterministicLimit(1);
        }
        List<PostgreSQLColumn> selectedColumns = Randomly.nonEmptySubset(columns,
                Randomly.getNotCachedInteger(1, Math.min(columns.size(), 3) + 1));
        String projection = projectionList(selectedColumns, "t0", 1);
        StringBuilder query = new StringBuilder("SELECT ");
        query.append(projection)
                .append(" FROM ")
                .append(quoteIdentifier(table.getName()))
                .append(" AS t0");
        appendWhereClause(query, table, "t0");
        query.append(deterministicLimit(selectedColumns.size()));
        return query.toString();
    }

    private String generateJoinSelect(List<PostgreSQLTable> tables) {
        if (tables.size() < 2) {
            return null;
        }
        PostgreSQLTable left = Randomly.fromList(tables);
        PostgreSQLTable right = Randomly.fromList(tables);
        List<ColumnPair> joinPairs = compatibleJoinPairs(left, right);
        if (joinPairs.isEmpty()) {
            return null;
        }
        ColumnPair joinPair = Randomly.fromList(joinPairs);
        List<ProjectedColumn> projectedColumns = new ArrayList<>();
        for (PostgreSQLColumn column : selectableColumns(left)) {
            projectedColumns.add(new ProjectedColumn("t0", column));
        }
        for (PostgreSQLColumn column : selectableColumns(right)) {
            projectedColumns.add(new ProjectedColumn("t1", column));
        }
        if (projectedColumns.isEmpty()) {
            return null;
        }
        List<ProjectedColumn> selectedColumns = Randomly.nonEmptySubset(projectedColumns,
                Randomly.getNotCachedInteger(1, Math.min(projectedColumns.size(), 3) + 1));
        String projection = projectedColumns(selectedColumns);
        return "SELECT " + projection
                + " FROM " + quoteIdentifier(left.getName()) + " AS t0"
                + " INNER JOIN " + quoteIdentifier(right.getName()) + " AS t1"
                + " ON " + qualifiedColumn("t0", joinPair.left)
                + " = " + qualifiedColumn("t1", joinPair.right)
                + deterministicLimit(selectedColumns.size());
    }

    private String generateAggregateSelect(List<PostgreSQLTable> tables) {
        PostgreSQLTable table = Randomly.fromList(tables);
        List<PostgreSQLColumn> groupableColumns = table.getColumns().stream()
                .filter(this::isGroupable)
                .collect(Collectors.toList());
        if (groupableColumns.isEmpty()) {
            return null;
        }
        PostgreSQLColumn groupColumn = Randomly.fromList(groupableColumns);
        String groupExpression = qualifiedColumn("t0", groupColumn);
        return "SELECT " + castAsText(groupExpression) + " AS ca1, COUNT(*) AS ca2"
                + " FROM " + quoteIdentifier(table.getName()) + " AS t0"
                + " GROUP BY " + groupExpression
                + deterministicLimit(2);
    }

    private String generateCompoundSelect(List<PostgreSQLTable> tables) {
        if (tables.size() < 2) {
            return null;
        }
        PostgreSQLTable left = Randomly.fromList(tables);
        PostgreSQLTable right = Randomly.fromList(tables);
        List<PostgreSQLColumn> leftColumns = selectableColumns(left);
        List<PostgreSQLColumn> rightColumns = selectableColumns(right);
        if (leftColumns.isEmpty() || rightColumns.isEmpty()) {
            return null;
        }
        PostgreSQLColumn leftColumn = Randomly.fromList(leftColumns);
        PostgreSQLColumn rightColumn = Randomly.fromList(rightColumns);
        String operator = Randomly.fromOptions("UNION ALL", "UNION", "INTERSECT", "EXCEPT");
        return "SELECT CAST(" + qualifiedColumn("t0", leftColumn) + " AS TEXT) AS ca1"
                + " FROM " + quoteIdentifier(left.getName()) + " AS t0"
                + " " + operator + " "
                + "SELECT CAST(" + qualifiedColumn("t1", rightColumn) + " AS TEXT) AS ca1"
                + " FROM " + quoteIdentifier(right.getName()) + " AS t1"
                + deterministicLimit(1);
    }

    private String generateCteSelect(List<PostgreSQLTable> tables) {
        PostgreSQLTable table = Randomly.fromList(tables);
        List<PostgreSQLColumn> columns = selectableColumns(table);
        if (columns.isEmpty()) {
            return null;
        }
        List<PostgreSQLColumn> selectedColumns = Randomly.nonEmptySubset(columns,
                Randomly.getNotCachedInteger(1, Math.min(columns.size(), 3) + 1));
        String innerProjection = projectionList(selectedColumns, "t0", 1);
        String outerProjection = outerCteProjection(selectedColumns.size());
        StringBuilder cte = new StringBuilder("WITH q AS (SELECT ");
        cte.append(innerProjection)
                .append(" FROM ")
                .append(quoteIdentifier(table.getName()))
                .append(" AS t0");
        appendWhereClause(cte, table, "t0");
        cte.append(deterministicLimit(selectedColumns.size())).append(") ");
        cte.append("SELECT ").append(outerProjection).append(" FROM q")
                .append(deterministicLimit(selectedColumns.size()));
        return cte.toString();
    }

    private void appendWhereClause(StringBuilder query, PostgreSQLTable table, String alias) {
        List<PostgreSQLColumn> columns = table.getColumns().stream()
                .filter(this::isPredicateColumn)
                .collect(Collectors.toList());
        if (columns.isEmpty() || !Randomly.getBoolean()) {
            return;
        }
        PostgreSQLColumn column = Randomly.fromList(columns);
        query.append(" WHERE ").append(predicate(alias, column));
    }

    private String predicate(String alias, PostgreSQLColumn column) {
        String columnName = qualifiedColumn(alias, column);
        switch (column.getType()) {
            case INT:
                return columnName + " >= " + Randomly.getNotCachedInteger(-50, 51);
            case DECIMAL:
            case FLOAT:
            case REAL:
            case MONEY:
                return columnName + " IS NOT NULL";
            case BOOLEAN:
                return columnName + " IS " + (Randomly.getBoolean() ? "TRUE" : "FALSE");
            case TEXT:
            case ENUM:
                return "CAST(" + columnName + " AS TEXT) LIKE '" + Randomly.fromOptions("a", "b", "c") + "_%'";
            case DATE:
                return columnName + " >= DATE '2023-01-01'";
            case TIME:
            case TIMESTAMP:
            case UUID:
            case INET:
            case CIDR:
            case MACADDR:
            case BIT:
                return columnName + " IS NOT NULL";
            default:
                return columnName + " IS NOT NULL";
        }
    }

    private List<PostgreSQLTable> candidateTables() {
        return state.getSchema().getDatabaseTablesWithoutViews().stream()
                .filter(table -> !table.getColumns().isEmpty())
                .collect(Collectors.toList());
    }

    private List<PostgreSQLColumn> selectableColumns(PostgreSQLTable table) {
        return table.getColumns().stream()
                .filter(this::isSelectable)
                .collect(Collectors.toList());
    }

    private List<ColumnPair> compatibleJoinPairs(PostgreSQLTable left, PostgreSQLTable right) {
        List<ColumnPair> pairs = new ArrayList<>();
        for (PostgreSQLColumn leftColumn : left.getColumns()) {
            if (!isJoinable(leftColumn)) {
                continue;
            }
            for (PostgreSQLColumn rightColumn : right.getColumns()) {
                if (areJoinCompatible(leftColumn, rightColumn)) {
                    pairs.add(new ColumnPair(leftColumn, rightColumn));
                }
            }
        }
        return pairs;
    }

    private String projectionList(List<PostgreSQLColumn> columns, String alias, int firstAliasIndex) {
        List<ProjectedColumn> projectedColumns = new ArrayList<>();
        for (PostgreSQLColumn column : columns) {
            projectedColumns.add(new ProjectedColumn(alias, column));
        }
        return projectedColumns(projectedColumns, firstAliasIndex);
    }

    private String projectedColumns(List<ProjectedColumn> columns) {
        return projectedColumns(columns, 1);
    }

    private String projectedColumns(List<ProjectedColumn> columns, int firstAliasIndex) {
        List<String> expressions = new ArrayList<>();
        int aliasIndex = firstAliasIndex;
        for (ProjectedColumn column : columns) {
            expressions.add(castAsText(qualifiedColumn(column.tableAlias, column.column)) + " AS ca" + aliasIndex++);
        }
        return String.join(", ", expressions);
    }

    private String outerCteProjection(int columnCount) {
        List<String> expressions = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            expressions.add("q.ca" + i + " AS ca" + i);
        }
        return String.join(", ", expressions);
    }

    private boolean isSelectable(PostgreSQLColumn column) {
        return column.getType() != PostgreSQLDataType.COMPOSITE
                && column.getType() != PostgreSQLDataType.ARRAY
                && column.getType() != PostgreSQLDataType.TSVECTOR
                && column.getType() != PostgreSQLDataType.TSQUERY;
    }

    private boolean isPredicateColumn(PostgreSQLColumn column) {
        return typeFamily(column) != TypeFamily.OTHER;
    }

    private boolean isJoinable(PostgreSQLColumn column) {
        TypeFamily family = typeFamily(column);
        return family != TypeFamily.OTHER;
    }

    private boolean areJoinCompatible(PostgreSQLColumn leftColumn, PostgreSQLColumn rightColumn) {
        if (!isJoinable(leftColumn) || !isJoinable(rightColumn)) {
            return false;
        }
        PostgreSQLDataType leftType = leftColumn.getType();
        PostgreSQLDataType rightType = rightColumn.getType();
        if (typeFamily(leftColumn) == TypeFamily.NUMERIC && typeFamily(rightColumn) == TypeFamily.NUMERIC) {
            return true;
        }
        if ((leftType == PostgreSQLDataType.INET || leftType == PostgreSQLDataType.CIDR)
                && (rightType == PostgreSQLDataType.INET || rightType == PostgreSQLDataType.CIDR)) {
            return true;
        }
        if (leftType != rightType) {
            return false;
        }
        if (leftType == PostgreSQLDataType.ENUM) {
            String leftUdtName = leftColumn.getUdtName();
            String rightUdtName = rightColumn.getUdtName();
            return leftUdtName != null && leftUdtName.equals(rightUdtName);
        }
        return true;
    }

    private boolean isGroupable(PostgreSQLColumn column) {
        TypeFamily family = typeFamily(column);
        return family != TypeFamily.OTHER;
    }

    private TypeFamily typeFamily(PostgreSQLColumn column) {
        switch (column.getType()) {
            case INT:
            case DECIMAL:
            case FLOAT:
            case REAL:
                return TypeFamily.NUMERIC;
            case MONEY:
                return TypeFamily.MONEY;
            case TEXT:
            case ENUM:
                return TypeFamily.TEXT;
            case BOOLEAN:
                return TypeFamily.BOOLEAN;
            case DATE:
                return TypeFamily.DATE;
            case TIME:
                return TypeFamily.TIME;
            case TIMESTAMP:
                return TypeFamily.TIMESTAMP;
            case UUID:
                return TypeFamily.UUID;
            case INET:
            case CIDR:
            case MACADDR:
                return TypeFamily.NETWORK;
            case BIT:
                return TypeFamily.BIT;
            default:
                return TypeFamily.OTHER;
        }
    }

    private String qualifiedColumn(String alias, PostgreSQLColumn column) {
        return alias + "." + quoteIdentifier(column.getName());
    }

    private String castAsText(String expression) {
        return "CAST(" + expression + " AS TEXT)";
    }

    private String deterministicLimit(int columnCount) {
        return " ORDER BY " + orderByAliases(columnCount) + " LIMIT " + randomLimit();
    }

    private String orderByAliases(int columnCount) {
        List<String> aliases = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            aliases.add("ca" + i);
        }
        return String.join(", ", aliases);
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private int randomLimit() {
        return Randomly.getNotCachedInteger(1, MAX_LIMIT + 1);
    }

    private enum SelectKind {
        SIMPLE,
        JOIN,
        AGGREGATE,
        COMPOUND,
        CTE
    }

    private enum TypeFamily {
        NUMERIC,
        MONEY,
        TEXT,
        BOOLEAN,
        DATE,
        TIME,
        TIMESTAMP,
        UUID,
        NETWORK,
        BIT,
        OTHER
    }

    private static final class ColumnPair {
        private final PostgreSQLColumn left;
        private final PostgreSQLColumn right;

        private ColumnPair(PostgreSQLColumn left, PostgreSQLColumn right) {
            this.left = left;
            this.right = right;
        }
    }

    private static final class ProjectedColumn {
        private final String tableAlias;
        private final PostgreSQLColumn column;

        private ProjectedColumn(String tableAlias, PostgreSQLColumn column) {
            this.tableAlias = tableAlias;
            this.column = column;
        }
    }
}
