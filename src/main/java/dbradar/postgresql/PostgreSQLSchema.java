package dbradar.postgresql;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.common.schema.AbstractRelationalTable;
import dbradar.common.schema.AbstractSchema;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.common.schema.AbstractTrigger;
import dbradar.common.schema.TableIndex;

import dbradar.postgresql.PostgreSQLSchema.PostgreSQLTable;
import dbradar.postgresql.PostgreSQLSchema.PostgreSQLTrigger;

public class PostgreSQLSchema extends AbstractSchema<PostgreSQLGlobalState, PostgreSQLTable, PostgreSQLTrigger> {

    public PostgreSQLSchema(List<PostgreSQLTable> databaseTables) {
        super(databaseTables, Collections.emptyList());
    }

    public static class PostgreSQLTable extends AbstractRelationalTable<PostgreSQLColumn, PostgreSQLIndex, PostgreSQLGlobalState> {

        public enum TableType {
            STANDARD, TEMPORARY
        }

        public enum PartitionStrategy {
            NONE, RANGE
        }

        private final TableType tableType;
        private final List<PostgreSQLStatisticsObject> statistics;
        private final boolean isInsertable;
        private final boolean partitionedTable;
        private final boolean partition;
        private final String partitionParentName;
        private final PartitionStrategy partitionStrategy;
        private final String partitionKeyDefinition;
        private final String partitionBound;

        public PostgreSQLTable(String tableName, List<PostgreSQLColumn> columns, List<PostgreSQLIndex> indexes,
                               TableType tableType, List<PostgreSQLStatisticsObject> statistics, boolean isView,
                               boolean isInsertable, boolean partitionedTable, boolean partition,
                               String partitionParentName, String partitionKeyDefinition, String partitionBound) {
            super(tableName, columns, indexes, false, isView);
            this.statistics = statistics;
            this.isInsertable = isInsertable;
            this.tableType = tableType;
            this.partitionedTable = partitionedTable;
            this.partition = partition;
            this.partitionParentName = partitionParentName;
            this.partitionKeyDefinition = partitionKeyDefinition;
            this.partitionBound = partitionBound;
            this.partitionStrategy = parsePartitionStrategy(partitionKeyDefinition);
        }

        public List<PostgreSQLStatisticsObject> getStatistics() {
            return statistics;
        }

        public TableType getTableType() {
            return tableType;
        }

        public boolean isInsertable() {
            return isInsertable;
        }

        public boolean isTemporary() {
            return tableType == TableType.TEMPORARY;
        }

        public boolean isPartitionedTable() {
            return partitionedTable;
        }

        public boolean isPartition() {
            return partition;
        }

        public String getPartitionParentName() {
            return partitionParentName;
        }

        public PartitionStrategy getPartitionStrategy() {
            return partitionStrategy;
        }

        public String getPartitionKeyDefinition() {
            return partitionKeyDefinition;
        }

        public String getPartitionBound() {
            return partitionBound;
        }

        private static PartitionStrategy parsePartitionStrategy(String partitionKeyDefinition) {
            if (partitionKeyDefinition == null) {
                return PartitionStrategy.NONE;
            }
            String normalized = partitionKeyDefinition.toUpperCase();
            if (normalized.startsWith("RANGE ")) {
                return PartitionStrategy.RANGE;
            }
            return PartitionStrategy.NONE;
        }

    }

    public static class PostgreSQLColumn extends AbstractTableColumn<PostgreSQLTable, PostgreSQLDataType> {

        private String columnName;
        private String columnDefault;
        private boolean isNullable;
        private String dataType;
        private long characterMaximumLength;
        private int numericPrecision;
        private int numericScale;

        public PostgreSQLColumn(String columnName,
                                String columnDefault, boolean isNullable, String dataType,
                                long characterMaximumLength, int numericPrecision, int numericScale) {
            super(columnName, null, getPostgreSQLDataType(dataType));
            this.columnName = columnName;
            this.columnDefault = columnDefault;
            this.isNullable = isNullable;
            this.dataType = dataType;
            this.characterMaximumLength = characterMaximumLength;
            this.numericPrecision = numericPrecision;
            this.numericScale = numericScale;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getColumnDefault() {
            return columnDefault;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public String getDataType() {
            return dataType;
        }

        public long getCharacterMaximumLength() {
            return characterMaximumLength;
        }

        public int getNumericPrecision() {
            return numericPrecision;
        }

        public int getNumericScale() {
            return numericScale;
        }
    }

    public enum PostgreSQLDataType {
        INT, BOOLEAN, TEXT, DECIMAL, FLOAT, REAL, RANGE, MONEY, BIT, INET;
    }

    public static class PostgreSQLIndex extends TableIndex {

        private PostgreSQLTable table;
        private boolean isUnique;
        private boolean isPrimaryKey;
        private List<PostgreSQLColumn> columns = new ArrayList<>();

        public PostgreSQLIndex(String indexName, String tableName, boolean isUnique,
                               boolean isPrimaryKey, List<String> columnNames) {
            super(indexName, tableName, columnNames, isUnique);
            this.isUnique = isUnique;
            this.isPrimaryKey = isPrimaryKey;
        }

        public void setTable(PostgreSQLTable table) {
            this.table = table;
        }

        public PostgreSQLTable getTable() {
            return table;
        }

        public boolean isUnique() {
            return isUnique;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public List<PostgreSQLColumn> getColumns() {
            return columns;
        }
    }

    public PostgreSQLIndex getRandomIndex() {
        List<PostgreSQLIndex> indexes = new ArrayList<>();
        for (PostgreSQLTable table : getDatabaseTables()) {
            for (PostgreSQLIndex index : table.getIndexes()) {
                if (!index.isPrimaryKey()) {
                    indexes.add(index);
                }
            }
        }

        if (indexes.isEmpty()) {
            throw new IgnoreMeException("No indexes found");
        } else {
            return Randomly.fromList(indexes);
        }
    }

    public PostgreSQLTable getRandomPartitionedTableWithoutDefaultPartition() {
        return getRandomTable(t -> t.isPartitionedTable()
                && t.getPartitionStrategy() == PostgreSQLTable.PartitionStrategy.RANGE
                && !hasDefaultPartition(t));
    }

    public PostgreSQLTable getRandomPartitionedTableWithPartitions() {
        return getRandomTable(t -> t.isPartitionedTable()
                && !getPartitions(t).isEmpty());
    }

    public PostgreSQLTable getRandomPartitionOf(PostgreSQLTable parent) {
        List<PostgreSQLTable> partitions = getPartitions(parent);
        if (partitions.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(partitions);
    }

    public PostgreSQLTable getRandomDetachedPartitionCandidate(PostgreSQLTable parent) {
        return getRandomTable(t -> !t.isView()
                && !t.isPartition()
                && !t.isPartitionedTable()
                && !t.getName().equals(parent.getName())
                && hasMatchingTableDefinition(parent, t));
    }

    public List<PostgreSQLTable> getPartitions(PostgreSQLTable parent) {
        List<PostgreSQLTable> partitions = new ArrayList<>();
        for (PostgreSQLTable table : getDatabaseTables()) {
            if (table.isPartition() && parent.getName().equals(table.getPartitionParentName())) {
                partitions.add(table);
            }
        }
        return partitions;
    }

    public boolean hasDefaultPartition(PostgreSQLTable parent) {
        for (PostgreSQLTable partition : getPartitions(parent)) {
            if ("DEFAULT".equalsIgnoreCase(partition.getPartitionBound())) {
                return true;
            }
        }
        return false;
    }

    private boolean hasMatchingTableDefinition(PostgreSQLTable first, PostgreSQLTable second) {
        List<PostgreSQLColumn> firstColumns = first.getColumns();
        List<PostgreSQLColumn> secondColumns = second.getColumns();
        if (firstColumns.size() != secondColumns.size()) {
            return false;
        }
        for (int i = 0; i < firstColumns.size(); i++) {
            PostgreSQLColumn left = firstColumns.get(i);
            PostgreSQLColumn right = secondColumns.get(i);
            if (!left.getName().equals(right.getName())) {
                return false;
            }
            if (!left.getDataType().equals(right.getDataType())) {
                return false;
            }
            if (left.isNullable() != right.isNullable()) {
                return false;
            }
        }
        return true;
    }

    public static class PostgreSQLTrigger extends AbstractTrigger {

        public PostgreSQLTrigger(String name) {
            super(name);
        }
    }

    public static final class PostgreSQLStatisticsObject {
        private final String name;

        public PostgreSQLStatisticsObject(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static PostgreSQLSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        try {
            List<PostgreSQLTable> databaseTables = new ArrayList<>();
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery(
                        "SELECT t.table_name,\n" +
                                "       t.table_schema,\n" +
                                "       t.table_type,\n" +
                                "       t.is_insertable_into,\n" +
                                "       c.relkind,\n" +
                                "       c.relispartition,\n" +
                                "       parent.relname AS partition_parent_name,\n" +
                                "       pg_get_partkeydef(c.oid) AS partition_key_definition,\n" +
                                "       pg_get_expr(c.relpartbound, c.oid) AS partition_bound\n" +
                                "FROM information_schema.tables t\n" +
                                "JOIN pg_namespace n ON n.nspname = t.table_schema\n" +
                                "JOIN pg_class c ON c.relname = t.table_name AND c.relnamespace = n.oid\n" +
                                "LEFT JOIN pg_inherits i ON i.inhrelid = c.oid\n" +
                                "LEFT JOIN pg_class parent ON parent.oid = i.inhparent\n" +
                                "WHERE t.table_schema='public' OR t.table_schema LIKE 'pg_temp_%'\n" +
                                "ORDER BY t.table_name;")) {
                    while (rs.next()) {
                        String tableName = rs.getString("table_name");
                        String tableTypeSchema = rs.getString("table_schema");
                        boolean isInsertable = rs.getBoolean("is_insertable_into");
                        boolean isPartition = rs.getBoolean("relispartition");
                        boolean isPartitionedTable = "p".equals(rs.getString("relkind"));
                        String partitionParentName = rs.getString("partition_parent_name");
                        String partitionKeyDefinition = rs.getString("partition_key_definition");
                        String partitionBound = rs.getString("partition_bound");
                        // TODO: also check insertable
                        // TODO: insert into view?
                        boolean isView = tableName.startsWith("v"); // tableTypeStr.contains("VIEW") ||
                        // tableTypeStr.contains("LOCAL TEMPORARY") &&
                        // !isInsertable;
                        PostgreSQLTable.TableType tableType = getTableType(tableTypeSchema);
                        List<PostgreSQLColumn> columns = getTableColumns(con, tableName);
                        List<PostgreSQLIndex> indexes = getIndexes(con, tableName);
                        List<PostgreSQLStatisticsObject> statistics = getStatistics(con);
                        PostgreSQLTable table = new PostgreSQLTable(tableName, columns, indexes, tableType, statistics,
                                isView, isInsertable, isPartitionedTable, isPartition, partitionParentName,
                                partitionKeyDefinition, partitionBound);
                        for (PostgreSQLColumn c : columns) {
                            c.setTable(table);
                        }
                        for (PostgreSQLIndex index : indexes) {
                            index.setTable(table);
                            for (String columnName : index.getColumnNames()) {
                                for (PostgreSQLColumn column : columns) {
                                    if (columnName.equals(column.getName())) {
                                        index.getColumns().add(column);
                                        break;
                                    }
                                }
                            }
                        }
                        // To avoid some situations that columns can not be retrieved.
                        if (columns.isEmpty()) {
                            continue;
                        }
                        databaseTables.add(table);
                    }
                }
            }
            return new PostgreSQLSchema(databaseTables);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new AssertionError(e);
        }
    }

    protected static List<PostgreSQLStatisticsObject> getStatistics(SQLConnection con) throws SQLException {
        List<PostgreSQLStatisticsObject> statistics = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT stxname FROM pg_statistic_ext ORDER BY stxname;")) {
                while (rs.next()) {
                    statistics.add(new PostgreSQLStatisticsObject(rs.getString("stxname")));
                }
            }
        }
        return statistics;
    }

    protected static PostgreSQLTable.TableType getTableType(String tableTypeStr) throws AssertionError {
        PostgreSQLTable.TableType tableType;
        if (tableTypeStr.contentEquals("public")) {
            tableType = PostgreSQLTable.TableType.STANDARD;
        } else if (tableTypeStr.startsWith("pg_temp")) {
            tableType = PostgreSQLTable.TableType.TEMPORARY;
        } else {
            throw new AssertionError(tableTypeStr);
        }
        return tableType;
    }

    protected static List<PostgreSQLIndex> getIndexes(SQLConnection con, String tableName) throws SQLException {
        List<PostgreSQLIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String
                    .format("SELECT i.relname              AS indexname,\n" +
                            "       pg_get_indexdef(i.oid) AS indexdef,\n" +
                            "       ARRAY(\n" +
                            "               SELECT a.attname\n" +
                            "               FROM pg_index idx\n" +
                            "                        JOIN pg_class c ON c.oid = idx.indrelid\n" +
                            "                        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY (idx.indkey)\n" +
                            "               WHERE idx.indexrelid = i.oid\n" +
                            "               ORDER BY a.attnum\n" +
                            "       )                      AS column_names\n" +
                            "FROM pg_class c\n" +
                            "         JOIN pg_namespace n ON n.oid = c.relnamespace\n" +
                            "         JOIN pg_index idx ON idx.indrelid = c.oid\n" +
                            "         JOIN pg_class i ON i.oid = idx.indexrelid\n" +
                            "WHERE n.nspname = 'public'\n" +
                            "  AND c.relname = '%s'\n" +
                            "  AND i.relkind IN ('i', 'I') -- 普通索引或分区索引\n" +
                            "ORDER BY c.relname, i.relname;", tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("indexname");
                    String indexDef = rs.getString("indexdef");
                    boolean isUnique = indexDef.contains("UNIQUE");
                    boolean isPrimary = indexName.contains("pkey");
                    Array columns = rs.getArray("column_names");
                    PostgreSQLIndex index = new PostgreSQLIndex(indexName, tableName, isUnique, isPrimary, new ArrayList<>());
                    if (columns != null) {
                        String[] columnNames = (String[]) columns.getArray();
                        for (String columnName : columnNames) {
                            index.getColumnNames().add(columnName);
                        }
                    }
                    indexes.add(index);
                }
            }
        }
        return indexes;
    }

    protected static List<PostgreSQLColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<PostgreSQLColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"
                            + tableName + "' ORDER BY column_name")) {
                while (rs.next()) {
                    PostgreSQLColumn column = new PostgreSQLColumn(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("COLUMN_DEFAULT"),
                            "YES".equals(rs.getString("IS_NULLABLE")),
                            rs.getString("DATA_TYPE"),
                            rs.getLong("CHARACTER_MAXIMUM_LENGTH"),
                            rs.getInt("NUMERIC_PRECISION"),
                            rs.getInt("NUMERIC_SCALE")
                    );
                    columns.add(column);
                }
            }
        }
        return columns;
    }

    public static PostgreSQLDataType getPostgreSQLDataType(String typeString) {
        switch (typeString) {
            case "smallint":
            case "integer":
            case "bigint":
                return PostgreSQLDataType.INT;
            case "boolean":
                return PostgreSQLDataType.BOOLEAN;
            case "text":
            case "character":
            case "character varying":
            case "name":
            case "regclass":
                return PostgreSQLDataType.TEXT;
            case "numeric":
                return PostgreSQLDataType.DECIMAL;
            case "double precision":
                return PostgreSQLDataType.FLOAT;
            case "real":
                return PostgreSQLDataType.REAL;
            case "int4range":
                return PostgreSQLDataType.RANGE;
            case "money":
                return PostgreSQLDataType.MONEY;
            case "bit":
            case "bit varying":
                return PostgreSQLDataType.BIT;
            case "inet":
                return PostgreSQLDataType.INET;
            default:
                throw new AssertionError(typeString);
        }
    }
}
