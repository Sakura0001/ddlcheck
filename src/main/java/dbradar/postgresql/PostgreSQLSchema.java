package dbradar.postgresql;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        private final TableType tableType;
        private final List<PostgreSQLStatisticsObject> statistics;
        private final boolean isInsertable;

        public PostgreSQLTable(String tableName, List<PostgreSQLColumn> columns, List<PostgreSQLIndex> indexes,
                               TableType tableType, List<PostgreSQLStatisticsObject> statistics, boolean isView, boolean isInsertable) {
            super(tableName, columns, indexes, false, isView);
            this.statistics = statistics;
            this.isInsertable = isInsertable;
            this.tableType = tableType;
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

    }

    public static class PostgreSQLColumn extends AbstractTableColumn<PostgreSQLTable, PostgreSQLDataType> {

        private String columnName;
        private String columnDefault;
        private boolean isNullable;
        private String dataType;
        private String udtName;
        private String domainName;
        private PostgreSQLCustomTypeKind customTypeKind;
        private long characterMaximumLength;
        private int numericPrecision;
        private int numericScale;

        public PostgreSQLColumn(String columnName,
                                String columnDefault, boolean isNullable, String dataType, String udtName,
                                String domainName, PostgreSQLCustomTypeKind customTypeKind,
                                long characterMaximumLength, int numericPrecision, int numericScale) {
            super(columnName, null, getPostgreSQLDataType(dataType, udtName, customTypeKind));
            this.columnName = columnName;
            this.columnDefault = columnDefault;
            this.isNullable = isNullable;
            this.dataType = dataType;
            this.udtName = udtName;
            this.domainName = domainName;
            this.customTypeKind = customTypeKind;
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

        public String getUdtName() {
            return udtName;
        }

        public String getDomainName() {
            return domainName;
        }

        public PostgreSQLCustomTypeKind getCustomTypeKind() {
            return customTypeKind;
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
        INT, BOOLEAN, TEXT, DECIMAL, FLOAT, REAL, RANGE, MULTIRANGE, MONEY, BIT, INET,
        BYTEA, DATE, TIME, TIMESTAMP, INTERVAL, POINT, BOX, LSEG, PATH, POLYGON, CIRCLE,
        CIDR, MACADDR, TSVECTOR, TSQUERY, UUID,
        XML, JSON, JSONB, PG_LSN, OID, ARRAY, ENUM, COMPOSITE;
    }

    public enum PostgreSQLCustomTypeKind {
        ENUM, COMPOSITE
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
            Map<String, PostgreSQLCustomTypeKind> customTypeKinds = getCustomTypeKinds(con);
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery(
                        "SELECT t.table_name, t.table_schema, t.table_type, t.is_insertable_into,"
                                + " EXISTS (SELECT 1 FROM information_schema.views v"
                                + " WHERE v.table_schema = t.table_schema AND v.table_name = t.table_name) AS is_view"
                                + " FROM information_schema.tables t"
                                + " WHERE t.table_schema='public' OR t.table_schema LIKE 'pg_temp_%'"
                                + " ORDER BY t.table_name;")) {
                    while (rs.next()) {
                        String tableName = rs.getString("table_name");
                        String tableTypeSchema = rs.getString("table_schema");
                        boolean isInsertable = rs.getBoolean("is_insertable_into");
                        boolean isView = rs.getBoolean("is_view");
                        PostgreSQLTable.TableType tableType = getTableType(tableTypeSchema);
                        List<PostgreSQLColumn> columns;
                        List<PostgreSQLIndex> indexes;
                        List<PostgreSQLStatisticsObject> statistics;
                        try {
                            columns = getTableColumns(con, tableName, customTypeKinds);
                            indexes = getIndexes(con, tableName);
                            statistics = getStatistics(con);
                        } catch (SQLException e) {
                            if (isTransientSchemaLookupFailure(e)) {
                                continue;
                            }
                            throw e;
                        }
                        PostgreSQLTable table = new PostgreSQLTable(tableName, columns, indexes, tableType, statistics,
                                isView, isInsertable);
                        for (PostgreSQLColumn c : columns) {
                            c.setTable(table);
                        }
                        for (PostgreSQLIndex index : indexes) {
                            index.setTable(table);
                            for (String columnName : index.getColumnNames()) {
                                for (PostgreSQLColumn column : columns) {
                                    if (columnName.equals(column.getName())) {
                                        index.getColumns().add(column);
                                        if (index.isPrimaryKey()) {
                                            column.setPrimaryKey(true);
                                            column.setNotNull(true);
                                        }
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

    private static boolean isTransientSchemaLookupFailure(SQLException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        return message.contains("could not open relation with OID")
                || message.contains("cache lookup failed for relation")
                || message.contains("does not exist");
    }

    protected static Map<String, PostgreSQLCustomTypeKind> getCustomTypeKinds(SQLConnection con) throws SQLException {
        Map<String, PostgreSQLCustomTypeKind> customTypeKinds = new HashMap<>();
        String query = "SELECT t.typname, t.typtype, c.relkind "
                + "FROM pg_type t "
                + "JOIN pg_namespace n ON n.oid = t.typnamespace "
                + "LEFT JOIN pg_class c ON c.oid = t.typrelid "
                + "WHERE n.nspname = 'public'";
        try (Statement s = con.createStatement(); ResultSet rs = s.executeQuery(query)) {
            while (rs.next()) {
                String typeName = rs.getString("typname");
                String typeKind = rs.getString("typtype");
                String relationKind = rs.getString("relkind");
                if ("e".equals(typeKind)) {
                    customTypeKinds.put(typeName, PostgreSQLCustomTypeKind.ENUM);
                } else if ("c".equals(typeKind) && "c".equals(relationKind)) {
                    customTypeKinds.put(typeName, PostgreSQLCustomTypeKind.COMPOSITE);
                }
            }
        }
        return customTypeKinds;
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
                            "  AND i.relkind = 'i' -- 确保是索引对象\n" +
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

    protected static List<PostgreSQLColumn> getTableColumns(SQLConnection con, String tableName,
            Map<String, PostgreSQLCustomTypeKind> customTypeKinds) throws SQLException {
        List<PostgreSQLColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"
                            + tableName + "' ORDER BY ordinal_position")) {
                while (rs.next()) {
                    String udtName = rs.getString("UDT_NAME");
                    String domainName = rs.getString("DOMAIN_NAME");
                    PostgreSQLCustomTypeKind customTypeKind = udtName == null ? null : customTypeKinds.get(udtName);
                    PostgreSQLColumn column = new PostgreSQLColumn(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("COLUMN_DEFAULT"),
                            "YES".equals(rs.getString("IS_NULLABLE")),
                            rs.getString("DATA_TYPE"),
                            udtName,
                            domainName,
                            customTypeKind,
                            rs.getLong("CHARACTER_MAXIMUM_LENGTH"),
                            rs.getInt("NUMERIC_PRECISION"),
                            rs.getInt("NUMERIC_SCALE")
                    );
                    column.setGenerated("ALWAYS".equals(rs.getString("IS_GENERATED")));
                    column.setNotNull("NO".equals(rs.getString("IS_NULLABLE")));
                    columns.add(column);
                }
            }
        }
        return columns;
    }

    public static PostgreSQLDataType getPostgreSQLDataType(String typeString, String udtName,
            PostgreSQLCustomTypeKind customTypeKind) {
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
            case "bytea":
                return PostgreSQLDataType.BYTEA;
            case "date":
                return PostgreSQLDataType.DATE;
            case "time without time zone":
            case "time with time zone":
                return PostgreSQLDataType.TIME;
            case "timestamp without time zone":
            case "timestamp with time zone":
                return PostgreSQLDataType.TIMESTAMP;
            case "interval":
                return PostgreSQLDataType.INTERVAL;
            case "uuid":
                return PostgreSQLDataType.UUID;
            case "json":
                return PostgreSQLDataType.JSON;
            case "jsonb":
                return PostgreSQLDataType.JSONB;
            case "xml":
                return PostgreSQLDataType.XML;
            case "point":
                return PostgreSQLDataType.POINT;
            case "box":
                return PostgreSQLDataType.BOX;
            case "lseg":
                return PostgreSQLDataType.LSEG;
            case "path":
                return PostgreSQLDataType.PATH;
            case "polygon":
                return PostgreSQLDataType.POLYGON;
            case "circle":
                return PostgreSQLDataType.CIRCLE;
            case "cidr":
                return PostgreSQLDataType.CIDR;
            case "macaddr":
            case "macaddr8":
                return PostgreSQLDataType.MACADDR;
            case "tsvector":
                return PostgreSQLDataType.TSVECTOR;
            case "tsquery":
                return PostgreSQLDataType.TSQUERY;
            case "pg_lsn":
                return PostgreSQLDataType.PG_LSN;
            case "oid":
                return PostgreSQLDataType.OID;
            case "ARRAY":
                return PostgreSQLDataType.ARRAY;
            case "int4range":
                return PostgreSQLDataType.RANGE;
            case "int4multirange":
                return PostgreSQLDataType.MULTIRANGE;
            case "money":
                return PostgreSQLDataType.MONEY;
            case "bit":
            case "bit varying":
                return PostgreSQLDataType.BIT;
            case "inet":
                return PostgreSQLDataType.INET;
            case "USER-DEFINED":
                if (customTypeKind == PostgreSQLCustomTypeKind.ENUM) {
                    return PostgreSQLDataType.ENUM;
                }
                if (customTypeKind == PostgreSQLCustomTypeKind.COMPOSITE) {
                    return PostgreSQLDataType.COMPOSITE;
                }
                if (udtName != null && udtName.endsWith("multirange")) {
                    return PostgreSQLDataType.MULTIRANGE;
                }
                if (udtName != null && udtName.endsWith("range")) {
                    return PostgreSQLDataType.RANGE;
                }
                return PostgreSQLDataType.TEXT;
            default:
                throw new AssertionError(typeString);
        }
    }
}
