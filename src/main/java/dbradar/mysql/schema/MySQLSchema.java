package dbradar.mysql.schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.common.schema.AbstractRelationalTable;
import dbradar.common.schema.AbstractSchema;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.common.schema.AbstractTables;
import dbradar.common.schema.TableIndex;
import dbradar.mysql.MySQLGlobalState;

public class MySQLSchema extends AbstractSchema<MySQLGlobalState, MySQLSchema.MySQLTable, MySQLTrigger> {

    private static final int NR_SCHEMA_READ_TRIES = 10;
    private List<MySQLForeignKey> foreignKeys;

    public enum MySQLDataType {
        INT, TEXT, FLOAT, DOUBLE, DECIMAL, BIT, BLOB, CHAR, DATE, TIME, DATETIME, BOOLEAN, JSON;

        public boolean isNumeric() {
            switch (this) {
                case INT:
                case DOUBLE:
                case FLOAT:
                case DECIMAL:
                    return true;
                case TEXT:
                case BIT:
                case BLOB:
                case CHAR:
                case DATE:
                case TIME:
                case DATETIME:
                case BOOLEAN:
                case JSON:
                    return false;
                default:
                    throw new AssertionError(this);
            }
        }

        public boolean isTextOrBlob() {
            return this == TEXT || this == BLOB;
        }

    }

    public static class MySQLColumn extends AbstractTableColumn<MySQLTable, MySQLDataType> {

        private String columnName;
        private String columnDefault;
        private boolean isNullable;
        private String dataType;
        private long characterMaximumLength;
        private int numericPrecision;
        private int numericScale;
        private String columnKey;

        public MySQLColumn(String columnName,
                           String columnDefault, boolean isNullable, String dataType,
                           long characterMaximumLength, int numericPrecision, int numericScale,
                           String columnKey) {
            super(columnName, null, getMySQLDataType(dataType));
            this.columnName = columnName;
            this.columnDefault = columnDefault;
            this.isNullable = isNullable;
            this.dataType = dataType;
            this.characterMaximumLength = characterMaximumLength;
            this.numericPrecision = numericPrecision;
            this.numericScale = numericScale;
            this.columnKey = columnKey;
            setPrimaryKey(columnKey.equals("PRI"));
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

        public Long getCharacterMaximumLength() {
            return characterMaximumLength;
        }

        public Integer getNumericPrecision() {
            return numericPrecision;
        }

        public Integer getNumericScale() {
            return numericScale;
        }

        public String getColumnKey() {
            return columnKey;
        }

        @Override
        public String getFullQualifiedName() {
            return getTable().getName() + "." + columnName;
        }

        @Override
        public String toString() {
            return "Column{" +
                    "tableName='" + getTable().getName() + '\'' +
                    ", columnName='" + columnName + '\'' +
                    ", columnDefault='" + columnDefault + '\'' +
                    ", isNullable=" + isNullable +
                    ", dataType='" + dataType + '\'' +
                    ", characterMaximumLength=" + characterMaximumLength +
                    ", numericPrecision=" + numericPrecision +
                    ", numericScale=" + numericScale +
                    ", columnKey='" + columnKey + '\'' +
                    '}';
        }
    }

    public static class MySQLTables extends AbstractTables<MySQLTable, MySQLColumn> {

        public MySQLTables(List<MySQLTable> tables) {
            super(tables);
        }

    }

    private static MySQLDataType getMySQLDataType(String typeString) {
        if (typeString == null) return MySQLDataType.TEXT;
        switch (typeString.toLowerCase()) {
            case "tinyint":
            case "int1":
            case "smallint":
            case "int2":
            case "mediumint":
            case "int3":
            case "int":
            case "integer":
            case "int4":
            case "bigint":
            case "int8":
            case "serial":
                return MySQLDataType.INT;
            case "char":
            case "character":
            case "nchar":
            case "national char":
                return MySQLDataType.CHAR;
            case "varchar":
            case "nvarchar":
            case "national varchar":
            case "tinytext":
            case "mediumtext":
            case "text":
            case "longtext":
                return MySQLDataType.TEXT;
            case "double":
            case "double precision":
                return MySQLDataType.DOUBLE;
            case "float":
            case "float4":
            case "float8":
            case "real":
                return MySQLDataType.FLOAT;
            case "decimal":
            case "numeric":
            case "dec":
            case "fixed":
                return MySQLDataType.DECIMAL;
            case "bit":
                return MySQLDataType.BIT;
            case "binary":
            case "blob":
            case "varbinary":
            case "tinyblob":
            case "mediumblob":
            case "longblob":
                return MySQLDataType.BLOB;
            case "date":
                return MySQLDataType.DATE;
            case "time":
                return MySQLDataType.TIME;
            case "datetime":
            case "timestamp":
                return MySQLDataType.DATETIME;
            case "bool":
            case "boolean":
                return MySQLDataType.BOOLEAN;
            case "json":
            case "enum":
            case "set":
                return MySQLDataType.JSON;
            case "year":
                return MySQLDataType.INT;
            default:
                return MySQLDataType.TEXT;
        }
    }

    public static class MySQLForeignKey {
        private String constraintName;
        private MySQLTable table;
        private List<MySQLColumn> columns;
        private MySQLTable referencedTable;
        private List<MySQLColumn> referencedColumns;

        public MySQLForeignKey(String constraintName, MySQLTable table, List<MySQLColumn> columns,
                               MySQLTable referencedTable, List<MySQLColumn> referencedColumns) {
            this.constraintName = constraintName;
            this.table = table;
            this.columns = columns;
            this.referencedTable = referencedTable;
            this.referencedColumns = referencedColumns;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public MySQLTable getTable() {
            return table;
        }

        public List<MySQLColumn> getColumns() {
            return columns;
        }

        public MySQLTable getReferencedTable() {
            return referencedTable;
        }

        public List<MySQLColumn> getReferencedColumns() {
            return referencedColumns;
        }

        @Override
        public String toString() {
            String tableName = table.getName();
            List<String> columnNames = new ArrayList<>();
            for (MySQLColumn column : columns) {
                columnNames.add(column.getName());
            }
            String referencedTableName = referencedTable.getName();
            List<String> referencedColumnNames = new ArrayList<>();
            for (MySQLColumn column : referencedColumns) {
                referencedColumnNames.add(column.getName());
            }
            return "MySQLForeignKey{" +
                    "constraintName='" + constraintName + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", columnNames=" + columnNames +
                    ", referencedTableName='" + referencedTableName + '\'' +
                    ", referencedColumnNames=" + referencedColumnNames +
                    '}';
        }
    }

    public static class MySQLTable extends AbstractRelationalTable<MySQLColumn, MySQLIndex, MySQLGlobalState> {

        public enum MySQLEngine {
            INNO_DB("InnoDB"), MY_ISAM("MyISAM"), MEMORY("MEMORY"), HEAP("HEAP"), CSV("CSV"), MERGE("MERGE"),
            ARCHIVE("ARCHIVE"), FEDERATED("FEDERATED");

            private String s;

            MySQLEngine(String s) {
                this.s = s;
            }

            public static MySQLEngine get(String val) {
                return Stream.of(values()).filter(engine -> engine.s.equalsIgnoreCase(val)).findFirst().get();
            }

        }

        private final MySQLEngine engine;

        public MySQLTable(String tableName, List<MySQLColumn> columns, List<MySQLIndex> indexes, MySQLEngine engine, boolean isView) {
            super(tableName, columns, indexes, false, isView);
            this.engine = engine;
        }

        public MySQLEngine getEngine() {
            return engine;
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

    }

    public static final class MySQLIndex extends TableIndex {

        private MySQLTable table;
        private int nonUnique;
        private List<MySQLColumn> columns = new ArrayList<>();
        private List<String> columnNames;
        private boolean nullable;
        private String indexType;

        // 构造函数
        public MySQLIndex(String tableName, int nonUnique, String indexName,
                          List<String> columnNames, boolean nullable, String indexType) {
            super(indexName, tableName, columnNames, nonUnique == 0);
            this.table = null;
            this.nonUnique = nonUnique;
            this.columnNames = columnNames;
            this.nullable = nullable;
            this.indexType = indexType;
        }

        public void setTable(MySQLTable table) {
            this.table = table;
        }

        @Override
        public String getTableName() {
            return table.getName();
        }

        public int getNonUnique() {
            return nonUnique;
        }

        public List<MySQLColumn> getColumns() {
            return columns;
        }

        public boolean isNullable() {
            return nullable;
        }

        public String getIndexType() {
            return indexType;
        }

        @Override
        public String toString() {
            return "MySQLIndex{" +
                    "tableName='" + table.getName() + '\'' +
                    ", nonUnique=" + nonUnique +
                    ", indexName='" + getName() + '\'' +
                    ", columnNames=" + columnNames +
                    ", nullable=" + nullable +
                    ", indexType='" + indexType + '\'' +
                    '}';
        }
    }

    public List<MySQLForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public MySQLIndex getRandomIndex() {
        List<MySQLIndex> indexes = new ArrayList<>();
        for (MySQLTable table : getDatabaseTables()) {
            for (MySQLIndex index : table.getIndexes()) {
                if (!index.getName().equals("PRIMARY")) { // cannot add or drop primary as index
                    indexes.add(index);
                }
            }
        }
        if (!indexes.isEmpty()) {
            return Randomly.fromList(indexes);
        } else {
            throw new IgnoreMeException("No indexes found");
        }
    }

    public static MySQLSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        /* the loop is a workaround for https://bugs.mysql.com/bug.php?id=95929 */
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<MySQLTable> databaseTables = new ArrayList<>();
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(
                            "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
                                    + databaseName + "';")) {
                        while (rs.next()) {
                            String tableName = rs.getString("TABLE_NAME");
                            boolean isView = tableName.startsWith("v"); // check whether is a view
                            MySQLTable.MySQLEngine engine = null;
                            String tableEngineStr = rs.getString("ENGINE");
                            if (!isView) {
                                engine = MySQLTable.MySQLEngine.get(tableEngineStr);
                            }
                            List<MySQLColumn> columns = getTableColumns(con, tableName, databaseName);
                            List<MySQLIndex> indexes = getIndexes(con, tableName, databaseName);
                            MySQLTable t = new MySQLTable(tableName, columns, indexes, engine, isView);
                            for (MySQLColumn c : columns) {
                                c.setTable(t);
                            }
                            for (MySQLIndex index : indexes) {
                                index.setTable(t);
                                for (String columnName : index.getColumnNames()) {
                                    if (columnName == null) continue;
                                    for (MySQLColumn column : columns) {
                                        if (columnName.equals(column.getName())) {
                                            index.getColumns().add(column);
                                            break;
                                        }
                                    }
                                }
                            }
                            databaseTables.add(t);
                        }
                    }
                }
                List<MySQLForeignKey> foreignKeys = new ArrayList<>(fetchForeignKeys(con, databaseName, databaseTables));
                return new MySQLSchema(databaseTables, foreignKeys);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<MySQLForeignKey> fetchForeignKeys(SQLConnection con, String databaseName, List<MySQLTable> databaseTables) throws SQLException {
        Map<String, MySQLForeignKey> foreignKeyMap = new HashMap<>();

        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '" + databaseName + "' AND REFERENCED_TABLE_NAME IS NOT NULL"
            );

            while (rs.next()) {
                String constraintName = rs.getString("CONSTRAINT_NAME");
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                String referencedTableName = rs.getString("REFERENCED_TABLE_NAME");
                String referencedColumnName = rs.getString("REFERENCED_COLUMN_NAME");

                MySQLForeignKey foreignKey = foreignKeyMap.get(constraintName);
                MySQLTable table = null;
                MySQLTable referencedTable = null;
                if (foreignKey == null) {
                    for (MySQLTable temp : databaseTables) {
                        if (temp.getName().equals(tableName)) {
                            table = temp;
                        }
                        if (temp.getName().equals(referencedTableName)) {
                            referencedTable = temp;
                        }
                    }
                    foreignKey = new MySQLForeignKey(constraintName, table, new ArrayList<>(), referencedTable, new ArrayList<>());
                    foreignKeyMap.put(constraintName, foreignKey);
                }
                MySQLColumn column = null;
                assert table != null;
                for (MySQLColumn temp : table.getColumns()) {
                    if (temp.getName().equals(columnName)) {
                        column = temp;
                        break;
                    }
                }
                MySQLColumn referencedColumn = null;
                assert referencedTable != null;
                for (MySQLColumn temp : referencedTable.getColumns()) {
                    if (temp.getName().equals(referencedColumnName)) {
                        referencedColumn = temp;
                        break;
                    }
                }
                foreignKey.getColumns().add(column);
                foreignKey.getReferencedColumns().add(referencedColumn);
            }
        }

        return new ArrayList<>(foreignKeyMap.values());
    }

    private static List<MySQLIndex> getIndexes(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        Map<String, MySQLIndex> indexMap = new HashMap<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    String columnName = rs.getString("COLUMN_NAME");
                    if (columnName == null) {
                        continue;
                    }
                    boolean nullable = "YES".equals(rs.getString("NULLABLE"));
                    int nonUnique = rs.getInt("NON_UNIQUE");
                    String indexType = rs.getString("INDEX_TYPE");
                    MySQLIndex index = indexMap.get(indexName);

                    if (index == null) {
                        index = new MySQLIndex(tableName, nonUnique, indexName, new ArrayList<>(), nullable, indexType);
                        indexMap.put(indexName, index);
                    }
                    index.getColumnNames().add(columnName);
                }
            }
        }
        return new ArrayList<>(indexMap.values());
    }

    private static List<MySQLColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MySQLColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    MySQLColumn column = new MySQLColumn(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("COLUMN_DEFAULT"),
                            "YES".equals(rs.getString("IS_NULLABLE")),
                            rs.getString("DATA_TYPE"),
                            rs.getLong("CHARACTER_MAXIMUM_LENGTH"),
                            rs.getInt("NUMERIC_PRECISION"),
                            rs.getInt("NUMERIC_SCALE"),
                            rs.getString("COLUMN_KEY")
                    );
                    String extra = rs.getString("EXTRA");
                    if (extra != null && (extra.contains("VIRTUAL GENERATED") || extra.contains("STORED GENERATED"))) {
                        column.setGenerated(true);
                    }
                    columns.add(column);
                }
            }
        }
        return columns;
    }

    public MySQLSchema(List<MySQLTable> databaseTables, List<MySQLForeignKey> foreignKeys) {
        super(databaseTables, Collections.emptyList());
        this.foreignKeys = foreignKeys;
    }

    public MySQLTables getRandomTableNonEmptyTables() {
        return new MySQLTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

}
