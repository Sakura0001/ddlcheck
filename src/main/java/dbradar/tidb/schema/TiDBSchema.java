package dbradar.tidb.schema;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.common.schema.AbstractRelationalTable;
import dbradar.common.schema.AbstractSchema;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.common.schema.AbstractTables;
import dbradar.common.schema.TableIndex;
import dbradar.tidb.schema.TiDBSchema.TiDBTable;
import dbradar.tidb.TiDBGlobalState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TiDBSchema extends AbstractSchema<TiDBGlobalState, TiDBTable, TiDBTrigger> {
    private List<TiDBForeignKey> foreignKeyList;

    public enum TiDBDataType {
        BIT, INT, FLOAT, DOUBLE, DECIMAL, NUMERIC, TEXT, BLOB, CHAR;

        public static TiDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }

        public boolean isNumeric() {
            switch (this) {
                case INT:
                case FLOAT:
                case DOUBLE:
                case DECIMAL:
                case NUMERIC:
                    return true;
                case BIT:
                case TEXT:
                case BLOB:
                case CHAR:
                    return false;
                default:
                    throw new AssertionError(this);
            }
        }

        public boolean isTextOrBlob() {
            return this == TEXT || this == BLOB;
        }
    }

    public static class TiDBColumn extends AbstractTableColumn<TiDBTable, TiDBDataType> {

        private String columnName;
        private String columnDefault;
        private boolean isNullable;
        private String dataType;
        private Long characterMaximumLength;
        private Integer numericPrecision;
        private Integer numericScale;
        private String columnKey;

        public TiDBColumn(String columnName,
                          String columnDefault, boolean isNullable, String dataType,
                          Long characterMaximumLength, Integer numericPrecision, Integer numericScale,
                          String columnKey) {
            super(columnName, null, getTiDBDataType(dataType));
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

    public static class TiDBTables extends AbstractTables<TiDBTable, TiDBColumn> {

        public TiDBTables(List<TiDBTable> tables) {
            super(tables);
        }

    }

    public static class TiDBTable extends AbstractRelationalTable<TiDBColumn, TiDBIndex, TiDBGlobalState> {

        public TiDBTable(String tableName, List<TiDBColumn> columns, List<TiDBIndex> indexes, boolean isView) {
            super(tableName, columns, Collections.emptyList(), indexes, false, isView);
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

    }


    public static class TiDBForeignKey {
        private String constraintName;
        private TiDBTable table;
        private List<TiDBColumn> columns;
        private TiDBTable referencedTable;
        private List<TiDBColumn> referencedColumns;

        public TiDBForeignKey(String constraintName, TiDBTable table, List<TiDBColumn> columns,
                              TiDBTable referencedTable, List<TiDBColumn> referencedColumns) {
            this.constraintName = constraintName;
            this.table = table;
            this.columns = columns;
            this.referencedTable = referencedTable;
            this.referencedColumns = referencedColumns;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public TiDBTable getTable() {
            return table;
        }

        public List<TiDBColumn> getColumns() {
            return columns;
        }

        public TiDBTable getReferencedTable() {
            return referencedTable;
        }

        public List<TiDBColumn> getReferencedColumns() {
            return referencedColumns;
        }

        @Override
        public String toString() {
            String tableName = table.getName();
            List<String> columnNames = new ArrayList<>();
            for (TiDBColumn column : columns) {
                columnNames.add(column.getName());
            }
            String referencedTableName = referencedTable.getName();
            List<String> referencedColumnNames = new ArrayList<>();
            for (TiDBColumn column : referencedColumns) {
                referencedColumnNames.add(column.getName());
            }
            return "TiDBForeignKey{" +
                    "constraintName='" + constraintName + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", columnNames=" + columnNames +
                    ", referencedTableName='" + referencedTableName + '\'' +
                    ", referencedColumnNames=" + referencedColumnNames +
                    '}';
        }
    }

    public static final class TiDBIndex extends TableIndex {

        private TiDBTable table;
        private int nonUnique;
        private List<TiDBColumn> columns = new ArrayList<>();
        private List<String> columnNames;
        private boolean nullable;
        private String indexType;

        public TiDBIndex(String tableName, int nonUnique, String indexName,
                         List<String> columnNames, boolean nullable, String indexType) {
            super(indexName, tableName, columnNames, nonUnique == 0);
            this.table = null;
            this.nonUnique = nonUnique;
            this.columnNames = columnNames;
            this.nullable = nullable;
            this.indexType = indexType;
        }

        public void setTable(TiDBTable table) {
            this.table = table;
        }

        @Override
        public String getTableName() {
            return table.getName();
        }

        public int getNonUnique() {
            return nonUnique;
        }

        public List<TiDBColumn> getColumns() {
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
            return "TiDBIndex{" +
                    "tableName='" + table.getName() + '\'' +
                    ", nonUnique=" + nonUnique +
                    ", indexName='" + getName() + '\'' +
                    ", columnNames=" + columnNames +
                    ", nullable=" + nullable +
                    ", indexType='" + indexType + '\'' +
                    '}';
        }
    }

    public TiDBIndex getRandomIndex() {
        List<TiDBIndex> indexes = new ArrayList<>();
        for (TiDBTable table : getDatabaseTables()) {
            for (TiDBIndex index : table.getIndexes()) {
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

    public TiDBSchema(List<TiDBTable> databaseTables, List<TiDBForeignKey> foreignKeyList) {
        super(databaseTables, Collections.emptyList());
        this.foreignKeyList = foreignKeyList;
    }

    public List<TiDBForeignKey> getForeignKeyList() {
        return foreignKeyList;
    }

    private static TiDBDataType getTiDBDataType(String typeString) {
        switch (typeString) {
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
                return TiDBDataType.INT;
            case "float":
                return TiDBDataType.FLOAT;
            case "double":
                return TiDBDataType.DOUBLE;
            case "decimal":
                return TiDBDataType.DECIMAL;
            case "numeric":
                return TiDBDataType.NUMERIC;
            case "char":
            case "character":
                return TiDBDataType.CHAR;
            case "varchar":
            case "tinytext":
            case "mediumtext":
            case "text":
            case "longtext":
                return TiDBDataType.TEXT;
            case "bit":
                return TiDBDataType.BIT;
            case "binary":
            case "blob":
            case "varbinary":
            case "tinyblob":
            case "mediumblob":
            case "longblob":
                return TiDBDataType.BLOB;
            default:
                throw new AssertionError(typeString);
        }
    }

    public static TiDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<TiDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            List<TiDBColumn> columns = getTableColumns(con, tableName, databaseName);
            List<TiDBIndex> indexes = getIndexes(con, tableName, databaseName);
            boolean isView = tableName.startsWith("v");
            TiDBTable t = new TiDBTable(tableName, columns, indexes, isView);
            for (TiDBColumn c : columns) {
                c.setTable(t);
            }
            for (TiDBIndex index : indexes) {
                index.setTable(t);
                for (String columnName : index.getColumnNames()) {
                    for (TiDBColumn column : columns) {
                        if (columnName.equals(column.getName())) {
                            index.getColumns().add(column);
                            break;
                        }
                    }
                }
            }
            databaseTables.add(t);

        }

        List<TiDBForeignKey> foreignKeys = new ArrayList<>(fetchForeignKeys(con, databaseName, databaseTables));
        return new TiDBSchema(databaseTables, foreignKeys);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            ResultSet tableRs = s.executeQuery("SHOW TABLES");
            while (tableRs.next()) {
                String tableName = tableRs.getString(1);
                tableNames.add(tableName);
            }
        }
        return tableNames;
    }

    private static List<TiDBIndex> getIndexes(SQLConnection con, String tableName, String databaseName) throws SQLException {
        Map<String, TiDBIndex> indexMap = new HashMap<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    String columnName = rs.getString("COLUMN_NAME");
                    boolean nullable = "YES".equals(rs.getString("NULLABLE"));
                    int nonUnique = rs.getInt("NON_UNIQUE");
                    String indexType = rs.getString("INDEX_TYPE");
                    TiDBIndex index = indexMap.get(indexName);

                    if (index == null) {
                        index = new TiDBIndex(tableName, nonUnique, indexName, new ArrayList<>(), nullable, indexType);
                        indexMap.put(indexName, index);
                    }
                    index.getColumnNames().add(columnName);
                }
            }
        }
        return new ArrayList<>(indexMap.values());
    }

    private static List<TiDBColumn> getTableColumns(SQLConnection con, String tableName, String databaseName) throws SQLException {
        List<TiDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    TiDBColumn column = new TiDBColumn(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("COLUMN_DEFAULT"),
                            "YES".equals(rs.getString("IS_NULLABLE")),
                            rs.getString("DATA_TYPE"),
                            rs.getLong("CHARACTER_MAXIMUM_LENGTH"),
                            rs.getInt("NUMERIC_PRECISION"),
                            rs.getInt("NUMERIC_SCALE"),
                            rs.getString("COLUMN_KEY")
                    );
                    columns.add(column);
                }
            }
        }
        return columns;
    }

    private static List<TiDBForeignKey> fetchForeignKeys(SQLConnection con, String databaseName, List<TiDBTable> databaseTables) throws SQLException {
        List<TiDBForeignKey> foreignKeys;
        Map<String, TiDBForeignKey> foreignKeyMap = new HashMap<>();

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

                TiDBForeignKey foreignKey = foreignKeyMap.get(constraintName);
                TiDBTable table = null;
                TiDBTable referencedTable = null;
                if (foreignKey == null) {
                    for (TiDBTable temp : databaseTables) {
                        if (temp.getName().equals(tableName)) {
                            table = temp;
                        }
                        if (temp.getName().equals(referencedTableName)) {
                            referencedTable = temp;
                        }
                    }
                    foreignKey = new TiDBForeignKey(constraintName, table, new ArrayList<>(), referencedTable, new ArrayList<>());
                    foreignKeyMap.put(constraintName, foreignKey);
                }
                TiDBColumn column = null;
                assert table != null;
                for (TiDBColumn temp : table.getColumns()) {
                    if (temp.getName().equals(columnName)) {
                        column = temp;
                        break;
                    }
                }
                TiDBColumn referencedColumn = null;
                assert referencedTable != null;
                for (TiDBColumn temp : referencedTable.getColumns()) {
                    if (temp.getName().equals(referencedColumnName)) {
                        referencedColumn = temp;
                        break;
                    }
                }
                foreignKey.getColumns().add(column);
                foreignKey.getReferencedColumns().add(referencedColumn);
            }

            foreignKeys = new ArrayList<>(foreignKeyMap.values());
        }

        return foreignKeys;
    }
}
