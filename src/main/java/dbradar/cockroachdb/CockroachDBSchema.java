package dbradar.cockroachdb;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import dbradar.common.schema.AbstractRelationalTable;
import dbradar.common.schema.AbstractSchema;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.common.schema.AbstractTables;
import dbradar.common.schema.AbstractTrigger;
import dbradar.common.schema.TableIndex;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dbradar.cockroachdb.CockroachDBSchema.CockroachDBTable;
import dbradar.cockroachdb.CockroachDBSchema.CockroachDBTrigger;

public class CockroachDBSchema extends AbstractSchema<CockroachDBGlobalState, CockroachDBTable, CockroachDBTrigger> {

    private List<CockroachDBForeignKey> foreignKeys;

    public enum CockroachDBDataType {

        INT, BOOL, STRING, FLOAT, BYTES, BIT, VARBIT, /*SERIAL,*/ INTERVAL, TIMESTAMP, TIMESTAMPTZ, DECIMAL, JSONB,
        ARRAY, CHAR, DATE;

        public static CockroachDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static class CockroachDBCompositeDataType {

        private final CockroachDBDataType dataType;

        private final int size;

        private CockroachDBCompositeDataType elementType;

        public CockroachDBCompositeDataType(CockroachDBDataType dataType) {
            this.dataType = dataType;
            this.size = -1;
        }

        public CockroachDBCompositeDataType(CockroachDBDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public CockroachDBCompositeDataType(CockroachDBDataType dataType, CockroachDBCompositeDataType elementType) {
            if (dataType != CockroachDBDataType.ARRAY) {
                throw new IllegalArgumentException();
            }
            this.dataType = dataType;
            this.size = -1;
            this.elementType = elementType;
        }

        public CockroachDBDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public boolean isString() {
            return dataType == CockroachDBDataType.STRING;
        }

        public static CockroachDBCompositeDataType getInt(int size) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.INT, size);
        }

        public static CockroachDBCompositeDataType getBit(int size) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.BIT, size);
        }

        public static CockroachDBCompositeDataType getVarBit(int maxSize) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.VARBIT, maxSize);
        }

        public CockroachDBCompositeDataType getElementType() {
            return elementType;
        }

    }

    public static class CockroachDBForeignKey {
        private String constraintName;
        private CockroachDBTable table;
        private List<CockroachDBColumn> columns;
        private CockroachDBTable referencedTable;
        private List<CockroachDBColumn> referencedColumns;

        public CockroachDBForeignKey(String constraintName, CockroachDBTable table, List<CockroachDBColumn> columns,
                                     CockroachDBTable referencedTable, List<CockroachDBColumn> referencedColumns) {
            this.constraintName = constraintName;
            this.table = table;
            this.columns = columns;
            this.referencedTable = referencedTable;
            this.referencedColumns = referencedColumns;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public CockroachDBTable getTable() {
            return table;
        }

        public List<CockroachDBColumn> getColumns() {
            return columns;
        }

        public CockroachDBTable getReferencedTable() {
            return referencedTable;
        }

        public List<CockroachDBColumn> getReferencedColumns() {
            return referencedColumns;
        }
    }


    public static class CockroachDBColumn extends AbstractTableColumn<CockroachDBTable, CockroachDBDataType> {

        private String dataType;
        private boolean isPrimaryKey;
        private boolean isNullable;
        private String defaultValue;

        public CockroachDBColumn(String name, String dataType, boolean isPrimaryKey,
                                 boolean isNullable, String defaultValue) {
            super(name, null, getColumnType(dataType).getPrimitiveDataType());
            this.dataType = dataType;
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
            this.defaultValue = defaultValue;
        }

        public String getDataType() {
            return dataType;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public String getDefaultValue() {
            return defaultValue;
        }
    }

    public static class CockroachDBTables extends AbstractTables<CockroachDBTable, CockroachDBColumn> {

        public CockroachDBTables(List<CockroachDBTable> tables) {
            super(tables);
        }

    }

    public CockroachDBSchema(List<CockroachDBTable> databaseTables, List<CockroachDBForeignKey> foreignKeys) {
        super(databaseTables, Collections.emptyList());
        this.foreignKeys = foreignKeys;
    }

    public CockroachDBTables getRandomTableNonEmptyTables() {
        return new CockroachDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public CockroachDBTables getRandomTableNonEmptyTables(int nr) {
        return new CockroachDBTables(Randomly.nonEmptySubsetLeast(getDatabaseTables(), nr));
    }

    private static CockroachDBCompositeDataType getColumnType(String typeString) {
        if (typeString.endsWith("[]")) {
            String substring = typeString.substring(0, typeString.length() - 2);
            CockroachDBCompositeDataType elementType = getColumnType(substring);
            return new CockroachDBCompositeDataType(CockroachDBDataType.ARRAY, elementType);
        }
        if (typeString.startsWith("STRING") || typeString.startsWith("VARCHAR") || typeString.startsWith("CHAR(")) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.STRING);
        }
        if (typeString.startsWith("BIT(")) {
            int val = Integer.parseInt(typeString.substring(4, typeString.length() - 1));
            return CockroachDBCompositeDataType.getBit(val);
        }
        if (typeString.startsWith("VARBIT(")) {
            int val = Integer.parseInt(typeString.substring(7, typeString.length() - 1));
            return CockroachDBCompositeDataType.getBit(val);
        }
        if (typeString.startsWith("INTERVAL")) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.INTERVAL);
        }
        if (typeString.startsWith("DECIMAL")) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.DECIMAL);
        }
        if (typeString.startsWith("TIMESTAMP") || typeString.startsWith("TIME")) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.TIMESTAMP);
        }
        if (typeString.startsWith("TIMESTAMPTZ") || typeString.startsWith("TIMETZ")) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.TIMESTAMPTZ);
        }
        if (typeString.startsWith("FLOAT")) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.FLOAT);
        }
        switch (typeString) {
            case "VARBIT":
                return CockroachDBCompositeDataType.getVarBit(-1);
            case "BIT":
                return CockroachDBCompositeDataType.getBit(1);
            case "INT8":
                return CockroachDBCompositeDataType.getInt(8);
            case "INT4":
                return CockroachDBCompositeDataType.getInt(4);
            case "INT2":
                return CockroachDBCompositeDataType.getInt(2);
            case "BOOL":
                return new CockroachDBCompositeDataType(CockroachDBDataType.BOOL);
            case "BYTES":
                return new CockroachDBCompositeDataType(CockroachDBDataType.BYTES);
            case "JSONB":
                return new CockroachDBCompositeDataType(CockroachDBDataType.JSONB);
            case "CHAR":
            case "CHARACTER":
                return new CockroachDBCompositeDataType(CockroachDBDataType.CHAR);
            case "DATE":
                return new CockroachDBCompositeDataType(CockroachDBDataType.DATE);
            default:
                throw new AssertionError(typeString);
        }
    }

    public static class CockroachDBTable
            extends AbstractRelationalTable<CockroachDBColumn, CockroachDBIndex, CockroachDBGlobalState> {

        private boolean isMatView;

        public CockroachDBTable(String tableName, List<CockroachDBColumn> columns, List<CockroachDBIndex> indexes,
                                boolean isView) {
            super(tableName, columns, indexes, false, isView);
            if (isView) {
                isMatView = tableName.startsWith("vt");
            }
        }

        public boolean isMatView() {
            return isMatView;
        }
    }

    public static class CockroachDBIndex extends TableIndex {

        private CockroachDBTable table;
        private boolean nonUnique;
        private boolean isPrimaryKey;
        private List<CockroachDBColumn> columns = new ArrayList<>();

        public CockroachDBIndex(String indexName, String tableName, boolean nonUnique,
                                boolean isPrimaryKey, List<String> columnNames) {
            super(indexName, tableName, columnNames, !nonUnique);
            this.nonUnique = nonUnique;
            this.isPrimaryKey = isPrimaryKey;
        }

        public void setTable(CockroachDBTable table) {
            this.table = table;
        }

        public CockroachDBTable getTable() {
            return table;
        }

        public boolean isNonUnique() {
            return nonUnique;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public List<CockroachDBColumn> getColumns() {
            return columns;
        }
    }

    public CockroachDBIndex getRandomIndex() {
        List<CockroachDBIndex> indexes = new ArrayList<>();
        for (CockroachDBTable table : getDatabaseTables()) {
            for (CockroachDBIndex index : table.getIndexes()) {
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

    public int getIndexCount() {
        int count = 0;
        for (CockroachDBTable table : getDatabaseTables()) {
            count += table.getIndexes().size();
        }
        return count;
    }

    public static class CockroachDBTrigger extends AbstractTrigger {

        public CockroachDBTrigger(String name) {
            super(name);
        }
    }


    public static CockroachDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<CockroachDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            List<CockroachDBColumn> columns = getTableColumns(con, tableName);
            List<CockroachDBIndex> indexes = getIndexes(con, tableName);
            boolean isView = tableName.startsWith("v");
            CockroachDBTable table = new CockroachDBTable(tableName, columns, indexes, isView);
            for (CockroachDBColumn column : columns) {
                column.setTable(table);
            }
            for (CockroachDBIndex index : indexes) {
                index.setTable(table);
                for (String columnName : index.getColumnNames()) {
                    for (CockroachDBColumn column : columns) {
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
        List<CockroachDBForeignKey> foreignKeys = new ArrayList<>(fetchForeignKeys(con, databaseName, databaseTables));
        return new CockroachDBSchema(databaseTables, foreignKeys);
    }

    private static List<CockroachDBForeignKey> fetchForeignKeys(SQLConnection con, String databaseName, List<CockroachDBTable> databaseTables) throws SQLException {
        Map<String, CockroachDBForeignKey> foreignKeyMap = new HashMap<>();

        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT\n" +
                    "    kcu.constraint_name AS foreign_key_name,\n" +
                    "    kcu.table_name AS table_with_fk,\n" +
                    "    kcu.column_name AS fk_column,\n" +
                    "    ccu.table_name AS referenced_table,\n" +
                    "    ccu.column_name AS referenced_column\n" +
                    "FROM\n" +
                    "    information_schema.key_column_usage kcu\n" +
                    "JOIN\n" +
                    "    information_schema.table_constraints tc\n" +
                    "    ON kcu.constraint_name = tc.constraint_name\n" +
                    "JOIN\n" +
                    "    information_schema.constraint_column_usage ccu\n" +
                    "    ON tc.constraint_name = ccu.constraint_name\n" +
                    "WHERE\n" +
                    "    tc.constraint_type = 'FOREIGN KEY'\n");

            while (rs.next()) {
                String constraintName = rs.getString("foreign_key_name");
                String tableName = rs.getString("table_with_fk");
                String columnName = rs.getString("fk_column");
                String referencedTableName = rs.getString("referenced_table");
                String referencedColumnName = rs.getString("referenced_column");

                CockroachDBForeignKey foreignKey = foreignKeyMap.get(constraintName);
                CockroachDBTable table = null;
                CockroachDBTable referencedTable = null;
                if (foreignKey == null) {
                    for (CockroachDBTable temp : databaseTables) {
                        if (temp.getName().equals(tableName)) {
                            table = temp;
                        }
                        if (temp.getName().equals(referencedTableName)) {
                            referencedTable = temp;
                        }
                    }
                    foreignKey = new CockroachDBForeignKey(constraintName, table, new ArrayList<>(), referencedTable, new ArrayList<>());
                    foreignKeyMap.put(constraintName, foreignKey);
                }
                CockroachDBColumn column = null;
                assert table != null;
                for (CockroachDBColumn temp : table.getColumns()) {
                    if (temp.getName().equals(columnName)) {
                        column = temp;
                        break;
                    }
                }
                CockroachDBColumn referencedColumn = null;
                assert referencedTable != null;
                for (CockroachDBColumn temp : referencedTable.getColumns()) {
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

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            ResultSet tableRs = s.executeQuery(
                    "SELECT table_name FROM information_schema.tables WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW');");
            while (tableRs.next()) {
                String tableName = tableRs.getString(1);
                tableNames.add(tableName);
            }
        }
        return tableNames;
    }

    private static List<CockroachDBIndex> getIndexes(SQLConnection con, String tableName) throws SQLException {
        Map<String, CockroachDBIndex> indexMap = new HashMap<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("SHOW INDEX FROM %s", tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("index_name");
                    String columnName = rs.getString("column_name");
                    if (columnName.equals("rowid")) continue;
                    boolean nonUnique = rs.getBoolean("non_unique");
                    boolean isPrimaryKey = indexName.contains("pkey");
                    CockroachDBIndex index = indexMap.get(indexName);

                    if (index == null) {
                        index = new CockroachDBIndex(indexName, tableName, nonUnique, isPrimaryKey, new ArrayList<>());
                        indexMap.put(indexName, index);
                    }
                    index.getColumnNames().add(columnName);
                }
            }
        }
        return new ArrayList<>(indexMap.values());
    }

    private static List<CockroachDBColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<CockroachDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SHOW COLUMNS FROM " + tableName)) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    if (columnName.contains("crdb_internal") || columnName.equals("rowid")) {
                        continue; // created for CREATE INDEX ON t0(c0) USING HASH WITH BUCKET_COUNT = 1;
                    }
                    String dataType = rs.getString("data_type");
                    boolean isNullable = rs.getBoolean("is_nullable");
                    String indices = rs.getString("indices");
                    boolean isPrimaryKey = indices != null && indices.contains("primary");
                    String defaultValue = rs.getString("column_default");
                    CockroachDBColumn c = new CockroachDBColumn(columnName, dataType, isPrimaryKey,
                            isNullable, defaultValue);
                    columns.add(c);
                }
            } catch (SQLException e) {
                if (CockroachDBBugs.bug85394 && e.getMessage().contains("incompatible type annotation for ARRAY")) {
                    return columns;
                }
                throw e;
            }
        }
        return columns;
    }

    public CockroachDBTable getRandomMaterializedView() {
        return getRandomTable(t -> t.isView() && t.getName().startsWith("vt"));
    }

    public CockroachDBTable getRandomNormalView() {
        return getRandomTable(t -> t.isView() && !t.getName().startsWith("vt"));
    }

    public List<CockroachDBForeignKey> getForeignKeys() {
        return foreignKeys;
    }
}
