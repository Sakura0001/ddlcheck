package dbradar.mariadb;

import dbradar.IgnoreMeException;
import dbradar.common.schema.AbstractTrigger;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.common.schema.AbstractRelationalTable;
import dbradar.common.schema.AbstractSchema;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.common.schema.TableIndex;
import dbradar.mariadb.MariaDBProvider.MariaDBGlobalState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MariaDBSchema extends AbstractSchema<MariaDBGlobalState, MariaDBSchema.MariaDBTable, MariaDBSchema.MariaDBTrigger> {

    private static final int NR_SCHEMA_READ_TRIES = 10;
    private List<MariaDBForeignKey> foreignKeyList;

    public enum MariaDBDataType {
        INT, VARCHAR, FLOAT, DOUBLE, DECIMAL, BIT, BLOB;
    }

    public static class MariaDBColumn extends AbstractTableColumn<MariaDBTable, MariaDBDataType> {

        private String columnName;
        private String columnDefault;
        private boolean isNullable;
        private String dataType;
        private Long characterMaximumLength;
        private Integer numericPrecision;
        private Integer numericScale;
        private String columnKey;

        public MariaDBColumn(String columnName,
                             String columnDefault, boolean isNullable, String dataType,
                             Long characterMaximumLength, Integer numericPrecision, Integer numericScale,
                             String columnKey) {
            super(columnName, null, getMariaDBDataType(dataType));
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

    public static class MariaDBTables {
        private final List<MariaDBTable> tables;
        private final List<MariaDBColumn> columns;

        public MariaDBTables(List<MariaDBTable> tables) {
            this.tables = tables;
            columns = new ArrayList<>();
            for (MariaDBTable t : tables) {
                columns.addAll(t.getColumns());
            }
        }

        public String tableNamesAsString() {
            return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
        }

        public List<MariaDBTable> getTables() {
            return tables;
        }

        public List<MariaDBColumn> getColumns() {
            return columns;
        }

        public String columnNamesAsString() {
            return getColumns().stream().map(t -> t.getTable().getName() + "." + t.getName())
                    .collect(Collectors.joining(", "));
        }

        public String columnNamesAsString(Function<MariaDBColumn, String> function) {
            return getColumns().stream().map(function).collect(Collectors.joining(", "));
        }
    }

    private static MariaDBDataType getMariaDBDataType(String typeString) {
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
                return MariaDBDataType.INT;
            case "char":
            case "character":
            case "varchar":
            case "tinytext":
            case "mediumtext":
            case "text":
            case "longtext":
                return MariaDBDataType.VARCHAR;
            case "double":
                return MariaDBDataType.DOUBLE;
            case "float":
                return MariaDBDataType.FLOAT;
            case "decimal":
                return MariaDBDataType.DECIMAL;
            case "bit":
                return MariaDBDataType.BIT;
            case "binary":
            case "blob":
            case "varbinary":
            case "tinyblob":
            case "mediumblob":
            case "longblob":
                return MariaDBDataType.BLOB;
            default:
                throw new AssertionError(typeString);
        }
    }

    public static class MariaDBForeignKey {
        private String constraintName;
        private MariaDBTable table;
        private List<MariaDBColumn> columns;
        private MariaDBTable referencedTable;
        private List<MariaDBColumn> referencedColumns;

        public MariaDBForeignKey(String constraintName, MariaDBTable table, List<MariaDBColumn> columns,
                                 MariaDBTable referencedTable, List<MariaDBColumn> referencedColumns) {
            this.constraintName = constraintName;
            this.table = table;
            this.columns = columns;
            this.referencedTable = referencedTable;
            this.referencedColumns = referencedColumns;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public MariaDBTable getTable() {
            return table;
        }

        public List<MariaDBColumn> getColumns() {
            return columns;
        }

        public MariaDBTable getReferencedTable() {
            return referencedTable;
        }

        public List<MariaDBColumn> getReferencedColumns() {
            return referencedColumns;
        }

        @Override
        public String toString() {
            String tableName = table.getName();
            List<String> columnNames = new ArrayList<>();
            for (MariaDBColumn column : columns) {
                columnNames.add(column.getName());
            }
            String referencedTableName = referencedTable.getName();
            List<String> referencedColumnNames = new ArrayList<>();
            for (MariaDBColumn column : referencedColumns) {
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

    public static class MariaDBTable extends AbstractRelationalTable<MariaDBColumn, MariaDBIndex, MariaDBGlobalState> {

        public enum MariaDBEngine {

            INNO_DB("InnoDB"), MY_ISAM("MyISAM"), ARIA("Aria"), MEMORY("MEMORY"), CSV("CSV");

            private String s;

            MariaDBEngine(String s) {
                this.s = s;
            }

            public String getTextRepresentation() {
                return s;
            }

            public static MariaDBEngine get(String val) {
                Optional<MariaDBEngine> target = Stream.of(values()).filter(engine -> engine.s.equalsIgnoreCase(val)).findFirst();
                if (target.isPresent()) {
                    return target.get();
                } else {
                    throw new RuntimeException("Does not define such engine type: " + val);
                }
            }

            public static MariaDBEngine getRandomEngine() {
                return Randomly.fromOptions(MariaDBEngine.values());
            }

        }

        private final MariaDBEngine engine;

        public MariaDBTable(String tableName, List<MariaDBColumn> columns, List<MariaDBIndex> indexes,
                            MariaDBEngine engine, boolean isView) {
            super(tableName, columns, indexes, false, isView);
            this.engine = engine;
        }

        public MariaDBEngine getEngine() {
            return engine;
        }

    }

    public static final class MariaDBTrigger extends AbstractTrigger {

        public MariaDBTrigger(String name) {
            super(name);
        }
    }


    public static final class MariaDBIndex extends TableIndex {

        private MariaDBTable table;
        private int nonUnique;
        private List<MariaDBColumn> columns = new ArrayList<>();
        private List<String> columnNames;
        private boolean nullable;
        private String indexType;

        public MariaDBIndex(String tableName, int nonUnique, String indexName,
                            List<String> columnNames, boolean nullable, String indexType) {
            super(indexName, tableName, columnNames, nonUnique == 0);
            this.table = null;
            this.nonUnique = nonUnique;
            this.columnNames = columnNames;
            this.nullable = nullable;
            this.indexType = indexType;
        }

        public void setTable(MariaDBTable table) {
            this.table = table;
        }

        @Override
        public String getTableName() {
            return table.getName();
        }

        public int getNonUnique() {
            return nonUnique;
        }

        public List<MariaDBColumn> getColumns() {
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
            return "MariaDBIndex{" +
                    "tableName='" + table.getName() + '\'' +
                    ", nonUnique=" + nonUnique +
                    ", indexName='" + getName() + '\'' +
                    ", columnNames=" + columnNames +
                    ", nullable=" + nullable +
                    ", indexType='" + indexType + '\'' +
                    '}';
        }

    }

    public List<MariaDBForeignKey> getForeignKeyList() {
        return foreignKeyList;
    }

    public MariaDBIndex getRandomIndex() {
        List<MariaDBIndex> indexes = new ArrayList<>();
        for (MariaDBTable table : getDatabaseTables()) {
            for (MariaDBIndex index : table.getIndexes()) {
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

    public static MariaDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        /* the loop is a workaround for https://bugs.MariaDB.com/bug.php?id=95929 */
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<MariaDBTable> databaseTables = new ArrayList<>();
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(
                            "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
                                    + databaseName + "';")) {
                        while (rs.next()) {
                            String tableName = rs.getString("TABLE_NAME");
                            boolean isView = tableName.startsWith("v"); // check whether is a view, todo: find a better way
                            MariaDBTable.MariaDBEngine engine = null;
                            String tableEngineStr = rs.getString("ENGINE");
                            if (!isView) {
                                engine = MariaDBTable.MariaDBEngine.get(tableEngineStr);
                            }
                            List<MariaDBColumn> columns = getTableColumns(con, tableName, databaseName);
                            List<MariaDBIndex> indexes = getIndexes(con, tableName, databaseName);
                            MariaDBTable t = new MariaDBTable(tableName, columns, indexes, engine, isView);
                            for (MariaDBColumn c : columns) {
                                c.setTable(t);
                            }
                            for (MariaDBIndex index : indexes) {
                                index.setTable(t);
                                for (String columnName : index.getColumnNames()) {
                                    for (MariaDBColumn column : columns) {
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
                List<MariaDBForeignKey> foreignKeys = new ArrayList<>(fetchForeignKeys(con, databaseName, databaseTables));
                return new MariaDBSchema(databaseTables, foreignKeys);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<MariaDBIndex> getIndexes(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        Map<String, MariaDBIndex> indexMap = new HashMap<>();
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
                    MariaDBIndex index = indexMap.get(indexName);

                    if (index == null) {
                        index = new MariaDBIndex(tableName, nonUnique, indexName, new ArrayList<>(), nullable, indexType);
                        indexMap.put(indexName, index);
                    }
                    index.getColumnNames().add(columnName);
                }
            }
        }
        return new ArrayList<>(indexMap.values());
    }

    private static List<MariaDBColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MariaDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    MariaDBColumn column = new MariaDBColumn(
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

    private static List<MariaDBForeignKey> fetchForeignKeys(SQLConnection con, String databaseName, List<MariaDBTable> databaseTables) throws SQLException {
        List<MariaDBForeignKey> foreignKeys;
        Map<String, MariaDBForeignKey> foreignKeyMap = new HashMap<>();

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

                MariaDBForeignKey foreignKey = foreignKeyMap.get(constraintName);
                MariaDBTable table = null;
                MariaDBTable referencedTable = null;
                if (foreignKey == null) {
                    for (MariaDBTable temp : databaseTables) {
                        if (temp.getName().equals(tableName)) {
                            table = temp;
                        }
                        if (temp.getName().equals(referencedTableName)) {
                            referencedTable = temp;
                        }
                    }
                    foreignKey = new MariaDBForeignKey(constraintName, table, new ArrayList<>(), referencedTable, new ArrayList<>());
                    foreignKeyMap.put(constraintName, foreignKey);
                }
                MariaDBColumn column = null;
                assert table != null;
                for (MariaDBColumn temp : table.getColumns()) {
                    if (temp.getName().equals(columnName)) {
                        column = temp;
                        break;
                    }
                }
                MariaDBColumn referencedColumn = null;
                assert referencedTable != null;
                for (MariaDBColumn temp : referencedTable.getColumns()) {
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

    public MariaDBSchema(List<MariaDBTable> databaseTables, List<MariaDBForeignKey> foreignKeyList) {
        super(databaseTables, Collections.emptyList());
        this.foreignKeyList = foreignKeyList;
    }

}
