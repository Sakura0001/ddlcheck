package dbradar.sqlite3.schema;

import dbradar.IgnoreMeException;
import dbradar.SQLConnection;
import dbradar.common.schema.AbstractTableColumn;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SQLite3Column extends AbstractTableColumn<SQLite3Table, SQLite3DataType> {

    private final boolean isInteger; // "INTEGER" type, not "INT"
    private final SQLite3CollateSequence collate;
    private final boolean isGenerated;
    private final boolean isPrimaryKey;
    private final boolean isNotNull;

    public enum SQLite3CollateSequence {
        NOCASE, RTRIM, BINARY;

        public static SQLite3CollateSequence getCollate(String sql, boolean isView) {
            SQLite3CollateSequence collate;
            if (isView) {
                collate = BINARY;
            } else {
                if (sql.contains("collate binary")) {
                    collate = BINARY;
                } else if (sql.contains("collate rtrim")) {
                    collate = RTRIM;
                } else if (sql.contains("collate nocase")) {
                    collate = NOCASE;
                } else {
                    collate = BINARY;
                }
            }
            return collate;
        }
    }

    public SQLite3Column(String name, SQLite3DataType columnType, boolean isInteger, boolean isPrimaryKey,
                         SQLite3CollateSequence collate, boolean isGenerated, boolean isNotNull) {
        super(name, null, columnType);
        this.isInteger = isInteger;
        this.isPrimaryKey = isPrimaryKey;
        this.collate = collate;
        this.isGenerated = isGenerated;
        this.isNotNull = isNotNull;
        assert !isInteger || columnType == SQLite3DataType.INTEGER;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public boolean isOnlyPrimaryKey() {
        return isPrimaryKey && getTable().getColumns().stream().filter(SQLite3Column::isPrimaryKey).count() == 1;
    }

    // see https://www.sqlite.org/lang_createtable.html#rowid

    /**
     * If a table has a single column primary key and the declared type of that
     * column is "INTEGER" and the table is not a WITHOUT ROWID table, then the
     * column is known as an INTEGER PRIMARY KEY.
     *
     * @return whether the column is an INTEGER PRIMARY KEY
     */
    public boolean isIntegerPrimaryKey() {
        return isInteger && isOnlyPrimaryKey() && !getTable().hasWithoutRowid();
    }

    public SQLite3CollateSequence getCollateSequence() {
        return collate;
    }

    public boolean isGenerated() {
        return isGenerated;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    public static List<SQLite3Column> getTableColumns(SQLConnection con, String tableName, String sql, boolean isView,
                                                      boolean isDbStatsTable) throws SQLException {
        List<SQLite3Column> databaseColumns = new ArrayList<>();
        try (Statement stmt = con.createStatement()) {
            String getTableInfo = String.format("PRAGMA table_xinfo(%s)", tableName);
            try (ResultSet columnRs = stmt.executeQuery(getTableInfo)) {
                String[] columnCreates = sql.split(",");
                int columnCreateIndex = 0;
                while (columnRs.next()) {
                    String columnName = columnRs.getString("name");
                    if (columnName.contentEquals("docid") || columnName.contentEquals("rank")
                            || columnName.contentEquals(tableName) || columnName.contentEquals("__langid")) {
                        continue; // internal column names of FTS tables
                    }
                    if (isDbStatsTable && columnName.contentEquals("aggregate")) {
                        // see https://www.sqlite.org/src/tktview?name=a3713a5fca
                        continue;
                    }
                    String dataType = columnRs.getString("type");
                    boolean isPrimaryKey = columnRs.getBoolean("pk");
                    boolean isGenerated = columnRs.getInt("hidden") == 2;
                    boolean isNotNull = columnRs.getBoolean("notnull");
                    SQLite3DataType columnType = SQLite3DataType.getSQLiteDateType(dataType);
                    SQLite3CollateSequence collate;
                    if (!isDbStatsTable && columnCreateIndex < columnCreates.length) {
                        String columnSql = columnCreates[columnCreateIndex];
                        columnCreateIndex++;
                        collate = SQLite3CollateSequence.getCollate(columnSql, isView);
                    } else {
                        collate = SQLite3CollateSequence.BINARY;
                    }
                    databaseColumns.add(new SQLite3Column(columnName, columnType,
                            dataType.contentEquals("INTEGER"), isPrimaryKey, collate, isGenerated, isNotNull));
                }
            }
        } catch (SQLException ignored) {
        }
        if (databaseColumns.isEmpty()) {
            // only generated columns
            throw new IgnoreMeException();
        }
        return databaseColumns;
    }
}
