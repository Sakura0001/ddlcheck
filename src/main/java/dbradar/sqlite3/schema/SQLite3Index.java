package dbradar.sqlite3.schema;

import dbradar.SQLConnection;
import dbradar.common.schema.TableIndex;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SQLite3Index extends TableIndex {

    private boolean isTemporary;
    List<SQLite3Column> columns;

    public SQLite3Index(String indexName, String tableName, List<String> columnNames, List<SQLite3Column> columns, boolean isUnique, boolean isTemporary) {
        super(indexName, tableName, columnNames, isUnique);
        this.columns = columns;
        this.isTemporary = isTemporary;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public List<SQLite3Column> getColumns() {
        return columns;
    }

    public static void getTableIndexes(SQLConnection con, SQLite3Table table) throws SQLException {
        if (table.isVirtual()) {
            return; // virtual table generally does not have indexes
        }

        List<SQLite3Index> indexes = new ArrayList<>();
        String tableName = table.getName();
        String fetchIndex = String.format("PRAGMA index_list('%s')", tableName);
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(fetchIndex)) {
                while (rs.next()) {
                    String indexName = rs.getString("name");
                    boolean isUnique = rs.getInt("unique") == 1;
                    String getIndexInfo = String.format("PRAGMA index_info('%s')", indexName);
                    try (ResultSet indexRs = s.executeQuery(getIndexInfo)) {
                        List<String> columnNames = new ArrayList<>();
                        while (indexRs.next()) {
                            columnNames.add(indexRs.getString("name"));
                        }
                        List<SQLite3Column> columns = new ArrayList<>();
                        for (String columnName : columnNames) {
                            for (SQLite3Column column : table.getColumns()) {
                                if (column.getName().equals(columnName)) {
                                    columns.add(column);
                                    break;
                                }
                            }
                        }
                        SQLite3Index index = new SQLite3Index(indexName, tableName, columnNames, columns, isUnique, table.isTemporary());
                        indexes.add(index);
                    }
                }
            } catch (SQLException ignored) {
            }

            table.setIndexes(indexes);
        }
    }
}
