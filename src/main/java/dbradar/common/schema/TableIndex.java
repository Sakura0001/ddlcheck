package dbradar.common.schema;

import java.util.List;

public class TableIndex {

    private String name;
    private String tableName;
    private List<String> columnNames;
    private boolean isUnique;

    public TableIndex(String name, String tableName, List<String> columnNames, boolean isUnique) {
        this.name = name;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.isUnique = isUnique;
    }

    public TableIndex(String indexName) {
        this.name = indexName;
    }

    public TableIndex(String indexName, String tableName) {
        this.name = indexName;
        this.tableName = tableName;
    }

    public String getName() {
        return name;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public boolean isUnique() {
        return isUnique;
    }

    @Override
    public String toString() {
        return name;
    }
}
