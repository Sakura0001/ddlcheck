package dbradar.sqlite3.schema;

import dbradar.common.schema.AbstractRelationalTable;
import dbradar.sqlite3.SQLite3GlobalState;

import java.util.Collections;
import java.util.List;

public class SQLite3Table extends AbstractRelationalTable<SQLite3Column, SQLite3Index, SQLite3GlobalState> {

    public enum TableKind {
        MAIN, TEMP
    }

    private final TableKind tableType;
    private SQLite3Column rowid;
    private final boolean withoutRowid;
    private final boolean isVirtual;
    private final boolean isReadOnly;

    public SQLite3Table(String tableName, List<SQLite3Column> columns, TableKind tableType, boolean withoutRowid,
                        boolean isView, boolean isVirtual, boolean isReadOnly) {
        super(tableName, columns, Collections.emptyList(), tableType.equals(TableKind.TEMP), isView);
        this.tableType = tableType;
        this.withoutRowid = withoutRowid;
        this.isVirtual = isVirtual;
        this.isReadOnly = isReadOnly;
    }

    public boolean hasWithoutRowid() {
        return withoutRowid;
    }

    public void addRowid(SQLite3Column rowid) {
        this.rowid = rowid;
    }

    public SQLite3Column getRowid() {
        return rowid;
    }

    public TableKind getTableType() {
        return tableType;
    }

    public boolean isVirtual() {
        return isVirtual;
    }

    public boolean isSystemTable() {
        return getName().startsWith("sqlit");
    }

    public boolean isTemp() {
        return tableType == TableKind.TEMP;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

}
