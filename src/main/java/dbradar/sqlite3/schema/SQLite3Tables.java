package dbradar.sqlite3.schema;

import dbradar.common.schema.AbstractTables;

import java.util.List;

public class SQLite3Tables extends AbstractTables<SQLite3Table, SQLite3Column> {

    public SQLite3Tables(List<SQLite3Table> tables) {
        super(tables);
    }

}
