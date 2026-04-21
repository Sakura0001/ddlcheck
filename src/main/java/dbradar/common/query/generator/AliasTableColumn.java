package dbradar.common.query.generator;

import dbradar.common.schema.AbstractTableColumn;

public class AliasTableColumn<U> extends AbstractTableColumn<AliasTable<?, ?, ?>, U> {

    public AliasTableColumn(String name, AliasTable<?, ?, ?> table, U type) {
        super(name, table, type);
    }
}
