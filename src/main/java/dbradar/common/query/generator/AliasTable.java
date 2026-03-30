package dbradar.common.query.generator;

import java.util.ArrayList;
import java.util.List;

import dbradar.GlobalState;
import dbradar.common.schema.AbstractTable;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.common.schema.TableIndex;

public class AliasTable<C extends AbstractTableColumn<?, ?>, I extends TableIndex, G extends GlobalState>
        extends AbstractTable<C, I, G> {

    public AliasTable(String name, List<C> columns, List<I> indexes) {
        super(name, columns, new ArrayList<>(), indexes, false, false);
    }

    @Override
    public long getNrRows(GlobalState globalState) {
        return NO_ROW_COUNT_AVAILABLE;
    }
}
