package dbradar.common.schema;

import java.util.Collections;
import java.util.List;

import dbradar.GlobalState;
import dbradar.IgnoreMeException;
import dbradar.SQLGlobalState;
import dbradar.common.query.DBRadarResultSet;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.schema.constraint.DataConstraint;

public class AbstractRelationalTable<C extends AbstractTableColumn<?, ?>, I extends TableIndex, G extends SQLGlobalState>
        extends AbstractTable<C, I, G> {

    public AbstractRelationalTable(String name, boolean isTemporary, boolean isView) {
        super(name, isTemporary, isView);
    }

    public AbstractRelationalTable(String name, List<C> columns, List<I> indexes,
                                   boolean isTemporary, boolean isView) {
        super(name, columns, Collections.emptyList(), indexes, isTemporary, isView);
    }

    public AbstractRelationalTable(String name, List<C> columns, List<DataConstraint> constraints, List<I> indexes,
                                   boolean isTemporary, boolean isView) {
        super(name, columns, constraints, indexes, isTemporary, isView);
    }

    @Override
    public long getNrRows(GlobalState globalState) {
        if (rowCount == NO_ROW_COUNT_AVAILABLE) {
            SQLQueryAdapter q = new SQLQueryAdapter("SELECT COUNT(*) FROM " + name);
            try (DBRadarResultSet query = q.executeAndGet(globalState)) {
                if (query == null) {
                    throw new IgnoreMeException();
                }
                query.next();
                rowCount = query.getLong(1);
                return rowCount;
            } catch (Throwable t) {
                // an exception might be expected, for example, when invalid view is created
                throw new IgnoreMeException();
            }
        } else {
            return rowCount;
        }
    }

}
