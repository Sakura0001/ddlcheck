package dbradar.common.schema;

import java.util.ArrayList;
import java.util.List;

import dbradar.GlobalState;
import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.common.schema.constraint.DataConstraint;

public abstract class AbstractTable<C extends AbstractTableColumn<?, ?>, I extends TableIndex, G extends GlobalState>
        implements Comparable<AbstractTable<?, ?, ?>> {

    protected static final int NO_ROW_COUNT_AVAILABLE = -1;
    protected final String name;
    private List<C> columns = new ArrayList<>();
    private List<DataConstraint> constraints = new ArrayList<>();
    private List<I> indexes = new ArrayList<>();
    private boolean isTemporary;
    private final boolean isView;
    protected long rowCount = NO_ROW_COUNT_AVAILABLE;

    public AbstractTable(String name, boolean isTemporary, boolean isView) {
        this.name = name;
        this.isTemporary = isTemporary;
        this.isView = isView;
    }

    public AbstractTable(String name, List<C> columns, List<DataConstraint> constraints, List<I> indexes,
                         boolean isTemporary, boolean isView) {
        this.name = name;
        this.columns = columns;
        this.constraints = constraints;
        this.indexes = indexes;
        this.isTemporary = isTemporary;
        this.isView = isView;
    }

    public String getName() {
        return name;
    }

    public void setColumns(List<C> columns) {
        this.columns = columns;
    }

    public List<C> getColumns() {
        return columns;
    }

    public void addConstraints(DataConstraint constraint) {
        constraints.add(constraint);
    }

    public List<DataConstraint> getConstraints() {
        return constraints;
    }

    public void setIndexes(List<I> indexes) {
        this.indexes = indexes;
    }

    public List<I> getIndexes() {
        return indexes;
    }

    public void setTemporary(boolean temporary) {
        isTemporary = temporary;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public AbstractTableColumn<?, ?> getPrimaryColumn() {
        for (AbstractTableColumn<?, ?> col : getColumns()) {
            if (col.isPrimaryKey()) {
                return col;
            }
        }

        throw new IgnoreMeException();
    }

    public boolean hasPrimaryKey() {
        for (AbstractTableColumn<?, ?> col : getColumns()) {
            if (col.isPrimaryKey()) {
                return true;
            }
        }

        return false;
    }

    public String getFreeColumnName() {
        int i = getColumns().size();
        do {
            String columnName = String.format("c%d", i++);
            if (getColumns().stream().noneMatch(t -> t.getName().contentEquals(columnName))) {
                return columnName;
            }
        } while (true);
    }

    public boolean isView() {
        return isView;
    }

    @Override
    public int compareTo(AbstractTable<?, ?, ?> o) {
        return o.getName().compareTo(getName());
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getName());
        sb.append("\n");
        for (C c : getColumns()) {
            sb.append("\t").append(c).append("\n");
        }
        return sb.toString();
    }

    public void recomputeCount() {
        rowCount = NO_ROW_COUNT_AVAILABLE;
    }

    public abstract long getNrRows(GlobalState globalState);

    public boolean hasIndexes() {
        return !getIndexes().isEmpty();
    }

    public TableIndex getRandomIndex() {
        if (getIndexes().isEmpty()) {
            throw new IgnoreMeException("There is no available index");
        }
        return Randomly.fromList(getIndexes());
    }
}
