package dbradar.common.schema;

import dbradar.common.schema.constraint.DataConstraint;

public abstract class AbstractTableColumn<T extends AbstractTable<?, ?, ?>, U>
        implements Comparable<AbstractTableColumn<T, U>> {

    private String name;
    private U type;
    private boolean isPrimaryKey;
    private boolean isGenerated;
    private boolean isNotNull;
    private DataConstraint defaultConstraint;
    private T table;

    public AbstractTableColumn() {
    }

    public AbstractTableColumn(String name, T table, U type) {
        this.name = name;
        this.table = table;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Return the simple data type
     */
    public U getType() {
        return type;
    }

    public void setType(U type) {
        this.type = type;
    }

    public void setDefaultConstraint(DataConstraint defaultConstraint) {
        this.defaultConstraint = defaultConstraint;
    }

    public DataConstraint getDefaultConstraint() {
        return defaultConstraint;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    public boolean isGenerated() {
        return isGenerated;
    }

    public void setGenerated(boolean generated) {
        isGenerated = generated;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    public void setNotNull(boolean notNull) {
        isNotNull = notNull;
    }

    public T getTable() {
        return table;
    }

    public void setTable(T table) {
        this.table = table;
    }

    public String getFullQualifiedName() {
        if (table != null) {
            return table.getName() + "." + name;
        }
        return name;
    }

    @Override
    public String toString() {
        if (table == null) {
            return String.format("%s: %s", getName(), getType());
        } else {
            return String.format("%s.%s: %s", table.getName(), getName(), getType());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AbstractTableColumn)) {
            return false;
        } else {
            @SuppressWarnings("unchecked")
            AbstractTableColumn<T, U> c = (AbstractTableColumn<T, U>) obj;
            if (c.getTable() == null) {
                return getName().equals(c.getName());
            }
            return table.getName().contentEquals(c.getTable().getName()) && getName().equals(c.getName());
        }
    }

    @Override
    public int hashCode() {
        return getName().hashCode() + 11 * getType().hashCode();
    }

    @Override
    public int compareTo(AbstractTableColumn<T, U> o) {
        if (o.getTable().equals(this.getTable())) {
            return getName().compareTo(o.getName());
        } else {
            return o.getTable().compareTo(getTable());
        }
    }
}
