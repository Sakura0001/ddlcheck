package dbradar.common.schema.constraint;

import java.util.ArrayList;
import java.util.List;

public class AbstractUniqueKey implements DataConstraint {

    List<String> columns;

    public AbstractUniqueKey(List<String> columns) {
        this.columns = columns;
    }

    public AbstractUniqueKey(String column) {
        columns = new ArrayList<>();
        columns.add(column);
    }

    public List<String> getColumns() {
        return columns;
    }

    @Override
    public DataConstraint.ConstraintType getType() {
        return DataConstraint.ConstraintType.UNIQUE_KEY;
    }

}
