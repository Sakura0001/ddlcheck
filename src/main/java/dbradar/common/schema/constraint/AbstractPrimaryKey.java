package dbradar.common.schema.constraint;

import java.util.ArrayList;
import java.util.List;

public class AbstractPrimaryKey implements DataConstraint {

    List<String> columns;

    public AbstractPrimaryKey(List<String> columns) {
        this.columns = columns;
    }

    public AbstractPrimaryKey(String column) {
        columns = new ArrayList<>();
        columns.add(column);
    }

    public List<String> getColumns() {
        return columns;
    }

    @Override
    public DataConstraint.ConstraintType getType() {
        return DataConstraint.ConstraintType.PRIMARY_KEY;
    }

}
