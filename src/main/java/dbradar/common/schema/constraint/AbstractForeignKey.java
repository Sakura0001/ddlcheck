package dbradar.common.schema.constraint;

public abstract class AbstractForeignKey implements DataConstraint {

    String sourceColumn;
    String targetTable;
    String targetColumn;

    public AbstractForeignKey(String sourceColumn, String targetTable, String targetColumn) {
        this.sourceColumn = sourceColumn;
        this.targetTable = targetTable;
        this.targetColumn = targetColumn;
    }

    public String getSourceColumn() {
        return sourceColumn;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    @Override
    public ConstraintType getType() {
        return ConstraintType.FOREIGN_KEY;
    }
}
