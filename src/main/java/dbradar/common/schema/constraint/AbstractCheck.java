package dbradar.common.schema.constraint;

public abstract class AbstractCheck implements DataConstraint {

    String expression;

    public AbstractCheck(String expression) {
        this.expression = expression;
    }

    public String getExpression() {
        return expression;
    }

    @Override
    public ConstraintType getType() {
        return ConstraintType.CHECK;
    }
}
