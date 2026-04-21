package dbradar.common.schema.constraint;

public abstract class AbstractDefault implements DataConstraint {

    String expression;

    public AbstractDefault(String expression) {
        this.expression = expression;
    }

    public String getExpression() {
        return expression;
    }

    @Override
    public ConstraintType getType() {
        return ConstraintType.DEFAULT;
    }
}
