package dbradar.common.schema.constraint;

public abstract class AbstractGenerated implements DataConstraint {

    String expression;

    public AbstractGenerated(String expression) {
        this.expression = expression;
    }

    public String getExpression() {
        return expression;
    }

    @Override
    public ConstraintType getType() {
        return ConstraintType.GENERATED;
    }
}
