package dbradar.common.schema.constraint;

public interface DataConstraint {

    enum ConstraintType {
        FOREIGN_KEY, PRIMARY_KEY, UNIQUE_KEY, GENERATED, NOT_NULL, DEFAULT, CHECK;
    }

    ConstraintType getType();
}
