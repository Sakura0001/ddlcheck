package dbradar.common.query.generator.data;

import dbradar.GlobalState;
import dbradar.Randomly;

public class RangeGenerator implements Generator {

    private final long left;
    private final boolean leftIsInclusive;
    private final long right;
    private final boolean rightIsInclusive;

    public RangeGenerator(boolean leftIsInclusive, boolean rightIsInclusive) {
        Randomly r = new Randomly();
        long left = r.getInteger();
        long right = r.getInteger();
        long realLeft;
        long realRight;
        if (left > right) {
            realRight = left;
            realLeft = right;
        } else {
            realLeft = left;
            realRight = right;
        }

        this.left = realLeft;
        this.leftIsInclusive = leftIsInclusive;
        this.right = realRight;
        this.rightIsInclusive = rightIsInclusive;
    }

    @Override
    public String generate(GlobalState state) {
        StringBuilder sb = new StringBuilder();
        sb.append("'");
        if (leftIsInclusive) {
            sb.append("[");
        } else {
            sb.append("(");
        }
        sb.append(left);
        sb.append(",");
        sb.append(right);
        if (rightIsInclusive) {
            sb.append("]");
        } else {
            sb.append(")");
        }
        sb.append("'");
        sb.append("::int4range");
        return sb.toString();
    }
}
