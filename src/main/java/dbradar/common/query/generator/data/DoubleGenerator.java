package dbradar.common.query.generator.data;

import dbradar.GlobalState;
import dbradar.Randomly;

public class DoubleGenerator implements Generator {

    private double minValue;
    private double maxValue;
    private String format;

    public DoubleGenerator(double minValue, double maxValue, String format) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.format = format;
    }

    @Override
    public String generate(GlobalState state) {
        double value;
        if (minValue > maxValue) {
            throw new RuntimeException("Generate float value: minValue is larger than maxValue.");
        } else {
            value = Randomly.getNonCachedDouble(minValue, maxValue);
            if (value == Double.POSITIVE_INFINITY) {
                value = maxValue;
            }
            if (value == Double.NEGATIVE_INFINITY) {
                value = minValue;
            }
        }
        if (format.isEmpty()) {
            return Double.toString(value);
        } else {
            return String.format(format, value);
        }
    }
}
