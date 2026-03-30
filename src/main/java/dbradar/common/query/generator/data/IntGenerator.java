package dbradar.common.query.generator.data;

import dbradar.GlobalState;
import dbradar.Randomly;

public class IntGenerator implements Generator {
    private long minValue;
    private long maxValue;
    private String format;

    public IntGenerator(long minValue, long maxValue, String format) {
        this.maxValue = maxValue;
        this.minValue = minValue;
        this.format = format;
    }

    @Override
    public String generate(GlobalState state) {
        long valueGenerated = Randomly.getNotCachedLong(minValue, maxValue);
        if (format.isEmpty()) {
            return Long.toString(valueGenerated);
        }
        return String.format(format, valueGenerated);
    }
}
