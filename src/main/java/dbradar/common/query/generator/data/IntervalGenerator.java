package dbradar.common.query.generator.data;

import dbradar.GlobalState;

public class IntervalGenerator implements Generator {
    @Override
    public String generate(GlobalState state) {
        long year = Long.parseLong(new IntGenerator(0, 100000, "").generate(state));
        long month = Long.parseLong(new IntGenerator(1, 12, "").generate(state));
        long day = Long.parseLong(new IntGenerator(1, 30, "").generate(state));
        long hour = Long.parseLong(new IntGenerator(0, 24, "").generate(state));
        long minute = Long.parseLong(new IntGenerator(0, 60, "").generate(state));
        long second = Long.parseLong(new IntGenerator(0, 60, "").generate(state));

        return String.valueOf(String.format("(INTERVAL '%d year %d months %d days %d hours %d minutes %d seconds')",
                year, month, day, hour, minute, second));
    }
}
