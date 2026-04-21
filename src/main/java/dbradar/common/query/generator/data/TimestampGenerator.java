package dbradar.common.query.generator.data;

import dbradar.GlobalState;
import dbradar.common.query.generator.data.TemporalGenerator.TemporalType;

public class TimestampGenerator implements Generator {

    private Generator generator;

    private String format;

    public TimestampGenerator(String format) {
        generator = new TemporalGenerator(TemporalType.YEAR, TemporalType.SECOND);
        this.format = format;
    }

    /**
     * Generate a random timestamp in the format of "YYYYMMDDHHmmss" e.g.
     * "20190528125959"
     * 
     * @return a random timestamp
     */
    @Override
    public String generate(GlobalState state) {
        String temporal = generator.generate(state);
        // TODO need to format, e.g., in MySQL, timestamp: '0000-00-00 00:00:00'
        if (format.isEmpty()) {
            format = "%04d%02d%02d%02d%02d%02d";
        }
        return String.format(format, Integer.parseInt(temporal.substring(1, 5)), // year
                Integer.parseInt(temporal.substring(6, 8)), // month
                Integer.parseInt(temporal.substring(9, 11)), // day
                Integer.parseInt(temporal.substring(12, 14)), // hour
                Integer.parseInt(temporal.substring(15, 17)), // minute
                Integer.parseInt(temporal.substring(18, 20))); // second
    }
}
