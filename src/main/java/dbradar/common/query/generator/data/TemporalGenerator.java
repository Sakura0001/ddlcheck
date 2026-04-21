package dbradar.common.query.generator.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dbradar.GlobalState;
import dbradar.Randomly;

public class TemporalGenerator implements Generator {

    public enum TemporalType {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MICROSECOND
    }

    private List<Integer> thirtyDay = new ArrayList<>(Arrays.asList(4, 6, 9, 11));

    private TemporalType maxTimeScale;
    private TemporalType minTimeScale;

    private List<String> timeList = new ArrayList<>();

    /*
     * The prefixes are used to connect the generated values e.g., 2019-01-01
     * 2019-01-01 00:00:00 2019-01-01 00:00:00.000000
     */
    private String[] prefixes = { "", "-", "-", " ", ":", ":", "." };

    public TemporalGenerator(TemporalType maxTimeScale, TemporalType minTimeScale) {
        this.maxTimeScale = maxTimeScale;
        this.minTimeScale = minTimeScale;

        // year
        int year = Randomly.getNotCachedInteger(2000, 2023);
        timeList.add(String.valueOf(year));

        // month
        int month = Randomly.getNotCachedInteger(0, 12) + 1;
        timeList.add(getTime(month));

        // day
        int day;
        if (month == 2) {
            day = isLeapYear(year) ? Randomly.getNotCachedInteger(0, 29) + 1 : Randomly.getNotCachedInteger(0, 28) + 1;
        } else if (isThirtyDay(month)) {
            day = Randomly.getNotCachedInteger(0, 30) + 1;
        } else {
            day = Randomly.getNotCachedInteger(0, 31) + 1;
        }
        timeList.add(getTime(day));

        int hour = Randomly.getNotCachedInteger(0, 24); // hour
        timeList.add(getTime(hour));
        int minute = Randomly.getNotCachedInteger(0, 60); // minute
        timeList.add(getTime(minute));
        int second = Randomly.getNotCachedInteger(0, 60); // second
        timeList.add(getTime(second));
        int microsecond = Randomly.getNotCachedInteger(0, 1000000);
        timeList.add(getTime(microsecond));
    }

    @Override
    public String generate(GlobalState state) {
        StringBuilder sb = new StringBuilder();
        int maxTimeScaleIndex = maxTimeScale.ordinal();
        int minTimeScaleIndex = minTimeScale.ordinal();
        sb.append(timeList.get(maxTimeScaleIndex));
        for (int i = maxTimeScaleIndex + 1; i <= minTimeScaleIndex; i++) {
            sb.append(prefixes[i]).append(timeList.get(i));
        }
        return "'" + sb + "'";
    }

    public boolean isLeapYear(int year) {
        if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) {
            return true;
        }
        return false;
    }

    public String getTime(int time) {
        if (time < 10) {
            return "0" + time;
        } else {
            return String.valueOf(time);
        }
    }

    public boolean isThirtyDay(int month) {
        return thirtyDay.contains(month);
    }
}
