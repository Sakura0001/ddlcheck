package dbradar.common.duplicate;

import java.util.Comparator;

public class DistanceComparator implements Comparator<DistanceName> {
    @Override
    public int compare(DistanceName d1, DistanceName d2) {
        // 从大到小排序
        if (d1.getDistance() > d2.getDistance()) {
            return 1;
        } else if (d1.getDistance() < d2.getDistance()) {
            return -1;
        }
        return 0;
    }
}
