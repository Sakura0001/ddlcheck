package dbradar.common.duplicate;

public class DistanceName {
    private double distance;
    private Integer caseId1;
    private Integer caseId2;

    public DistanceName() {
    }

    public DistanceName(double distance, Integer caseId1, Integer caseId2) {
        this.distance = distance;
        this.caseId1 = caseId1;
        this.caseId2 = caseId2;
    }

    public Integer getCaseId1() {
        return caseId1;
    }

    public void setCaseId1(Integer caseId1) {
        this.caseId1 = caseId1;
    }

    public Integer getCaseId2() {
        return caseId2;
    }

    public void setCaseId2(Integer caseId2) {
        this.caseId2 = caseId2;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }
}
