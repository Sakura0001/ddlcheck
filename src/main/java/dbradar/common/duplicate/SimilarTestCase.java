package dbradar.common.duplicate;

public class SimilarTestCase {
    private int caseId;
    private int caseOne;
    private double similarityOne;
    private int caseTwo;
    private double similarityTwo;
    private int caseThree;
    private double similarityThree;
    private int caseFour;
    private double similarityFour;
    private int caseFive;
    private double similarityFive;

    public SimilarTestCase() {
    }

    public SimilarTestCase(int caseId, int caseOne, double similarityOne, int caseTwo, double similarityTwo, int caseThree, double similarityThree,
                           int caseFour, double similarityFour, int caseFive, double similarityFive) {
        this.caseId = caseId;
        this.caseOne = caseOne;
        this.similarityOne = similarityOne;
        this.caseTwo = caseTwo;
        this.similarityTwo = similarityTwo;
        this.caseThree = caseThree;
        this.similarityThree = similarityThree;
        this.caseFour = caseFour;
        this.similarityFour = similarityFour;
        this.caseFive = caseFive;
        this.similarityFive = similarityFive;
    }

    public int getCaseFive() {
        return caseFive;
    }

    public void setCaseFive(int caseFive) {
        this.caseFive = caseFive;
    }

    public int getCaseFour() {
        return caseFour;
    }

    public void setCaseFour(int caseFour) {
        this.caseFour = caseFour;
    }

    public int getCaseId() {
        return caseId;
    }

    public void setCaseId(int caseId) {
        this.caseId = caseId;
    }

    public int getCaseOne() {
        return caseOne;
    }

    public void setCaseOne(int caseOne) {
        this.caseOne = caseOne;
    }

    public int getCaseThree() {
        return caseThree;
    }

    public void setCaseThree(int caseThree) {
        this.caseThree = caseThree;
    }

    public int getCaseTwo() {
        return caseTwo;
    }

    public void setCaseTwo(int caseTwo) {
        this.caseTwo = caseTwo;
    }

    public double getSimilarityFour() {
        return similarityFour;
    }

    public void setSimilarityFour(double getSimilarityFour) {
        this.similarityFour = getSimilarityFour;
    }

    public double getSimilarityFive() {
        return similarityFive;
    }

    public void setSimilarityFive(double similarityFive) {
        this.similarityFive = similarityFive;
    }

    public double getSimilarityOne() {
        return similarityOne;
    }

    public void setSimilarityOne(double similarityOne) {
        this.similarityOne = similarityOne;
    }

    public double getSimilarityThree() {
        return similarityThree;
    }

    public void setSimilarityThree(double similarityThree) {
        this.similarityThree = similarityThree;
    }

    public double getSimilarityTwo() {
        return similarityTwo;
    }

    public void setSimilarityTwo(double similarityTwo) {
        this.similarityTwo = similarityTwo;
    }
}
