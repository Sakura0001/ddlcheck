package dbradar.common.duplicate;

import java.util.Date;

public class TestCase {
    private Integer caseId;
    private String testTarget;
    private String content;
    private String reducedCase;
    private String vector;
    private String oracleName;
    private Date bugFoundTime;
    private String bugStatus;
    private String bugSubmitUrl;
    private String comments;
    private Integer deleted;

    public TestCase() {

    }

    public TestCase(Date bugFoundTime, String bugStatus, String bugSubmitUrl, Integer caseId, String comments,
                    String content, Integer deleted, String oracleName, String reducedCase, String testTarget, String vector) {
        this.bugFoundTime = bugFoundTime;
        this.bugStatus = bugStatus;
        this.bugSubmitUrl = bugSubmitUrl;
        this.caseId = caseId;
        this.comments = comments;
        this.content = content;
        this.deleted = deleted;
        this.oracleName = oracleName;
        this.reducedCase = reducedCase;
        this.testTarget = testTarget;
        this.vector = vector;
    }


    public Date getBugFoundTime() {
        return bugFoundTime;
    }

    public void setBugFoundTime(Date bugFoundTime) {
        this.bugFoundTime = bugFoundTime;
    }

    public String getBugStatus() {
        return bugStatus;
    }

    public void setBugStatus(String bugStatus) {
        this.bugStatus = bugStatus;
    }

    public String getBugSubmitUrl() {
        return bugSubmitUrl;
    }

    public void setBugSubmitUrl(String bugSubmitUrl) {
        this.bugSubmitUrl = bugSubmitUrl;
    }

    public int getCaseId() {
        return caseId;
    }

    public void setCaseId(int caseId) {
        this.caseId = caseId;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Integer isDeleted() {
        return deleted;
    }

    public void setDeleted(Integer deleted) {
        this.deleted = deleted;
    }

    public String getOracleName() {
        return oracleName;
    }

    public void setOracleName(String oracleName) {
        this.oracleName = oracleName;
    }

    public String getReducedCase() {
        return reducedCase;
    }

    public void setReducedCase(String reducedCase) {
        this.reducedCase = reducedCase;
    }

    public String getTestTarget() {
        return testTarget;
    }

    public void setTestTarget(String testTarget) {
        this.testTarget = testTarget;
    }

    public String getVector() {
        return vector;
    }

    public void setVector(String vector) {
        this.vector = vector;
    }
}
