package dbradar.common.duplicate;

import static java.util.Objects.hash;

public class TestCaseKey {
    public Integer testcase1;
    public Integer testcase2;

    public TestCaseKey(Integer testcase1, Integer testcase2) {
        this.testcase1 = testcase1;
        this.testcase2 = testcase2;
    }

    /**
     * Override the hashCode() method
     */
    @Override
    public int hashCode() {
        return hash(testcase1 + "." + testcase2);
    }

    /**
     * Override the equals() method
     */
    public boolean equals(TestCaseKey obj) {
        return testcase1.equals(obj.testcase1) && testcase2.equals(obj.testcase2);
    }
}
