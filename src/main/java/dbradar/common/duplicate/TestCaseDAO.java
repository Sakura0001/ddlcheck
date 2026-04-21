package dbradar.common.duplicate;

import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.util.List;

public class TestCaseDAO extends AbstractDAO {
    public List<TestCase> getTestCasesByDBType(String testTarget) {
        String sql = "SELECT * FROM test_case WHERE deleted = 0 AND test_target = '" + testTarget + "'";
        return super.getJdbcTemplate().query(sql, BeanPropertyRowMapper.newInstance(TestCase.class));
    }

    public int addTestCase(TestCase testCase) {
        String sql = "INSERT INTO test_case(test_target, content, reduced_case, vector, oracle_name, bug_found_time," +
                "bug_status, bug_submit_url, comments, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Object[] params = {testCase.getTestTarget(), testCase.getContent(), testCase.getReducedCase(), testCase.getVector(), testCase.getOracleName(),
                testCase.getBugFoundTime(), testCase.getBugStatus(), testCase.getBugSubmitUrl(), testCase.getComments(), testCase.isDeleted()};
        return super.getJdbcTemplate().update(sql, params);
    }

    public int getMaxTestCaseId() {
        String sql = "SELECT max(case_id) FROM test_case";
        return super.getJdbcTemplate().queryForObject(sql, Integer.class);
    }

    public TestCase getTestCaseById(int caseId) {
        String sql = "SELECT * FROM test_case WHERE case_id = ?";
        return super.getJdbcTemplate().queryForObject(sql, BeanPropertyRowMapper.newInstance(TestCase.class), caseId);
    }
}
