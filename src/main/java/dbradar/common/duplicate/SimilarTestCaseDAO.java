package dbradar.common.duplicate;

import org.springframework.jdbc.core.BeanPropertyRowMapper;

public class SimilarTestCaseDAO extends AbstractDAO {
    public SimilarTestCase getSimilarTestCasesById(int caseId) {
        String sql = "select * from similar_test_case where case_id = ?";
        return super.getJdbcTemplate().queryForObject(sql, BeanPropertyRowMapper.newInstance(SimilarTestCase.class), caseId);
    }

    public int insertSimilarTestCase(SimilarTestCase similarTestCase) {
        String sql = "insert into similar_test_case (case_id, case_one, similarity_one, case_two, similarity_two, " +
                "case_three, similarity_three, case_four, similarity_four, case_five, similarity_five) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Object[] params = new Object[]{
                similarTestCase.getCaseId(), similarTestCase.getCaseOne(), similarTestCase.getSimilarityOne(), similarTestCase.getCaseTwo(),
                similarTestCase.getSimilarityTwo(), similarTestCase.getCaseThree(), similarTestCase.getSimilarityThree(),
                similarTestCase.getCaseFour(), similarTestCase.getSimilarityFour(), similarTestCase.getCaseFive(), similarTestCase.getSimilarityFive()
        };
        return super.getJdbcTemplate().update(sql, params);
    }

    public int updateSimilarTestCase(SimilarTestCase similarTestCase) {
        if (isSimilarCaseExist(similarTestCase.getCaseId())) {
            //update
            String sql = "update similar_test_case set case_one = ?, similarity_one = ?, case_two = ?, similarity_two = ?," +
                    "case_three = ?, similarity_three = ?, case_four = ?, similarity_four = ?, case_five = ?, similarity_five = ?  where case_id = ?";
            Object[] params = new Object[]{similarTestCase.getCaseOne(), similarTestCase.getSimilarityOne(), similarTestCase.getCaseTwo(),
                    similarTestCase.getSimilarityTwo(), similarTestCase.getCaseThree(), similarTestCase.getSimilarityThree(),
                    similarTestCase.getCaseFour(), similarTestCase.getSimilarityFour(), similarTestCase.getCaseFive(), similarTestCase.getSimilarityFive(), similarTestCase.getCaseId()};
            return super.getJdbcTemplate().update(sql, params);
        } else {
            //insert
            return insertSimilarTestCase(similarTestCase);
        }
    }

    private boolean isSimilarCaseExist(int caseId) {
        String sql = "select count(*) from similar_test_case where case_id = ?";
        return super.getJdbcTemplate().queryForObject(sql, Integer.class, caseId) > 0;
    }
}
