package dbradar.common.duplicate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class WeightDAO extends AbstractDAO {

    ObjectMapper objectMapper = new ObjectMapper();


    public Weight getWeightByTargetType(String targetType) {
        String sql = String.format("select * from weight where test_target = '%s'", targetType);
        WeightString weightString = super.getJdbcTemplate().queryForObject(sql, BeanPropertyRowMapper.newInstance(WeightString.class));
        HashMap<String, LinkedHashMap> keyToCaseNameString;
        LinkedList<Integer> caseIds;
        HashMap<Integer, DistanceName> keyToCaseName = new HashMap<>();
        HashMap<Integer, Double> keyToDistance;
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try {
            assert weightString != null;
            caseIds = objectMapper.readValue(weightString.getCaseIds(), LinkedList.class);
            keyToCaseNameString = objectMapper.readValue(weightString.getKeyToCaseName(), HashMap.class);
            keyToDistance = objectMapper.readValue(weightString.getKeyToDistance(), HashMap.class);
            for (Map.Entry<String, LinkedHashMap> entry : keyToCaseNameString.entrySet()) {
                keyToCaseName.put(Integer.valueOf(entry.getKey()), new DistanceName((Double) entry.getValue().get("distance"),
                        (Integer) entry.getValue().get("caseId1"), (Integer) entry.getValue().get("caseId2")));
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return new Weight(weightString.columnWeight, weightString.columnMaxGap, weightString.constraintWeight, weightString.constraintMaxGap,
                weightString.operationWeight, weightString.operationMaxGap, weightString.functionWeight, weightString.functionMaxGap, caseIds, keyToCaseName,
                keyToDistance, weightString.testTarget);
    }

    public int updateWeight(String targetType, Weight weight) {
        ObjectMapper mapper = new ObjectMapper();
        String caseIds;
        String keyToCaseName;
        String keyToDistance;
        try {
            caseIds = mapper.writeValueAsString(weight.getCaseIds());
            keyToCaseName = mapper.writeValueAsString(weight.getKeyToCaseName());
            keyToDistance = mapper.writeValueAsString(weight.getKeyToDistance());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        String sql;
        Object[] params = new Object[]{weight.getColumnWeight(), weight.getConstraintWeight(), weight.getOperationWeight(),
                weight.getFunctionWeight(), weight.getColumnMaxGap(), weight.getConstraintMaxGap(),
                weight.getOperationMaxGap(), weight.getFunctionMaxGap(), caseIds, keyToCaseName,
                keyToDistance, targetType};
        //首先查询数据库中是否有此dbtype的权重信息，如果有就更新，否则就插入
        if (isWeightExist(targetType)) {
            sql = "update weight set column_weight = ?, constraint_weight = ?, operation_weight = ?," +
                    "function_weight = ?, column_max_gap = ?, constraint_max_gap = ?, operation_max_gap = ?, function_max_gap = ?, " +
                    "case_ids = ?, key_to_case_name = ?, key_to_distance = ?" +
                    " where test_target = ?";

        } else {
            sql = "insert into weight(column_weight, constraint_weight, operation_weight, function_weight, column_max_gap, " +
                    "constraint_max_gap, operation_max_gap, function_max_gap, case_ids, key_to_case_name, key_to_distance, test_target) " +
                    "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        }
        return super.getJdbcTemplate().update(sql, params);
    }

    private boolean isWeightExist(String targetType) {
        String sql = "select count(*) from weight where test_target = ?";
        return super.getJdbcTemplate().queryForObject(sql, Integer.class, targetType) > 0;
    }

    public static class WeightString {
        private double columnWeight;
        private double constraintWeight;
        private double operationWeight;
        private double functionWeight;
        private double columnMaxGap;
        private double constraintMaxGap;
        private double operationMaxGap;
        private double functionMaxGap;
        private String testTarget;
        private String caseIds;
        //key:case1.hashcode + case2.hashcode
        private String keyToCaseName;
        private String keyToDistance;

        public WeightString() {
        }

        public WeightString(double columnWeight, double columnMaxGap, double constraintWeight,
                            double constraintMaxGap, double operationWeight, double operationMaxGap, double functionWeight,
                            double functionMaxGap, String testTarget, String caseIds, String keyToCaseName, String keyToDistance) {
            this.caseIds = caseIds;
            this.columnMaxGap = columnMaxGap;
            this.columnWeight = columnWeight;
            this.constraintMaxGap = constraintMaxGap;
            this.constraintWeight = constraintWeight;
            this.functionMaxGap = functionMaxGap;
            this.functionWeight = functionWeight;
            this.keyToCaseName = keyToCaseName;
            this.keyToDistance = keyToDistance;
            this.operationMaxGap = operationMaxGap;
            this.operationWeight = operationWeight;
            this.testTarget = testTarget;
        }

        public String getCaseIds() {
            return caseIds;
        }

        public void setCaseIds(String caseIds) {
            this.caseIds = caseIds;
        }

        public double getColumnMaxGap() {
            return columnMaxGap;
        }

        public void setColumnMaxGap(double columnMaxGap) {
            this.columnMaxGap = columnMaxGap;
        }

        public double getColumnWeight() {
            return columnWeight;
        }

        public void setColumnWeight(double columnWeight) {
            this.columnWeight = columnWeight;
        }

        public double getConstraintMaxGap() {
            return constraintMaxGap;
        }

        public void setConstraintMaxGap(double constraintMaxGap) {
            this.constraintMaxGap = constraintMaxGap;
        }

        public double getConstraintWeight() {
            return constraintWeight;
        }

        public void setConstraintWeight(double constraintWeight) {
            this.constraintWeight = constraintWeight;
        }

        public double getFunctionMaxGap() {
            return functionMaxGap;
        }

        public void setFunctionMaxGap(double functionMaxGap) {
            this.functionMaxGap = functionMaxGap;
        }

        public double getFunctionWeight() {
            return functionWeight;
        }

        public void setFunctionWeight(double functionWeight) {
            this.functionWeight = functionWeight;
        }

        public String getKeyToCaseName() {
            return keyToCaseName;
        }

        public void setKeyToCaseName(String keyToCaseName) {
            this.keyToCaseName = keyToCaseName;
        }

        public String getKeyToDistance() {
            return keyToDistance;
        }

        public void setKeyToDistance(String keyToDistance) {
            this.keyToDistance = keyToDistance;
        }

        public double getOperationMaxGap() {
            return operationMaxGap;
        }

        public void setOperationMaxGap(double operationMaxGap) {
            this.operationMaxGap = operationMaxGap;
        }

        public double getOperationWeight() {
            return operationWeight;
        }

        public void setOperationWeight(double operationWeight) {
            this.operationWeight = operationWeight;
        }

        public String getTestTarget() {
            return testTarget;
        }

        public void setTestTarget(String testTarget) {
            this.testTarget = testTarget;
        }
    }
}
