package dbradar.common.duplicate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.LinkedList;

public class Weight {
    private double columnWeight;
    private double constraintWeight;
    private double operationWeight;
    private double functionWeight;
    private double columnMaxGap;
    private double constraintMaxGap;
    private double operationMaxGap;
    private double functionMaxGap;
    private String testTarget;
    private LinkedList<Integer> caseIds;
    //key:case1.hashcode + case2.hashcode
    private HashMap<Integer, DistanceName> keyToCaseName;
    private HashMap<Integer, Double> keyToDistance;

    public Weight() {

    }

    public Weight(double columnMaxGap, double columnWeight, double constraintMaxGap, double constraintWeight,
                  double functionMaxGap, double functionWeight, double operationMaxGap, double operationWeight,
                  LinkedList<Integer> caseIds, HashMap<Integer, DistanceName> keyToCaseName, HashMap<Integer, Double> keyToDistance, String testTarget) {
        this.columnMaxGap = columnMaxGap;
        this.columnWeight = columnWeight;
        this.constraintMaxGap = constraintMaxGap;
        this.constraintWeight = constraintWeight;
        this.functionMaxGap = functionMaxGap;
        this.functionWeight = functionWeight;
        this.operationMaxGap = operationMaxGap;
        this.operationWeight = operationWeight;
        this.caseIds = caseIds;
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            String content = objectMapper.writeValueAsString(keyToCaseName);
            this.keyToCaseName = objectMapper.readValue(content, HashMap.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        this.keyToDistance = keyToDistance;
        this.testTarget = testTarget;
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

    public LinkedList<Integer> getCaseIds() {
        return caseIds;
    }

    public void setCaseIds(LinkedList<Integer> caseIds) {
        this.caseIds = caseIds;
    }

    public HashMap<Integer, DistanceName> getKeyToCaseName() {
        return keyToCaseName;
    }

    public void setKeyToCaseName(HashMap<Integer, DistanceName> keyToCaseName) {
        this.keyToCaseName = keyToCaseName;
    }

    public HashMap<Integer, Double> getKeyToDistance() {
        return keyToDistance;
    }

    public void setKeyToDistance(HashMap<Integer, Double> keyToDistance) {
        this.keyToDistance = keyToDistance;
    }

    public String getTestTarget() {
        return testTarget;
    }

    public void setTestTarget(String testTarget) {
        this.testTarget = testTarget;
    }
}
