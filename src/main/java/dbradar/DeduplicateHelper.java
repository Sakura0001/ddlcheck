package dbradar;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dbradar.common.duplicate.*;

import java.util.*;

import static dbradar.common.duplicate.Feature.calculateSimilarity;

public class DeduplicateHelper {
    public static TestCaseDAO testCaseDAO = new TestCaseDAO();
    public static WeightDAO weightDAO = new WeightDAO();
    public static SimilarTestCaseDAO similarTestCaseDAO = new SimilarTestCaseDAO();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final List<Integer> collectionUnsorted;
    private final List<Integer> collectionSorted;
    private final Map<Integer, PriorityQueue<DistanceName>> mostSimilarCases;
    private final HashMap<Integer, DistanceName> keyToCaseName;
    private final HashMap<Integer, Double> keyToDistance;
    private final Map<Integer, Feature> keyToFeature;
    private LinkedList<Integer> caseIds;
    private List<TestCase> testCases;
    private Weight weight;

    public enum WithoutDimension {
        ALL, NO_COLUMN, NO_CONSTRAINT, NO_OPERATION, NO_FUNCTION
    }

    public enum DBType {
        MYSQL, MARIADB, SQLITE, TIDB
    }

    public DeduplicateHelper() {
        collectionUnsorted = new ArrayList<>();
        collectionSorted = new ArrayList<>();
        mostSimilarCases = new HashMap<>();
        keyToCaseName = new HashMap<>();
        keyToDistance = new HashMap<>();
        keyToFeature = new HashMap<>();
        caseIds = new LinkedList<>();
        testCases = new ArrayList<>();
        weight = new Weight();
    }

    /**
     * Select the K least similar test cases from the set of test cases(Further point first),
     *
     * @param caseCount        equals 0 represent list all test cases,
     *                         k equals non-zero represent list first-k test cases
     * @param cur              The feature vector of the new test case, used to calculate the distance from the existing test case vector
     * @param recalculate      is true represent Recalculate weights and similarity，
     *                         is false represent use the previous weights and similarity to reduce the cost.
     * @param withoutDimension 0-4    0:Compute similarity using all feature vectors; 1:without column dimension;2:without constraint dimension;
     *                         3:without operator dimension;4:without functions dimension;
     */
    public void pickKCases(int caseCount, Feature cur, boolean recalculate, Integer caseId, WithoutDimension withoutDimension, DBType dbtype) {
        int selectCaseCount = 0;
        // The initialization weights and feature vector information are selected according to recalculate
        initFurthestPointFirst(recalculate, withoutDimension, String.valueOf(dbtype).toLowerCase(), caseId);

        // If the weights do not need to be recalculated, the existing feature vectors and weights are directly obtained,
        // and the similarity calculation is performed with the latest use case
        if (!recalculate && cur != null && caseId != -1) {
            int key;
            double distance;
            for (Integer testcase : collectionSorted) {
                key = new TestCaseKey(testcase, caseId).hashCode();
                distance = calculateSimilarity(cur, keyToFeature.get(testcase), weight);
                keyToDistance.put(key, distance);
                keyToCaseName.put(key, new DistanceName(distance, caseId, testcase));
            }
        }
        if (caseCount > collectionUnsorted.size()) {
            throw new AssertionError("k should not be greater than collectionUnsorted.size");
        }

        pickFirstCase(keyToDistance, keyToCaseName);

        //A caseCount equal to 0 means that all test cases are sorted
        caseCount = caseCount == 0 ? collectionUnsorted.size() : caseCount;

        Integer selectCase;
        while (selectCaseCount < caseCount) {
            selectCase = furthestCase();
            collectionUnsorted.add(selectCase);
            collectionSorted.remove(selectCase);
            selectCaseCount++;
        }
    }


    /**
     * First select a test case as the head
     * Theoretically, the first test case selected should be the one between the test case pairs with the largest distance in the map.
     */
    private void pickFirstCase(Map<Integer, Double> keyToDistance, Map<Integer, DistanceName> keyToCaseName) {
        int key = 0;
        double maxDistance = 0;
        for (Map.Entry<Integer, Double> entry : keyToDistance.entrySet()) {
            if (maxDistance < entry.getValue()) {
                key = entry.getKey();
            }
        }
        DistanceName distanceName = keyToCaseName.get(key);
        if (distanceName != null) {
            Integer case1 = distanceName.getCaseId1();
            Integer case2 = distanceName.getCaseId2();
            collectionUnsorted.remove(case1);
            collectionUnsorted.remove(case2);
            collectionSorted.add(case1);
            collectionSorted.add(case2);
        }
    }

    /**
     * Calculate the maximum minimum distance between each use case in collectionB and the use case in collectionA,
     * and then return the use case with the greatest distance and add it to collectionA.
     */
    private Integer furthestCase() {
        Integer selectCase = 0;
        double minDistance;
        Map<Double, Integer> sortedToUnSorted = new HashMap<>();
        for (Integer case2 : collectionSorted) {
            minDistance = Double.MAX_VALUE;
            for (Integer case1 : collectionUnsorted) {
                TestCaseKey key1 = new TestCaseKey(case1, case2);
                TestCaseKey key2 = new TestCaseKey(case2, case1);
                if (keyToDistance.containsKey(key1.hashCode()) || keyToDistance.containsKey(key2.hashCode())) {
                    if (minDistance > keyToDistance.getOrDefault(key1.hashCode(), Double.MAX_VALUE)) {
                        minDistance = keyToDistance.get(key1.hashCode());
                        selectCase = case2;
                    }
                    if (minDistance > keyToDistance.getOrDefault(key2.hashCode(), Double.MAX_VALUE)) {
                        minDistance = keyToDistance.get(key2.hashCode());
                        selectCase = case2;
                    }
                }
            }
            sortedToUnSorted.put(minDistance, selectCase);
        }

        double max = -Double.MIN_VALUE;
        for (Map.Entry<Double, Integer> entry : sortedToUnSorted.entrySet()) {
            max = Math.max(max, entry.getKey());
        }
        return sortedToUnSorted.get(max);
    }


    /**
     * Read all test cases from the file, then calculate the similarity between the two test cases,
     * and put them into the priorityQueue to arrange them from high to low.
     *
     * @param recalculate      is true represent Recalculate weights and similarity，
     *                         startAll is false represent use the previous weights and similarity to reduce the cost.
     * @param withoutDimension 0-4    0:Compute similarity using all feature vectors; 1:without column dimension;2:without constraint dimension;
     *                         3:without operator dimension;4:without functions dimension;
     */
    public void initFurthestPointFirst(boolean recalculate, WithoutDimension withoutDimension, String dbType, int newCaseId) {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        //All properties of this need to be assigned
        testCases = testCaseDAO.getTestCasesByDBType(dbType);
        for (TestCase testCase : testCases) {
            caseIds.add(testCase.getCaseId());
            mostSimilarCases.put(testCase.getCaseId(), new PriorityQueue<>(5, new DistanceComparator()));
            //First, all the queried test cases are added to the unsorted collection
            collectionUnsorted.add(testCase.getCaseId());
            //The vector in the queried test case fills keyToFeature
            try {
                keyToFeature.put(testCase.getCaseId(), objectMapper.readValue(testCase.getVector(), Feature.class));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        Integer case1;
        Integer case2;
        int key;
        double similarity;
        PriorityQueue<DistanceName> case1PriorityQueue;
        PriorityQueue<DistanceName> case2PriorityQueue;

        // Recalculate and save weights and similarity
        if (recalculate) {
            weight = calculateWeight(withoutDimension);
            weight.setTestTarget(dbType);
        }
        // Use existing weights and similarity calculations
        else {
            weight = weightDAO.getWeightByTargetType(dbType);
            caseIds = weight.getCaseIds();
            caseIds.add(newCaseId);
        }
        for (int i = 0; i < caseIds.size(); i++) {
            collectionSorted.add(caseIds.get(i));
            case1 = caseIds.get(i);
            for (int j = i + 1; j < caseIds.size(); j++) {
                case2 = caseIds.get(j);
                key = new TestCaseKey(case1, case2).hashCode();
                similarity = calculateSimilarity(keyToFeature.get(case1), keyToFeature.get(case2), weight);
                keyToDistance.put(key, similarity);
                keyToCaseName.put(key, new DistanceName(similarity, case1, case2));
                case1PriorityQueue = mostSimilarCases.get(case1);
                case1PriorityQueue.add(new DistanceName(similarity, case1, case2));
                case2PriorityQueue = mostSimilarCases.get(case2);
                case2PriorityQueue.add(new DistanceName(similarity, case2, case1));
            }
            collectionSorted.addAll(caseIds);
        }
        weight.setTestTarget(dbType);
        weight.setKeyToCaseName(keyToCaseName);
        weight.setKeyToDistance(keyToDistance);
        weightDAO.updateWeight(dbType, weight);

        //Store the five most similar test cases for each test case in SQLite
        storeMostSimilarTestCase();
    }


    /**
     * Calculate the weight of each dimension based on the existing similarity
     */
    public Weight calculateWeight(WithoutDimension withoutDimension) {
        double meanCol;
        double meanCons;
        double meanOp;
        double meanFunc;

        List<Double> col = new ArrayList<>();
        List<Double> cons = new ArrayList<>();
        List<Double> op = new ArrayList<>();
        List<Double> func = new ArrayList<>();

        Feature case1;
        Feature case2;
        for (int i = 0; i < collectionUnsorted.size(); i++) {
            for (int j = i + 1; j < collectionUnsorted.size(); j++) {
                case1 = keyToFeature.get(collectionUnsorted.get(i));
                case2 = keyToFeature.get(collectionUnsorted.get(j));

                switch (withoutDimension) {
                    case ALL:
                        col.add(Feature.product(case1.getColumn(), case2.getColumn()));
                        cons.add(Feature.product(case1.getConstraint(), case2.getConstraint()));
                        op.add(Feature.product(case1.getOperator(), case2.getOperator()));
                        func.add(Feature.product(case1.getFunction(), case2.getFunction()));
                        break;
                    case NO_COLUMN:
                        cons.add(Feature.product(case1.getConstraint(), case2.getConstraint()));
                        op.add(Feature.product(case1.getOperator(), case2.getOperator()));
                        func.add(Feature.product(case1.getFunction(), case2.getFunction()));
                        break;
                    case NO_CONSTRAINT:
                        col.add(Feature.product(case1.getColumn(), case2.getColumn()));
                        op.add(Feature.product(case1.getOperator(), case2.getOperator()));
                        func.add(Feature.product(case1.getFunction(), case2.getFunction()));
                        break;
                    case NO_OPERATION:
                        col.add(Feature.product(case1.getColumn(), case2.getColumn()));
                        cons.add(Feature.product(case1.getConstraint(), case2.getConstraint()));
                        func.add(Feature.product(case1.getFunction(), case2.getFunction()));
                        break;
                    case NO_FUNCTION:
                        col.add(Feature.product(case1.getColumn(), case2.getColumn()));
                        cons.add(Feature.product(case1.getConstraint(), case2.getConstraint()));
                        op.add(Feature.product(case1.getOperator(), case2.getOperator()));
                        break;
                }
            }
        }

        // Normalize the distance within each dimension separately
        col = minMax(col, WithoutDimension.NO_COLUMN);
        cons = minMax(cons, WithoutDimension.NO_CONSTRAINT);
        op = minMax(op, WithoutDimension.NO_OPERATION);
        func = minMax(func, WithoutDimension.NO_FUNCTION);

        meanCol = mean(col);
        meanCons = mean(cons);
        meanOp = mean(op);
        meanFunc = mean(func);

        // Calculate weights based on whether to use a single dimensional vector
        switch (withoutDimension) {
            case ALL:
                weight.setColumnWeight(1.0);
                weight.setConstraintWeight(meanCol / meanCons);
                weight.setOperationWeight((meanCol * weight.getColumnWeight() + meanCons * weight.getConstraintWeight()) / meanOp);
                weight.setFunctionWeight((meanCol * weight.getColumnWeight() + meanCons * weight.getConstraintWeight() + meanOp * weight.getOperationWeight()) / meanFunc);
                break;
            case NO_COLUMN:
                weight.setColumnWeight(0);
                weight.setConstraintWeight(1.0);
                weight.setOperationWeight((meanCol * weight.getColumnWeight() + meanCons * weight.getConstraintWeight()) / meanOp);
                weight.setFunctionWeight((meanCol * weight.getColumnWeight() + meanCons * weight.getConstraintWeight() + meanOp * weight.getOperationWeight()) / meanFunc);
                break;
            case NO_CONSTRAINT:
                weight.setColumnWeight(1.0);
                weight.setConstraintWeight(0);
                weight.setOperationWeight((meanCol * weight.getColumnWeight() + meanCons * weight.getConstraintWeight()) / meanOp);
                weight.setFunctionWeight((meanCol * weight.getColumnWeight() + meanCons * weight.getConstraintWeight() + meanOp * weight.getOperationWeight()) / meanFunc);
                break;
            case NO_OPERATION:
                weight.setColumnWeight(1.0);
                weight.setConstraintWeight(meanCol / meanCons);
                weight.setOperationWeight(0);
                weight.setFunctionWeight((meanCol * weight.getColumnWeight() + meanCons * weight.getConstraintWeight() + meanOp * weight.getOperationWeight()) / meanFunc);
                break;
            case NO_FUNCTION:
                weight.setColumnWeight(1.0);
                weight.setConstraintWeight(meanCol / meanCons);
                weight.setOperationWeight((meanCol * weight.getColumnWeight() + meanCons * weight.getConstraintWeight()) / meanOp);
                weight.setFunctionWeight(0);
        }
        weight.setCaseIds(caseIds);
        return weight;
    }

    /**
     * The weights are calculated according to the withoutDimension and the min-max normalized similarity
     */
    private List<Double> minMax(List<Double> array, WithoutDimension withoutDimension) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

        List<Double> res = new ArrayList<>();
        for (Double num : array) {
            max = Math.max(max, num);
            min = Math.min(min, num);
        }
        if (max == Double.MIN_VALUE) {
            max = 0.0;
        }
        if (min == Double.MAX_VALUE) {
            min = 0.0;
        }
        switch (withoutDimension) {
            case NO_COLUMN:
                weight.setColumnMaxGap(max - min);
                break;
            case NO_CONSTRAINT:
                weight.setConstraintMaxGap(max - min);
                break;
            case NO_OPERATION:
                weight.setOperationMaxGap(max - min);
                break;
            case NO_FUNCTION:
                weight.setFunctionMaxGap(max - min);
                break;
            case ALL:
                break;
        }
        //[min,max]->[0,1]
        //𝑣𝑖^′=𝑛𝑒𝑤𝑚𝑖𝑛+(𝑣𝑖−𝑚𝑖𝑛)/(𝑚𝑎𝑥−𝑚𝑖𝑛)∗(𝑛𝑒𝑤𝑚𝑎𝑥−𝑛𝑒𝑤𝑚𝑖𝑛)
        if (max - min != 0) {
            for (Double num : array) {
                res.add((num - min) / (max - min));
            }
        } else {
            for (int i = 0; i < array.size(); i++) {
                res.add(0.0);
            }
        }
        return res;
    }

    private double mean(List<Double> arr) {
        if (arr.isEmpty()) {
            return 0;
        }
        double res = 0;
        for (Double num : arr) {
            res += num;
        }
        return res / arr.size();
    }

    public static DBType checkDBType(String dbVersion) {
        dbVersion = dbVersion.toLowerCase();
        if (dbVersion.contains("mysql")) {
            return DBType.MYSQL;
        } else if (dbVersion.contains("mariadb")) {
            return DBType.MARIADB;
        } else if (dbVersion.contains("sqlite")) {
            return DBType.SQLITE;
        } else if (dbVersion.contains("tidb")) {
            return DBType.TIDB;
        }
        return DBType.SQLITE;
    }

    /**
     * Store the five most similar test cases for each test case in SQLite
     */
    private void storeMostSimilarTestCase() {
        int length;
        SimilarTestCase similarTestCase = new SimilarTestCase();
        PriorityQueue<DistanceName> queue;
        DistanceName curSimilarCase;

        //Store the five most similar test cases for each test case in SQLite
        for (Map.Entry<Integer, PriorityQueue<DistanceName>> entry : mostSimilarCases.entrySet()) {
            queue = mostSimilarCases.get(entry.getKey());
            length = Math.min(queue.size(), 5);
            similarTestCase.setCaseId(entry.getKey());
            for (int i = 0; i < length; i++) {
                if (queue.isEmpty()) {
                    break;
                }
                curSimilarCase = queue.poll();
                switch (i) {
                    case 0:
                        similarTestCase.setCaseOne(curSimilarCase.getCaseId1());
                        similarTestCase.setSimilarityOne(curSimilarCase.getDistance());
                        break;
                    case 1:
                        similarTestCase.setCaseTwo(curSimilarCase.getCaseId1());
                        similarTestCase.setSimilarityTwo(curSimilarCase.getDistance());
                        break;
                    case 2:
                        similarTestCase.setCaseThree(curSimilarCase.getCaseId1());
                        similarTestCase.setSimilarityThree(curSimilarCase.getDistance());
                        break;
                    case 3:
                        similarTestCase.setCaseFour(curSimilarCase.getCaseId1());
                        similarTestCase.setSimilarityFour(curSimilarCase.getDistance());
                        break;
                    case 4:
                        similarTestCase.setCaseFive(curSimilarCase.getCaseId1());
                        similarTestCase.setSimilarityFive(curSimilarCase.getDistance());
                        break;
                }
            }

            similarTestCaseDAO.updateSimilarTestCase(similarTestCase);
        }
    }

    public List<Integer> getCollectionSorted() {
        return collectionSorted;
    }

    public List<Integer> getCollectionUnsorted() {
        return collectionUnsorted;
    }

    public Map<Integer, DistanceName> getKeyToCaseName() {
        return keyToCaseName;
    }

    public Map<Integer, Double> getKeyToDistance() {
        return keyToDistance;
    }

    public Map<Integer, Feature> getKeyToFeature() {
        return keyToFeature;
    }

    public Map<Integer, PriorityQueue<DistanceName>> getMostSimilarCases() {
        return mostSimilarCases;
    }

    public List<Integer> getCaseIds() {
        return caseIds;
    }

    public Weight getWeight() {
        return weight;
    }
}
