package dbradar;

import com.fasterxml.jackson.databind.ObjectMapper;
import dbradar.common.duplicate.DistanceName;
import dbradar.common.duplicate.Feature;
import dbradar.common.duplicate.TestCase;
import dbradar.common.duplicate.TestCaseDAO;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static dbradar.DeduplicateHelper.checkDBType;

public class DBMSExecutor {

    private final DatabaseProvider provider;
    private final MainOptions options;
    private final DBMSSpecificOptions command;
    private final String databaseName;
    private StateLogger logger;
    private StateToReproduce stateToRepro;
    private final Randomly r;

    private List<Map<Integer, Map<Integer, Integer>>> seqCounterList = null;

    public DBMSExecutor(DatabaseProvider provider, MainOptions options, DBMSSpecificOptions dbmsSpecificOptions,
                        String databaseName, Randomly r) {
        this.provider = provider;
        this.options = options;
        this.databaseName = databaseName;
        this.command = dbmsSpecificOptions;
        this.r = r;
    }

    private GlobalState createGlobalState() {
        try {
            return provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public DBMSSpecificOptions getCommand() {
        return command;
    }

    public void testConnection() throws Exception {
        GlobalState state = getInitializedGlobalState(options.getRandomSeed());
        try (DatabaseConnection con = provider.createDatabase(state)) {
            return;
        }
    }

    public void run() throws Exception {
        GlobalState state = createGlobalState();
        stateToRepro = provider.getStateToReproduce(databaseName);
        stateToRepro.seedValue = r.getSeed();
        state.setState(stateToRepro);
        logger = new StateLogger(databaseName, provider, options);
        state.setRandomly(r);
        state.setDatabaseName(databaseName);
        state.setMainOptions(options);
        state.setDbmsSpecificOptions(command);
        try (DatabaseConnection con = provider.createDatabase(state)) {
            QueryManager manager = new QueryManager(state);
            try {
                stateToRepro.databaseVersion = con.getDatabaseVersion();
            } catch (Exception e) {
                // ignore
            }
            state.setConnection(con);
            state.setStateLogger(logger);
            state.setManager(manager);
            if (seqCounterList != null) {
                state.setSeqCounterList(seqCounterList);
            }
            if (options.logEachSelect()) {
                logger.writeCurrent(state.getState());
            }
            Reproducer reproducer = provider.generateAndTestDatabase(state);
            try {
                logger.getCurrentFileWriter().close();
                logger.currentFileWriter = null;
            } catch (IOException e) {
                throw new AssertionError(e);
            }

            if (options.reduceAST() && !options.useReducer()) {
                throw new AssertionError("To reduce AST, use-reducer option must be enabled first");
            }
            if (reproducer != null && options.useReducer()) {
                System.out.println("EXPERIMENTAL: Trying to reduce queries using a simple reducer.");
                // System.out.println("Reduced query will be output to stdout but not logs.");
                GlobalState newGlobalState = createGlobalState();
                newGlobalState.setState(stateToRepro);
                newGlobalState.setRandomly(r);
                newGlobalState.setDatabaseName(databaseName);
                newGlobalState.setMainOptions(options);
                newGlobalState.setDbmsSpecificOptions(command);
                QueryManager newManager = new QueryManager(newGlobalState);
                newGlobalState.setStateLogger(new StateLogger(databaseName, provider, options));
                newGlobalState.setManager(newManager);
                newGlobalState.setCurrentOracle(state.getCurrentOracle());

                Reducer reducer = new StatementReducer(provider, newGlobalState, reproducer);
                TestCase testCase = reducer.reduce();

                if (options.isDuplicateDetection()) {
                    TestCaseDAO testCaseDAO = new TestCaseDAO();
                    System.out.println("EXPERIMENTAL: Trying to find five most similar test cases.");
                    //保存测试用例的四维特征（仅保留缺陷触发相关特征，可以减少数据量）
                    Integer caseId = testCaseDAO.getMaxTestCaseId();
                    Feature curCase = new Feature().recordFactor(newGlobalState, reproducer);

                    //set vector
                    testCase.setVector(new ObjectMapper().writeValueAsString(curCase));

                    //将测试用例信息持久化
                    testCaseDAO.addTestCase(testCase);

                    DeduplicateHelper.DBType dbType = checkDBType(state.getState().getDatabaseVersion());

                    DeduplicateHelper deduplicateHelper = new DeduplicateHelper();
                    deduplicateHelper.pickKCases(0, curCase, options.isReCalculateWeight(),
                            caseId, DeduplicateHelper.WithoutDimension.ALL, dbType);
                    System.out.println("Current test case: " + caseId);
                    System.out.println("Sorted test case sequence:");
                    int count = 1;
                    for (Integer id : deduplicateHelper.getCollectionSorted()) {
                        System.out.println(count++ + ":" + id);
                    }
                    int position = deduplicateHelper.getCollectionSorted().indexOf(caseId);
                    System.out.println("The position of the current test case in the sequence: " + position + "/" + deduplicateHelper.getCollectionSorted().size());
                    System.out.println("The five test cases most similar to the current use case are: ");
                    PriorityQueue<DistanceName> similarCases = deduplicateHelper.getMostSimilarCases().get(caseId);
                    int caseCount = Math.min(similarCases.size(), 5);
                    for (int i = 0; i < caseCount; i++) {
                        DistanceName distanceName = similarCases.poll();
                        assert distanceName != null;
                        System.out.println("caseName:" + distanceName.getCaseId2() + ",similarity:" + distanceName.getDistance());
                    }


                }

                throw new AssertionError("Found a potential bug");
            }
        }
    }

    private GlobalState getInitializedGlobalState(long seed) {
        GlobalState state = createGlobalState();
        stateToRepro = provider.getStateToReproduce(databaseName);
        stateToRepro.seedValue = seed;
        state.setState(stateToRepro);
        logger = new StateLogger(databaseName, provider, options);
        Randomly r = new Randomly(seed);
        state.setRandomly(r);
        state.setDatabaseName(databaseName);
        state.setMainOptions(options);
        state.setDbmsSpecificOptions(command);
        return state;
    }

    public StateLogger getLogger() {
        return logger;
    }

    public StateToReproduce getStateToReproduce() {
        return stateToRepro;
    }

    public void setSeqCounterList(List<Map<Integer, Map<Integer, Integer>>> seqCounterList) {
        this.seqCounterList = seqCounterList;
    }
}