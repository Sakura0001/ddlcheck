package dbradar;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import dbradar.StateToReproduce.OracleRunReproductionState;
import dbradar.common.oracle.CompositeTestOracle;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.oracle.TestOracle;
import dbradar.mysql.oracle.MySQLStressOracle;
import dbradar.mysql.MySQLOptions;

public abstract class ProviderAdapter implements DatabaseProvider {

    private final Class<? extends GlobalState> globalClass;
    private final Class<? extends DBMSSpecificOptions> optionClass;

    protected ProviderAdapter(Class<? extends GlobalState> globalClass,
                              Class<? extends DBMSSpecificOptions> optionClass) {
        this.globalClass = globalClass;
        this.optionClass = optionClass;
    }

    @Override
    public StateToReproduce getStateToReproduce(String databaseName) {
        return new StateToReproduce(databaseName, this);
    }

    @Override
    public Class<? extends GlobalState> getGlobalStateClass() {
        return globalClass;
    }

    @Override
    public Class<? extends DBMSSpecificOptions> getOptionClass() {
        return optionClass;
    }

    @Override
    public Reproducer generateAndTestDatabase(GlobalState globalState) throws Exception {
        boolean useEquation = false;
        TestOracle oracle = null;
        try {
            generateDatabase(globalState);

            oracle = getTestOracle(globalState);
            List<TestOracle> oracleList = ((CompositeTestOracle) oracle).getOracles();

            boolean hasStress = oracleList.stream().anyMatch(o -> o instanceof MySQLStressOracle);
            if (hasStress) {
                return runStressRounds(globalState);
            } else {
                checkViewsAreValid(globalState);
                globalState.getManager().incrementCreateDatabase();
                TestOracle firstOracle = oracleList.get(0);
                if (firstOracle instanceof EDCBase) {
                    useEquation = true;
                }
            }

            for (int i = 0; i < globalState.getOptions().getNrQueries(); i++) {
                try (OracleRunReproductionState localState = globalState.getState().createLocalState()) {
                    assert localState != null;
                    try {
                        oracle.check();
                        globalState.getManager().incrementSelectQueryCount();
                    } catch (IgnoreMeException e) {

                    } catch (AssertionError e) {
                        Reproducer reproducer = oracle.getLastReproducer();
                        if (reproducer != null) {
                            return reproducer;
                        }
                        throw e;
                    }
                    assert localState != null;
                    localState.executedWithoutError();
                }
            }
        } finally {
            if (useEquation && oracle != null) {
                ((EDCBase<?>) ((CompositeTestOracle) oracle).getOracles().get(0)).closeConnection();
            } else {
                globalState.getConnection().close();
            }
        }
        return null;
    }

    private Reproducer runStressRounds(GlobalState globalState) throws Exception {
        MySQLOptions options = (MySQLOptions) globalState.getDbmsSpecificOptions();
        for (int round = 0; round < options.getStressRoundsPerDb(); round++) {
            if (round > 0) {
                globalState.getConnection().close();
                globalState.setConnection(createDatabase(globalState));
                globalState.setSchema(null);
            }
            checkViewsAreValid(globalState);
            globalState.getManager().incrementCreateDatabase();
            TestOracle oracle = getStressOnlyOracle(globalState);
            try (OracleRunReproductionState localState = globalState.getState().createLocalState()) {
                oracle.check();
                globalState.getManager().incrementSelectQueryCount();
                localState.executedWithoutError();
            }
        }
        return null;
    }

    private TestOracle getStressOnlyOracle(GlobalState globalState) throws Exception {
        List<TestOracle> oracleList = ((CompositeTestOracle) getTestOracle(globalState)).getOracles();
        List<TestOracle> stressOnly = new ArrayList<>();
        for (TestOracle candidate : oracleList) {
            if (candidate instanceof MySQLStressOracle) {
                stressOnly.add(candidate);
                break;
            }
        }
        return new CompositeTestOracle(stressOnly, globalState);
    }

    protected abstract void checkViewsAreValid(GlobalState globalState) throws SQLException;

    protected abstract TestOracle getTestOracle(GlobalState globalState) throws Exception;

    public abstract void generateDatabase(GlobalState globalState) throws Exception;
}
