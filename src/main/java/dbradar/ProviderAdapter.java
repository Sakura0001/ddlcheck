package dbradar;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import dbradar.StateToReproduce.OracleRunReproductionState;
import dbradar.common.oracle.CompositeTestOracle;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.oracle.TestOracle;
import dbradar.mysql.oracle.MySQLStressOracle;

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
        boolean useStress = false;
        TestOracle oracle = null;
        try {
            generateDatabase(globalState);
            checkViewsAreValid(globalState);
            globalState.getManager().incrementCreateDatabase();

            oracle = getTestOracle(globalState);
            List<TestOracle> oracleList = ((CompositeTestOracle) oracle).getOracles();

            boolean hasStress = oracleList.stream().anyMatch(o -> o instanceof MySQLStressOracle);
            if (hasStress) {
                useStress = true;
                List<TestOracle> stressOnly = new ArrayList<>();
                for (TestOracle o : oracleList) {
                    if (o instanceof MySQLStressOracle) {
                        stressOnly.add(o);
                        break;
                    }
                }
                oracle = new CompositeTestOracle(stressOnly, globalState);
            } else {
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

    protected abstract void checkViewsAreValid(GlobalState globalState) throws SQLException;

    protected abstract TestOracle getTestOracle(GlobalState globalState) throws Exception;

    public abstract void generateDatabase(GlobalState globalState) throws Exception;
}
