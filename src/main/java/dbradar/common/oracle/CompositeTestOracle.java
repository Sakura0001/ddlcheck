package dbradar.common.oracle;

import java.util.List;

import dbradar.GlobalState;
import dbradar.Reproducer;

public class CompositeTestOracle implements TestOracle {

    private final List<TestOracle> oracles;
    private final GlobalState globalState;
    private int i;
    private int iLast;

    public CompositeTestOracle(List<TestOracle> oracles, GlobalState globalState) {
        this.globalState = globalState;
        this.oracles = oracles;
    }

    @Override
    public void check() throws Exception {
        try {
            globalState.setCurrentOracle(oracles.get(i).getOracleName());
            oracles.get(i).check();
            iLast = i;
            boolean lastOracleIndex = i == oracles.size() - 1;
            if (!lastOracleIndex) {
                globalState.getManager().incrementSelectQueryCount();
            }
        } finally {
            i = (i + 1) % oracles.size();
        }
    }

    @Override
    public String getLastQueryString() {
        return oracles.get(iLast).getLastQueryString();
    }

    public List<TestOracle> getOracles() {
        return oracles;
    }

    @Override
    public Reproducer getLastReproducer() {
        return getOracles().get(0).getLastReproducer();
    }
}
