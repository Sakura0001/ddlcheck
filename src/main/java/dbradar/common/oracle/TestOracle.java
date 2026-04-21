package dbradar.common.oracle;

import dbradar.Reproducer;

public interface TestOracle {

    void check() throws Exception;

    default Reproducer getLastReproducer() {
        return null;
    }

    default String getLastQueryString() {
        throw new AssertionError("Not supported!");
    }

    default String getOracleName() {
        throw new AssertionError("Not supported!");
    }
}
