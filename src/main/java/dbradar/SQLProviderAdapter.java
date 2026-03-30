package dbradar;

import java.util.List;
import java.util.stream.Collectors;

import dbradar.common.log.LoggableFactory;
import dbradar.common.log.SQLLoggableFactory;
import dbradar.common.oracle.CompositeTestOracle;
import dbradar.common.oracle.TestOracle;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.schema.AbstractTable;

public abstract class SQLProviderAdapter extends ProviderAdapter {

    protected SQLProviderAdapter(Class<? extends GlobalState> globalClass,
            Class<? extends DBMSSpecificOptions> optionClass) {
        super(globalClass, optionClass);
    }

    @Override
    public LoggableFactory getLoggableFactory() {
        return new SQLLoggableFactory();
    }

    @Override
    protected void checkViewsAreValid(GlobalState globalState) {
        List<? extends AbstractTable<?, ?, ?>> views = globalState.getSchema().getViews();
        for (AbstractTable<?, ?, ?> view : views) {
            SQLQueryAdapter q = new SQLQueryAdapter("SELECT 1 FROM " + view.getName() + " LIMIT 1");
            try {
                if (!q.execute(globalState)) {
                    dropView(globalState, view.getName());
                }
            } catch (Throwable t) {
                dropView(globalState, view.getName());
            }
        }
    }

    private void dropView(GlobalState globalState, String viewName) {
        try {
            globalState.executeStatement(new SQLQueryAdapter("DROP VIEW " + viewName, true));
        } catch (Throwable t2) {
            throw new IgnoreMeException();
        }
    }

    @Override
    protected TestOracle getTestOracle(GlobalState state) throws Exception {

        SQLGlobalState globalState = (SQLGlobalState) state;

        List<? extends OracleFactory> testOracleFactory = globalState
                .getDbmsSpecificOptions().getTestOracleFactory();

        boolean testOracleRequiresMoreThanZeroRows = testOracleFactory.stream()
                .anyMatch(OracleFactory::requiresAllTablesToContainRows);
        boolean userRequiresMoreThanZeroRows = globalState.getOptions().testOnlyWithMoreThanZeroRows();
        boolean checkZeroRows = testOracleRequiresMoreThanZeroRows || userRequiresMoreThanZeroRows;
        return new CompositeTestOracle(testOracleFactory.stream().map(o -> {
            try {
                return o.create(globalState);
            } catch (Exception e1) {
                throw new AssertionError(e1);
            }
        }).collect(Collectors.toList()), globalState);
    }
}
