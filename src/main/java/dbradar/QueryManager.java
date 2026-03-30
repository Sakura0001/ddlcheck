package dbradar;

import dbradar.common.query.DBRadarResultSet;
import dbradar.common.query.Query;

public class QueryManager {

    private final GlobalState globalState;

    public QueryManager(GlobalState globalState) {
        this.globalState = globalState;
    }

    public boolean execute(Query q, String... fills) throws Exception {
        boolean success;
        success = q.execute(globalState, fills);
        Main.nrSuccessfulActions.addAndGet(1);
        if (globalState.getOptions().loggerPrintFailed() || success) {
            globalState.getState().logStatement(q);
        }
        return success;
    }

    public DBRadarResultSet executeAndGet(Query q, String... fills) throws Exception {
        globalState.getState().logStatement(q);
        DBRadarResultSet result;
        result = q.executeAndGet(globalState, fills);
        Main.nrSuccessfulActions.addAndGet(1);
        return result;
    }

    public void incrementSelectQueryCount() {
        Main.nrQueries.addAndGet(1);
    }

    public Long getSelectQueryCount() {
        return Main.nrQueries.get();
    }

    public void incrementCreateDatabase() {
        Main.nrDatabases.addAndGet(1);
    }

}