package dbradar;

import dbradar.common.query.Query;

import java.sql.SQLException;

public abstract class SQLGlobalState extends GlobalState {

    @Override
    protected void executeEpilogue(Query q, boolean success, ExecutionTimer timer) throws Exception {
        boolean logExecutionTime = getOptions().logExecutionTime();
        if (success && getOptions().printSucceedingStatements()) {
            System.out.println(q.getQueryString());
        }
        if (logExecutionTime) {
            getLogger().writeCurrent(" -- " + timer.end().asString());
        }
        if (q.couldAffectSchema()) {
            updateSchema();
        }
    }

    @Override
    public SQLConnection getConnection() {
        return (SQLConnection) super.getConnection();
    }

    public abstract SQLConnection createDatabase() throws SQLException;
}
