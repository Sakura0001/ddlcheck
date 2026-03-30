package dbradar;

import java.util.Map;

import dbradar.common.query.Query;
import dbradar.common.query.generator.QueryGenerationException;

public class StatementExecutor {

    private final GlobalState globalState;
    private final Map<QueryProvider, Integer> queryWeights;
    private final AfterQueryAction queryConsumer;

    @FunctionalInterface
    public interface AfterQueryAction {
        void notify(Query q) throws Exception;
    }

    public StatementExecutor(GlobalState globalState, Map<QueryProvider, Integer> queryWeights,
            AfterQueryAction queryConsumer) {
        this.globalState = globalState;
        this.queryWeights = queryWeights;
        this.queryConsumer = queryConsumer;
    }

    public void executeStatements() throws Exception {
        Randomly r = globalState.getRandomly();
        int nStmt = r.getInteger(20, 50);
        for (int i = 0; i < nStmt; i++) {
            QueryProvider action = Randomly.fromWeights(queryWeights);
            Query query = null;
            try {
                boolean success;
                int nrTries = 0;
                do {
                    query = action.getQuery(globalState);
                    success = globalState.executeStatement(query);
                } while (action.canBeRetried() && !success
                        && nrTries++ < globalState.getOptions().getNrStatementRetryCount());
            } catch (IgnoreMeException | QueryGenerationException ignored) {

            }
            if (query != null && query.couldAffectSchema()) {
                globalState.updateSchema();
                queryConsumer.notify(query);
            }
        }
    }
}
