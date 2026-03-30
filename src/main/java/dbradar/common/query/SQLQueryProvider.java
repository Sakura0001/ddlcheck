package dbradar.common.query;

import dbradar.GlobalState;

@FunctionalInterface
public interface SQLQueryProvider {
    SQLQueryAdapter getQuery(GlobalState globalState) throws Exception;
}
