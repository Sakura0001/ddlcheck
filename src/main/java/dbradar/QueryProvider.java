package dbradar;

import dbradar.common.query.Query;

public interface QueryProvider {

    Query getQuery(GlobalState globalState) throws Exception;

    boolean canBeRetried();

}
