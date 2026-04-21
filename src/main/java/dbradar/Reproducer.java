package dbradar;

import dbradar.common.query.SQLQueryAdapter;

import java.util.List;

public interface Reproducer {

    boolean bugStillTriggers(GlobalState globalState);

    default List<SQLQueryAdapter> getOracleStatements() {
        return null;
    }

    default void setOracleStatements(List<SQLQueryAdapter> oracleStatements) {}

}
