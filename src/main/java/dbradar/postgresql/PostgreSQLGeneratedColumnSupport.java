package dbradar.postgresql;

import dbradar.common.query.SQLQueryAdapter;

import java.util.ArrayList;
import java.util.List;

public final class PostgreSQLGeneratedColumnSupport {

    public static final int VIRTUAL_GENERATED_COLUMNS_MIN_SERVER_VERSION = 180000;

    private PostgreSQLGeneratedColumnSupport() {
    }

    public enum GeneratedColumnKind {
        STORED("STORED"),
        VIRTUAL("VIRTUAL");

        private final String keyword;

        GeneratedColumnKind(String keyword) {
            this.keyword = keyword;
        }

        public String getKeyword() {
            return keyword;
        }
    }

    public static boolean supportsVirtualGeneratedColumns(int serverVersionNum) {
        return serverVersionNum >= VIRTUAL_GENERATED_COLUMNS_MIN_SERVER_VERSION;
    }

    public static List<GeneratedColumnKind> getBootstrapGeneratedColumnKinds(int serverVersionNum, int ddlCount) {
        List<GeneratedColumnKind> kinds = new ArrayList<>();
        if (ddlCount <= 0) {
            return kinds;
        }
        kinds.add(GeneratedColumnKind.STORED);
        if (supportsVirtualGeneratedColumns(serverVersionNum) && ddlCount >= 5) {
            kinds.add(GeneratedColumnKind.VIRTUAL);
        }
        return kinds;
    }

    public static List<GeneratedColumnScenario> createBootstrapScenarios(PostgreSQLGlobalState state) {
        List<GeneratedColumnScenario> scenarios = new ArrayList<>();
        for (GeneratedColumnKind kind : getBootstrapGeneratedColumnKinds(state.getServerVersionNum(),
                state.getOptions().getDdlCount())) {
            scenarios.add(createGeneratedTable(state, kind));
        }
        return scenarios;
    }

    public static String renderGeneratedColumnClause(String expression, GeneratedColumnKind kind) {
        return String.format("GENERATED ALWAYS AS (%s) %s", expression, kind.getKeyword());
    }

    public static GeneratedColumnScenario createGeneratedTable(PostgreSQLGlobalState state, GeneratedColumnKind kind) {
        String tableName = state.getSchema().getFreeTableName(state.getGeneratedObjectNamePrefix());
        String createTable = String.format(
                "CREATE TABLE %s ("
                        + "c1 INT NOT NULL, "
                        + "c2 INT NOT NULL, "
                        + "c3 BOOLEAN, "
                        + "c4 TEXT, "
                        + "c5 FLOAT, "
                        + "c6 DECIMAL, "
                        + "c7 BIT(1), "
                        + "c8 INT %s"
                        + ")",
                tableName,
                renderGeneratedColumnClause("c1 + c2", kind));
        String insert = String.format(
                "INSERT INTO %s (c1,c2,c3,c4,c5,c6,c7) VALUES (1,2,TRUE,'generated-row',1.009,12.991,b'1')",
                tableName);
        String validation = String.format("SELECT * FROM %s", tableName);
        return new GeneratedColumnScenario(
                tableName,
                kind,
                new SQLQueryAdapter(createTable, true),
                new SQLQueryAdapter(insert),
                new SQLQueryAdapter(validation));
    }

    public static final class GeneratedColumnScenario {
        private final String tableName;
        private final GeneratedColumnKind kind;
        private final SQLQueryAdapter createTableQuery;
        private final SQLQueryAdapter insertQuery;
        private final SQLQueryAdapter validationQuery;

        private GeneratedColumnScenario(String tableName, GeneratedColumnKind kind, SQLQueryAdapter createTableQuery,
                SQLQueryAdapter insertQuery, SQLQueryAdapter validationQuery) {
            this.tableName = tableName;
            this.kind = kind;
            this.createTableQuery = createTableQuery;
            this.insertQuery = insertQuery;
            this.validationQuery = validationQuery;
        }

        public String getTableName() {
            return tableName;
        }

        public GeneratedColumnKind getKind() {
            return kind;
        }

        public SQLQueryAdapter getCreateTableQuery() {
            return createTableQuery;
        }

        public SQLQueryAdapter getInsertQuery() {
            return insertQuery;
        }

        public SQLQueryAdapter getValidationQuery() {
            return validationQuery;
        }
    }
}
