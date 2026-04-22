package dbradar.postgresql;

import dbradar.common.query.SQLQueryAdapter;

public final class PostgreSQLGeneratedColumnSupport {

    private PostgreSQLGeneratedColumnSupport() {
    }

    public static GeneratedColumnScenario createStoredGeneratedTable(PostgreSQLGlobalState state) {
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
                        + "c8 INT GENERATED ALWAYS AS (c1 + c2) STORED"
                        + ")",
                tableName);
        String insert = String.format(
                "INSERT INTO %s (c1,c2,c3,c4,c5,c6,c7) VALUES (1,2,TRUE,'generated-row',1.009,12.991,b'1')",
                tableName);
        String validation = String.format("SELECT * FROM %s", tableName);
        return new GeneratedColumnScenario(
                tableName,
                new SQLQueryAdapter(createTable, true),
                new SQLQueryAdapter(insert),
                new SQLQueryAdapter(validation));
    }

    public static final class GeneratedColumnScenario {
        private final String tableName;
        private final SQLQueryAdapter createTableQuery;
        private final SQLQueryAdapter insertQuery;
        private final SQLQueryAdapter validationQuery;

        private GeneratedColumnScenario(String tableName, SQLQueryAdapter createTableQuery, SQLQueryAdapter insertQuery,
                SQLQueryAdapter validationQuery) {
            this.tableName = tableName;
            this.createTableQuery = createTableQuery;
            this.insertQuery = insertQuery;
            this.validationQuery = validationQuery;
        }

        public String getTableName() {
            return tableName;
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
