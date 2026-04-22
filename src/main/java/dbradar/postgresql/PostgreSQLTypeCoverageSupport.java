package dbradar.postgresql;

import dbradar.common.query.SQLQueryAdapter;

public final class PostgreSQLTypeCoverageSupport {

    private PostgreSQLTypeCoverageSupport() {
    }

    public static CoveragePlan createCoveragePlan(PostgreSQLGlobalState state) {
        String prefix = state.getGeneratedObjectNamePrefix();
        String enumTypeName = prefix + "coverage_enum";
        String domainTypeName = prefix + "coverage_domain";
        String compositeTypeName = prefix + "coverage_composite";
        String builtInTableName = getFreeCoverageTableName(state, prefix + "typecov_builtin");
        String userDefinedTableName = getFreeCoverageTableName(state, prefix + "typecov_user");

        String typeSetup = String.format(
                "DO $$ BEGIN "
                        + "IF to_regtype('public.%1$s') IS NULL THEN "
                        + "EXECUTE 'CREATE TYPE %1$s AS ENUM (''red'',''green'',''blue'')'; "
                        + "END IF; "
                        + "IF to_regtype('public.%2$s') IS NULL THEN "
                        + "EXECUTE 'CREATE DOMAIN %2$s AS integer'; "
                        + "END IF; "
                        + "IF to_regtype('public.%3$s') IS NULL THEN "
                        + "EXECUTE 'CREATE TYPE %3$s AS (x integer, y text)'; "
                        + "END IF; "
                        + "END $$",
                enumTypeName, domainTypeName, compositeTypeName);

        String builtInCreate = String.format(
                "CREATE TABLE %s ("
                        + "c1 UUID, "
                        + "c2 JSONB, "
                        + "c3 XML, "
                        + "c4 BYTEA, "
                        + "c5 DATE, "
                        + "c6 TIME, "
                        + "c7 TIMESTAMP, "
                        + "c8 INTERVAL, "
                        + "c9 POINT, "
                        + "c10 CIDR, "
                        + "c11 MACADDR, "
                        + "c12 TSVECTOR, "
                        + "c13 PG_LSN, "
                        + "c14 OID, "
                        + "c15 INTEGER[]"
                        + ")",
                builtInTableName);
        String builtInInsert = String.format(
                "INSERT INTO %s VALUES ("
                        + "'00000000-0000-0000-0000-000000000001'::uuid, "
                        + "'{\"value\":1}'::jsonb, "
                        + "xmlparse(content '<root><value>1</value></root>'), "
                        + "'\\\\xDEADBEEF'::bytea, "
                        + "DATE '2024-01-01', "
                        + "TIME '12:34:56', "
                        + "TIMESTAMP '2024-01-01 12:34:56', "
                        + "INTERVAL '1 day 2 hours', "
                        + "point(1,2), "
                        + "'192.168.0.0/24'::cidr, "
                        + "'08:00:2b:01:02:03'::macaddr, "
                        + "to_tsvector('simple', 'alpha beta gamma'), "
                        + "'0/16B6C50'::pg_lsn, "
                        + "1::oid, "
                        + "ARRAY[1,2,3])",
                builtInTableName);

        String userDefinedCreate = String.format(
                "CREATE TABLE %s ("
                        + "c1 INT4RANGE, "
                        + "c2 INT4MULTIRANGE, "
                        + "c3 %s, "
                        + "c4 %s, "
                        + "c5 %s, "
                        + "c6 TEXT[], "
                        + "c7 JSON, "
                        + "c8 INET, "
                        + "c9 BOX, "
                        + "c10 LSEG, "
                        + "c11 PATH, "
                        + "c12 POLYGON, "
                        + "c13 CIRCLE, "
                        + "c14 MACADDR8, "
                        + "c15 TSQUERY"
                        + ")",
                userDefinedTableName, enumTypeName, domainTypeName, compositeTypeName);
        String userDefinedInsert = String.format(
                "INSERT INTO %s VALUES ("
                        + "'[1,10)'::int4range, "
                        + "'{[1,4),[8,12)}'::int4multirange, "
                        + "'green'::%s, "
                        + "42, "
                        + "ROW(7, 'composite')::%s, "
                        + "ARRAY['alpha','beta'], "
                        + "'{\"kind\":\"custom\"}'::json, "
                        + "'10.0.0.1'::inet, "
                        + "box(point(0,0), point(3,4)), "
                        + "lseg(point(0,0), point(2,2)), "
                        + "path '((0,0),(1,1),(2,0))', "
                        + "polygon '((0,0),(2,0),(2,2),(0,2))', "
                        + "circle(point(1,1), 5), "
                        + "'08:00:2b:01:02:03:04:05'::macaddr8, "
                        + "to_tsquery('simple', 'alpha & beta'))",
                userDefinedTableName, enumTypeName, compositeTypeName);

        return new CoveragePlan(
                new SQLQueryAdapter(typeSetup, true),
                new BootstrapScenario(
                        builtInTableName,
                        new SQLQueryAdapter(builtInCreate, true),
                        new SQLQueryAdapter(builtInInsert),
                        new SQLQueryAdapter("SELECT * FROM " + builtInTableName)),
                new BootstrapScenario(
                        userDefinedTableName,
                        new SQLQueryAdapter(userDefinedCreate, true),
                        new SQLQueryAdapter(userDefinedInsert),
                        new SQLQueryAdapter("SELECT * FROM " + userDefinedTableName)));
    }

    private static String getFreeCoverageTableName(PostgreSQLGlobalState state, String baseName) {
        int suffix = 0;
        while (true) {
            String candidate = suffix == 0 ? baseName : baseName + "_" + suffix;
            boolean exists = state.getSchema().getDatabaseTables().stream()
                    .anyMatch(table -> table.getName().equalsIgnoreCase(candidate));
            if (!exists) {
                return candidate;
            }
            suffix++;
        }
    }

    public static final class CoveragePlan {
        private final SQLQueryAdapter typeSetupQuery;
        private final BootstrapScenario builtInScenario;
        private final BootstrapScenario userDefinedScenario;

        private CoveragePlan(SQLQueryAdapter typeSetupQuery, BootstrapScenario builtInScenario,
                BootstrapScenario userDefinedScenario) {
            this.typeSetupQuery = typeSetupQuery;
            this.builtInScenario = builtInScenario;
            this.userDefinedScenario = userDefinedScenario;
        }

        public SQLQueryAdapter getTypeSetupQuery() {
            return typeSetupQuery;
        }

        public BootstrapScenario getBuiltInScenario() {
            return builtInScenario;
        }

        public BootstrapScenario getUserDefinedScenario() {
            return userDefinedScenario;
        }
    }

    public static final class BootstrapScenario {
        private final String tableName;
        private final SQLQueryAdapter createTableQuery;
        private final SQLQueryAdapter insertQuery;
        private final SQLQueryAdapter validationQuery;

        private BootstrapScenario(String tableName, SQLQueryAdapter createTableQuery, SQLQueryAdapter insertQuery,
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
