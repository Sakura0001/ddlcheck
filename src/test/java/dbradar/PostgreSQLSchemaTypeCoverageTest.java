package dbradar;

import com.beust.jcommander.JCommander;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLOptions;
import dbradar.postgresql.PostgreSQLProvider;
import dbradar.postgresql.PostgreSQLSchema;

import java.sql.Statement;
import java.util.Map;
import java.util.stream.Collectors;

public final class PostgreSQLSchemaTypeCoverageTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;
    private static final String DATABASE_NAME = "task5_schema_cover";

    private PostgreSQLSchemaTypeCoverageTest() {
    }

    public static void main(String[] args) throws Exception {
        PostgreSQLGlobalState state = createState(DATABASE_NAME);
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute("CREATE TYPE task5_enum AS ENUM ('red','green')");
            statement.execute("CREATE DOMAIN task5_domain AS integer");
            statement.execute("CREATE TYPE task5_composite AS (x integer, y text)");
            statement.execute("CREATE TABLE task5_type_table ("
                    + "c1 uuid, "
                    + "c2 jsonb, "
                    + "c3 xml, "
                    + "c4 bytea, "
                    + "c5 point, "
                    + "c6 cidr, "
                    + "c7 tsvector, "
                    + "c8 pg_lsn, "
                    + "c9 integer[], "
                    + "c10 int4range, "
                    + "c11 int4multirange, "
                    + "c12 task5_enum, "
                    + "c13 task5_domain, "
                    + "c14 task5_composite, "
                    + "c15 money, "
                    + "c16 box, "
                    + "c17 lseg, "
                    + "c18 path, "
                    + "c19 polygon, "
                    + "c20 circle, "
                    + "c21 macaddr8, "
                    + "c22 tsquery"
                    + ")");
        }
        state.updateSchema();

        PostgreSQLSchema.PostgreSQLTable table = state.getSchema().getDatabaseTablesWithoutViews().stream()
                .filter(candidate -> candidate.getName().equals("task5_type_table"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected task5_type_table to exist"));

        Map<String, PostgreSQLSchema.PostgreSQLColumn> columns = table.getColumns().stream()
                .collect(Collectors.toMap(PostgreSQLSchema.PostgreSQLColumn::getColumnName, column -> column));

        require(columns.get("c1").getType() == PostgreSQLSchema.PostgreSQLDataType.UUID, "Expected UUID mapping");
        require(columns.get("c2").getType() == PostgreSQLSchema.PostgreSQLDataType.JSONB, "Expected JSONB mapping");
        require(columns.get("c3").getType() == PostgreSQLSchema.PostgreSQLDataType.XML, "Expected XML mapping");
        require(columns.get("c4").getType() == PostgreSQLSchema.PostgreSQLDataType.BYTEA, "Expected BYTEA mapping");
        require(columns.get("c5").getType() == PostgreSQLSchema.PostgreSQLDataType.POINT, "Expected POINT mapping");
        require(columns.get("c6").getType() == PostgreSQLSchema.PostgreSQLDataType.CIDR, "Expected CIDR mapping");
        require(columns.get("c7").getType() == PostgreSQLSchema.PostgreSQLDataType.TSVECTOR, "Expected TSVECTOR mapping");
        require(columns.get("c8").getType() == PostgreSQLSchema.PostgreSQLDataType.PG_LSN, "Expected PG_LSN mapping");
        require(columns.get("c9").getType() == PostgreSQLSchema.PostgreSQLDataType.ARRAY, "Expected ARRAY mapping");
        require(columns.get("c10").getType() == PostgreSQLSchema.PostgreSQLDataType.RANGE, "Expected RANGE mapping");
        require(columns.get("c11").getType() == PostgreSQLSchema.PostgreSQLDataType.MULTIRANGE,
                "Expected MULTIRANGE mapping");
        require(columns.get("c12").getType() == PostgreSQLSchema.PostgreSQLDataType.ENUM, "Expected ENUM mapping");
        require(columns.get("c13").getDomainName().equals("task5_domain"), "Expected DOMAIN_NAME to be preserved");
        require(columns.get("c14").getType() == PostgreSQLSchema.PostgreSQLDataType.COMPOSITE,
                "Expected COMPOSITE mapping");
        require(columns.get("c15").getType() == PostgreSQLSchema.PostgreSQLDataType.MONEY, "Expected MONEY mapping");
        require(columns.get("c16").getType() == PostgreSQLSchema.PostgreSQLDataType.BOX, "Expected BOX mapping");
        require(columns.get("c17").getType() == PostgreSQLSchema.PostgreSQLDataType.LSEG, "Expected LSEG mapping");
        require(columns.get("c18").getType() == PostgreSQLSchema.PostgreSQLDataType.PATH, "Expected PATH mapping");
        require(columns.get("c19").getType() == PostgreSQLSchema.PostgreSQLDataType.POLYGON,
                "Expected POLYGON mapping");
        require(columns.get("c20").getType() == PostgreSQLSchema.PostgreSQLDataType.CIRCLE,
                "Expected CIRCLE mapping");
        require(columns.get("c21").getType() == PostgreSQLSchema.PostgreSQLDataType.MACADDR,
                "Expected MACADDR8 to reuse MACADDR mapping");
        require(columns.get("c22").getType() == PostgreSQLSchema.PostgreSQLDataType.TSQUERY,
                "Expected TSQUERY mapping");
        require(columns.get("c12").getUdtName().equals("task5_enum"), "Expected enum UDT name");
        require(columns.get("c14").getUdtName().equals("task5_composite"), "Expected composite UDT name");

        state.getConnection().close();
    }

    private static PostgreSQLGlobalState createState(String databaseName) throws Exception {
        MainOptions options = new MainOptions();
        DBMSExecutorFactory<?> executorFactory = new DBMSExecutorFactory<>(new PostgreSQLProvider(), options);
        JCommander.newBuilder()
                .addObject(options)
                .addCommand("postgresql", executorFactory.getCommand())
                .build()
                .parse("--host", HOST, "--port", String.valueOf(PORT), "--username", USERNAME, "--password", PASSWORD,
                        "postgresql");

        PostgreSQLGlobalState state = new PostgreSQLGlobalState();
        Randomly.initialize(options);
        state.setMainOptions(options);
        state.setDbmsSpecificOptions((PostgreSQLOptions) executorFactory.getCommand());
        state.setRandomly(new Randomly(1));
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase());
        state.updateSchema();
        return state;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
