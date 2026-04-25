package dbradar;

import dbradar.common.query.SQLQueryAdapter;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLProvider;
import dbradar.postgresql.PostgreSQLOptions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PostgreSQLTypeModifierCoverageTest {

    private static final String HOST = "127.0.0.1";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int PORT = 5432;
    private static final int MIN_MODIFIER = 500;
    private static final int MAX_MODIFIER = 1000;
    private static final Pattern VARCHAR_PATTERN = Pattern.compile("\\bVARCHAR\\((\\d+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CHAR_PATTERN = Pattern.compile("\\bCHAR\\((\\d+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern BIT_PATTERN = Pattern.compile("\\bBIT\\((\\d+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern VARBIT_PATTERN = Pattern.compile("\\bVARBIT\\((\\d+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("\\b(?:NUMERIC|DECIMAL)\\((\\d+),(\\d+)\\)",
            Pattern.CASE_INSENSITIVE);

    private PostgreSQLTypeModifierCoverageTest() {
    }

    public static void main(String[] args) throws Exception {
        verifyRandomCreateTableCoversSizedTypesInUpperHalfRange();
    }

    private static void verifyRandomCreateTableCoversSizedTypesInUpperHalfRange() throws Exception {
        PostgreSQLGlobalState state = createState("type_modifier_coverage_state");
        boolean sawVarchar = false;
        boolean sawChar = false;
        boolean sawBit = false;
        boolean sawVarbit = false;
        boolean sawNumeric = false;
        try {
            for (long seed = 1; seed <= 2000; seed++) {
                state.setRandomly(new Randomly(seed));
                SQLQueryAdapter createTable;
                try {
                    createTable = PostgreSQLProvider.PostgreSQLQueryProvider.CREATE_TABLE.getQuery(state);
                } catch (IgnoreMeException ignored) {
                    continue;
                }
                String sql = createTable.getQueryString();
                sawVarchar |= containsUpperHalfModifier(VARCHAR_PATTERN, sql);
                sawChar |= containsUpperHalfModifier(CHAR_PATTERN, sql);
                sawBit |= containsUpperHalfModifier(BIT_PATTERN, sql);
                sawVarbit |= containsUpperHalfModifier(VARBIT_PATTERN, sql);
                sawNumeric |= containsUpperHalfNumeric(NUMERIC_PATTERN, sql);
                if (createTable.execute(state.getConnection(), false)) {
                    state.updateSchema();
                }
                if (sawVarchar && sawChar && sawBit && sawVarbit && sawNumeric) {
                    return;
                }
            }
            throw new AssertionError(String.format(
                    "Missing sized type coverage: varchar=%s char=%s bit=%s varbit=%s numeric=%s",
                    sawVarchar, sawChar, sawBit, sawVarbit, sawNumeric));
        } finally {
            closeQuietly(state);
        }
    }

    private static boolean containsUpperHalfModifier(Pattern pattern, String sql) {
        Matcher matcher = pattern.matcher(sql);
        boolean sawMatch = false;
        while (matcher.find()) {
            sawMatch = true;
            int length = Integer.parseInt(matcher.group(1));
            require(length >= MIN_MODIFIER && length <= MAX_MODIFIER,
                    "Expected type modifier in upper-half range but observed " + length + " in: " + sql);
        }
        return sawMatch;
    }

    private static boolean containsUpperHalfNumeric(Pattern pattern, String sql) {
        Matcher matcher = pattern.matcher(sql);
        boolean sawMatch = false;
        while (matcher.find()) {
            sawMatch = true;
            int precision = Integer.parseInt(matcher.group(1));
            int scale = Integer.parseInt(matcher.group(2));
            require(precision >= MIN_MODIFIER && precision <= MAX_MODIFIER,
                    "Expected numeric precision in upper-half range but observed " + precision + " in: " + sql);
            require(scale >= precision / 2 && scale < precision,
                    "Expected numeric scale in upper-half precision range but observed " + scale + " in: " + sql);
        }
        return sawMatch;
    }

    private static PostgreSQLGlobalState createState(String databaseName) throws Exception {
        MainOptions options = new MainOptions();
        Randomly.initialize(options);

        PostgreSQLOptions dbOptions = new PostgreSQLOptions();
        PostgreSQLGlobalState state = new PostgreSQLGlobalState();
        state.setMainOptions(options);
        state.setDbmsSpecificOptions(dbOptions);
        state.setRandomly(new Randomly(0));
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase(HOST, PORT, USERNAME, PASSWORD, databaseName));
        return state;
    }

    private static void closeQuietly(PostgreSQLGlobalState state) {
        if (state == null || state.getConnection() == null) {
            return;
        }
        try {
            state.getConnection().close();
        } catch (Exception ignored) {
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
