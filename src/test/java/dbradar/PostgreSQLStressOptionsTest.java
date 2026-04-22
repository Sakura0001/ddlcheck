package dbradar;

import com.beust.jcommander.JCommander;
import dbradar.postgresql.PostgreSQLOptions;

import java.lang.reflect.Field;

public final class PostgreSQLStressOptionsTest {

    private PostgreSQLStressOptionsTest() {
    }

    public static void main(String[] args) throws Exception {
        verifiesStressOracleParsing();
        verifiesStressTopologyParsing();
        verifiesStressThreadsPerDbParsing();
        verifiesEffectiveStressThreadsPerDbFallbacks();
        verifiesEffectiveStressThreadsPerDbNormalization();
    }

    private static void verifiesStressOracleParsing() {
        PostgreSQLOptions pgOptions = parseOptions("--oracle", "stress");
        require("STRESS".equals(pgOptions.oracle.get(0).name()),
                "Expected --oracle stress to select the STRESS oracle");
    }

    private static void verifiesStressTopologyParsing() throws Exception {
        PostgreSQLOptions pgOptions = parseOptions("--oracle", "stress", "--stress-topology", "shared");
        Field topologyField = PostgreSQLOptions.class.getDeclaredField("stressTopology");
        topologyField.setAccessible(true);
        Object topologyValue = topologyField.get(pgOptions);
        require(topologyValue != null, "Expected stressTopology to be initialized");
        require("SHARED".equals(topologyValue.toString()),
                "Expected --stress-topology shared to select SHARED");
    }

    private static void verifiesStressThreadsPerDbParsing() {
        PostgreSQLOptions pgOptions = parseOptions("--oracle", "stress", "--stress-threads-per-db", "4");
        require(pgOptions.getStressThreadsPerDb() == 4,
                "Expected --stress-threads-per-db 4 to be parsed");
    }

    private static void verifiesEffectiveStressThreadsPerDbFallbacks() {
        PostgreSQLOptions sharedOptions = parseOptions("--oracle", "stress", "--stress-topology", "shared");
        require(sharedOptions.getEffectiveStressThreadsPerDb(8) == 8,
                "Expected shared fallback to use all threads");

        PostgreSQLOptions isolatedOptions = parseOptions("--oracle", "stress", "--stress-topology", "isolated");
        require(isolatedOptions.getEffectiveStressThreadsPerDb(8) == 1,
                "Expected isolated fallback to use one thread per database");
    }

    private static void verifiesEffectiveStressThreadsPerDbNormalization() {
        PostgreSQLOptions groupedOptions = parseOptions("--oracle", "stress", "--stress-threads-per-db", "2");
        require(groupedOptions.getEffectiveStressThreadsPerDb(4) == 2,
                "Expected grouped value 2 to remain unchanged");

        PostgreSQLOptions oversizedOptions = parseOptions("--oracle", "stress", "--stress-threads-per-db", "99");
        require(oversizedOptions.getEffectiveStressThreadsPerDb(4) == 4,
                "Expected oversized grouped value to clamp to numThreads");

        PostgreSQLOptions invalidOptions = parseOptions("--oracle", "stress", "--stress-threads-per-db", "0");
        require(invalidOptions.getEffectiveStressThreadsPerDb(4) == 1,
                "Expected non-positive grouped value to normalize to one thread per database");
    }

    private static PostgreSQLOptions parseOptions(String... postgresqlArgs) {
        MainOptions options = new MainOptions();
        DBMSExecutorFactory<?> executorFactory = new DBMSExecutorFactory<>(new dbradar.postgresql.PostgreSQLProvider(),
                options);
        JCommander.newBuilder()
                .addObject(options)
                .addCommand("postgresql", executorFactory.getCommand())
                .build()
                .parse(concat(new String[]{"postgresql"}, postgresqlArgs));
        return (PostgreSQLOptions) executorFactory.getCommand();
    }

    private static String[] concat(String[] left, String[] right) {
        String[] result = new String[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
