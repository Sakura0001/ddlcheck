package dbradar.ddlCheck;

import dbradar.IgnoreMeException;
import dbradar.Main;
import dbradar.MainOptions;
import dbradar.Randomly;
import dbradar.common.query.generator.QueryGenerator;
import dbradar.postgresql.PostgreSQLGlobalState;
import dbradar.postgresql.PostgreSQLKeyFunctionManager;
import dbradar.postgresql.PostgreSQLOptions;
import dbradar.postgresql.PostgreSQLSchema;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class PostgreSQLPartitionWorkload {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 5432;
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "Taurus_123";
    private static final int THREADS = 2;

    private PostgreSQLPartitionWorkload() {
    }

    public static void main(String[] args) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            final int threadId = i;
            tasks.add(() -> {
                runThread(threadId);
                return null;
            });
        }
        List<Future<Void>> futures = executorService.invokeAll(tasks);
        for (Future<Void> future : futures) {
            future.get();
        }
        executorService.shutdownNow();
    }

    private static void runThread(int threadId) throws Exception {
        PostgreSQLGlobalState state = createState("partition_workload_" + threadId);
        File logDir = new File(Main.LOG_DIRECTORY, "postgresql");
        if (!logDir.exists() && !logDir.mkdirs()) {
            throw new AssertionError("Failed to create log directory: " + logDir);
        }
        File logFile = new File(logDir, "partition_workload_" + threadId + "-cur.log");

        try (Writer writer = new FileWriter(logFile, false)) {
            writer.write(String.format("-- Partition workload thread %d%n", threadId));

            long seedBase = 6000L + threadId * 100;
            executeGenerated(state, writer, "create_partitioned_table", seedBase + 1);
            state.updateSchema();
            if (hasPartitionedTableForNewPartition(state.getSchema())) {
                executeGenerated(state, writer, "create_table_partition", seedBase + 2);
                state.updateSchema();
            }
            if (hasPartitionedTableWithPartitions(state.getSchema())) {
                executeGenerated(state, writer, "alter_table_detach_partition", seedBase + 3);
                state.updateSchema();
            }
            if (hasDetachedPartitionCandidate(state.getSchema())) {
                executeGenerated(state, writer, "alter_table_attach_partition", seedBase + 4);
                state.updateSchema();
            }

            for (int i = 0; i < 6; i++) {
                String root = chooseRandomRoot(state, seedBase + 10 + i);
                executeGenerated(state, writer, root, seedBase + 20 + i);
                state.updateSchema();
            }

            exerciseListPartitionScenario(state, writer, threadId, seedBase + 100);
            exerciseHashPartitionScenario(state, writer, threadId, seedBase + 200);
            exerciseMultiColumnRangeScenario(state, writer, threadId, seedBase + 300);
        } finally {
            if (state.getConnection() != null) {
                state.getConnection().close();
            }
        }
    }

    private static PostgreSQLGlobalState createState(String databaseName) throws Exception {
        dropDatabaseIfExists(databaseName);
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

    private static void dropDatabaseIfExists(String databaseName) throws Exception {
        String adminUrl = String.format("jdbc:postgresql://%s:%d/postgres", HOST, PORT);
        try (Connection connection = DriverManager.getConnection(adminUrl, USERNAME, PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + databaseName + " WITH (FORCE)");
        }
    }

    private static void executeGenerated(PostgreSQLGlobalState state, Writer writer, String root, long seed) throws Exception {
        String sql = generateQuery(state, root, seed);
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            writer.write("-- ERROR " + e.getMessage() + System.lineSeparator());
            writer.write(sql + ";" + System.lineSeparator());
            writer.flush();
            throw e;
        }
        writer.write(sql + ";" + System.lineSeparator());
        writer.flush();
    }

    private static String generateQuery(PostgreSQLGlobalState state, String root, long seed) {
        state.setRandomly(new Randomly(seed));
        QueryGenerator generator = new QueryGenerator(state, state.getGrammar(), root, new PostgreSQLKeyFunctionManager(state));
        return generator.getRandomQuery().toQueryString();
    }

    private static String chooseRandomRoot(PostgreSQLGlobalState state, long seed) throws Exception {
        PostgreSQLSchema schema = state.getSchema();
        List<String> candidates = new ArrayList<>();
        candidates.add("create_partitioned_table");
        if (hasPartitionedTableForNewPartition(schema)) {
            candidates.add("create_table_partition");
        }
        if (hasPartitionedTableWithPartitions(schema)) {
            candidates.add("alter_table_detach_partition");
        }
        if (hasDetachedPartitionCandidate(schema)) {
            candidates.add("alter_table_attach_partition");
        }

        state.setRandomly(new Randomly(seed));
        return Randomly.fromList(candidates);
    }

    private static boolean hasPartitionedTableForNewPartition(PostgreSQLSchema schema) {
        try {
            schema.getRandomPartitionedTableForPartitionCreation();
            return true;
        } catch (IgnoreMeException ignored) {
            return false;
        }
    }

    private static boolean hasPartitionedTableWithPartitions(PostgreSQLSchema schema) {
        try {
            schema.getRandomPartitionedTableWithPartitions();
            return true;
        } catch (IgnoreMeException ignored) {
            return false;
        }
    }

    private static boolean hasDetachedPartitionCandidate(PostgreSQLSchema schema) {
        try {
            PostgreSQLSchema.PostgreSQLTable parent = schema.getRandomPartitionedTableWithoutDefaultPartition();
            schema.getRandomDetachedPartitionCandidate(parent);
            return true;
        } catch (IgnoreMeException ignored) {
            return false;
        }
    }

    private static void exerciseListPartitionScenario(PostgreSQLGlobalState state, Writer writer, int threadId, long seed)
            throws Exception {
        writer.write(String.format("-- LIST scenario thread %d%n", threadId));
        state.setRandomly(new Randomly(seed));
        int explicitPartitionCount = Randomly.getNotCachedInteger(1, 4);
        String parent = "list_parent_" + threadId;
        executeSql(state, writer,
                String.format("CREATE TABLE %s (partition_key1 INT NOT NULL, payload TEXT) PARTITION BY LIST (partition_key1)",
                        parent));

        int nextValue = 1;
        List<String> childPartitions = new ArrayList<>();
        for (int i = 0; i < explicitPartitionCount; i++) {
            int groupSize = Randomly.getNotCachedInteger(1, 4);
            List<Integer> values = new ArrayList<>();
            for (int j = 0; j < groupSize; j++) {
                values.add(nextValue++);
            }
            String child = parent + "_p" + i;
            childPartitions.add(child);
            executeSql(state, writer,
                    String.format("CREATE TABLE %s PARTITION OF %s FOR VALUES IN (%s)",
                            child, parent, joinIntegers(values)));
        }

        String defaultPartition = parent + "_default";
        executeSql(state, writer, String.format("CREATE TABLE %s PARTITION OF %s DEFAULT", defaultPartition, parent));
        executeSql(state, writer,
                String.format("INSERT INTO %s (partition_key1, payload) VALUES (%d, 'explicit'), (%d, 'default')",
                        parent, 1, nextValue + 10));

        requireSingleValue(state, String.format("SELECT count(*) FROM %s", childPartitions.get(0)), 1);
        requireSingleValue(state, String.format("SELECT count(*) FROM %s", defaultPartition), 1);
    }

    private static void exerciseHashPartitionScenario(PostgreSQLGlobalState state, Writer writer, int threadId, long seed)
            throws Exception {
        writer.write(String.format("-- HASH scenario thread %d%n", threadId));
        state.setRandomly(new Randomly(seed));
        int keyCount = Randomly.fromOptions(1, 2);
        int modulus = Randomly.getNotCachedInteger(2, 5);
        String parent = "hash_parent_" + threadId;
        String keyColumns = keyCount == 1 ? "partition_key1" : "partition_key1, partition_key2";
        String columnDefinition = keyCount == 1
                ? "partition_key1 INT NOT NULL, payload TEXT"
                : "partition_key1 INT NOT NULL, partition_key2 INT NOT NULL, payload TEXT";
        executeSql(state, writer,
                String.format("CREATE TABLE %s (%s) PARTITION BY HASH (%s)", parent, columnDefinition, keyColumns));

        List<String> childPartitions = new ArrayList<>();
        for (int remainder = 0; remainder < modulus; remainder++) {
            String child = parent + "_p" + remainder;
            childPartitions.add(child);
            executeSql(state, writer,
                    String.format("CREATE TABLE %s PARTITION OF %s FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
                            child, parent, modulus, remainder));
        }

        if (keyCount == 1) {
            executeSql(state, writer,
                    String.format("INSERT INTO %s (partition_key1, payload) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
                            parent));
        } else {
            executeSql(state, writer,
                    String.format(
                            "INSERT INTO %s (partition_key1, partition_key2, payload) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd')",
                            parent));
        }

        requireSingleValue(state, String.format("SELECT count(*) FROM %s", parent), 4);
        int childRowCount = 0;
        for (String child : childPartitions) {
            childRowCount += querySingleValue(state, String.format("SELECT count(*) FROM %s", child));
        }
        if (childRowCount != 4) {
            throw new AssertionError("Hash partitions did not receive all rows, got " + childRowCount);
        }
    }

    private static void exerciseMultiColumnRangeScenario(PostgreSQLGlobalState state, Writer writer, int threadId,
                                                         long seed) throws Exception {
        writer.write(String.format("-- RANGE multi-column scenario thread %d%n", threadId));
        state.setRandomly(new Randomly(seed));
        int explicitPartitionCount = Randomly.getNotCachedInteger(1, 4);
        String parent = "range_parent_" + threadId;
        executeSql(state, writer,
                String.format(
                        "CREATE TABLE %s (partition_key1 INT NOT NULL, partition_key2 INT NOT NULL, payload TEXT) PARTITION BY RANGE (partition_key1, partition_key2)",
                        parent));

        List<String> childPartitions = new ArrayList<>();
        int base = 0;
        for (int i = 0; i < explicitPartitionCount; i++) {
            String child = parent + "_p" + i;
            childPartitions.add(child);
            executeSql(state, writer,
                    String.format(
                            "CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d, %d) TO (%d, %d)",
                            child, parent, base, base, base + 100, base + 100));
            base += 100;
        }

        String defaultPartition = parent + "_default";
        executeSql(state, writer, String.format("CREATE TABLE %s PARTITION OF %s DEFAULT", defaultPartition, parent));
        executeSql(state, writer,
                String.format(
                        "INSERT INTO %s (partition_key1, partition_key2, payload) VALUES (10, 10, 'explicit'), (%d, %d, 'default')",
                        parent, base + 10, base + 10));

        requireSingleValue(state, String.format("SELECT count(*) FROM %s", childPartitions.get(0)), 1);
        requireSingleValue(state, String.format("SELECT count(*) FROM %s", defaultPartition), 1);
    }

    private static void executeSql(PostgreSQLGlobalState state, Writer writer, String sql) throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            writer.write("-- ERROR " + e.getMessage() + System.lineSeparator());
            writer.write(sql + ";" + System.lineSeparator());
            writer.flush();
            throw e;
        }
        writer.write(sql + ";" + System.lineSeparator());
        writer.flush();
        state.updateSchema();
    }

    private static int querySingleValue(PostgreSQLGlobalState state, String sql) throws Exception {
        try (Statement statement = state.getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                throw new AssertionError("No result for query: " + sql);
            }
            return resultSet.getInt(1);
        }
    }

    private static void requireSingleValue(PostgreSQLGlobalState state, String sql, int expected) throws Exception {
        int actual = querySingleValue(state, sql);
        if (actual != expected) {
            throw new AssertionError(String.format("Expected %d for [%s] but got %d", expected, sql, actual));
        }
    }

    private static String joinIntegers(List<Integer> values) {
        return values.stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(", "));
    }
}
