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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        executorService.invokeAll(tasks);
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
            executeGenerated(state, writer, "create_table_partition", seedBase + 2);
            state.updateSchema();
            executeGenerated(state, writer, "alter_table_detach_partition", seedBase + 3);
            state.updateSchema();
            executeGenerated(state, writer, "alter_table_attach_partition", seedBase + 4);
            state.updateSchema();

            for (int i = 0; i < 6; i++) {
                String root = chooseRandomRoot(state, seedBase + 10 + i);
                executeGenerated(state, writer, root, seedBase + 20 + i);
                state.updateSchema();
            }
        } finally {
            if (state.getConnection() != null) {
                state.getConnection().close();
            }
        }
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
        if (hasPartitionedTableWithoutDefault(schema)) {
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

    private static boolean hasPartitionedTableWithoutDefault(PostgreSQLSchema schema) {
        try {
            schema.getRandomPartitionedTableWithoutDefaultPartition();
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
}
