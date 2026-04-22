package dbradar;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.JCommander.Builder;
import dbradar.postgresql.PostgreSQLOptions;
import dbradar.postgresql.PostgreSQLProvider;

public final class Main {

    public static final File LOG_DIRECTORY = new File("logs");
    public static volatile AtomicLong nrQueries = new AtomicLong();
    public static volatile AtomicLong nrDatabases = new AtomicLong();
    public static volatile AtomicLong nrSuccessfulActions = new AtomicLong();
    public static volatile AtomicLong nrUnsuccessfulActions = new AtomicLong();
    public static volatile AtomicLong threadsShutdown = new AtomicLong();
    static boolean progressMonitorStarted;

    static {
        System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "ERROR");
        if (!LOG_DIRECTORY.exists()) {
            LOG_DIRECTORY.mkdir();
        }
    }

    private Main() {
    }

    public static void main(String[] args) {
        System.exit(executeMain(args));
    }

    public static int executeMain(String... args) throws AssertionError {
        List<DatabaseProvider> providers = getDBMSProviders();
        Map<String, DBMSExecutorFactory<?>> nameToProvider = new HashMap<>();
        MainOptions options = new MainOptions();
        Builder commandBuilder = JCommander.newBuilder().addObject(options);
        for (DatabaseProvider provider : providers) {
            String name = provider.getDBMSName();
            DBMSExecutorFactory<?> executorFactory = new DBMSExecutorFactory<>(provider, options);
            commandBuilder = commandBuilder.addCommand(name, executorFactory.getCommand());
            nameToProvider.put(name, executorFactory);
        }
        JCommander jc = commandBuilder.programName("ddlcheck").build();
        jc.parse(args);

        if (jc.getParsedCommand() == null || options.isHelp()) {
            jc.usage();
            return options.getErrorExitCode();
        }

        resetRuntimeState();
        Randomly.initialize(options);
        if (options.printProgressInformation()) {
            startProgressMonitor();
            if (options.printProgressSummary()) {
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

                    @Override
                    public void run() {
                        System.out.println("Overall execution statistics");
                        System.out.println("============================");
                        System.out.println(formatInteger(nrQueries.get()) + " queries");
                        System.out.println(formatInteger(nrDatabases.get()) + " databases");
                        System.out.println(
                                formatInteger(nrSuccessfulActions.get()) + " successfully-executed statements");
                        System.out.println(
                                formatInteger(nrUnsuccessfulActions.get()) + " unsuccessfuly-executed statements");
                    }

                    private String formatInteger(long intValue) {
                        if (intValue > 1000) {
                            return String.format("%,9dk", intValue / 1000);
                        } else {
                            return String.format("%,10d", intValue);
                        }
                    }
                }));
            }
        }

        ExecutorService execService = Executors.newFixedThreadPool(options.getNumberConcurrentThreads());
        DBMSExecutorFactory<?> executorFactory = nameToProvider.get(jc.getParsedCommand());
        PostgreSQLOptions postgreSQLOptions = executorFactory.getCommand() instanceof PostgreSQLOptions
                ? (PostgreSQLOptions) executorFactory.getCommand()
                : null;

        if (options.performConnectionTest()) {
            try {
                executorFactory.getDBMSExecutor(options.getDatabasePrefix() + "connectiontest", new Randomly())
                        .testConnection();
            } catch (Exception e) {
                System.err.println(
                        "dbradar failed creating a test database, indicating that dbradar might have failed connecting to the DBMS. In order to change the username, password, host and port, you can use the --username, --password, --host and --port options.\n\n");
                e.printStackTrace();
                return options.getErrorExitCode();
            }
        }
        final AtomicBoolean someOneFails = new AtomicBoolean(false);
        final List<Map<Integer, Map<Integer, Integer>>> seqCounterList = new ArrayList<>();
        int submittedTaskCount = submitExecutionTasks(options, execService, executorFactory, postgreSQLOptions,
                someOneFails, seqCounterList);
        try {
            if (options.getTimeoutSeconds() == -1) {
                execService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            } else {
                execService.awaitTermination(options.getTimeoutSeconds(), TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return someOneFails.get() ? options.getErrorExitCode() : 0;
    }

    /**
     * To register a new provider, it is necessary to implement the DatabaseProvider
     * interface and add an additional configuration file, see
     * https://docs.oracle.com/javase/9/docs/api/java/util/ServiceLoader.html.
     * Currently, we use an @AutoService annotation to create the configuration file
     * automatically. This allows randgen to pick up providers in other JARs on the
     * classpath.
     *
     * @return The list of service providers on the classpath
     */
    static List<DatabaseProvider> getDBMSProviders() {
        List<DatabaseProvider> providers = new ArrayList<>();
        providers.add(new PostgreSQLProvider());

        return providers;
    }

    private static int submitExecutionTasks(MainOptions options, ExecutorService execService,
                                            DBMSExecutorFactory<?> executorFactory,
                                            PostgreSQLOptions postgreSQLOptions, AtomicBoolean someOneFails,
                                            List<Map<Integer, Map<Integer, Integer>>> seqCounterList) {
        if (postgreSQLOptions != null && postgreSQLOptions.useStress()) {
            if (postgreSQLOptions.getStressTopology() == PostgreSQLOptions.PostgreSQLStressTopology.SHARED) {
                return submitSharedStressTasks(options, execService, executorFactory, someOneFails, seqCounterList);
            }
            return submitIsolatedStressTasks(options, execService, executorFactory, someOneFails, seqCounterList);
        }
        return submitNonStressTasks(options, execService, executorFactory, someOneFails, seqCounterList);
    }

    private static int submitNonStressTasks(MainOptions options, ExecutorService execService,
                                            DBMSExecutorFactory<?> executorFactory, AtomicBoolean someOneFails,
                                            List<Map<Integer, Map<Integer, Integer>>> seqCounterList) {
        int submittedTaskCount = options.getTotalNumberTries();
        for (int taskIndex = 0; taskIndex < submittedTaskCount; taskIndex++) {
            final int workerIndex = taskIndex;
            execService.execute(() -> {
                String databaseName = options.getDatabasePrefix() + workerIndex;
                Thread.currentThread().setName(databaseName);
                try {
                    int maxNrDbs = options.getMaxGeneratedDatabases();
                    for (int round = 0; round < maxNrDbs || maxNrDbs == -1; round++) {
                        Randomly randomly = new Randomly(resolveSeed(options, workerIndex, round));
                        DBMSExecutor executor = executorFactory.getDBMSExecutor(databaseName, randomly);
                        if (!runExecutor(options, executor, seqCounterList)) {
                            someOneFails.set(true);
                            break;
                        }
                    }
                } finally {
                    finishWorker(execService, submittedTaskCount);
                }
            });
        }
        return submittedTaskCount;
    }

    private static int submitIsolatedStressTasks(MainOptions options, ExecutorService execService,
                                                 DBMSExecutorFactory<?> executorFactory, AtomicBoolean someOneFails,
                                                 List<Map<Integer, Map<Integer, Integer>>> seqCounterList) {
        int submittedTaskCount = options.getNumberConcurrentThreads();
        for (int taskIndex = 0; taskIndex < submittedTaskCount; taskIndex++) {
            final int workerIndex = taskIndex;
            execService.execute(() -> {
                String workerName = options.getDatabasePrefix() + workerIndex;
                Thread.currentThread().setName(workerName);
                try {
                    int maxNrDbs = options.getMaxGeneratedDatabases();
                    for (int round = 0; round < maxNrDbs || maxNrDbs == -1; round++) {
                        String databaseName = buildIsolatedDatabaseName(options.getDatabasePrefix(), workerIndex, round);
                        Randomly randomly = new Randomly(resolveSeed(options, workerIndex, round));
                        DBMSExecutor executor = executorFactory.getDBMSExecutor(databaseName, databaseName, randomly,
                                true, "");
                        if (!runExecutor(options, executor, seqCounterList)) {
                            someOneFails.set(true);
                            break;
                        }
                    }
                } finally {
                    finishWorker(execService, submittedTaskCount);
                }
            });
        }
        return submittedTaskCount;
    }

    private static int submitSharedStressTasks(MainOptions options, ExecutorService execService,
                                               DBMSExecutorFactory<?> executorFactory, AtomicBoolean someOneFails,
                                               List<Map<Integer, Map<Integer, Integer>>> seqCounterList) {
        int submittedTaskCount = options.getNumberConcurrentThreads();
        CyclicBarrier prepareBarrier = new CyclicBarrier(submittedTaskCount);
        CyclicBarrier finishBarrier = new CyclicBarrier(submittedTaskCount);
        AtomicReference<Throwable> sharedFailure = new AtomicReference<>();

        for (int taskIndex = 0; taskIndex < submittedTaskCount; taskIndex++) {
            final int workerIndex = taskIndex;
            execService.execute(() -> {
                String workerName = options.getDatabasePrefix() + "shared-thread" + workerIndex;
                Thread.currentThread().setName(workerName);
                try {
                    int maxNrDbs = options.getMaxGeneratedDatabases();
                    for (int round = 0; round < maxNrDbs || maxNrDbs == -1; round++) {
                        if (sharedFailure.get() != null) {
                            someOneFails.set(true);
                            break;
                        }
                        String databaseName = options.getDatabasePrefix() + round;
                        if (workerIndex == 0) {
                            DBMSExecutor prepareExecutor = executorFactory.getDBMSExecutor(databaseName,
                                    databaseName + "-prepare", new Randomly(resolveSeed(options, workerIndex, round)),
                                    true, buildSharedObjectPrefix(workerIndex));
                            try {
                                prepareExecutor.prepareDatabase();
                            } catch (Throwable throwable) {
                                sharedFailure.compareAndSet(null, throwable);
                                prepareBarrier.reset();
                            }
                        }
                        if (!awaitBarrier(prepareBarrier, sharedFailure.get())) {
                            someOneFails.set(true);
                            break;
                        }
                        if (sharedFailure.get() != null) {
                            someOneFails.set(true);
                            finishBarrier.reset();
                            break;
                        }

                        DBMSExecutor executor = executorFactory.getDBMSExecutor(databaseName,
                                databaseName + "-thread" + workerIndex,
                                new Randomly(resolveSeed(options, workerIndex, round)),
                                false, buildSharedObjectPrefix(workerIndex));
                        boolean succeeded = runExecutor(options, executor, seqCounterList);
                        if (!succeeded) {
                            someOneFails.set(true);
                            sharedFailure.compareAndSet(null, new AssertionError(
                                    "Shared stress worker " + workerIndex + " failed on " + databaseName));
                            finishBarrier.reset();
                            break;
                        }
                        if (!awaitBarrier(finishBarrier, sharedFailure.get())) {
                            someOneFails.set(true);
                            break;
                        }
                    }
                } finally {
                    finishWorker(execService, submittedTaskCount);
                }
            });
        }
        return submittedTaskCount;
    }

    private static boolean runExecutor(MainOptions options, DBMSExecutor executor,
                                       List<Map<Integer, Map<Integer, Integer>>> seqCounterList) {
        executor.setSeqCounterList(seqCounterList);
        try {
            executor.run();
            return true;
        } catch (IgnoreMeException e) {
            return true;
        } catch (Throwable reduce) {
            reduce.printStackTrace();
            if (executor.getStateToReproduce() != null) {
                executor.getStateToReproduce().exception = reduce.getMessage();
            }
            if (executor.getLogger() != null) {
                executor.getLogger().logFileWriter = null;
                executor.getLogger().logException(reduce, executor.getStateToReproduce());
            }
            return false;
        } finally {
            closeCurrentLog(options, executor);
        }
    }

    private static void closeCurrentLog(MainOptions options, DBMSExecutor executor) {
        try {
            if (options.logEachSelect() && executor.getLogger() != null) {
                if (executor.getLogger().currentFileWriter != null) {
                    executor.getLogger().currentFileWriter.close();
                }
                executor.getLogger().currentFileWriter = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void finishWorker(ExecutorService execService, int submittedTaskCount) {
        threadsShutdown.addAndGet(1);
        if (threadsShutdown.get() == submittedTaskCount) {
            execService.shutdown();
        }
    }

    private static boolean awaitBarrier(CyclicBarrier barrier, Throwable priorFailure) {
        if (priorFailure != null) {
            return false;
        }
        try {
            barrier.await();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (BrokenBarrierException e) {
            return false;
        }
    }

    private static long resolveSeed(MainOptions options, int workerIndex, int round) {
        long seedBase = options.getRandomSeed() == -1 ? System.currentTimeMillis() : options.getRandomSeed();
        return seedBase + (workerIndex * 1000L) + round;
    }

    private static String buildIsolatedDatabaseName(String databasePrefix, int workerIndex, int round) {
        if (round == 0) {
            return databasePrefix + workerIndex;
        }
        return databasePrefix + workerIndex + "_" + round;
    }

    private static String buildSharedObjectPrefix(int workerIndex) {
        return "thr" + workerIndex + "_";
    }

    private static void resetRuntimeState() {
        nrQueries.set(0);
        nrDatabases.set(0);
        nrSuccessfulActions.set(0);
        nrUnsuccessfulActions.set(0);
        threadsShutdown.set(0);
    }

    private static synchronized void startProgressMonitor() {
        if (progressMonitorStarted) {
            /*
             * it might be already started if, for example, the main method is called
             * multiple times in a test (see
             * https://github.com/randgen.sqlancer/randgen.sqlancer/issues/90).
             */
            return;
        } else {
            progressMonitorStarted = true;
        }
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new Runnable() {

            private long timeMillis = System.currentTimeMillis();
            private long lastNrQueries;
            private long lastNrDbs;

            {
                timeMillis = System.currentTimeMillis();
            }

            @Override
            public void run() {
                long elapsedTimeMillis = System.currentTimeMillis() - timeMillis;
                long currentNrQueries = nrQueries.get();
                long nrCurrentQueries = currentNrQueries - lastNrQueries;
                double throughput = nrCurrentQueries / (elapsedTimeMillis / 1000d);
                long currentNrDbs = nrDatabases.get();
                long nrCurrentDbs = currentNrDbs - lastNrDbs;
                double throughputDbs = nrCurrentDbs / (elapsedTimeMillis / 1000d);
                long successfulStatementsRatio = (long) (100.0 * nrSuccessfulActions.get()
                        / (nrSuccessfulActions.get() + nrUnsuccessfulActions.get()));
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                System.out.println(String.format(
                        "[%s] Executed %d queries (%d queries/s; %.2f/s dbs, successful statements: %2d%%). Threads shut down: %d.",
                        dateFormat.format(date), currentNrQueries, (int) throughput, throughputDbs,
                        successfulStatementsRatio, threadsShutdown.get()));
                timeMillis = System.currentTimeMillis();
                lastNrQueries = currentNrQueries;
                lastNrDbs = currentNrDbs;
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

}
