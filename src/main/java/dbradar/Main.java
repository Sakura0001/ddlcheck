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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.JCommander.Builder;
import dbradar.cockroachdb.CockroachDBProvider;
import dbradar.mysql.MySQLOptions;
import dbradar.mysql.MySQLProvider;
import dbradar.postgresql.PostgreSQLProvider;
import dbradar.sqlite3.SQLite3Provider;
import dbradar.tidb.TiDBProvider;

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
        nrQueries.set(0);
        nrDatabases.set(0);
        nrSuccessfulActions.set(0);
        nrUnsuccessfulActions.set(0);
        threadsShutdown.set(0);
        StateLogger.resetInitializedProviders();

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
        DBMSExecutorFactory<?> executorFactory = nameToProvider.get(jc.getParsedCommand());
        boolean roundBasedStress = isRoundBasedMySQLStress(executorFactory.getCommand());
        if (!roundBasedStress && options.getTotalNumberTries() <= 0) {
            System.err.printf("Invalid --num-tries value: %d. It must be greater than 0.%n",
                    options.getTotalNumberTries());
            return options.getErrorExitCode();
        }
        final int taskCount = roundBasedStress ? options.getNumberConcurrentThreads() : options.getTotalNumberTries();
        if (taskCount <= 0) {
            System.err.printf("Invalid task count: %d. It must be greater than 0.%n", taskCount);
            return options.getErrorExitCode();
        }

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

        for (int i = 0; i < taskCount; i++) {
            final String databaseName = options.getDatabasePrefix() + i;
            final long seed;
            if (options.getRandomSeed() == -1) {
                seed = System.currentTimeMillis() + i;
            } else {
                seed = options.getRandomSeed() + i;
            }
            execService.execute(new Runnable() {

                @Override
                public void run() {
                    Thread.currentThread().setName(databaseName);
                    runThread(databaseName);
                }

                private void runThread(final String databaseName) {
                    Randomly r = new Randomly(seed);
                    try {
                        int maxNrDbs = roundBasedStress ? 1 : options.getMaxGeneratedDatabases();
                        // run without a limit if maxNrDbs == -1
                        for (int i = 0; i < maxNrDbs || maxNrDbs == -1; i++) {
                            Boolean continueRunning = run(options, execService, executorFactory, r, databaseName);
                            if (!continueRunning) {
                                someOneFails.set(true);
                                break;
                            }
                        }
                    } finally {
                        threadsShutdown.addAndGet(1);
                        if (threadsShutdown.get() == taskCount) {
                            execService.shutdown();
                        }
                    }
                }

                private boolean run(MainOptions options, ExecutorService execService,
                                    DBMSExecutorFactory<?> executorFactory, Randomly r, final String databaseName) {
                    DBMSExecutor executor = executorFactory.getDBMSExecutor(databaseName, r);
                    executor.setSeqCounterList(seqCounterList);
                    try {
                        executor.run();
                        return true;
                    } catch (IgnoreMeException e) {
                        return true;
                    } catch (Throwable reduce) {
                        reduce.printStackTrace();
                        executor.getStateToReproduce().exception = reduce.getMessage();
                        executor.getLogger().logFileWriter = null;
                        executor.getLogger().logException(reduce, executor.getStateToReproduce());
                        return false;
                    } finally {
                        try {
                            if (options.logEachSelect()) {
                                if (executor.getLogger().currentFileWriter != null) {
                                    executor.getLogger().currentFileWriter.close();
                                }
                                executor.getLogger().currentFileWriter = null;
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        try {
            if (options.getTimeoutSeconds() == -1) {
                execService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            } else {
                execService.awaitTermination(options.getTimeoutSeconds(), TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            execService.shutdownNow();
        }

        return someOneFails.get() ? options.getErrorExitCode() : 0;
    }

    private static boolean isRoundBasedMySQLStress(DBMSSpecificOptions command) {
        return command instanceof MySQLOptions && ((MySQLOptions) command).useStress();
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
        providers.add(new CockroachDBProvider());
        providers.add(new MySQLProvider());
        providers.add(new PostgreSQLProvider());
        providers.add(new SQLite3Provider());
        providers.add(new TiDBProvider());

        return providers;
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
