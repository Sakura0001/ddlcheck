package dbradar;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DBMSExecutor {

    private final DatabaseProvider provider;
    private final MainOptions options;
    private final DBMSSpecificOptions command;
    private final String databaseName;
    private final String logName;
    private StateLogger logger;
    private StateToReproduce stateToRepro;
    private final Randomly r;
    private final boolean createDatabaseOnRun;
    private final String generatedObjectNamePrefix;

    private List<Map<Integer, Map<Integer, Integer>>> seqCounterList = null;

    public DBMSExecutor(DatabaseProvider provider, MainOptions options, DBMSSpecificOptions dbmsSpecificOptions,
                        String databaseName, String logName, Randomly r, boolean createDatabaseOnRun,
                        String generatedObjectNamePrefix) {
        this.provider = provider;
        this.options = options;
        this.databaseName = databaseName;
        this.logName = logName;
        this.command = dbmsSpecificOptions;
        this.r = r;
        this.createDatabaseOnRun = createDatabaseOnRun;
        this.generatedObjectNamePrefix = generatedObjectNamePrefix == null ? "" : generatedObjectNamePrefix;
    }

    private GlobalState createGlobalState() {
        try {
            return provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public DBMSSpecificOptions getCommand() {
        return command;
    }

    public void testConnection() throws Exception {
        GlobalState state = getInitializedGlobalState(options.getRandomSeed());
        try (DatabaseConnection con = provider.createDatabase(state)) {
            return;
        }
    }

    public void prepareDatabase() throws Exception {
        GlobalState state = getInitializedGlobalState(r.getSeed());
        try (DatabaseConnection con = provider.createDatabase(state)) {
            return;
        }
    }

    public void run() throws Exception {
        GlobalState state = createGlobalState();
        stateToRepro = provider.getStateToReproduce(databaseName);
        stateToRepro.seedValue = r.getSeed();
        state.setState(stateToRepro);
        logger = new StateLogger(logName, provider, options);
        state.setRandomly(r);
        state.setDatabaseName(databaseName);
        state.setGeneratedObjectNamePrefix(generatedObjectNamePrefix);
        state.setMainOptions(options);
        state.setDbmsSpecificOptions(command);
        try (DatabaseConnection con = createDatabaseOnRun ? provider.createDatabase(state) : provider.createConnection(state)) {
            QueryManager manager = new QueryManager(state);
            try {
                stateToRepro.databaseVersion = con.getDatabaseVersion();
            } catch (Exception e) {
                // ignore
            }
            state.setConnection(con);
            state.setStateLogger(logger);
            state.setManager(manager);
            if (seqCounterList != null) {
                state.setSeqCounterList(seqCounterList);
            }
            if (options.logEachSelect()) {
                logger.writeCurrent(state.getState());
            }
            Reproducer reproducer = provider.generateAndTestDatabase(state);
            try {
                logger.getCurrentFileWriter().close();
                logger.currentFileWriter = null;
            } catch (IOException e) {
                throw new AssertionError(e);
            }

            if (reproducer != null) {
                throw new AssertionError("Found a potential bug");
            }
        }
    }

    private GlobalState getInitializedGlobalState(long seed) {
        GlobalState state = createGlobalState();
        stateToRepro = provider.getStateToReproduce(databaseName);
        stateToRepro.seedValue = seed;
        state.setState(stateToRepro);
        logger = new StateLogger(logName, provider, options);
        Randomly r = new Randomly(seed);
        state.setRandomly(r);
        state.setDatabaseName(databaseName);
        state.setGeneratedObjectNamePrefix(generatedObjectNamePrefix);
        state.setMainOptions(options);
        state.setDbmsSpecificOptions(command);
        return state;
    }

    public StateLogger getLogger() {
        return logger;
    }

    public StateToReproduce getStateToReproduce() {
        return stateToRepro;
    }

    public void setSeqCounterList(List<Map<Integer, Map<Integer, Integer>>> seqCounterList) {
        this.seqCounterList = seqCounterList;
    }
}
