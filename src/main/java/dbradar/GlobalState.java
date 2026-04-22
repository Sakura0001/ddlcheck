package dbradar;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import grammar.Grammar;
import grammar.GrammarParser;
import dbradar.common.query.DBRadarResultSet;
import dbradar.common.query.Query;
import dbradar.common.schema.AbstractSchema;

public abstract class GlobalState {

    private DatabaseConnection databaseConnection;
    private MainOptions options;
    private DBMSSpecificOptions dbmsSpecificOptions;
    private Randomly r;
    private StateLogger logger;
    private StateToReproduce state;
    private QueryManager manager;
    private String databaseName;
    private AbstractSchema<?, ?, ?> schema;

    private String grammarPath = null;
    private Grammar grammar = null;
    private String currentOracle = null;
    private List<Map<Integer, Map<Integer, Integer>>> seqCounterList = null;
    private int currentSeqCounter = 0;

    private Map<String, String> regexErrorTypes;

    public void setConnection(DatabaseConnection con) {
        this.databaseConnection = con;
    }

    public DatabaseConnection getConnection() {
        return databaseConnection;
    }

    public MainOptions getOptions() {
        return options;
    }

    public void setMainOptions(MainOptions options) {
        this.options = options;
    }

    public void setDbmsSpecificOptions(DBMSSpecificOptions dbmsSpecificOptions) {
        this.dbmsSpecificOptions = dbmsSpecificOptions;
    }

    public DBMSSpecificOptions getDbmsSpecificOptions() {
        return dbmsSpecificOptions;
    }

    public void setRandomly(Randomly r) {
        this.r = r;
    }

    public Randomly getRandomly() {
        return r;
    }

    public void setStateLogger(StateLogger logger) {
        this.logger = logger;
    }

    public StateLogger getLogger() {
        return logger;
    }

    public void setState(StateToReproduce state) {
        this.state = state;
    }

    public StateToReproduce getState() {
        return state;
    }

    public QueryManager getManager() {
        return manager;
    }

    public void setManager(QueryManager manager) {
        this.manager = manager;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public AbstractSchema<?, ?, ?> getSchema() {
        if (schema == null) {
            try {
                updateSchema();
            } catch (Exception e) {
                throw new AssertionError(e.getMessage());
            }
        }
        return schema;
    }

    protected void setSchema(AbstractSchema<?, ?, ?> schema) {
        this.schema = schema;
    }

    public void updateSchema() throws Exception { // jiansen: may only read schema once
//        readSchema();
        setSchema(readSchema());
    }

    protected abstract AbstractSchema<?, ?, ?> readSchema() throws Exception;

    public void setGrammarPath(String grammarPath) {
        this.grammarPath = grammarPath;
        this.grammar = null; // for safely updating the grammar
    }

    public Grammar getGrammar() {
        if (grammar == null) {
            try {
                if (this.getOptions() != null && this.getOptions().getGrammarPath() != null) {
                    grammarPath = this.getOptions().getGrammarPath();
                }
                grammar = loadGrammar(grammarPath);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return grammar;
    }

    public Grammar loadGrammar(String grammarPath) {
        try {
            URL fileURL = GlobalState.class.getClassLoader().getResource(grammarPath);
            if (fileURL == null) {
                throw new RuntimeException(
                        String.format("GlobalState: grammar file %s does not exist", grammarPath));
            }
            String grammarStr = Files.readString(Paths.get(fileURL.toURI()));
            GrammarParser grammarParser = new GrammarParser(grammarStr);
            Grammar grammar = grammarParser.parse();
            if (grammar == null) {
                throw new RuntimeException("GlobalState: Fail to parse grammar");
            }
            return grammar;
        } catch (Exception e) {
            throw new RuntimeException("GlobalState: Fail to parse grammar");
        }
    }

    public abstract String getGeneratorConfigPath();

    public String getCurrentOracle() {
        return currentOracle;
    }

    public void setCurrentOracle(String currentOracle) {
        this.currentOracle = currentOracle;
    }

    private ExecutionTimer executePrologue(Query q) throws Exception {
        boolean logExecutionTime = getOptions().logExecutionTime();
        ExecutionTimer timer = null;
        if (logExecutionTime) {
            timer = new ExecutionTimer().start();
        }
        if (getOptions().printAllStatements()) {
            System.out.println(q.getLogString());
        }
        if (getOptions().logEachSelect()) {
            if (logExecutionTime) {
                getLogger().writeCurrentNoLineBreak(q.getLogString());
            } else {
                getLogger().writeCurrent(q.getLogString());
            }
        }
        return timer;
    }

    protected abstract void executeEpilogue(Query q, boolean success, ExecutionTimer timer) throws Exception;

    public boolean executeStatement(Query q, String... fills) throws Exception {
        ExecutionTimer timer = executePrologue(q);
        boolean success = manager.execute(q, fills);
        executeEpilogue(q, success, timer);
        return success;
    }

    public DBRadarResultSet executeStatementAndGet(Query q, String... fills) throws Exception {
        ExecutionTimer timer = executePrologue(q);
        DBRadarResultSet result = manager.executeAndGet(q, fills);
        boolean success = result != null;
        if (success) {
            result.registerEpilogue(() -> {
                try {
                    executeEpilogue(q, success, timer);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
        }
        return result;
    }

    public void setSeqCounterList(List<Map<Integer, Map<Integer, Integer>>> seqCounterList) {
        this.seqCounterList = seqCounterList;
    }

    public void increaseSeqCounter() {
        this.currentSeqCounter++;
    }

    public void resetSeqCounter() {
        this.currentSeqCounter = 0;
    }

    public Map<Integer, Integer> getSeqCounter(Integer queryRoot) {
        if (seqCounterList != null) {
            if (currentSeqCounter == seqCounterList.size()) {
                seqCounterList.add(new HashMap<>());
            }
            Map<Integer, Map<Integer, Integer>> seqCounter = seqCounterList.get(currentSeqCounter);
            if (!seqCounter.containsKey(queryRoot)) {
                seqCounter.put(queryRoot, new HashMap<>());
            }
            return seqCounter.get(queryRoot);
        } else {
            return null;
        }
    }

    public Map<String, String> getRegexErrorTypes() {
        return regexErrorTypes;
    }

    public void setRegexErrorTypes(Map<String, String> regexErrorTypes) {
        this.regexErrorTypes = regexErrorTypes;
    }

    /**
     * create new sqlConnection and do not influence the data of database
     */
    public SQLConnection createNewConnection(String databaseName) {
        return null;
    }
}
