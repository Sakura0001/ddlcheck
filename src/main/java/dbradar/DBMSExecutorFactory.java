package dbradar;

public class DBMSExecutorFactory<G extends GlobalState> {

    private final DatabaseProvider provider;
    private final MainOptions options;
    private final DBMSSpecificOptions command;

    public DBMSExecutorFactory(DatabaseProvider provider, MainOptions options) {
        this.provider = provider;
        this.options = options;
        this.command = createCommand();
    }

    private DBMSSpecificOptions createCommand() {
        try {
            return provider.getOptionClass().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public DBMSSpecificOptions getCommand() {
        return command;
    }

    public DBMSExecutor getDBMSExecutor(String databaseName, Randomly r) {
        return getDBMSExecutor(databaseName, databaseName, r, true, "");
    }

    public DBMSExecutor getDBMSExecutor(String databaseName, String logName, Randomly r, boolean createDatabaseOnRun,
                                        String generatedObjectNamePrefix) {
        try {
            return new DBMSExecutor(provider.getClass().getDeclaredConstructor().newInstance(), options, command,
                    databaseName, logName, r, createDatabaseOnRun, generatedObjectNamePrefix);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public DatabaseProvider getProvider() {
        return provider;
    }

}
