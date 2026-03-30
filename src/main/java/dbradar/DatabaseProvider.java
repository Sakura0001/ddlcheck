package dbradar;

import dbradar.common.log.LoggableFactory;

public interface DatabaseProvider {

    /**
     * Gets the {@link GlobalState} class.
     *
     * @return the class extending {@link GlobalState}
     */
    Class<? extends GlobalState> getGlobalStateClass();

    /**
     * Gets the JCommander option class.
     *
     * @return the class representing the DBMS-specific options.
     */
    Class<? extends DBMSSpecificOptions> getOptionClass();

    /**
     * Generates a single database and executes a test oracle a given number of
     * times.
     *
     * @param globalState the state created and is valid for this method call.
     * @return Reproducer if a bug is found and a reproducer is available.
     * @throws Exception if creating the database fails.
     */
    Reproducer generateAndTestDatabase(GlobalState globalState) throws Exception;

    DatabaseConnection createDatabase(GlobalState globalState) throws Exception;

    /**
     * The DBMS name is used to name the log directory and command to test the
     * respective DBMS.
     *
     * @return the DBMS' name
     */
    String getDBMSName();

    LoggableFactory getLoggableFactory();

    StateToReproduce getStateToReproduce(String databaseName);

    default DatabaseConnection createConnection(GlobalState globalState) throws Exception {
        return null;
    }
}
