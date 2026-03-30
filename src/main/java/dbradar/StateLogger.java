package dbradar;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import dbradar.common.log.Loggable;
import dbradar.common.query.Query;

public final class StateLogger {

    private final File loggerFile;
    private File curFile;
    private File queryPlanFile;
    private File reduceFile;
    public FileWriter logFileWriter;
    public FileWriter currentFileWriter;
    private FileWriter queryPlanFileWriter;

    private static final List<String> INITIALIZED_PROVIDER_NAMES = new ArrayList<>();
    private final boolean logEachSelect;
    private final boolean logQueryPlan;

    public static void resetInitializedProviders() {
        synchronized (INITIALIZED_PROVIDER_NAMES) {
            INITIALIZED_PROVIDER_NAMES.clear();
        }
    }

    private final boolean useReducer;
    private final DatabaseProvider databaseProvider;
    private String databaseName;
    private String dbmsName;

    private static final class AlsoWriteToConsoleFileWriter extends FileWriter {

        AlsoWriteToConsoleFileWriter(File file) throws IOException {
            super(file);
        }

        @Override
        public Writer append(CharSequence arg0) throws IOException {
            System.err.println(arg0);
            return super.append(arg0);
        }

        @Override
        public void write(String str) throws IOException {
            System.err.println(str);
            super.write(str);
        }
    }

    public StateLogger(String databaseName, DatabaseProvider provider, MainOptions options) {
        this.databaseName = databaseName;
        this.dbmsName = provider.getDBMSName();
        File dir = new File(Main.LOG_DIRECTORY, provider.getDBMSName());
        if (dir.exists() && !dir.isDirectory()) {
            throw new AssertionError(dir);
        }
        ensureExistsAndIsEmpty(dir, provider);
        loggerFile = new File(dir, databaseName + ".log");
        logEachSelect = options.logEachSelect();
        if (logEachSelect) {
            curFile = new File(dir, databaseName + "-cur.log");
        }
        logQueryPlan = options.logQueryPlan();
        if (logQueryPlan) {
            queryPlanFile = new File(dir, databaseName + "-plan.log");
        }
        this.useReducer = options.useReducer();
        if (useReducer) {
            File reduceFileDir = new File(dir, "reduce");
            if (!reduceFileDir.exists()) {
                reduceFileDir.mkdir();
            }
            this.reduceFile = new File(reduceFileDir, databaseName + "-reduce.log");

        }
        this.databaseProvider = provider;
    }

    private void ensureExistsAndIsEmpty(File dir, DatabaseProvider provider) {
        if (INITIALIZED_PROVIDER_NAMES.contains(provider.getDBMSName())) {
            return;
        }
        synchronized (INITIALIZED_PROVIDER_NAMES) {
            if (!dir.exists()) {
                try {
                    Files.createDirectories(dir.toPath());
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            File[] listFiles = dir.listFiles();
            assert listFiles != null : "directory was just created, so it should exist";
            for (File file : listFiles) {
                if (!file.isDirectory()) {
                    file.delete();
                }
            }
            INITIALIZED_PROVIDER_NAMES.add(provider.getDBMSName());
        }
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getDBMSName() {
        return dbmsName;
    }

    private FileWriter getLogFileWriter() {
        if (logFileWriter == null) {
            try {
                logFileWriter = new AlsoWriteToConsoleFileWriter(loggerFile);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        return logFileWriter;
    }

    public FileWriter getCurrentFileWriter() {
        if (!logEachSelect) {
            throw new UnsupportedOperationException();
        }
        if (currentFileWriter == null) {
            try {
                currentFileWriter = new FileWriter(curFile, false);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        return currentFileWriter;
    }

    public FileWriter getQueryPlanFileWriter() {
        if (!logQueryPlan) {
            throw new UnsupportedOperationException();
        }
        if (queryPlanFileWriter == null) {
            try {
                queryPlanFileWriter = new FileWriter(queryPlanFile, true);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        return queryPlanFileWriter;
    }

    public FileWriter getReduceFileWriter() {
        if (!useReducer) {
            throw new UnsupportedOperationException();
        }
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(reduceFile, false);
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        return fileWriter;
    }

    public void writeCurrent(StateToReproduce state) {
        if (!logEachSelect) {
            throw new UnsupportedOperationException();
        }
        printState(getCurrentFileWriter(), state);
        try {
            currentFileWriter.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeCurrent(String input) {
        write(databaseProvider.getLoggableFactory().createLoggable(input));
    }

    public void writeCurrentNoLineBreak(String input) {
        write(databaseProvider.getLoggableFactory().createLoggableWithNoLinebreak(input));
    }

    private void write(Loggable loggable) {
        if (!logEachSelect) {
            throw new UnsupportedOperationException();
        }
        try {
            getCurrentFileWriter().write(loggable.getLogString());

            currentFileWriter.flush();
        } catch (IOException e) {
            throw new AssertionError();
        }
    }

    public void writeQueryPlan(String queryPlan) {
        if (!logQueryPlan) {
            throw new UnsupportedOperationException();
        }
        try {
            getQueryPlanFileWriter().append(removeNamesFromQueryPlans(queryPlan));
            queryPlanFileWriter.flush();
        } catch (IOException e) {
            throw new AssertionError();
        }
    }

    public void logReduced(StateToReproduce state) {
        FileWriter reduceFileWriter = getReduceFileWriter();

        StringBuilder sb = new StringBuilder();
        for (Query s : state.getStatements()) {
            sb.append(databaseProvider.getLoggableFactory().createLoggable(s.getLogString()).getLogString());
        }
        try {
            reduceFileWriter.write(sb.toString());

        } catch (IOException e) {
            throw new AssertionError(e);
        } finally {
            try {
                reduceFileWriter.flush();
                reduceFileWriter.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public void logException(Throwable reduce, StateToReproduce state) {
        Loggable stackTrace = getStackTrace(reduce);
        FileWriter logFileWriter2 = getLogFileWriter();
        try {
            logFileWriter2.write(stackTrace.getLogString());
            printState(logFileWriter2, state);
        } catch (IOException e) {
            throw new AssertionError(e);
        } finally {
            try {
                logFileWriter2.flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private Loggable getStackTrace(Throwable e1) {
        return databaseProvider.getLoggableFactory().convertStacktraceToLoggable(e1);
    }

    private void printState(FileWriter writer, StateToReproduce state) {
        StringBuilder sb = new StringBuilder();

        sb.append(databaseProvider.getLoggableFactory()
                .getInfo(state.getDatabaseName(), state.getDatabaseVersion(), state.getSeedValue()).getLogString());

        for (Query s : state.getStatements()) {
            sb.append(databaseProvider.getLoggableFactory().createLoggable(s.getLogString()).getLogString());
        }
        try {
            writer.write(sb.toString());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private String removeNamesFromQueryPlans(String queryPlan) {
        String result = queryPlan;
        result = result.replaceAll("t[0-9]+", "t0"); // Avoid duplicate tables
        result = result.replaceAll("v[0-9]+", "v0"); // Avoid duplicate views
        result = result.replaceAll("i[0-9]+", "i0"); // Avoid duplicate indexes
        return result + "\n";
    }
}