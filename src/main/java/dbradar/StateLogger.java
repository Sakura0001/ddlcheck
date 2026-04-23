package dbradar;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import dbradar.common.log.Loggable;
import dbradar.common.query.Query;

public final class StateLogger {

    private static final String GLOBAL_EXECUTION_LOG = "global-execution.log";

    private final File loggerFile;
    private File curFile;
    private File queryPlanFile;
    public FileWriter logFileWriter;
    public FileWriter currentFileWriter;
    private FileWriter queryPlanFileWriter;

    private static final List<String> INITIALIZED_PROVIDER_NAMES = new ArrayList<>();
    private static final Object GLOBAL_LOG_LOCK = new Object();
    private static FileWriter globalExecutionLogWriter;
    private final boolean logEachSelect;
    private final boolean logQueryPlan;

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
        this.databaseProvider = provider;
    }

    public static void initializeGlobalExecutionLog(DatabaseProvider provider, MainOptions options) {
        synchronized (GLOBAL_LOG_LOCK) {
            closeGlobalExecutionLog();
            File dir = new File(Main.LOG_DIRECTORY, provider.getDBMSName());
            if (!dir.exists()) {
                try {
                    Files.createDirectories(dir.toPath());
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            Path globalLogPath = dir.toPath().resolve(GLOBAL_EXECUTION_LOG);
            try {
                Files.deleteIfExists(globalLogPath);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            if (!options.logGlobalExecution()) {
                return;
            }
            try {
                globalExecutionLogWriter = new FileWriter(globalLogPath.toFile(), false);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }

    public static void logGlobalExecutionEvent(int threadId, String databaseName, String sql, boolean success,
                                               String errorMessage) {
        synchronized (GLOBAL_LOG_LOCK) {
            if (globalExecutionLogWriter == null) {
                return;
            }
            try {
                globalExecutionLogWriter.write(formatGlobalExecutionEvent(threadId, databaseName, sql, success,
                        errorMessage));
                globalExecutionLogWriter.write(System.lineSeparator());
                globalExecutionLogWriter.flush();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }

    public static void closeGlobalExecutionLog() {
        synchronized (GLOBAL_LOG_LOCK) {
            if (globalExecutionLogWriter == null) {
                return;
            }
            try {
                globalExecutionLogWriter.close();
            } catch (IOException e) {
                throw new AssertionError(e);
            } finally {
                globalExecutionLogWriter = null;
            }
        }
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
                if (!file.isDirectory() && !GLOBAL_EXECUTION_LOG.equals(file.getName())) {
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

    private static String formatGlobalExecutionEvent(int threadId, String databaseName, String sql, boolean success,
                                                     String errorMessage) {
        String normalizedError = success ? "-" : sanitizeLine(errorMessage);
        return String.format("thread=%d db=%s status=%s error=%s sql=%s",
                threadId,
                sanitizeLine(databaseName),
                success ? "SUCCESS" : "FAIL",
                normalizedError,
                sanitizeLine(sql));
    }

    private static String sanitizeLine(String value) {
        if (value == null || value.isBlank()) {
            return "-";
        }
        return value.replace("\r", " ").replace("\n", " ").trim().replaceAll("\\s+", " ");
    }
}
