package dbradar.ddlCheck;

import dbradar.SQLGlobalState;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.query.SQLQueryAdapter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class TestEDCOracleBase<S extends SQLGlobalState, O extends EDCBase<S>> {

    public interface Reproducer {
        void bugStillTrigger(BugReport report, List<String> knownToReproduce) throws SQLException;
    }

    protected static String getDatabaseName(String path) {
        // Define the pattern for extracting the database name
        Pattern pattern = Pattern.compile(".*\\\\(database\\d+)(?:-cur)?\\.log$");
        Matcher matcher = pattern.matcher(path);

        // Find the match and extract the group
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            // If no match found, return null or throw an exception based on your requirements
            return null;
        }
    }

    protected abstract S getState() throws SQLException;

    protected abstract S getSemiState() throws SQLException;

    protected abstract O getOracle(S state);

    protected void checkExistenceOfBugs(String folderPath) throws IOException {
        File folder = new File(folderPath);
        if (!folder.exists()) {
            throw new RuntimeException("Folder " + folderPath + " does not exist");
        }

        FilenameFilter filter = (dir, name) -> !name.endsWith("-cur.log");
        File[] reports = folder.listFiles(filter);
        if (reports == null || reports.length == 0) {
            System.out.println("Do not find bugs in " + folderPath);
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (File f : reports) {
            sb.append(f.getAbsolutePath()).append("\n");
        }
        String intoFile = folderPath + File.separator + "1BugReports.txt";
        flushIntoFile(sb.toString(), intoFile);
    }

    public void reproduceBug(BugReport report, List<String> knownToReproduce, List<String> witnessQueries) throws SQLException {
        S state = getState();
        S semiState = getSemiState();

        try (Statement statement = state.getConnection().createStatement()) {
            for (String stmt : report.initDBStmts) {
                try {
                    System.out.println(stmt);
                    statement.execute(stmt);
                } catch (SQLException ignored) {
                }
            }
            for (String stmt : knownToReproduce) {
                try {
                    System.out.println(stmt);
                    statement.execute(stmt);
                } catch (SQLException ignored) {
                }
            }
        }

        try (Statement statement = semiState.getConnection().createStatement()) {
            for (String stmt : report.initSemiDBStmts) {
                System.out.println(stmt);
                statement.execute(stmt);
            }
            for (String stmt : knownToReproduce) {
                try {
                    System.out.println(stmt);
                    statement.execute(stmt);
                } catch (SQLException ignored) {
                }
            }
        }
        try {
            if (witnessQueries.get(0).startsWith("SELECT")) {
                EDCBase.checkDQLStmt(new SQLQueryAdapter(witnessQueries.get(0)), new SQLQueryAdapter(witnessQueries.get(1)), state, semiState);
            } else {
                O oracle = getOracle(state);
                oracle.checkStmt(witnessQueries.get(0), state, semiState);
            }
        } finally {
            state.getConnection().close();
            semiState.getConnection().close();
        }
    }

    public void flushIntoFile(String content, String intoFile) throws IOException {
        FileWriter fileWriter = new FileWriter(intoFile);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write(content);
        bufferedWriter.close();
        fileWriter.close();
        System.out.printf("Flush into %s", intoFile);
    }

    public List<String> reduceByStatement(BugReport report, List<String> statements, Reproducer reproducer) {
        int partitionNum = 2;

        List<String> knownToReproduce = new ArrayList<>(statements); // reduce me
        while (knownToReproduce.size() >= 2) {
            boolean observedChange = false;

            int start = 0;
            int subLength = knownToReproduce.size() / partitionNum;

            while (start < knownToReproduce.size()) {
                List<String> candidateStatements = new ArrayList<>(knownToReproduce);
                int endPoint = Math.min(start + subLength, candidateStatements.size());
                candidateStatements.subList(start, endPoint).clear();

                try {
                    reproducer.bugStillTrigger(report, candidateStatements);
                } catch (AssertionError error) {
                    observedChange = true;
                    knownToReproduce = candidateStatements;
                    partitionNum = Math.max(partitionNum - 1, 2);
                    break;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                start = start + subLength;
            }

            if (!observedChange) {
                if (partitionNum == knownToReproduce.size()) {
                    break;
                }
                // increase the search granularity
                partitionNum = Math.min(partitionNum * 2, knownToReproduce.size());
            }
        }

        return knownToReproduce;
    }

    public static class BugReport {
        List<String> initDBStmts = new ArrayList<>();
        List<String> initSemiDBStmts = new ArrayList<>();
        List<String> knownToReproduce = new ArrayList<>();
        List<String> witnessSettings = new ArrayList<>();
        List<String> witnessQueries = new ArrayList<>();
        String seed = null;
        File reportFile;

        public BugReport(String reportPath) throws IOException {
            readFromReport(reportPath);
        }

        public void readFromReport(String reportPath) throws IOException {
            reportFile = new File(reportPath);
            if (!reportFile.exists()) {
                System.out.println("Report file not found: " + reportPath);
                return;
            }

            BufferedReader br = new BufferedReader(new FileReader(reportFile));
            String line;
            while ((line = br.readLine()) != null) { // skip
                if (line.startsWith("-- seed value")) {
                    seed = line;
                    break;
                }
            }

            while ((line = br.readLine()) != null) {
                if (line.equals("==== Start SemiState ====;")) {
                    break;
                }
                initDBStmts.add(line); // cache init stmts for db
            }

            while ((line = br.readLine()) != null) {
                if (line.equals("==== End SemiState ====;")) {
                    break;
                }
                initSemiDBStmts.add(line); // cache init stmts for semiDb
            }

            boolean hasWitnessQueries = false;
            boolean hasWitnessSettings = false;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("-- Plan in state")) {
                    break;
                } else if (line.startsWith("==== Witness Queries ====;")) {
                    hasWitnessQueries = true;
                    break;
                } else if (line.startsWith("==== Witness Settings ====;")) {
                    hasWitnessSettings = true;
                    break;
                }
                knownToReproduce.add(line); // cache fuzzing stmts
            }

            if (hasWitnessSettings) {
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("==== Witness Queries =====;")) {
                        hasWitnessQueries = true;
                        break;
                    }
                    witnessSettings.add(line); // cache fuzzing stmts
                }
            }

            if (hasWitnessQueries) {
                while ((line = br.readLine()) != null) {
                    witnessQueries.add(line); // cache fuzzing stmts
                }
                assert witnessQueries.size() == 2;
            }

            br.close();
        }

        public void reformat(List<String> knownToReproduce, List<String> witnessQueries) {
            this.initDBStmts.clear();
            this.initSemiDBStmts.clear();
            this.knownToReproduce.clear();
            this.witnessQueries.clear();
            this.knownToReproduce.addAll(knownToReproduce);
            this.witnessQueries.addAll(witnessQueries);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(seed).append("\n");
            for (String stmt : initDBStmts) {
                sb.append(stmt).append("\n");
            }
            sb.append("==== Start SemiState ====;\n");
            for (String stmt : initSemiDBStmts) {
                sb.append(stmt).append("\n");
            }
            sb.append("==== End SemiState ====;\n");
            for (String stmt : knownToReproduce) {
                sb.append(stmt).append("\n");
            }
            if (!witnessSettings.isEmpty()) { // optional
                sb.append("==== Witness Settings ====;\n");
                for (String stmt : witnessSettings) {
                    sb.append(stmt).append("\n");
                }
            }
            if (!witnessQueries.isEmpty()) { // optional
                sb.append("==== Witness Queries ====;\n");
                for (String stmt : witnessQueries) {
                    sb.append(stmt).append("\n");
                }
            }

            return sb.toString();
        }

    }

}
