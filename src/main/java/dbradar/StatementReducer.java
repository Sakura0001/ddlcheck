package dbradar;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import dbradar.common.duplicate.TestCase;
import dbradar.common.query.Query;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.schema.AbstractTable;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.common.schema.TableIndex;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static dbradar.DeduplicateHelper.checkDBType;

public class StatementReducer implements Reducer {
    private final DatabaseProvider provider;
    private final GlobalState newState;
    private final Reproducer reproducer;
    private final ReducerBase reducer;

    public StatementReducer(DatabaseProvider provider, GlobalState state, Reproducer reproducer) {
        this.provider = provider;
        this.newState = state;
        this.reproducer = reproducer;
        this.reducer = new ReducerBase(newState);
    }

    @Override
    public TestCase reduce() {
        TestCase caseDetail = new TestCase();
        DeduplicateHelper.DBType dbType = checkDBType(newState.getState().getDatabaseVersion());
        caseDetail.setTestTarget(dbType.toString().toLowerCase());
        caseDetail.setContent(formTestCase());
        caseDetail.setOracleName(newState.getCurrentOracle());
        caseDetail.setBugFoundTime(new Date());
        caseDetail.setBugStatus("open");
        caseDetail.setBugSubmitUrl("www.moni.com");
        caseDetail.setComments("comments");
        caseDetail.setDeleted(0);
        List<Query> reproduceBugStatements = newState.getState().getStatements();
        System.out.println("EXPERIMENTAL: Original test case.");
        printInitializationStatement(reproduceBugStatements);
        printOracleStatement(reproducer.getOracleStatements());

        if (newState.getCurrentOracle().toLowerCase().contains("fuzz")) {
            restartServer();
            reproduceBugStatements = reduceFuzz(reproduceBugStatements);
            System.out.println("Reduced test case.");
            printInitializationStatement(reproduceBugStatements);
            caseDetail.setReducedCase(formTestCase());
            return caseDetail;
        }
        newState.setConnection(newState.createNewConnection(newState.getDatabaseName()));
        reproduceBugStatements = deltaDebugging(newState.getState().getStatements());
        System.out.println("Reduced initialization statement.");
        printInitializationStatement(reproduceBugStatements);
        reduceWhereClause();
        dropUsedColumn(reproduceBugStatements);
        dropUnusedConstraints(reproduceBugStatements);

        caseDetail.setReducedCase(recordReducedStmts());
        return caseDetail;
    }

    private void printInitializationStatement(List<Query> reproduceBugStatements) {
        for (Query sql : reproduceBugStatements) {
            System.out.println(sql.getQueryString());
        }
    }

    private void printOracleStatement(List<SQLQueryAdapter> oracleStatements) {
        for (Query sql : oracleStatements) {
            System.out.println(sql.getQueryString());
        }
    }

    private List<Query> deltaDebugging(List<Query> reproduceBugStatements) {
        int setCount = 2;
        while (reproduceBugStatements.size() >= 2) {
            List<List<Query>> subsets = split(reproduceBugStatements, setCount);
            boolean complementFailing = false;

            for (List<Query> subset : subsets) {
                execute(subset);
                if (reproducer.bugStillTriggers(newState)) {
                    reproduceBugStatements = subset;
                    setCount = Math.max(setCount - 1, 2);
                    complementFailing = true;
                    break;
                }
                List<Query> complement = difference(reproduceBugStatements, subset);
                execute(complement);
                if (reproducer.bugStillTriggers(newState)) {
                    reproduceBugStatements = complement;
                    setCount = Math.max(setCount - 1, 2);
                    complementFailing = true;
                    break;
                }
            }
            if (!complementFailing) {
                if (setCount == reproduceBugStatements.size()) {
                    break;
                }
                setCount = Math.min(setCount * 2, reproduceBugStatements.size());
            }
        }
        recreateTable(reproduceBugStatements);
        return reproduceBugStatements;
    }

    private void execute(List<Query> queries) {
        try {
            DatabaseConnection con = provider.createDatabase(newState);
            newState.setConnection(con);
            for (Query s : queries) {
                try {
                    s.execute(newState);
                } catch (Throwable ignored) {
                }
            }
        } catch (Exception ignored) {

        }
    }


    private <E> List<E> difference(List<E> a, List<E> b) {
        List<E> result = new LinkedList<>(a);
        result.removeAll(b);
        return result;
    }

    private <E> List<List<E>> split(List<E> s, int n) {
        List<List<E>> subsets = new LinkedList<>();
        int position = 0;
        for (int i = 0; i < n; i++) {
            List<E> subset = s.subList(position, position + (s.size() - position) / (n - i));
            subsets.add(subset);
            position += subset.size();
        }
        return subsets;
    }

    /**
     * The where predicate is reduced hierarchically, and the bug judgment is made on the result of the reduction
     */
    private void reduceWhereClause() {
        List<SQLQueryAdapter> oracleStatements = reproducer.getOracleStatements();
        ASTNode whereClauseNode = oracleStatements.get(0).getQueryAST().getChildByName("where_clause");

        ASTNode mutatedWhereClauseNode = reduceASTNode(whereClauseNode.getChildByName("where_clause").getChildByName("expr"));

        boolean continueReduce = true;
        List<SQLQueryAdapter> mutatedStmts = new ArrayList<>();
        while (continueReduce) {
            mutatedStmts.clear();
            setMutatedStmts(mutatedStmts, oracleStatements, mutatedWhereClauseNode);

            reproducer.setOracleStatements(mutatedStmts);

            if (reproducer.bugStillTriggers(newState)) {
                if (finishReduce(mutatedWhereClauseNode)) {
                    continueReduce = false;
                } else {
                    whereClauseNode = mutatedWhereClauseNode.clone();
                    mutatedWhereClauseNode = reduceASTNode(whereClauseNode);
                }
            } else {
                mutatedWhereClauseNode = reduceASTNode(whereClauseNode);
            }
        }
        System.out.println("Reduced oracle statement.");
        printOracleStatement(mutatedStmts);
    }

    private ASTNode reduceASTNode(ASTNode where) {
        return this.reducer.reduce(where);
    }

    /**
     * Get suspicious columns (columns that are not used by the test oracle)
     */
    private List<String> getSuspiciousColumns() {
        List<String> allColumns = new ArrayList<>();
        List<String> usedColumns = new ArrayList<>();

        for (AbstractTable<?, ?, ?> table : newState.getSchema().getDatabaseTables()) {
            if (table.getColumns().size() > 1) {
                for (AbstractTableColumn<?, ?> column : table.getColumns()) {
                    allColumns.add(column.getFullQualifiedName());
                }
            }
        }

        SQLQueryAdapter oracleStatement = reproducer.getOracleStatements().get(0);
        ASTNode where = oracleStatement.getQueryAST().getChildByName("where_clause");
        acquireUsedColumns(where, usedColumns);
        usedColumns.add(oracleStatement.getQueryAST().getChildByName("_column").toQueryString());
        //If there is no column in the where predicate, at least one column needs to be reserved
//        if (usedColumns.isEmpty()) {
//            usedColumns.add(allColumns.get(new Random().nextInt(allColumns.size())));
//        }

        return difference(allColumns, usedColumns);
    }

    /**
     * Recursively gets all the column names in WHERE
     */
    private void acquireUsedColumns(ASTNode where, List<String> usedColumns) {
        if (where.getToken().getValue().contains("column")) {
            usedColumns.add(where.getToken().getValue());
        }
        for (ASTNode child : where.getChildren()) {
            acquireUsedColumns(child, usedColumns);
        }
    }

    /**
     * If the WHERE predicate before the elimination is exactly the same as the WHERE predicate after the elimination, the elimination is over
     */
    private boolean finishReduce(ASTNode node) {
        boolean result = true;
        result &= (node.getReduceStep() == node.getExprCount() * 2 || node.getReduceStep() >= node.getExprCount() * 2);
        if (!result) {
            return false;
        }
        for (ASTNode child : node.getChildren()) {
            result = finishReduce(child);
            if (!result) {
                return false;
            }
        }
        return true;
    }

    /**
     * Gets all the data in the data table from the schema
     */
    private HashMap<String, HashMap<String, List<String>>> getTableData() {
        try {
            newState.updateSchema();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        HashMap<String, HashMap<String, List<String>>> tableColumn = new HashMap<>();
        List<String> tmpData;
        for (AbstractTable<?, ?, ?> table : newState.getSchema().getDatabaseTables()) {
            try {
                ResultSet colResultSet = new SQLQueryAdapter(String.format("SELECT * FROM %s", table.getName())).executeAndGet(newState).getRs();
                HashMap<String, List<String>> columnData = new HashMap<>();
                while (colResultSet.next()) {
                    for (int i = 1; i <= colResultSet.getMetaData().getColumnCount(); i++) {
                        if (!columnData.containsKey(colResultSet.getMetaData().getColumnName(i))) {
                            tmpData = new ArrayList<>();
                        } else {
                            tmpData = columnData.get(colResultSet.getMetaData().getColumnName(i));
                        }
                        tmpData.add(colResultSet.getString(i));
                        columnData.put(colResultSet.getMetaData().getColumnName(i), tmpData);
                    }
                }
                tableColumn.put(table.getName(), columnData);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        return tableColumn;
    }

    /**
     * Get all the constraint information from the database
     */
    private HashMap<String, HashMap<String, List<String>>> getConstraints() {
        //tableName:<indexName:<columns>>
        HashMap<String, HashMap<String, List<String>>> constraints = new HashMap<>();
        //indexName:<columns>
        HashMap<String, List<String>> tmpConstraints = new HashMap<>();
        List<String> tmpData = new ArrayList<>();
        for (AbstractTable<?, ?, ?> table : newState.getSchema().getDatabaseTables()) {
            for (TableIndex index : table.getIndexes()) {
                tmpData.clear();
                for (String column : index.getColumnNames()) {
                    if (!tmpConstraints.containsKey(index.getName())) {
                        tmpData = new ArrayList<>();
                    } else {
                        tmpData = tmpConstraints.get(index.getName());
                    }
                    tmpData.add(column);
                }
                tmpConstraints.put(index.getName(), tmpData);
            }
            constraints.put(table.getName(), tmpConstraints);
        }
        return constraints;
    }

    /**
     * Concatenate the insert statement
     */
    private List<SQLQueryAdapter> constructInsertStatements(HashMap<String, HashMap<String, List<String>>> tableData) {
        List<SQLQueryAdapter> insertStmts = new ArrayList<>();
        for (Map.Entry<String, HashMap<String, List<String>>> entry : tableData.entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ").append(entry.getKey()).append(" (");
            String column = "";
            for (Map.Entry<String, List<String>> entry2 : entry.getValue().entrySet()) {
                sb.append(entry2.getKey()).append(",");
                column = entry2.getKey();
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(") VALUES ");
            int length = tableData.get(entry.getKey()).get(column).size();
            int index = 0;
            while (index < length) {
                sb.append("(");
                sb.append("'").append(tableData.get(entry.getKey()).get(column).get(index)).append("'").append(",");
                sb.deleteCharAt(sb.length() - 1);
                sb.append("),");
                index++;
            }
            sb.deleteCharAt(sb.length() - 1);
            insertStmts.add(new SQLQueryAdapter(sb.toString()));
        }
        return insertStmts;
    }

    private void recreateTable(List<Query> reproduceBugStatements) {
        try {
            DatabaseConnection conn = provider.createConnection(newState);
            newState.setConnection(conn);
            for (Query sql : reproduceBugStatements) {
                sql.execute(newState);
            }
            newState.updateSchema();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private String recordReducedStmts() {
        StringBuilder reducedTestCase = new StringBuilder();
        for (SQLQueryAdapter sql : queryForCreateStmts()) {
            reducedTestCase.append(sql.toString()).append("\n");
        }
        for (SQLQueryAdapter sql : queryForInsertStmts()) {
            reducedTestCase.append(sql.toString()).append("\n");
        }
        for (SQLQueryAdapter sql : reproducer.getOracleStatements()) {
            reducedTestCase.append(sql.toString()).append("\n");
        }
        return reducedTestCase.toString();
    }

    private void restartServer() {
        String sshHost = newState.getOptions().getHost();
        String sshUser = newState.getOptions().getUserName();
        String sshPassword = newState.getOptions().getPassword();

        try {
            JSch jsch = new JSch();

            // Create SSH session
            Session session = jsch.getSession(sshUser, sshHost, 22); // The default SSH port is 22
            session.setPassword(sshPassword);
            session.setConfig("StrictHostKeyChecking", "no"); // The host key check function is disabled

            // Connect to remote server
            session.connect();

            // TODO: Execute remote commands, which may be modified according to your own needs
            String command = "docker start mariadb10.4"; // your remote command
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);

            // Get the output stream of command execution
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);

            // Connect and execute commands
            channel.connect();
            int tryCount = 0;
            while (isRestart() && tryCount < 5) {
                tryCount++;
            }
            // close connection
            channel.disconnect();
            session.disconnect();
        } catch (JSchException e) {
            throw new AssertionError();
        }
    }

    private boolean isRestart() {
        boolean isClosed;
        try {
            if (newState.getConnection() == null) {
                newState.setConnection(provider.createConnection(newState));
            }
            isClosed = ((SQLConnection) newState.getConnection()).isClosed();
        } catch (Exception e) {
            isClosed = true;
        }
        return isClosed;

    }

    private List<Query> reduceFuzz(List<Query> input) {
        int collectionSize = 2;
        while (input.size() >= 2) {
            List<List<Query>> collections = split(input, collectionSize);
            boolean complementFailing = false;

            for (List<Query> collection : collections) {
                try (DatabaseConnection con = provider.createConnection(newState)) {
                    newState.setConnection(con);
                    for (Query sql : collection) {
                        try {
                            sql.execute(newState);
                        } catch (Throwable ignored) {
                            if (reproducer.bugStillTriggers(newState)) {
                                restartServer();
                                input = collection;
                                collectionSize = Math.max(collectionSize - 1, 2);
                                complementFailing = true;
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    if (e.getMessage().contains("Connection refused")) {
                        restartServer();
                    }
                }

                List<Query> complement = difference(input, collection);
                try (DatabaseConnection con = provider.createConnection(newState)) {
                    newState.setConnection(con);
                    for (Query sql : complement) {
                        try {
                            sql.execute(newState);
                        } catch (Throwable ignored) {
                            if (reproducer.bugStillTriggers(newState)) {
                                //Run the script to restart the server
                                restartServer();
                                input = complement;
                                collectionSize = Math.max(collectionSize - 1, 2);
                                complementFailing = true;
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    if (e.getMessage().contains("Connection refused")) {
                        restartServer();
                    }
                }
                if (!complementFailing) {
                    if (collectionSize == input.size()) {
                        break;
                    }
                    collectionSize = Math.min(collectionSize * 2, input.size());
                }
            }
        }
        return input;
    }

    /**
     * construct origin test case
     */
    private String formTestCase() {
        StringBuilder sb = new StringBuilder();
        for (Query sql : newState.getState().getStatements()) {
            sb.append(sql.getQueryString()).append("\n");
        }

        for (Query sql : reproducer.getOracleStatements()) {
            sb.append(sql.getQueryString()).append("\n");
        }
        return sb.toString();
    }


    /**
     * Modify the test oracle statement based on the test oracle type
     */
    private void setMutatedStmts(List<SQLQueryAdapter> mutatedStmts, List<SQLQueryAdapter> oracleStatements, ASTNode mutatedWhereClauseNode) {
        String currentOracle = newState.getCurrentOracle().toLowerCase();
        if (currentOracle.contains("norec")) {
            ASTNode ast = oracleStatements.get(0).getQueryAST();
            ASTNode whereClause1 = oracleStatements.get(0).getQueryAST().getChildByName("where_clause");
            whereClause1.getChildren().set(1, mutatedWhereClauseNode);
            ast.setChildrenByName("where_clause", whereClause1);
            mutatedStmts.add(new SQLQueryAdapter(ast));
            ASTNode ast2 = oracleStatements.get(1).getQueryAST();
            ASTNode whereClause2 = oracleStatements.get(1).getQueryAST().getChildByName("where_clause");
            whereClause2.getChildren().set(1, mutatedWhereClauseNode);
            ast2.setChildrenByName("where_clause", whereClause2);
            mutatedStmts.add(new SQLQueryAdapter(ast2));
        } else if (currentOracle.contains("tlp")) {
            ASTNode ast1 = oracleStatements.get(1).getQueryAST();
            ASTNode whereClause1 = oracleStatements.get(0).getQueryAST().getChildByName("where_clause");
            whereClause1.getChildren().set(1, mutatedWhereClauseNode);
            mutatedStmts.add(new SQLQueryAdapter(ast1));
            ast1.setChildrenByName("where_clause", whereClause1);
            mutatedStmts.add(new SQLQueryAdapter(ast1));


            //TODO:need to determine the statement structure first
        } else if (currentOracle.contains("dqe")) {

        } else {
            throw new AssertionError("To implement oracle: " + currentOracle);
        }
    }

    /**
     * Attempts to delete unused data columns with alter-drop column
     */
    private void dropUsedColumn(List<Query> inits) {
        try {
            newState.updateSchema();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        List<String> suspiciousColumns = getSuspiciousColumns();
        for (String column : suspiciousColumns) {
            //alter table t0 drop column c0
            try {
                SQLQueryAdapter alter = new SQLQueryAdapter(String.format("ALTER TABLE %s DROP COLUMN %S", "1", column));
                alter.execute(newState);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            if (reproducer.bugStillTriggers(newState)) {
                //重建初始化语句序列
                inits.clear();
                inits.addAll(queryForCreateStmts());
                inits.addAll(queryForInsertStmts());
            } else {
                //删除column导致缺陷无法再次触发。需要使用上一步的变体重建数据库状态
                newState.createNewConnection(newState.getDatabaseName());
                try {
                    for (Query sql : inits) {
                        sql.execute(newState);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Attempts to delete unused data constraint with alter-drop constraint
     */
    private void dropUnusedConstraints(List<Query> inits) {
        try {
            newState.updateSchema();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        HashMap<String, HashMap<String, List<String>>> constraints = getConstraints();
        for (Map.Entry<String, HashMap<String, List<String>>> tableLevel : constraints.entrySet()) {
            for (String constraint : tableLevel.getValue().keySet()) {
                try {
                    SQLQueryAdapter alter = new SQLQueryAdapter(String.format("DROP INDEX %s", constraint));
                    alter.execute(newState);
                } catch (SQLException ignored) {

                }
                if (reproducer.bugStillTriggers(newState)) {
                    //重建初始化语句序列
                    inits.clear();
                    inits.addAll(queryForCreateStmts());
                    inits.addAll(queryForInsertStmts());
                } else {
                    //删除column导致缺陷无法再次触发。需要使用上一步的变体重建数据库状态
                    newState.createNewConnection(newState.getDatabaseName());
                    try {
                        for (Query sql : inits) {
                            sql.execute(newState);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private List<SQLQueryAdapter> queryForCreateStmts() {
        List<SQLQueryAdapter> result = new ArrayList<>();
        try {
            newState.updateSchema();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        SQLConnection con = (SQLConnection) newState.getConnection();
        try (Statement s = con.createStatement()) {
            for (AbstractTable<?, ?, ?> table : newState.getSchema().getDatabaseTables()) {
                String query = String.format("SELECT sql FROM sqlite_master WHERE type='table' AND name='%s';", table.getName());
                ResultSet rs = s.executeQuery(query);
                while (rs.next()) {
                    result.add(new SQLQueryAdapter(rs.getString("sql")));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private List<SQLQueryAdapter> queryForInsertStmts() {
        return constructInsertStatements(getTableData());
    }
}
