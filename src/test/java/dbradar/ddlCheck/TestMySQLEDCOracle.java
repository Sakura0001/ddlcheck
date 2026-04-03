package dbradar.ddlCheck;

import dbradar.ComparatorHelper;
import dbradar.Main;
import dbradar.common.oracle.edc.EDCBase;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.mysql.MySQLGlobalState;
import dbradar.mysql.oracle.MySQLEDCOracle;
import dbradar.mysql.oracle.MySQLEDCOracle.MySQLQueryPlan;
import dbradar.mysql.schema.MySQLSchema;
import dbradar.mysql.schema.MySQLSchema.MySQLTable;
import org.junit.jupiter.api.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMySQLEDCOracle extends TestEDCOracleBase<MySQLGlobalState, MySQLEDCOracle> {

    String dbName = "mysql";
    String host = "127.0.0.1";
    int port = 3306;
    String username = "root";
    String password = "Taurus_123";

    /**
     * 压测模式 - 单线程独立数据库
     * 每个数据库由一个worker持续执行多轮 DDL/DML/QUERY，轮次之间重建同一个数据库名
     */
    @Test
    public void testMySQLStressOracle() throws IOException {
        String databasePrefix = "mysqlstresssingle" + System.nanoTime();
        Path stressLogPath = Path.of("logs", "mysql", databasePrefix + "0-cur.log");

        assertEquals(0, Main.executeMain(
                // ============ 连接参数 ============
                "--username", username,                        // 数据库用户名
                "--password", password,                        // 数据库密码
                "--host", host,                                // 数据库主机地址
                "--port", String.valueOf(port),                // 数据库端口号

                // ============ 线程与执行控制 ============
                "--num-threads", "80",                         // 1个数据库任务
                "--timeout-seconds", "30",                    // 运行上限，防止测试卡死

                // ============ 日志控制 ============
                "--log-each-select", "true",                   // 是否记录每条执行的SQL语句
                "--log-execution-time", "false",               // 保留日志即可
                "--print-progress-information", "false",       // 测试中关闭进度打印

                // ============ 数据库配置 ============
                "--database-prefix", databasePrefix,           // 固定一个数据库名并按round反复重建
                "--use-connection-test", "true",               // 多线程启动前是否先测试连接可用性

                // ============ MySQL子命令 + MySQL专属参数 ============
                dbName,                                        // 子命令: "mysql"
                "--oracle", "STRESS",                          // Oracle模式: STRESS=压测模式, EQUATION=DDL等价验证
                "--stress-threads-per-db", "1",                // 每个数据库分配1个worker线程(单线程模式)
                "--stress-rounds-per-db", "1000000",                // 对同一个数据库名执行2轮
                "--stress-ddl-per-thread", "100",               // 每轮每线程执行的DDL数量
                "--stress-dml-per-thread", "100",               // 每轮每线程执行的DML数量
                "--stress-query-per-thread", "100"              // 每轮每线程执行的随机查询数量
        ));

        assertTrue(Main.nrDatabases.get() >= 2, "single-thread stress should recreate the same database across rounds");
        assertTrue(Files.exists(stressLogPath), "single-thread stress should create a mysql current log file");
        assertTrue(countStressExecutionLines(stressLogPath) > 0,
                "single-thread stress log should contain executed SQL records");
    }

    /**
     * 压测模式 - 多线程同库
     * 多个worker共享同一个数据库，按轮次执行 DDL/DML/QUERY
     */
    @Test
    public void testMySQLStressMultiThread() throws IOException {
        String databasePrefix = "mysqlstressmt" + System.nanoTime();
        Path stressLogPath = Path.of("logs", "mysql", databasePrefix + "0-cur.log");

        assertEquals(0, Main.executeMain(
                // ============ 连接参数 ============
                "--username", username,                        // 数据库用户名
                "--password", password,                        // 数据库密码
                "--host", host,                                // 数据库主机地址
                "--port", String.valueOf(port),                // 数据库端口号

                // ============ 线程与执行控制 ============
                "--num-threads", "10",                         // 启动1个数据库任务
                "--timeout-seconds", "30",                    // 集成测试限制在较短时间内完成

                // ============ 日志控制 ============
                "--log-each-select", "true",                   // 是否记录每条执行的SQL语句
                "--log-execution-time", "false",               // 验证日志文件生成即可，不额外记录执行时间
                "--print-progress-information", "false",       // 测试中关闭周期性进度打印，避免后台scheduler干扰

                // ============ 数据库配置 ============
                "--database-prefix", databasePrefix,           // 使用唯一前缀，避免读取到旧日志文件
                "--use-connection-test", "true",               // 多线程启动前是否先测试连接可用性

                // ============ MySQL子命令 + MySQL专属参数 ============
                dbName,                                        // 子命令: "mysql"
                "--oracle", "STRESS",                          // Oracle模式: STRESS=压测模式
                "--stress-threads-per-db", "2",                // 每个数据库分配2个worker线程并发操作
                                                               //   总有效线程 = num-threads × threads-per-db = 1×2 = 2
                                                               //   worker出错自动重连继续，不中断整个库的测试
                "--stress-rounds-per-db", "2",                // 对同一个数据库名执行2轮
                "--stress-ddl-per-thread", "1",               // 每个worker每轮执行的DDL语句数
                "--stress-dml-per-thread", "1",               // 每个worker每轮执行的DML语句数
                "--stress-query-per-thread", "1"              // 每个worker每轮执行的查询语句数
        ));

        long executedSqlCount = Main.nrSuccessfulActions.get() + Main.nrUnsuccessfulActions.get();
        assertTrue(Main.nrDatabases.get() >= 2, "multi-thread stress should repeat rounds on the same database task");
        assertTrue(executedSqlCount > 0, "stress test should execute at least one SQL statement");
        assertTrue(Files.exists(stressLogPath), "stress test should create a mysql current log file");
        assertTrue(countStressExecutionLines(stressLogPath) > 0,
                "stress log should contain at least one stress SQL execution record");
        assertTrue(countDistinctThreadNames(stressLogPath) >= 2,
                "multi-thread stress should log SQL from multiple worker threads on the same database");
    }

    private long countStressExecutionLines(Path stressLogPath) throws IOException {
        try (Stream<String> lines = Files.lines(stressLogPath)) {
            return lines.filter(line -> line.contains("[kind=")
                    && (line.contains("SUCCESS") || line.contains("FAIL(")))
                    .count();
        }
    }

    private long countDistinctThreadNames(Path stressLogPath) throws IOException {
        Pattern threadPattern = Pattern.compile("\\[thread=([^\\]]+)\\]");
        try (Stream<String> lines = Files.lines(stressLogPath)) {
            return lines.map(threadPattern::matcher)
                    .filter(Matcher::find)
                    .map(matcher -> matcher.group(1))
                    .distinct()
                    .count();
        }
    }

    /**
     * DDL等价验证模式 (legacy)
     * 通过生成DDL序列并用SHOW CREATE TABLE重建，验证两边schema和查询结果是否一致
     */
    @Test
    public void testMySQLEquationOracle() {
        assertEquals(0, Main.executeMain(
                // ============ 连接参数 ============
                "--username", username,                        // 数据库用户名
                "--password", password,                        // 数据库密码
                "--host", host,                                // 数据库主机地址
                "--port", String.valueOf(port),                // 数据库端口号

                // ============ 线程与执行控制 ============
                "--num-threads", "10",                         // 并发线程数
                "--num-tries", "100000",                       // 总共创建的数据库实例数
                "--num-queries", "5000",                       // 每个数据库执行的查询轮数
                "--max-generated-databases", "1",              // 每个线程最多生成1个数据库
                "--timeout-seconds", "600",                    // 全局超时时间(秒)

                // ============ 日志控制 ============
                "--log-each-select", "true",                   // 是否记录每条执行的SQL语句
                "--log-execution-time", "true",                // 是否记录每条SQL的执行时间
                "--print-progress-information", "true",        // 是否每5秒打印吞吐量进度

                // ============ 数据库配置 ============
                "--database-prefix", "database",               // 创建的数据库名称前缀
                "--use-connection-test", "true",               // 多线程启动前是否先测试连接可用性

                // ============ MySQL子命令 + MySQL专属参数 ============
                dbName,                                        // 子命令: "mysql"
                "--oracle", "EQUATION"                         // Oracle模式: EQUATION=DDL等价验证(会创建genDB和semiDB对比)
        ));
    }

    String folderPath = "folderPath";

    @Test
    public void filterOutMySQLBugs() throws IOException {
        checkExistenceOfBugs(folderPath);
    }

    String databaseName = null;
    String reportPath = "reportPath";
    List<String> witnessQueries = List.of(
            "SELECT1",
            "SELECT2"
    );

    @Test
    public void reproduceMySQLBug() throws SQLException, IOException {
        BugReport report = new BugReport(reportPath);
        reproduceBug(report, report.knownToReproduce, witnessQueries);
    }

    @Test
    public void reduceMySQLBugByStatement() throws Exception {
        BugReport report = new BugReport(reportPath);
        report.knownToReproduce = reduceByStatement(report, report.knownToReproduce, (report1, knownToReproduce) -> reproduceBug(report1, knownToReproduce, witnessQueries));
        report.witnessQueries = witnessQueries;
        String intoFile = report.reportFile.getParent() + File.separator + databaseName + "-statement.txt";
        flushIntoFile(report.toString(), intoFile);
    }

    @Test
    public void reduceMySQLBugByState() throws Exception {
        MySQLGlobalState state = getState();
        BugReport report = new BugReport(reportPath);
        Statement statement = state.getConnection().createStatement();
        for (String stmt : report.initDBStmts) {
            statement.execute(stmt);
        }
        for (String stmt : report.knownToReproduce) {
            statement.execute(stmt);
        }

        MySQLQueryPlan plan = MySQLEDCOracle.getMySQLQueryPlan(witnessQueries.get(0), state);
        List<String> tableNames = plan.getTableNames();
        List<MySQLTable> tables = state.getSchema().getDatabaseTables();
        int errorCount = 0;
        for (int i = 0; tables.size() > tableNames.size(); i++) {
            i = i % tables.size(); // valid range
            MySQLTable table = tables.get(i);
            String tableName = table.getName();
            if (!tableNames.contains(tableName)) { // drop unused tables
                try {
                    String dropStmt;
                    if (table.isView()) {
                        dropStmt = "DROP VIEW " + tableName;
                    } else {
                        dropStmt = "DROP TABLE " + tableName;
                    }
                    statement.execute(dropStmt);
                    tables.remove(i);
                } catch (SQLException e) {
                    errorCount++;
                    if (errorCount > 100) {
                        throw new AssertionError(e.getMessage());
                    }
                }
            }
        }
        state.updateSchema();

        MySQLEDCOracle oracle = getOracle(state);
        List<SQLQueryAdapter> createStmts = oracle.fetchCreateStmts(state);
        Map<String, List<String>> tblInsertStmts = new HashMap<>();
        for (String tableName : tableNames) {
            List<String> insertStmts = fetchInsertStmts(state, tableName);
            if (insertStmts != null) {
                tblInsertStmts.put(tableName, insertStmts);
            }
        }

        statement.close();
        state.getConnection().close();

        state.setConnection(state.createDatabase(host, port, username, password, databaseName));
        List<String> orderedStmts = oracle.replayCreateStmts(state, createStmts);
        List<String> knownToReproduce = new ArrayList<>(orderedStmts);
        statement = state.getConnection().createStatement();
        Pattern pattern = Pattern.compile("CREATE(?: TEMPORARY)? TABLE `(\\w+)`");
        for (String stmt : orderedStmts) {
            Matcher matcher = pattern.matcher(stmt);
            if (matcher.find()) {
                String tableName = matcher.group(1);
                if (tblInsertStmts.containsKey(tableName)) {
                    for (String insertStmt : tblInsertStmts.get(tableName)) {
                        statement.execute(insertStmt);
                    }
                    knownToReproduce.addAll(tblInsertStmts.get(tableName));
                }
            }
        }
        statement.close();

        boolean canBeReproduced = false;
        List<String> result0 = EDCBase.getQueryResult(new SQLQueryAdapter(witnessQueries.get(0)), state);
        List<String> result1 = EDCBase.getQueryResult(new SQLQueryAdapter(witnessQueries.get(1)), state);
        try {
            ComparatorHelper.assumeResultSetsAreEqual(result0, result1, witnessQueries.get(0), List.of(witnessQueries.get(1)), state);
        } catch (AssertionError ignored) {
            canBeReproduced = true;
        }

        state.getConnection().close();

        assert canBeReproduced;

        knownToReproduce = reduceByStatement(null, knownToReproduce, new Reproducer() {
            @Override
            public void bugStillTrigger(BugReport report, List<String> knownToReproduce) throws SQLException {
                MySQLGlobalState state = getState();
                try (Statement statement1 = state.getConnection().createStatement()) {
                    for (String stmt : knownToReproduce) {
                        try {
                            statement1.execute(stmt);
                        } catch (SQLException ignored) {
                        }
                    }

                    try {
                        EDCBase.checkDQLStmt(new SQLQueryAdapter(witnessQueries.get(0)), new SQLQueryAdapter(witnessQueries.get(1)), state, state);
                    } finally {
                        state.getConnection().close();
                    }
                }
            }
        });

        // reformat bug report
        report.reformat(knownToReproduce, witnessQueries);

        String intoFile = report.reportFile.getParent() + File.separator + databaseName + "-state.txt";
        flushIntoFile(report.toString(), intoFile);
    }

    @Test
    public void testMySQLSchema() throws Exception {
        MySQLGlobalState state = getState();
        try (Statement createTable1 = state.getConnection().createStatement();
             Statement createTable2 = state.getConnection().createStatement()) {
            createTable1.execute("CREATE TABLE `t1` (  `c1` int PRIMARY KEY,  UNIQUE KEY `idx_1` (c1));");
            createTable2.execute("CREATE TABLE `t2` (  `c1` int DEFAULT NULL,  CONSTRAINT `fk_1` FOREIGN KEY (`c1`) REFERENCES `t1` (`c1`));");
        }

        MySQLSchema schema1 = state.getSchema();
        assertTrue(schema1.getDatabaseTables().size() > 0);

        try (Statement dropTable1 = state.getConnection().createStatement();
             Statement dropTable2 = state.getConnection().createStatement()) {
            dropTable1.execute("DROP TABLE t2;");
            dropTable2.execute("DROP TABLE t1;");
        }
        try (Statement createTable1 = state.getConnection().createStatement();
             Statement createTable2 = state.getConnection().createStatement()) {
            createTable1.execute("CREATE TABLE `t3` (  `c1` int,  UNIQUE KEY `idx_2` (c1), PRIMARY KEY(c1)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;");
            createTable2.execute("CREATE TABLE `t4` (  `c1` int DEFAULT NULL,  CONSTRAINT `fk_1` FOREIGN KEY (`c1`) REFERENCES `t3` (`c1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;");
        }
        state.updateSchema();
        MySQLSchema schema2 = state.getSchema();
        assertTrue(schema2.getDatabaseTables().size() > 0);
    }

    @Override
    protected MySQLGlobalState getState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        if (databaseName == null) {
            databaseName = "test";
        }
        MySQLGlobalState state = new MySQLGlobalState();
        state.setDatabaseName(databaseName);
        state.setConnection(state.createDatabase(host, port, username, password, databaseName));

        return state;
    }

    @Override
    protected MySQLGlobalState getSemiState() throws SQLException {
        if (databaseName == null) {
            databaseName = getDatabaseName(reportPath);
        }
        assert databaseName != null;
        MySQLGlobalState semiState = new MySQLGlobalState();
        String semiDB = databaseName + "_semi";
        semiState.setDatabaseName(semiDB);
        semiState.setConnection(semiState.createDatabase(host, port, username, password, semiDB));

        return semiState;
    }

    @Override
    protected MySQLEDCOracle getOracle(MySQLGlobalState state) {
        return new MySQLEDCOracle(state);
    }

    private List<String> fetchInsertStmts(MySQLGlobalState state, String tableName) {
        if (!tableName.startsWith("t")) return null; // only fetch insert statements for tables without views
        List<String> insertStmts = new ArrayList<>();
        try (Statement statement = state.getConnection().createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            while (rs.next()) {
                StringBuilder insertStmt = new StringBuilder("INSERT INTO " + tableName + "(");

                // Append column name
                for (int i = 1; i <= columnCount; i++) {
                    insertStmt.append(rsmd.getColumnName(i));
                    if (i < columnCount) {
                        insertStmt.append(", ");
                    }
                }

                insertStmt.append(") VALUES (");

                // Append column values based on their types
                for (int i = 1; i <= columnCount; i++) {
                    int columnType = rsmd.getColumnType(i);
                    if (rs.getObject(i) == null) {
                        insertStmt.append("NULL");
                    } else {
                        switch (columnType) {
                            case Types.INTEGER:
                            case Types.BIGINT:
                            case Types.SMALLINT:
                            case Types.TINYINT:
                                insertStmt.append(rs.getInt(i));
                                break;
                            case Types.FLOAT:
                            case Types.REAL:
                            case Types.DOUBLE:
                                insertStmt.append(rs.getDouble(i));
                                break;
                            case Types.DECIMAL:
                            case Types.NUMERIC:
                                insertStmt.append(rs.getBigDecimal(i));
                                break;
                            case Types.DATE:
                                insertStmt.append("'").append(rs.getDate(i)).append("'");
                                break;
                            case Types.TIME:
                                insertStmt.append("'").append(rs.getTime(i)).append("'");
                                break;
                            case Types.TIMESTAMP:
                                insertStmt.append("'").append(rs.getTimestamp(i)).append("'");
                                break;
                            case Types.BIT:
                                insertStmt.append("0x").append(DatatypeConverter.printHexBinary(rs.getBytes(i)));
                                break;
                            default:
                                insertStmt.append("'").append(rs.getString(i)).append("'");
                        }
                    }

                    if (i < columnCount) {
                        insertStmt.append(", ");
                    }
                }

                insertStmt.append(");");

                insertStmts.add(insertStmt.toString());
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return insertStmts;
    }

}
