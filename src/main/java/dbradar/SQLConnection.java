package dbradar;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLConnection implements DatabaseConnection {

    private final Connection connection;
    private final String databaseName;
    private final int threadId;
    private final boolean logGlobalExecution;

    public SQLConnection(Connection connection) {
        this(connection, null, -1, false);
    }

    public SQLConnection(Connection connection, String databaseName, int threadId, boolean logGlobalExecution) {
        this.connection = connection;
        this.databaseName = databaseName;
        this.threadId = threadId;
        this.logGlobalExecution = logGlobalExecution;
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        return meta.getDatabaseProductVersion();
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    public Statement prepareStatement(String sql) throws SQLException {
        return wrapPreparedStatement(connection.prepareStatement(sql), sql);
    }

    public Statement createStatement() throws SQLException {
        return wrapStatement(connection.createStatement());
    }

    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    private Statement wrapStatement(Statement statement) {
        return (Statement) Proxy.newProxyInstance(statement.getClass().getClassLoader(),
                new Class[]{Statement.class}, new LoggingInvocationHandler(statement, null));
    }

    private PreparedStatement wrapPreparedStatement(PreparedStatement statement, String sql) {
        return (PreparedStatement) Proxy.newProxyInstance(statement.getClass().getClassLoader(),
                new Class[]{PreparedStatement.class}, new LoggingInvocationHandler(statement, sql));
    }

    private final class LoggingInvocationHandler implements InvocationHandler {
        private final Statement delegate;
        private final String preparedSql;

        private LoggingInvocationHandler(Statement delegate, String preparedSql) {
            this.delegate = delegate;
            this.preparedSql = preparedSql;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (!isExecutionMethod(method)) {
                return invokeDelegate(method, args);
            }

            String sql = resolveSql(args);
            try {
                Object result = invokeDelegate(method, args);
                log(sql, true, null);
                return result;
            } catch (Throwable throwable) {
                log(sql, false, throwable.getMessage());
                throw throwable;
            }
        }

        private Object invokeDelegate(Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }

        private String resolveSql(Object[] args) {
            if (args != null && args.length > 0 && args[0] instanceof String) {
                return (String) args[0];
            }
            return preparedSql;
        }

        private void log(String sql, boolean success, String errorMessage) {
            if (!logGlobalExecution || sql == null) {
                return;
            }
            StateLogger.logGlobalExecutionEvent(threadId, databaseName, sql, success, errorMessage);
        }
    }

    private static boolean isExecutionMethod(Method method) {
        String name = method.getName();
        return "execute".equals(name) || "executeQuery".equals(name) || "executeUpdate".equals(name)
                || "executeLargeUpdate".equals(name) || "executeBatch".equals(name) || "executeLargeBatch".equals(name);
    }
}
