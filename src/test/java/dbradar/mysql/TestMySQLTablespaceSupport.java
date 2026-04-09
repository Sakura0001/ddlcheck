package dbradar.mysql;

import dbradar.MainOptions;
import dbradar.SQLConnection;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.KeyFunc;
import dbradar.common.query.generator.QueryGenerationException;
import grammar.Token;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMySQLTablespaceSupport {

    @Test
    public void testExistTablespaceKeyFuncReadsDstoreTablespaces() {
        FakeMySQLState state = new FakeMySQLState();
        state.setMainOptions(new MainOptions());
        state.setConnection(new SQLConnection(createConnectionProxy(List.of(
                Map.of("name", "ts_alpha"),
                Map.of("name", "ts_beta")
        ))));

        MySQLKeyFuncManager keyFuncManager = new MySQLKeyFuncManager(state);
        KeyFunc keyFunc = keyFuncManager.getFuncByKey("_exist_tablespace");
        ASTNode parent = new ASTNode(new Token(Token.TokenType.KEYWORD, "_exist_tablespace"));

        keyFunc.generateAST(parent);

        String tablespace = parent.toQueryString();
        assertTrue(List.of("ts_alpha", "ts_beta").contains(tablespace),
                "tablespace key function should emit a fetched dstore tablespace name, but was: " + tablespace);
    }

    @Test
    public void testTablespaceNameKeyFuncUsesExpectedPrefix() {
        FakeMySQLState state = new FakeMySQLState();
        state.setMainOptions(new MainOptions());

        MySQLKeyFuncManager keyFuncManager = new MySQLKeyFuncManager(state);
        KeyFunc keyFunc = keyFuncManager.getFuncByKey("_tablespace_name");
        ASTNode parent = new ASTNode(new Token(Token.TokenType.KEYWORD, "_tablespace_name"));

        keyFunc.generateAST(parent);

        String generatedName = parent.toQueryString();
        assertTrue(generatedName.startsWith("ts_"),
                "tablespace names should use the ts_ prefix, but was: " + generatedName);
    }

    @Test
    public void testExistTablespaceKeyFuncFailsFastWhenGateIsSaturated() {
        FakeMySQLState state = new FakeMySQLState();
        state.setMainOptions(new MainOptions());
        state.setConnection(new SQLConnection(createConnectionProxy(List.of(Map.of("name", "ts_alpha")))));

        MySQLTablespaceGate.resetForTests();
        List<MySQLTablespaceGate.GateLease> leases = new java.util.ArrayList<>();
        try {
            for (int i = 0; i < MySQLTablespaceGate.getMaxConcurrency(); i++) {
                MySQLTablespaceGate.GateLease lease = MySQLTablespaceGate.tryAcquire(
                        "SELECT name FROM INFORMATION_SCHEMA.DSTORE_TABLESPACES WHERE name LIKE 'ts\\_%' ESCAPE '\\\\'");
                assertTrue(lease.isAcquired(), "test setup should saturate the tablespace gate");
                leases.add(lease);
            }

            MySQLKeyFuncManager keyFuncManager = new MySQLKeyFuncManager(state);
            KeyFunc keyFunc = keyFuncManager.getFuncByKey("_exist_tablespace");
            ASTNode parent = new ASTNode(new Token(Token.TokenType.KEYWORD, "_exist_tablespace"));

            org.junit.jupiter.api.Assertions.assertThrows(QueryGenerationException.class,
                    () -> keyFunc.generateAST(parent),
                    "_exist_tablespace should fail fast when the shared tablespace gate is saturated");
        } finally {
            for (MySQLTablespaceGate.GateLease lease : leases) {
                lease.close();
            }
            MySQLTablespaceGate.resetForTests();
        }
    }

    private Connection createConnectionProxy(List<Map<String, String>> rows) {
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                new StatementHandler(rows));

        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "createStatement":
                    return statement;
                case "close":
                    return null;
                case "isClosed":
                    return false;
                case "unwrap":
                    return null;
                case "isWrapperFor":
                    return false;
                default:
                    throw new UnsupportedOperationException("Unsupported Connection method: " + method.getName());
            }
        };
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                handler);
    }

    private static final class StatementHandler implements InvocationHandler {
        private final List<Map<String, String>> rows;

        private StatementHandler(List<Map<String, String>> rows) {
            this.rows = rows;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "executeQuery":
                    assertEquals(
                            "SELECT name FROM INFORMATION_SCHEMA.DSTORE_TABLESPACES WHERE name LIKE 'ts\\_%' ESCAPE '\\\\'",
                            args[0],
                            "tablespace key function should query the dstore tablespace catalog");
                    return Proxy.newProxyInstance(
                            ResultSet.class.getClassLoader(),
                            new Class<?>[]{ResultSet.class},
                            new ResultSetHandler(rows));
                case "close":
                    return null;
                case "unwrap":
                    return null;
                case "isWrapperFor":
                    return false;
                default:
                    throw new UnsupportedOperationException("Unsupported Statement method: " + method.getName());
            }
        }
    }

    private static final class ResultSetHandler implements InvocationHandler {
        private final List<Map<String, String>> rows;
        private int index = -1;

        private ResultSetHandler(List<Map<String, String>> rows) {
            this.rows = rows;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "next":
                    index++;
                    return index < rows.size();
                case "getString":
                    return rows.get(index).get(String.valueOf(args[0]));
                case "close":
                    return null;
                case "wasNull":
                    return false;
                case "unwrap":
                    return null;
                case "isWrapperFor":
                    return false;
                default:
                    throw new UnsupportedOperationException("Unsupported ResultSet method: " + method.getName());
            }
        }
    }

    private static final class FakeMySQLState extends MySQLGlobalState {
    }
}
