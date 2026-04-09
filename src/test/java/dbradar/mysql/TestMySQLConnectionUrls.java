package dbradar.mysql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMySQLConnectionUrls {

    @Test
    public void testDatabaseConnectionUrlDisablesSslForWorkerConnections() {
        String url = MySQLGlobalState.buildDatabaseConnectionUrl("192.168.1.193", 3306, "db0");

        assertEquals(
                "jdbc:mysql://192.168.1.193:3306/db0?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                url,
                "worker connections should use the same non-SSL JDBC options as the bootstrap connection");
    }

    @Test
    public void testServerConnectionUrlDisablesSslForAdminConnections() {
        String url = MySQLGlobalState.buildServerConnectionUrl("192.168.1.193", 3306);

        assertEquals(
                "jdbc:mysql://192.168.1.193:3306?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                url,
                "server-level bootstrap connection should disable SSL and allow public key retrieval");
    }
}
