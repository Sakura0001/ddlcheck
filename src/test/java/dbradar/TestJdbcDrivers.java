package dbradar;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcDrivers {

    @Test
    public void testEnsureDriverLoadedRegistersPostgreSQLDriverWithoutSpiMetadata() throws Exception {
        String url = "jdbc:postgresql://localhost:5432/test";
        List<Driver> removedDrivers = new ArrayList<>();
        deregisterDriversAccepting(url, removedDrivers);

        try {
            Assertions.assertThrows(SQLException.class, () -> DriverManager.getDriver(url),
                    "postgresql URL should not have a registered driver after deregistration");

            JdbcDrivers.ensureDriverLoaded(url);

            Driver driver = DriverManager.getDriver(url);
            Assertions.assertEquals("org.postgresql.Driver", driver.getClass().getName(),
                    "explicit driver loading should restore the postgresql JDBC driver");
        } finally {
            deregisterDriversAccepting(url, new ArrayList<>());
            for (Driver driver : removedDrivers) {
                DriverManager.registerDriver(driver);
            }
        }
    }

    private static void deregisterDriversAccepting(String url, List<Driver> removedDrivers) throws SQLException {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            try {
                if (driver.acceptsURL(url)) {
                    DriverManager.deregisterDriver(driver);
                    removedDrivers.add(driver);
                }
            } catch (SQLException ignored) {
                // Ignore drivers that reject URL probing and leave them untouched.
            }
        }
    }
}
