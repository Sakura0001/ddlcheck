package dbradar;

import java.lang.reflect.InvocationTargetException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;

public final class JdbcDrivers {

    private JdbcDrivers() {
    }

    public static void ensureDriverLoaded(String jdbcUrl) throws SQLException {
        if (jdbcUrl.startsWith("jdbc:postgresql:")) {
            loadDriverClass("org.postgresql.Driver", jdbcUrl);
        }
    }

    private static void loadDriverClass(String className, String jdbcUrl) throws SQLException {
        try {
            if (isDriverRegistered(className)) {
                return;
            }
            Class<?> driverClass = Class.forName(className);
            Driver driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
            DriverManager.registerDriver(driver);
        } catch (ClassNotFoundException e) {
            throw new SQLException("Required JDBC driver not found for URL " + jdbcUrl + ": " + className, e);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                 | NoSuchMethodException e) {
            throw new SQLException("Failed to initialize JDBC driver for URL " + jdbcUrl + ": " + className, e);
        }
    }

    private static boolean isDriverRegistered(String className) {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            if (drivers.nextElement().getClass().getName().equals(className)) {
                return true;
            }
        }
        return false;
    }
}
