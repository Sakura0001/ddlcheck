package dbradar;

public interface DatabaseConnection extends AutoCloseable {

    String getDatabaseVersion() throws Exception;
}
