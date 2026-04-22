package dbradar;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class PostgreSQLOnlyProjectTest {

    private static final Path PROJECT_ROOT = Path.of("").toAbsolutePath();

    private PostgreSQLOnlyProjectTest() {
    }

    public static void main(String[] args) throws Exception {
        rejectsNonPostgreSQLProviders();
        rejectsNonPostgreSQLArtifacts();
        rejectsSchemaGraphArtifacts();
    }

    private static void rejectsNonPostgreSQLProviders() {
        List<DatabaseProvider> providers = Main.getDBMSProviders();
        require(providers.size() == 1, "Expected exactly one provider but found " + providers.size());
        require("postgresql".equals(providers.get(0).getDBMSName()),
                "Expected PostgreSQL provider but found " + providers.get(0).getDBMSName());
    }

    private static void rejectsNonPostgreSQLArtifacts() {
        List<String> forbiddenPaths = List.of(
                "src/main/java/dbradar/mysql",
                "src/main/java/dbradar/mariadb",
                "src/main/java/dbradar/sqlite3",
                "src/main/java/dbradar/tidb",
                "src/main/java/dbradar/cockroachdb",
                "src/main/resources/dbradar/mysql",
                "src/main/resources/dbradar/mariadb",
                "src/main/resources/dbradar/sqlite3",
                "src/main/resources/dbradar/tidb",
                "src/main/resources/dbradar/cockroachdb",
                "src/main/java/dbradar/common/duplicate",
                "src/test/java/dbradar/ddlCheck/TestMySQLEDCOracle.java",
                "src/test/java/dbradar/ddlCheck/TestMariaDBEDCOracle.java",
                "src/test/java/dbradar/ddlCheck/TestSQLite3EDCOracle.java",
                "src/test/java/dbradar/ddlCheck/TestTiDBEDCOracle.java",
                "src/test/java/dbradar/ddlCheck/TestCockroachDBEDCOracle.java",
                "libs/mysql-connector-j-8.3.0.jar",
                "libs/mariadb-java-client-2.7.4.jar",
                "libs/sqlite-jdbc-3.46.1.3.jar",
                "libs/spring-jdbc-6.2.5.jar");

        for (String forbiddenPath : forbiddenPaths) {
            require(!Files.exists(PROJECT_ROOT.resolve(forbiddenPath)), "Forbidden artifact still exists: " + forbiddenPath);
        }
    }

    private static void rejectsSchemaGraphArtifacts() throws IOException {
        require(!Files.exists(PROJECT_ROOT.resolve("src/main/java/dbradar/common/oracle/edc/SchemaGraph.java")),
                "SchemaGraph should be removed");
        require(!Files.exists(PROJECT_ROOT.resolve("libs/jung-algorithms-2.1.1.jar")), "JUNG algorithms jar should be removed");
        require(!Files.exists(PROJECT_ROOT.resolve("libs/jung-graph-impl-2.1.1.jar")), "JUNG graph jar should be removed");
        require(!Files.exists(PROJECT_ROOT.resolve("libs/jung-visualization-2.1.1.jar")),
                "JUNG visualization jar should be removed");

        try (var files = Files.walk(PROJECT_ROOT.resolve("src/main/java"))) {
            files.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".java"))
                    .forEach(PostgreSQLOnlyProjectTest::rejectGraphReferences);
        }
    }

    private static void rejectGraphReferences(Path path) {
        try {
            String text = Files.readString(path);
            require(!text.contains("SchemaGraph"), "SchemaGraph reference remains in " + PROJECT_ROOT.relativize(path));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
