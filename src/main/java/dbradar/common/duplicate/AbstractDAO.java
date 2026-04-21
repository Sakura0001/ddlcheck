package dbradar.common.duplicate;

import org.apache.commons.io.IOUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public abstract class AbstractDAO {
    private static JdbcTemplate jdbcTemplate = null;

    public AbstractDAO() {
        if (jdbcTemplate == null) {
            init();
        }
    }

    protected JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    private static void init() {
        File dbDir = new File("." + File.separator + "report");
        if (!dbDir.exists()) {
            if (dbDir.mkdir()) {
                System.out.println("Create a SQLite database at " + dbDir.getAbsolutePath());
            } else {
                throw new AssertionError("Fail to create the directory");
            }
        }
        File dbFile = new File(dbDir, "report.db");
        boolean dbExist = dbFile.exists();

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:" + dbFile.getAbsolutePath());
        jdbcTemplate = new JdbcTemplate(dataSource);

        if (!dbExist) {
            System.out.println("Initialize a new database.");
            try (InputStream deploy = AbstractDAO.class.getClassLoader()
                    .getResourceAsStream("dbradar/deploy/sqlite.sql")) {
                if (deploy == null) {
                    throw new AssertionError("No SQLite schema file for initialization.");
                }
                String sql = IOUtils.toString(deploy, String.valueOf(StandardCharsets.UTF_8));
                sql = sql.replaceAll("(\\r\\n|\\n|\\\\n)", "");
                for (String query : sql.split("=======")) {
                    jdbcTemplate.execute(query);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            System.out.println("Use an existing database.");
        }
    }
}
