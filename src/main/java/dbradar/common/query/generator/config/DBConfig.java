package dbradar.common.query.generator.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DBConfig {

    private static final String DEFAULT_CONFIG_PATH = "dbradar/common/default.zz.lua";

    private static DBConfig dbConfig = null;

    private String configFile;

    private Map<String, List<String>> dataConfig = new HashMap<>();

    private DBConfig(String configFile) {
        this.configFile = configFile;
    }

    public Map<String, List<String>> getDataConfig() {
        return dataConfig;
    }

    public static void initDBConfig(String configPath) {
        dbConfig = new DBConfig(configPath);
        dbConfig.parse();
    }

    public static DBConfig getDBConfig() {
        if (dbConfig == null)
            initDBConfig(DEFAULT_CONFIG_PATH);
        return dbConfig;
    }

    private void parse() {
        DBConfig defaultConfig = loadDefaultConfig();

        internalParse();
        // TODO: remove default

        for (String prop : defaultConfig.dataConfig.keySet()) {
            if (dataConfig.get(prop) == null) {
                dataConfig.put(prop, defaultConfig.dataConfig.get(prop));
            }
        }
    }

    private DBConfig loadDefaultConfig() {
        DBConfig defaultConfig = new DBConfig(DEFAULT_CONFIG_PATH);
        defaultConfig.internalParse();
        return defaultConfig;
    }

    private void internalParse() {
        DBConfigParser parser;
        parser = new DBConfigParser(Objects.requireNonNullElse(configFile, DEFAULT_CONFIG_PATH));
        dataConfig = parser.extractConfig("data");
    }
}
