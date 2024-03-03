package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

import tech.ydb.jdbc.settings.YdbClientProperties;
import tech.ydb.jdbc.settings.YdbConnectionProperties;
import tech.ydb.jdbc.settings.YdbConnectionProperty;
import tech.ydb.jdbc.settings.YdbJdbcTools;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.jdbc.settings.YdbProperties;

/**
 *
 * @author Aleksandr Gorshenin
 */
// TODO: implement cache based on connection and client properties only (excluding operation properties)
public class YdbConfig {
    private final String url;
    private final Properties properties;
    private final YdbProperties config;

    public YdbConfig(String url, Properties properties) throws SQLException {
        this.url = url;
        this.properties = properties;
        this.config = YdbJdbcTools.from(url, properties);
    }

    public Properties getSafeProps() {
        Properties safe = new Properties();
        for (String key: properties.stringPropertyNames()) {
            if (isSensitive(key)) {
                safe.put(key, "***");
            } else {
                safe.put(key, properties.get(key));
            }
        }
        return safe;
    }

    private static boolean isSensitive(String key) {
        return YdbConnectionProperty.TOKEN.getName().equalsIgnoreCase(key)
                || "password".equalsIgnoreCase(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YdbConfig)) {
            return false;
        }
        YdbConfig that = (YdbConfig) o;
        return Objects.equals(url, that.url) && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, properties);
    }

    public String getUrl() {
        return url;
    }

    public String getSafeUrl() {
        return config.getConnectionProperties().getSafeUrl();
    }

    public YdbConnectionProperties getConnectionProperties() {
        return config.getConnectionProperties();
    }

    public YdbClientProperties getClientProperties() {
        return config.getClientProperties();
    }

    public YdbOperationProperties getOperationProperties() {
        return config.getOperationProperties();
    }
}
