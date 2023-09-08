package tech.ydb.jdbc.settings;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ydb.core.auth.StaticCredentials;


public class YdbConnectionProperties {
    private final String safeURL;
    private final String connectionString;
    private final String username;
    private final String password;
    private final Map<YdbConnectionProperty<?>, ParsedProperty> params;

    public YdbConnectionProperties(String safeURL, String connectionString, String username, String password,
                                   Map<YdbConnectionProperty<?>, ParsedProperty> params) {
        this.safeURL = safeURL;
        this.connectionString = Objects.requireNonNull(connectionString);
        this.username = username;
        this.password = password;
        this.params = Objects.requireNonNull(params);
    }

    public String getSafeUrl() {
        return safeURL;
    }

    public String getConnectionString() {
        return connectionString;
    }

    @Nullable
    public ParsedProperty getProperty(YdbConnectionProperty<?> property) {
        return params.get(property);
    }

    public Map<YdbConnectionProperty<?>, ParsedProperty> getParams() {
        return params;
    }

    public boolean hasStaticCredentials() {
        return username != null && !username.isEmpty();
    }

    public StaticCredentials getStaticCredentials() {
        return new StaticCredentials(username, password);
    }
}
