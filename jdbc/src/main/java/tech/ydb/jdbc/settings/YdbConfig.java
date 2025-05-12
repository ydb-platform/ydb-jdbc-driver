package tech.ydb.jdbc.settings;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import tech.ydb.core.utils.URITools;
import tech.ydb.jdbc.YdbConst;




/**
 *
 * @author Aleksandr Gorshenin
 */
// TODO: implement cache based on connection and client properties only (excluding operation properties)
public class YdbConfig {
    static final String TOKEN_KEY = "token";
    static final String USERNAME_KEY = "user";
    static final String PASSWORD_KEY = "password";

    static final YdbProperty<Boolean> CACHE_CONNECTIONS_IN_DRIVER = YdbProperty.bool(
            "cacheConnectionsInDriver",
            "Cache YDB connections in YdbDriver, cached by combination or url and properties",
            true
    );
    static final YdbProperty<Integer> PREPARED_STATEMENT_CACHE_SIZE = YdbProperty.integer(
            "preparedStatementCacheQueries",
            "Specifies the maximum number of entries in per-transport cache of prepared statements. A value of "
                    + "{@code 0} disables the cache.", 256
    );
    static final YdbProperty<Boolean> USE_QUERY_SERVICE = YdbProperty.bool("useQueryService",
            "Use QueryService instead of TableService", true
    );

    static final YdbProperty<String> USE_PREFIX_PATH = YdbProperty.string("usePrefixPath",
            "Add prefix path to all operations performed by driver");

    static final YdbProperty<Boolean> FULLSCAN_DETECTOR_ENABLED = YdbProperty.bool(
            "jdbcFullScanDetector", "Enable analizator for collecting query stats", false
    );
    static final YdbProperty<Boolean> TRANSACTION_TRACER = YdbProperty.bool(
            "enableTxTracer", "Enable collecting of transaction execution traces", false
    );
    static final YdbProperty<Integer> CACHED_TRANSPORT_COUNT = YdbProperty.integer(
            "cachedTransportsCount", "Use specified count of YDB transports in context cache", 1
    );

    private final String url;
    private final String username;
    private final String password;

    private final String safeUrl;
    private final String connectionString;

    private final Properties properties;
    private final boolean isCacheConnectionsInDriver;
    private final int preparedStatementsCacheSize;

    private final boolean useQueryService;
    private final YdbValue<String> usePrefixPath;

    private final boolean fullScanDetectorEnabled;
    private final boolean txTracerEnabled;
    private final int transportIndex;

    private YdbConfig(
            String url, String safeUrl, String connectionString, String username, String password, Properties props
    ) throws SQLException {
        this.url = url;
        this.username = username;
        this.password = password;
        this.safeUrl = safeUrl;
        this.connectionString = connectionString;
        this.properties = props;
        this.isCacheConnectionsInDriver = CACHE_CONNECTIONS_IN_DRIVER.readValue(props).getValue();
        this.preparedStatementsCacheSize = Math.max(0, PREPARED_STATEMENT_CACHE_SIZE.readValue(props).getValue());

        this.useQueryService = USE_QUERY_SERVICE.readValue(props).getValue();
        this.usePrefixPath = USE_PREFIX_PATH.readValue(props);

        this.fullScanDetectorEnabled = FULLSCAN_DETECTOR_ENABLED.readValue(props).getValue();
        this.txTracerEnabled = TRANSACTION_TRACER.readValue(props).getValue();

        int transportsCount = CACHED_TRANSPORT_COUNT.readValue(props).getValue();
        if (transportsCount > 1) {
            this.transportIndex = ThreadLocalRandom.current().nextInt(transportsCount);
        } else {
            this.transportIndex = 0;
        }
    }

    public Properties getSafeProps() {
        Properties safe = new Properties();
        for (String key: properties.stringPropertyNames()) {
            if (isSensetive(key)) {
                safe.put(key, "***");
            } else {
                safe.put(key, properties.get(key));
            }
        }
        return safe;
    }

    public String getConnectionString() {
        return this.connectionString;
    }

    public boolean isCacheConnectionsInDriver() {
        return this.isCacheConnectionsInDriver;
    }

    public int getPreparedStatementsCachecSize() {
        return this.preparedStatementsCacheSize;
    }

    public boolean isUseQueryService() {
        return this.useQueryService;
    }

    public boolean hasPrefixPath() {
        return usePrefixPath.hasValue();
    }

    public String getPrefixPath() {
        return usePrefixPath.getValue();
    }

    public boolean isFullScanDetectorEnabled() {
        return fullScanDetectorEnabled;
    }

    public boolean isTxTracedEnabled() {
        return txTracerEnabled;
    }

    static boolean isSensetive(String key) {
        return TOKEN_KEY.equalsIgnoreCase(key)  || PASSWORD_KEY.equalsIgnoreCase(key);
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
        return Objects.equals(url, that.url)
                && Objects.equals(properties, that.properties)
                && transportIndex == that.transportIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, properties, transportIndex);
    }

    public String getUrl() {
        return url;
    }

    public String getSafeUrl() {
        return safeUrl;
    }

    public String getUsername() {
        return username;
    }

    String getPassword() {
        return password;
    }

    Properties getProperties() {
        return properties;
    }

    public DriverPropertyInfo[] toPropertyInfo() throws SQLException {
        return new DriverPropertyInfo[] {
            YdbConfig.CACHE_CONNECTIONS_IN_DRIVER.toInfo(properties),
            YdbConfig.PREPARED_STATEMENT_CACHE_SIZE.toInfo(properties),
            YdbConfig.USE_QUERY_SERVICE.toInfo(properties),
            YdbConfig.USE_PREFIX_PATH.toInfo(properties),

            YdbConnectionProperties.LOCAL_DATACENTER.toInfo(properties),
            YdbConnectionProperties.USE_SECURE_CONNECTION.toInfo(properties),
            YdbConnectionProperties.SECURE_CONNECTION_CERTIFICATE.toInfo(properties),
            YdbConnectionProperties.TOKEN.toInfo(properties),
            YdbConnectionProperties.TOKEN_FILE.toInfo(properties),
            YdbConnectionProperties.SA_KEY_FILE.toInfo(properties),
            YdbConnectionProperties.USE_METADATA.toInfo(properties),
            YdbConnectionProperties.IAM_ENDPOINT.toInfo(properties),
            YdbConnectionProperties.METADATA_URL.toInfo(properties),
            YdbConnectionProperties.TOKEN_PROVIDER.toInfo(properties),
            YdbConnectionProperties.GRPC_COMPRESSION.toInfo(properties),

            YdbClientProperties.KEEP_QUERY_TEXT.toInfo(properties),
            YdbClientProperties.SESSION_KEEP_ALIVE_TIME.toInfo(properties),
            YdbClientProperties.SESSION_MAX_IDLE_TIME.toInfo(properties),
            YdbClientProperties.SESSION_POOL_SIZE_MIN.toInfo(properties),
            YdbClientProperties.SESSION_POOL_SIZE_MAX.toInfo(properties),

            YdbOperationProperties.USE_STREAM_RESULT_SETS.toInfo(properties),
            YdbOperationProperties.JOIN_DURATION.toInfo(properties),
            YdbOperationProperties.QUERY_TIMEOUT.toInfo(properties),
            YdbOperationProperties.SCAN_QUERY_TIMEOUT.toInfo(properties),
            YdbOperationProperties.FAIL_ON_TRUNCATED_RESULT.toInfo(properties),
            YdbOperationProperties.SESSION_TIMEOUT.toInfo(properties),
            YdbOperationProperties.DEADLINE_TIMEOUT.toInfo(properties),
            YdbOperationProperties.AUTOCOMMIT.toInfo(properties),
            YdbOperationProperties.TRANSACTION_LEVEL.toInfo(properties),
            YdbOperationProperties.SCHEME_QUERY_TX_MODE.toInfo(properties),
            YdbOperationProperties.SCAN_QUERY_TX_MODE.toInfo(properties),
            YdbOperationProperties.BULK_QUERY_TX_MODE.toInfo(properties),

            YdbQueryProperties.DISABLE_PREPARE_DATAQUERY.toInfo(properties),
            YdbQueryProperties.DISABLE_AUTO_PREPARED_BATCHES.toInfo(properties),
            YdbQueryProperties.DISABLE_DETECT_SQL_OPERATIONS.toInfo(properties),
            YdbQueryProperties.DISABLE_JDBC_PARAMETERS.toInfo(properties),
            YdbQueryProperties.DISABLE_JDBC_PARAMETERS_DECLARE.toInfo(properties),
            YdbQueryProperties.FORCE_JDBC_PARAMETERS.toInfo(properties),
            YdbQueryProperties.REPLACE_JDBC_IN_BY_YQL_LIST.toInfo(properties),

            YdbQueryProperties.REPLACE_INSERT_TO_UPSERT.toInfo(properties),
            YdbQueryProperties.FORCE_BULK_UPSERT.toInfo(properties),
            YdbQueryProperties.FORCE_SCAN_SELECT.toInfo(properties),
        };
    }

    public static boolean isYdb(String url) {
        return url.startsWith(YdbConst.JDBC_YDB_PREFIX);
    }

    public static YdbConfig from(String jdbcURL, Properties origin) throws SQLException {
        if (!isYdb(jdbcURL)) {
            String msg = "[" + jdbcURL + "] is not a YDB URL, must starts from " + YdbConst.JDBC_YDB_PREFIX;
            throw new SQLException(msg);
        }

        try {
            String ydbURL = jdbcURL.substring(YdbConst.JDBC_YDB_PREFIX.length());
            String connectionString = ydbURL;
            String safeURL = ydbURL;

            Properties properties = new Properties();
            String username = null;
            String password = null;

            if (origin != null) {
                properties.putAll(origin);
                username = origin.getProperty(YdbConfig.USERNAME_KEY);
                password = origin.getProperty(YdbConfig.PASSWORD_KEY);
            }

            if (!ydbURL.isEmpty()) {
                URI url = new URI(ydbURL.contains("://") ? ydbURL : "grpc://" + ydbURL);
                Map<String, List<String>> params = URITools.splitQuery(url);

                String userInfo = url.getUserInfo();
                if (username == null && userInfo != null) {
                    String[] parsed = userInfo.split(":", 2);
                    if (parsed.length > 0) {
                        username = parsed[0];
                    }
                    if (parsed.length > 1) {
                        password = parsed[1];
                    }
                }

                String database = url.getPath();

                // merge properties and query params
                for (Map.Entry<String, List<String>> entry: params.entrySet()) {
                    String value = entry.getValue().get(entry.getValue().size() - 1);
                    properties.put(entry.getKey(), value);
                    if ("database".equalsIgnoreCase(entry.getKey())) {
                        if (database == null || database.isEmpty()) {
                            database = value.startsWith("/") ? value : "/" + value;
                        }
                    }
                }
                StringBuilder sb = new StringBuilder();
                sb.append(url.getScheme()).append("://");
                sb.append(url.getHost());
                if (url.getPort() > 0) {
                    sb.append(":").append(url.getPort());
                }
                sb.append(database);

                connectionString = sb.toString();

                if (!params.isEmpty()) {
                    String prefix = "?";
                    for (Map.Entry<String, List<String>> entry: params.entrySet()) {
                        String value = entry.getValue().get(entry.getValue().size() - 1);
                        if (YdbConfig.isSensetive(entry.getKey())) {
                            value = "***";
                        }
                        if (value != null && !value.isEmpty()) {
                            sb.append(prefix);
                            sb.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                            sb.append("=");
                            sb.append(URLEncoder.encode(value, "UTF-8"));
                            prefix = "&";
                        }
                    }
                }

                safeURL = sb.toString();
            }

            return new YdbConfig(jdbcURL, safeURL, connectionString, username, password, properties);
        } catch (URISyntaxException | RuntimeException | UnsupportedEncodingException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }
}
