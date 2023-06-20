package tech.ydb.jdbc;

import java.io.File;
import java.io.IOException;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.io.Files;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import tech.ydb.auth.AuthProvider;
import tech.ydb.jdbc.exception.YdbConfigurationException;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.settings.ParsedProperty;
import tech.ydb.jdbc.settings.YdbClientProperty;
import tech.ydb.jdbc.settings.YdbConnectionProperties;
import tech.ydb.jdbc.settings.YdbConnectionProperty;
import tech.ydb.jdbc.settings.YdbJdbcTools;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.jdbc.settings.YdbOperationProperty;
import tech.ydb.jdbc.settings.YdbProperties;

public class YdbDriverProperitesTest {
    private static final Logger logger = Logger.getLogger(YdbDriverProperitesTest.class.getName());
    public static final String TOKEN_FROM_FILE = "token-from-file";
    public static final String CERTIFICATE_FROM_FILE = "certificate-from-file";

    private static File TOKEN_FILE;
    private static File CERTIFICATE_FILE;

    private YdbDriver driver;

    @BeforeAll
    public static void beforeAll() throws YdbConfigurationException, IOException {
        TOKEN_FILE = safeCreateFile(TOKEN_FROM_FILE);
        CERTIFICATE_FILE = safeCreateFile(CERTIFICATE_FROM_FILE);
    }

    @AfterAll
    public static void afterAll() throws SQLException {
        safeDeleteFile(TOKEN_FILE);
        safeDeleteFile(CERTIFICATE_FILE);
    }

    @BeforeEach
    public void beforeEach() {
        driver = new YdbDriver();
    }

    @AfterEach
    public void afterEach() {
        driver.close();
    }

    @Test
    public void connectToUnsupportedUrl() throws SQLException {
        Assertions.assertNull(driver.connect("jdbc:clickhouse:localhost:123", new Properties()));
    }

    @ParameterizedTest
    @MethodSource("urlsToParse")
    public void parseURL(String url, @Nullable String connectionString, @Nullable String localDatacenter)
            throws SQLException {
        YdbProperties props = YdbJdbcTools.from(url, new Properties());
        YdbConnectionProperties connectionProperties = props.getConnectionProperties();
        Assertions.assertEquals(connectionString, connectionProperties.getConnectionString());

        ParsedProperty dcProperty = connectionProperties.getParams().get(YdbConnectionProperty.LOCAL_DATACENTER);
        Assertions.assertEquals(localDatacenter, Optional.ofNullable(dcProperty)
                .map(ParsedProperty::getParsedValue)
                .orElse(null));

    }

    @ParameterizedTest
    @MethodSource("urlsToCheck")
    public void acceptsURL(String url, boolean accept, String connectionString) throws SQLException {
        Assertions.assertEquals(accept, driver.acceptsURL(url));
        DriverPropertyInfo[] properties = driver.getPropertyInfo(url, new Properties());
        Assertions.assertNotNull(properties);

        if (accept) {
            YdbProperties ydbProperties = YdbJdbcTools.from(url, new Properties());
            Assertions.assertNotNull(ydbProperties);
            Assertions.assertEquals(connectionString, ydbProperties.getConnectionProperties().getConnectionString());
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void getPropertyInfoDefault() throws SQLException {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/pr/testing/ci";

        Properties properties = new Properties();
        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, properties);
        Assertions.assertEquals(new Properties(), properties);

        List<String> actual = convertPropertyInfo(propertyInfo);
        actual.forEach(logger::info);

        List<String> expect = convertPropertyInfo(defaultPropertyInfo());
        Assertions.assertEquals(expect, actual);

        YdbProperties ydbProperties = YdbJdbcTools.from(url, properties);
        Assertions.assertEquals("grpc://ydb-demo.testhost.org:2135/test/pr/testing/ci",
                ydbProperties.getConnectionProperties().getConnectionString());
    }

    @Test
    public void getPropertyInfoAllFromUrl() throws SQLException {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/pr/testing/ci?" +
                customizedProperties().entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining("&"));

        Properties properties = new Properties();
        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, properties);
        Assertions.assertEquals(new Properties(), properties);

        List<String> actual = convertPropertyInfo(propertyInfo);
        actual.forEach(logger::info);

        List<String> expect = convertPropertyInfo(customizedPropertyInfo());
        Assertions.assertEquals(expect, actual);

        YdbProperties ydbProperties = YdbJdbcTools.from(url, properties);
        checkCustomizedProperties(ydbProperties);
    }

    @Test
    public void getPropertyInfoFromProperties() throws SQLException {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/pr/testing/ci";

        Properties properties = customizedProperties();
        Properties copy = new Properties();
        copy.putAll(properties);

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, properties);
        Assertions.assertEquals(copy, properties);

        List<String> actual = convertPropertyInfo(propertyInfo);
        actual.forEach(logger::info);

        List<String> expect = convertPropertyInfo(customizedPropertyInfo());
        Assertions.assertEquals(expect, actual);

        YdbProperties ydbProperties = YdbJdbcTools.from(url, properties);
        checkCustomizedProperties(ydbProperties);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void getPropertyInfoOverwrite() throws SQLException {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/testing/ydb?localDatacenter=sas";
        Properties properties = new Properties();
        properties.put("localDatacenter", "vla");

        Properties copy = new Properties();
        copy.putAll(properties);

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, properties);
        Assertions.assertEquals(copy, properties);

        List<String> actual = convertPropertyInfo(propertyInfo);
        actual.forEach(logger::info);

        // URL will always overwrite properties
        List<String> expect = convertPropertyInfo(defaultPropertyInfo("sas"));
        Assertions.assertEquals(expect, actual);

        YdbProperties ydbProperties = YdbJdbcTools.from(url, properties);
        Assertions.assertEquals("grpc://ydb-demo.testhost.org:2135/testing/ydb",
                ydbProperties.getConnectionProperties().getConnectionString());
    }

    @ParameterizedTest(name = "[{index}] {1}")
    @MethodSource("tokensToCheck")
    public void getTokenAs(String token, String expectValue) throws SQLException {
        if ("file:".equals(token)) {
            token += TOKEN_FILE.getAbsolutePath();
        }

        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/pr/testing/ci?token=" + token;
        Properties properties = new Properties();
        YdbProperties ydbProperties = YdbJdbcTools.from(url, properties);

        YdbConnectionProperties props = ydbProperties.getConnectionProperties();
        Assertions.assertEquals(expectValue, ((AuthProvider)props.getProperty(YdbConnectionProperty.TOKEN).getParsedValue())
                .createAuthIdentity(null).getToken());
    }

    @ParameterizedTest
    @MethodSource("unknownFiles")
    public void getTokenAsInvalid(String token, String expectException) {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/pr/testing/ci?token=" + token;
        ExceptionAssert.ydbConfiguration(expectException, () -> YdbJdbcTools.from(url, new Properties()));
    }

    @ParameterizedTest(name = "[{index}] {1}")
    @MethodSource("certificatesToCheck")
    public void getCaCertificateAs(String certificate, String expectValue) throws SQLException {
        if ("file:".equals(certificate)) {
            certificate += CERTIFICATE_FILE.getAbsolutePath();
        }
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/pr/testing/ci" +
                "?secureConnectionCertificate=" + certificate;
        Properties properties = new Properties();
        YdbProperties ydbProperties = YdbJdbcTools.from(url, properties);

        YdbConnectionProperties props = ydbProperties.getConnectionProperties();
        Assertions.assertArrayEquals(expectValue.getBytes(),
                props.getProperty(YdbConnectionProperty.SECURE_CONNECTION_CERTIFICATE).getParsedValue());
    }

    @ParameterizedTest
    @MethodSource("unknownFiles")
    public void getCaCertificateAsInvalid(String certificate, String expectException) {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/pr/testing/ci" +
                "?secureConnectionCertificate=" + certificate;
        ExceptionAssert.ydbConfiguration(expectException, () -> YdbJdbcTools.from(url, new Properties()));
    }

    @ParameterizedTest
    @MethodSource("invalidDurationParams")
    public void invalidDuration(String param) {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/pr/testing/ci?" + param + "=1bc";
        ExceptionAssert.ydbConfiguration("Unable to convert property " + param +
                        ": Unable to parse value [1bc] -> [PT1BC] as Duration: Text cannot be parsed to a Duration",
                () -> YdbJdbcTools.from(url, new Properties()));
    }

    @ParameterizedTest
    @MethodSource("invalidIntegerParams")
    public void invalidInteger(String param) {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/pr/testing/ci?" + param + "=1bc";
        ExceptionAssert.ydbConfiguration("Unable to convert property " + param +
                        ": Unable to parse value [1bc] as Integer: For input string: \"1bc\"",
                () -> YdbJdbcTools.from(url, new Properties()));
    }

    @Test
    @Disabled
    public void invalidAuthProviderProperty() {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/pr/testing/ci?authProvider=test";
        ExceptionAssert.ydbConfiguration("Unable to convert property authProvider: " +
                        "Property authProvider must be configured with object, not a string",
                () -> YdbJdbcTools.from(url, new Properties()));
    }

    @Test
    public void getMajorVersion() {
        Assertions.assertEquals(2, driver.getMajorVersion());
    }

    @Test
    public void getMinorVersion() {
        Assertions.assertTrue(driver.getMinorVersion() >= 0);
    }

    @Test
    public void jdbcCompliant() {
        Assertions.assertFalse(driver.jdbcCompliant());
    }

    @Test
    public void getParentLogger() throws SQLFeatureNotSupportedException {
        Assertions.assertNotNull(driver.getParentLogger());
    }

    static List<String> convertPropertyInfo(DriverPropertyInfo[] propertyInfo) {
        return Stream.of(propertyInfo)
                .map(YdbDriverProperitesTest::asString)
                .collect(Collectors.toList());
    }

    static DriverPropertyInfo[] defaultPropertyInfo() {
        return defaultPropertyInfo(null);
    }

    static DriverPropertyInfo[] defaultPropertyInfo(@Nullable String localDatacenter) {
        return new DriverPropertyInfo[]{
                YdbConnectionProperty.LOCAL_DATACENTER.toDriverPropertyInfo(localDatacenter),
                YdbConnectionProperty.SECURE_CONNECTION.toDriverPropertyInfo(null),
                YdbConnectionProperty.SECURE_CONNECTION_CERTIFICATE.toDriverPropertyInfo(null),
                YdbConnectionProperty.TOKEN.toDriverPropertyInfo(null),

                YdbConnectionProperty.SERVICE_ACCOUNT_FILE.toDriverPropertyInfo(null),
                YdbConnectionProperty.USE_METADATA.toDriverPropertyInfo(null),

                YdbClientProperty.KEEP_QUERY_TEXT.toDriverPropertyInfo(null),
                YdbClientProperty.SESSION_KEEP_ALIVE_TIME.toDriverPropertyInfo(null),
                YdbClientProperty.SESSION_MAX_IDLE_TIME.toDriverPropertyInfo(null),
                YdbClientProperty.SESSION_POOL_SIZE_MIN.toDriverPropertyInfo(null),
                YdbClientProperty.SESSION_POOL_SIZE_MAX.toDriverPropertyInfo(null),

                YdbOperationProperty.JOIN_DURATION.toDriverPropertyInfo("5m"),
                YdbOperationProperty.QUERY_TIMEOUT.toDriverPropertyInfo("0s"),
                YdbOperationProperty.SCAN_QUERY_TIMEOUT.toDriverPropertyInfo("5m"),
                YdbOperationProperty.FAIL_ON_TRUNCATED_RESULT.toDriverPropertyInfo("false"),
                YdbOperationProperty.SESSION_TIMEOUT.toDriverPropertyInfo("5s"),
                YdbOperationProperty.DEADLINE_TIMEOUT.toDriverPropertyInfo("0s"),
                YdbOperationProperty.AUTOCOMMIT.toDriverPropertyInfo("true"),
                YdbOperationProperty.TRANSACTION_LEVEL.toDriverPropertyInfo("8"),

                YdbOperationProperty.AUTO_PREPARED_BATCHES.toDriverPropertyInfo("true"),
                YdbOperationProperty.ENFORCE_SQL_V1.toDriverPropertyInfo("true"),
                YdbOperationProperty.ENFORCE_VARIABLE_PREFIX.toDriverPropertyInfo("true"),
                YdbOperationProperty.CACHE_CONNECTIONS_IN_DRIVER.toDriverPropertyInfo("true"),
                YdbOperationProperty.DETECT_SQL_OPERATIONS.toDriverPropertyInfo("true"),
                YdbOperationProperty.ALWAYS_PREPARE_DATAQUERY.toDriverPropertyInfo("true"),
                YdbOperationProperty.DISABLE_JDBC_PARAMETERS.toDriverPropertyInfo("false")
        };
    }

    static Properties customizedProperties() {
        Properties properties = new Properties();
        properties.setProperty("localDatacenter", "sas");
        properties.setProperty("secureConnection", "true");
        properties.setProperty("readTimeout", "2m");
        properties.setProperty("token", "x-secured-token");

        properties.setProperty("saFile", "x-secured-file");
        properties.setProperty("useMetadata", "true");

        properties.setProperty("keepQueryText", "true");
        properties.setProperty("sessionKeepAliveTime", "15m");
        properties.setProperty("sessionMaxIdleTime", "5m");
        properties.setProperty("sessionPoolSizeMin", "3");
        properties.setProperty("sessionPoolSizeMax", "4");

        properties.setProperty("joinDuration", "6m");
        properties.setProperty("keepInQueryCache", "true");
        properties.setProperty("queryTimeout", "2m");
        properties.setProperty("scanQueryTimeout", "3m");
        properties.setProperty("failOnTruncatedResult", "false");
        properties.setProperty("sessionTimeout", "6s");
        properties.setProperty("deadlineTimeout", "1s");
        properties.setProperty("autoCommit", "true");
        properties.setProperty("transactionLevel", "4");

        properties.setProperty("autoPreparedBatches", "false");
        properties.setProperty("enforceSqlV1", "false");
        properties.setProperty("enforceVariablePrefix", "false");
        properties.setProperty("cacheConnectionsInDriver", "false");
        properties.setProperty("detectSqlOperations", "false");
        properties.setProperty("alwaysPrepareDataQuery", "false");
        properties.setProperty("disableJdbcParameters", "true");
        return properties;
    }

    static DriverPropertyInfo[] customizedPropertyInfo() {
        return new DriverPropertyInfo[]{
                YdbConnectionProperty.LOCAL_DATACENTER.toDriverPropertyInfo("sas"),
                YdbConnectionProperty.SECURE_CONNECTION.toDriverPropertyInfo("true"),
                YdbConnectionProperty.SECURE_CONNECTION_CERTIFICATE.toDriverPropertyInfo(null),
                YdbConnectionProperty.TOKEN.toDriverPropertyInfo("x-secured-token"),

                YdbConnectionProperty.SERVICE_ACCOUNT_FILE.toDriverPropertyInfo("x-secured-file"),
                YdbConnectionProperty.USE_METADATA.toDriverPropertyInfo("true"),

                YdbClientProperty.KEEP_QUERY_TEXT.toDriverPropertyInfo("true"),
                YdbClientProperty.SESSION_KEEP_ALIVE_TIME.toDriverPropertyInfo("15m"),
                YdbClientProperty.SESSION_MAX_IDLE_TIME.toDriverPropertyInfo("5m"),
                YdbClientProperty.SESSION_POOL_SIZE_MIN.toDriverPropertyInfo("3"),
                YdbClientProperty.SESSION_POOL_SIZE_MAX.toDriverPropertyInfo("4"),

                YdbOperationProperty.JOIN_DURATION.toDriverPropertyInfo("6m"),
                YdbOperationProperty.QUERY_TIMEOUT.toDriverPropertyInfo("2m"),
                YdbOperationProperty.SCAN_QUERY_TIMEOUT.toDriverPropertyInfo("3m"),
                YdbOperationProperty.FAIL_ON_TRUNCATED_RESULT.toDriverPropertyInfo("false"),
                YdbOperationProperty.SESSION_TIMEOUT.toDriverPropertyInfo("6s"),
                YdbOperationProperty.DEADLINE_TIMEOUT.toDriverPropertyInfo("1s"),
                YdbOperationProperty.AUTOCOMMIT.toDriverPropertyInfo("true"),
                YdbOperationProperty.TRANSACTION_LEVEL.toDriverPropertyInfo("4"),

                YdbOperationProperty.AUTO_PREPARED_BATCHES.toDriverPropertyInfo("false"),
                YdbOperationProperty.ENFORCE_SQL_V1.toDriverPropertyInfo("false"),
                YdbOperationProperty.ENFORCE_VARIABLE_PREFIX.toDriverPropertyInfo("false"),
                YdbOperationProperty.CACHE_CONNECTIONS_IN_DRIVER.toDriverPropertyInfo("false"),
                YdbOperationProperty.DETECT_SQL_OPERATIONS.toDriverPropertyInfo("false"),
                YdbOperationProperty.ALWAYS_PREPARE_DATAQUERY.toDriverPropertyInfo("false"),
                YdbOperationProperty.DISABLE_JDBC_PARAMETERS.toDriverPropertyInfo("true")
        };
    }

    static void checkCustomizedProperties(YdbProperties properties) {
        YdbConnectionProperties conn = properties.getConnectionProperties();
        Assertions.assertEquals("grpc://ydb-demo.testhost.org:2135/test/pr/testing/ci",
                conn.getConnectionString());

        YdbOperationProperties ops = properties.getOperationProperties();
        Assertions.assertEquals(Duration.ofMinutes(6), ops.getJoinDuration());
        Assertions.assertEquals(Duration.ofMinutes(2), ops.getQueryTimeout());
        Assertions.assertEquals(Duration.ofMinutes(3), ops.getScanQueryTimeout());
        Assertions.assertFalse(ops.isFailOnTruncatedResult());
        Assertions.assertEquals(Duration.ofSeconds(6), ops.getSessionTimeout());
        Assertions.assertTrue(ops.isAutoCommit());
        Assertions.assertEquals(YdbConst.ONLINE_CONSISTENT_READ_ONLY, ops.getTransactionLevel());
        Assertions.assertFalse(ops.isAutoPreparedBatches());
        Assertions.assertFalse(ops.isEnforceSqlV1());
        Assertions.assertFalse(ops.isEnforceVariablePrefix());
        Assertions.assertFalse(ops.isCacheConnectionsInDriver());
        Assertions.assertFalse(ops.isDetectSqlOperations());
    }

    static String asString(DriverPropertyInfo info) {
        Assertions.assertNull(info.choices);
        return String.format("%s=%s (%s, required = %s)", info.name, info.value, info.description, info.required);
    }

    @SuppressWarnings("UnstableApiUsage")
    public static Collection<Arguments> urlsToParse() {
        return Arrays.asList(
                Arguments.of("jdbc:ydb:ydb-demo.testhost.org:2135",
                        "grpc://ydb-demo.testhost.org:2135",
                        null),
                Arguments.of("jdbc:ydb:ydb-demo.testhost.org:2135/demo/ci/testing/ci",
                        "grpc://ydb-demo.testhost.org:2135/demo/ci/testing/ci",
                        null),
                Arguments.of("jdbc:ydb:grpc://ydb-demo.testhost.org:2135/demo/ci/testing/ci?localDatacenter=man",
                        "grpc://ydb-demo.testhost.org:2135/demo/ci/testing/ci",
                        "man"),
                Arguments.of("jdbc:ydb:grpcs://ydb-demo.testhost.org:2135?localDatacenter=man",
                        "grpcs://ydb-demo.testhost.org:2135",
                        "man")
        );
    }

    public static Collection<Arguments> urlsToCheck() {
        return Arrays.asList(
                Arguments.of("jdbc:ydb:",
                        true, ""),
                Arguments.of("jdbc:ydb:ydb-demo.testhost.org:2135",
                        true, "grpc://ydb-demo.testhost.org:2135"),
                Arguments.of("jdbc:ydb:ydb-demo.testhost.org",
                        true, "grpc://ydb-demo.testhost.org"),
                Arguments.of("jdbc:ydb:ydb-demo.testhost.org:2135?database=test/pr/testing/ci",
                        true, "grpc://ydb-demo.testhost.org:2135/test/pr/testing/ci"),
                Arguments.of("jdbc:ydb:grpcs://ydb-demo.testhost.org",
                        true, "grpcs://ydb-demo.testhost.org"),
                Arguments.of("jdbc:ydb:ydb-demo.testhost.org:2170?database=/test/pr/testing/ci",
                        true, "grpc://ydb-demo.testhost.org:2170/test/pr/testing/ci"),
                Arguments.of("jdbc:ydb:ydb-demo.testhost.org:2133/test/pr/testing/ci",
                        true, "grpc://ydb-demo.testhost.org:2133/test/pr/testing/ci"),
                Arguments.of("jdbc:ydb:grpcs://ydb-demo.testhost.org?database=test/pr/testing/ci&dc=man",
                        true, "grpcs://ydb-demo.testhost.org/test/pr/testing/ci"),
                Arguments.of("jdbc:ydb:ydb-demo.testhost.org:2135/test/pr/testing/ci?dc=man",
                        true, "grpc://ydb-demo.testhost.org:2135/test/pr/testing/ci"),
                Arguments.of("ydb:",
                        false, null),
                Arguments.of("jdbc:ydb",
                        false, null),
                Arguments.of("jdbc:clickhouse://man",
                        false, null)
        );
    }

    public static Collection<Arguments> tokensToCheck() {
        return Arrays.asList(
                Arguments.of("classpath:data/token.txt", "token-from-classpath"),
                Arguments.of("file:", TOKEN_FROM_FILE));
    }

    public static Collection<Arguments> certificatesToCheck() {
        return Arrays.asList(
                Arguments.of("classpath:data/certificate.txt", "certificate-from-classpath"),
                Arguments.of("file:", CERTIFICATE_FROM_FILE));
    }

    public static Collection<Arguments> unknownFiles() {
        return Arrays.asList(
                Arguments.of("classpath:data/unknown-file.txt",
                        "Unable to find classpath resource: classpath:data/unknown-file.txt"),
                Arguments.of("file:data/unknown-file.txt",
                        "Unable to read resource from file:data/unknown-file.txt"));
    }

    public static Collection<Arguments> invalidDurationParams() {
        return Arrays.asList(
                Arguments.of("sessionKeepAliveTime"),
                Arguments.of("sessionMaxIdleTime"),
                Arguments.of("joinDuration"),
                Arguments.of("queryTimeout"),
                Arguments.of("scanQueryTimeout"),
                Arguments.of("sessionTimeout"),
                Arguments.of("deadlineTimeout")
        );
    }

    public static Collection<Arguments> invalidIntegerParams() {
        return Arrays.asList(
                Arguments.of("sessionPoolSizeMin"),
                Arguments.of("sessionPoolSizeMax"),
                Arguments.of("transactionLevel")
        );
    }

    @SuppressWarnings("UnstableApiUsage")
    private static File safeCreateFile(String content) throws IOException {
        File file = File.createTempFile("junit", "ydb");
        Files.write(content.getBytes(), file);
        return file;
    }

    private static void safeDeleteFile(@Nullable File file) {
        if (file != null) {
            if (!file.delete()) {
                file.deleteOnExit();
            }
        }
    }
}
