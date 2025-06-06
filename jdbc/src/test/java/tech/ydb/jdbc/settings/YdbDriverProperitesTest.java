package tech.ydb.jdbc.settings;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.io.Files;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbDriver;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;

public class YdbDriverProperitesTest {
    public static final String TOKEN_FROM_FILE = "token-from-file";
    public static final String CERTIFICATE_FROM_FILE = "certificate-from-file";

    private static File TOKEN_FILE;
    private static File CERTIFICATE_FILE;

    private YdbDriver driver;

    @BeforeAll
    public static void beforeAll() throws SQLException, IOException {
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
        driver.clear();
    }

    @Test
    public void connectToUnsupportedUrl() throws SQLException {
        Assertions.assertNull(driver.connect("jdbc:clickhouse:localhost:123", new Properties()));
    }

    @ParameterizedTest
    @MethodSource("urlsToParse")
    public void parseURL(String url, @Nullable String connectionString, @Nullable String localDatacenter)
            throws SQLException {
        YdbConfig config = YdbConfig.from(url, new Properties());
        Assertions.assertEquals(connectionString, config.getConnectionString());

        YdbConnectionProperties connectionProperties = new YdbConnectionProperties(config);
        Assertions.assertEquals(localDatacenter, connectionProperties.getLocalDataCenter());
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource(delimiter = ' ', value = {
        "jdbc:ydb: \'\'",
        "jdbc:ydb:ydb-demo.testhost.org:2135 grpc://ydb-demo.testhost.org:2135",
        "jdbc:ydb:ydb-demo.testhost.org grpc://ydb-demo.testhost.org",
        "jdbc:ydb:ydb-demo.testhost.org:2135?database=test/db grpc://ydb-demo.testhost.org:2135/test/db",
        "jdbc:ydb:grpcs://ydb-demo.testhost.org grpcs://ydb-demo.testhost.org",
        "jdbc:ydb:ydb-demo.testhost.org:2170?database=/test/db grpc://ydb-demo.testhost.org:2170/test/db",
        "jdbc:ydb:ydb-demo.testhost.org:2133/test/db grpc://ydb-demo.testhost.org:2133/test/db",
        "jdbc:ydb:grpcs://ydb-demo.testhost.org?database=test/db&dc=man grpcs://ydb-demo.testhost.org/test/db",
        "jdbc:ydb:ydb-demo.testhost.org:2135/test/db?dc=man grpc://ydb-demo.testhost.org:2135/test/db",
    })
    public void validURL(String url, String connectionString) throws SQLException {
        Assertions.assertTrue(driver.acceptsURL(url));

        DriverPropertyInfo[] properties = driver.getPropertyInfo(url, new Properties());
        Assertions.assertNotNull(properties);

        YdbConfig config = YdbConfig.from(url, new Properties());
        Assertions.assertNotNull(config);
        Assertions.assertEquals(connectionString, config.getConnectionString());
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {
        "ydb:",
        "jdbc:ydb",
        "jdbc:clickhouse://man",
    })
    public void notValidURL(String url) throws SQLException {
        Assertions.assertFalse(driver.acceptsURL(url));
        ExceptionAssert.sqlException("[" + url + "] is not a YDB URL, must starts from jdbc:ydb:",
                () -> YdbConfig.from(url, new Properties())
        );
    }

    @Test
    public void getPropertyInfoDefault() throws SQLException {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/db";

        Properties properties = new Properties();
        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, properties);
        Assertions.assertEquals(new Properties(), properties);

        assertPropertiesInfo(defaultPropertyInfo(), propertyInfo);

        YdbConfig config = YdbConfig.from(url, properties);
        Assertions.assertEquals("grpc://ydb-demo.testhost.org:2135/test/db", config.getConnectionString());
    }

    private static String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException ex) {
            return value;
        }
    }

    @Test
    public void getPropertyInfoAllFromUrl() throws SQLException {
        Stream<String> params = Stream.of(customizedPropertyInfo())
                .map(e -> e.name + "=" + urlEncode(e.value));
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/db?" + params.collect(Collectors.joining("&"));

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, null);

        assertPropertiesInfo(customizedPropertyInfo(), propertyInfo);

        YdbConfig config = YdbConfig.from(url, null);
        checkCustomizedProperties(config);
    }

    @Test
    public void getPropertyInfoFromProperties() throws SQLException {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/db";

        Properties properties = new Properties();
        for (DriverPropertyInfo info: customizedPropertyInfo()) {
            properties.put(info.name, info.value);
        }

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, properties);
        assertPropertiesInfo(customizedPropertyInfo(), propertyInfo);

        YdbConfig config = YdbConfig.from(url, properties);
        checkCustomizedProperties(config);
    }

    @Test
    public void getPropertyInfoOverwrite() throws SQLException {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/testing/ydb?localDatacenter=sas";
        Properties properties = new Properties();
        properties.put("localDatacenter", "vla");

        Properties copy = new Properties();
        copy.putAll(properties);

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, properties);
        Assertions.assertEquals(copy, properties);

        // URL will always overwrite properties
        assertPropertiesInfo(defaultPropertyInfo("sas"), propertyInfo);

        YdbConfig config = YdbConfig.from(url, properties);
        Assertions.assertEquals("grpc://ydb-demo.testhost.org:2135/testing/ydb",
                config.getConnectionString());
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @MethodSource("tokensToCheck")
    public void getTokenAs(String token, String expectValue) throws SQLException {
        if ("file:".equals(token)) {
            token += TOKEN_FILE.getAbsolutePath();
        }

        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/db?token=" + token;
        Properties properties = new Properties();
        YdbConfig config = YdbConfig.from(url, properties);

        YdbConnectionProperties props = new YdbConnectionProperties(config);

        Assertions.assertEquals(expectValue, props.getToken());
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource(delimiter = ',', value = {
        "classpath:data/unknown-file.txt,Unable to find classpath resource: classpath:data/unknown-file.txt",
        "file:data/unknown-file.txt,Unable to read resource from file:data/unknown-file.txt",
    })
    public void getTokenAsInvalid(String token, String expectException) {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/db?token=" + token;
        ExceptionAssert.sqlException("Unable to convert property token: " + expectException,
                () -> driver.getPropertyInfo(url, new Properties())
        );
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @MethodSource("certificatesToCheck")
    public void getCaCertificateAs(String certificate, String expectValue) throws SQLException {
        if ("file:".equals(certificate)) {
            certificate += CERTIFICATE_FILE.getAbsolutePath();
        }
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/db" +
                "?secureConnectionCertificate=" + certificate;
        Properties properties = new Properties();
        YdbConfig config = YdbConfig.from(url, properties);

        YdbConnectionProperties props = new YdbConnectionProperties(config);
        Assertions.assertArrayEquals(expectValue.getBytes(), props.getSecureConnectionCert());
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource(delimiter = ',', value = {
        "classpath:data/unknown-file.txt,resource not found",
        "file:data/unknown-file.txt,data/unknown-file.txt (No such file or directory)",
    })
    public void getCaCertificateAsInvalid(String value, String message) {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/db" +
                "?secureConnectionCertificate=" + value;
        ExceptionAssert.sqlException(
                "Cannot process value " + value + " for option secureConnectionCertificate: " + message,
                () -> driver.getPropertyInfo(url, new Properties())
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "sessionKeepAliveTime",
        "sessionMaxIdleTime",
        "joinDuration",
        "queryTimeout",
        "scanQueryTimeout",
        "sessionTimeout",
        "deadlineTimeout"
    })
    public void invalidDuration(String param) {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/db?" + param + "=1bc";
        ExceptionAssert.sqlException("Unable to convert property " + param +
                        ": Unable to parse value [1bc] -> [PT1BC] as Duration: Text cannot be parsed to a Duration",
                () -> driver.getPropertyInfo(url, null));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "preparedStatementCacheQueries",
        "sessionPoolSizeMin",
        "sessionPoolSizeMax",
        "transactionLevel"
    })
    public void invalidInteger(String param) {
        String url = "jdbc:ydb:ydb-demo.testhost.org:2135/test/db?" + param + "=1bc";
        ExceptionAssert.sqlException("Unable to convert property " + param +
                        ": Unable to parse value [1bc] as Integer: For input string: \"1bc\"",
                () -> driver.getPropertyInfo(url, null));
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

    static DriverPropertyInfo[] defaultPropertyInfo() {
        return defaultPropertyInfo("");
    }

    static DriverPropertyInfo[] defaultPropertyInfo(@Nullable String localDatacenter) {
        return new DriverPropertyInfo[]{
            new DriverPropertyInfo("cacheConnectionsInDriver", "true"),
            new DriverPropertyInfo("preparedStatementCacheQueries", "256"),
            new DriverPropertyInfo("useQueryService", "true"),
            new DriverPropertyInfo("usePrefixPath", ""),
            new DriverPropertyInfo("localDatacenter", localDatacenter),
            new DriverPropertyInfo("secureConnection", ""),
            new DriverPropertyInfo("secureConnectionCertificate", ""),
            new DriverPropertyInfo("token", ""),
            new DriverPropertyInfo("tokenFile", ""),
            new DriverPropertyInfo("saKeyFile", ""),
            new DriverPropertyInfo("useMetadata", ""),
            new DriverPropertyInfo("iamEndpoint", ""),
            new DriverPropertyInfo("metadataURL", ""),
            new DriverPropertyInfo("tokenProvider", ""),
            new DriverPropertyInfo("grpcCompression", ""),
            new DriverPropertyInfo("keepQueryText", ""),
            new DriverPropertyInfo("sessionKeepAliveTime", ""),
            new DriverPropertyInfo("sessionMaxIdleTime", ""),
            new DriverPropertyInfo("sessionPoolSizeMin", ""),
            new DriverPropertyInfo("sessionPoolSizeMax", ""),
            new DriverPropertyInfo("useStreamResultSets", "false"),
            new DriverPropertyInfo("joinDuration", "5m"),
            new DriverPropertyInfo("queryTimeout", "0s"),
            new DriverPropertyInfo("scanQueryTimeout", "5m"),
            new DriverPropertyInfo("failOnTruncatedResult", "false"),
            new DriverPropertyInfo("sessionTimeout", "5s"),
            new DriverPropertyInfo("deadlineTimeout", "0s"),
            new DriverPropertyInfo("autoCommit", "true"),
            new DriverPropertyInfo("transactionLevel", "8"),
            new DriverPropertyInfo("schemeQueryTxMode", "ERROR"),
            new DriverPropertyInfo("scanQueryTxMode", "ERROR"),
            new DriverPropertyInfo("bulkUpsertQueryTxMode", "ERROR"),
            new DriverPropertyInfo("forceSignedDatetimes", "false"),
            new DriverPropertyInfo("disablePrepareDataQuery", "false"),
            new DriverPropertyInfo("disableAutoPreparedBatches", "false"),
            new DriverPropertyInfo("disableDetectSqlOperations", "false"),
            new DriverPropertyInfo("disableJdbcParameters", "false"),
            new DriverPropertyInfo("disableJdbcParameterDeclare", "false"),
            new DriverPropertyInfo("forceJdbcParameters", "false"),
            new DriverPropertyInfo("replaceJdbcInByYqlList", "true"),
            new DriverPropertyInfo("replaceInsertByUpsert", "false"),
            new DriverPropertyInfo("forceBulkUpsert", "false"),
            new DriverPropertyInfo("forceScanSelect", "false"),
        };
    }

    static DriverPropertyInfo[] customizedPropertyInfo() {
        return new DriverPropertyInfo[]{
            new DriverPropertyInfo("cacheConnectionsInDriver", "false"),
            new DriverPropertyInfo("preparedStatementCacheQueries", "100"),
            new DriverPropertyInfo("useQueryService", "false"),
            new DriverPropertyInfo("usePrefixPath", "/demo/oltp"),
            new DriverPropertyInfo("localDatacenter", "sas"),
            new DriverPropertyInfo("secureConnection", "true"),
            new DriverPropertyInfo("secureConnectionCertificate", "classpath:data/certificate.txt"),
            new DriverPropertyInfo("token", "x-secured-token"),
            new DriverPropertyInfo("tokenFile", "classpath:data/token.txt"),
            new DriverPropertyInfo("saKeyFile", "classpath:data/token.txt"),
            new DriverPropertyInfo("useMetadata", "true"),
            new DriverPropertyInfo("iamEndpoint", "iam.endpoint.com"),
            new DriverPropertyInfo("metadataURL", "https://metadata.com"),
            new DriverPropertyInfo("tokenProvider", "tech.ydb.jdbc.settings.StaticTokenProvider"),
            new DriverPropertyInfo("grpcCompression", "gzip"),
            new DriverPropertyInfo("keepQueryText", "true"),
            new DriverPropertyInfo("sessionKeepAliveTime", "15m"),
            new DriverPropertyInfo("sessionMaxIdleTime", "5m"),
            new DriverPropertyInfo("sessionPoolSizeMin", "3"),
            new DriverPropertyInfo("sessionPoolSizeMax", "4"),
            new DriverPropertyInfo("useStreamResultSets", "true"),
            new DriverPropertyInfo("joinDuration", "6m"),
            new DriverPropertyInfo("queryTimeout", "2m"),
            new DriverPropertyInfo("scanQueryTimeout", "3m"),
            new DriverPropertyInfo("failOnTruncatedResult", "false"),
            new DriverPropertyInfo("sessionTimeout", "6s"),
            new DriverPropertyInfo("deadlineTimeout", "1s"),
            new DriverPropertyInfo("autoCommit", "true"),
            new DriverPropertyInfo("transactionLevel", "16"),
            new DriverPropertyInfo("schemeQueryTxMode", "SHADOW_COMMIT"),
            new DriverPropertyInfo("scanQueryTxMode", "FAKE_TX"),
            new DriverPropertyInfo("bulkUpsertQueryTxMode", "SHADOW_COMMIT"),
            new DriverPropertyInfo("forceSignedDatetimes", "true"),
            new DriverPropertyInfo("disablePrepareDataQuery", "true"),
            new DriverPropertyInfo("disableAutoPreparedBatches", "true"),
            new DriverPropertyInfo("disableDetectSqlOperations", "true"),
            new DriverPropertyInfo("disableJdbcParameters", "true"),
            new DriverPropertyInfo("disableJdbcParameterDeclare", "true"),
            new DriverPropertyInfo("forceJdbcParameters", "true"),
            new DriverPropertyInfo("replaceJdbcInByYqlList", "false"),
            new DriverPropertyInfo("replaceInsertByUpsert", "true"),
            new DriverPropertyInfo("forceBulkUpsert", "true"),
            new DriverPropertyInfo("forceScanSelect", "true"),
        };
    }

    static void checkCustomizedProperties(YdbConfig config) throws SQLException {
        Assertions.assertEquals("grpc://ydb-demo.testhost.org:2135/test/db",
                config.getConnectionString());

        YdbOperationProperties ops = new YdbOperationProperties(config);
        Assertions.assertEquals(Duration.ofMinutes(6), ops.getJoinDuration());
        Assertions.assertEquals(Duration.ofMinutes(2), ops.getQueryTimeout());
        Assertions.assertEquals(Duration.ofMinutes(3), ops.getScanQueryTimeout());
        Assertions.assertFalse(ops.isFailOnTruncatedResult());
        Assertions.assertEquals(Duration.ofSeconds(6), ops.getSessionTimeout());
        Assertions.assertTrue(ops.isAutoCommit());
        Assertions.assertEquals(YdbConst.ONLINE_CONSISTENT_READ_ONLY, ops.getTransactionLevel());
        Assertions.assertFalse(config.isCacheConnectionsInDriver());
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

    private static void assertPropertiesInfo(DriverPropertyInfo[] expected, DriverPropertyInfo[] actual) {
        Assertions.assertEquals(expected.length, actual.length, "Wrong size of driver properties array");
        for (int idx = 0; idx < expected.length; idx += 1) {
            Assertions.assertEquals(expected[idx].name, actual[idx].name, "Wrong name of property " + idx);
            String name = expected[idx].name;
            Assertions.assertEquals(expected[idx].value, actual[idx].value, "Wrong value of property " + name);
            Assertions.assertFalse(actual[idx].required, "Wrong required of property " + name);
            Assertions.assertNotNull(actual[idx].description, "Empty description of property " + name);
        }
    }
}
