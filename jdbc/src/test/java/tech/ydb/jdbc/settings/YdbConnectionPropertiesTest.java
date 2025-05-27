package tech.ydb.jdbc.settings;

import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.ydb.auth.AuthIdentity;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbConnectionPropertiesTest {
    private static final String YDB_URL = "grpc://localhost/local";

    @Test
    public void tokenProviderObject() throws SQLException {
        Properties props = new Properties();
        props.put("tokenProvider", (Supplier<String>) () -> "SUPPLIER");
        YdbConnectionProperties cp = new YdbConnectionProperties(null, null, props);
        GrpcTransportBuilder builder = cp.applyToGrpcTransport(GrpcTransport.forConnectionString(YDB_URL));
        try (AuthIdentity identity = builder.getAuthProvider().createAuthIdentity(null)) {
            Assertions.assertEquals("SUPPLIER", identity.getToken());
        }
    }

    @Test
    public void tokenProviderToStringTest() throws SQLException {
        Properties props = new Properties();
        props.put("tokenProvider", (Supplier<Object>) () -> new StringBuilder("test"));
        YdbConnectionProperties cp = new YdbConnectionProperties(null, null, props);
        GrpcTransportBuilder builder = cp.applyToGrpcTransport(GrpcTransport.forConnectionString(YDB_URL));
        try (AuthIdentity identity = builder.getAuthProvider().createAuthIdentity(null)) {
            Assertions.assertEquals("test", identity.getToken());
        }
    }

    @Test
    public void tokenProviderClassTest() throws SQLException {
        Properties props = new Properties();
        props.put("tokenProvider", "tech.ydb.jdbc.settings.StaticTokenProvider");
        YdbConnectionProperties cp = new YdbConnectionProperties(null, null, props);
        GrpcTransportBuilder builder = cp.applyToGrpcTransport(GrpcTransport.forConnectionString(YDB_URL));
        try (AuthIdentity identity = builder.getAuthProvider().createAuthIdentity(null)) {
            Assertions.assertEquals("STATIC", identity.getToken());
        }
    }

    @Test
    public void tokenProviderWrongClassNameTest() throws SQLException {
        Properties props = new Properties();
        props.put("tokenProvider", "1tech.ydb.jdbc.settings.StaticTokenProvider");
        YdbConnectionProperties cp = new YdbConnectionProperties(null, null, props);
        ExceptionAssert.sqlException(
                "tokenProvider must be full class name or instance of Supplier<String>",
                () -> cp.applyToGrpcTransport(GrpcTransport.forConnectionString(YDB_URL))
        );
    }

    @Test
    public void tokenProviderUnknownClassTest() throws SQLException {
        Properties props = new Properties();
        props.put("tokenProvider", "tech.ydb.jdbc.settings.TestTokenProvider");
        YdbConnectionProperties cp = new YdbConnectionProperties(null, null, props);
        ExceptionAssert.sqlException(
                "tokenProvider tech.ydb.jdbc.settings.TestTokenProvider not found",
                () -> cp.applyToGrpcTransport(GrpcTransport.forConnectionString(YDB_URL))
        );
    }

    @Test
    public void tokenProviderInvalidClassTest() throws SQLException {
        Properties props = new Properties();
        props.put("tokenProvider", "tech.ydb.jdbc.settings.BadTokenProvider");
        YdbConnectionProperties cp = new YdbConnectionProperties(null, null, props);
        ExceptionAssert.sqlException(
                "Cannot construct tokenProvider tech.ydb.jdbc.settings.BadTokenProvider",
                () -> cp.applyToGrpcTransport(GrpcTransport.forConnectionString(YDB_URL))
        );
    }

    @Test
    public void tokenProviderNonSupplierClassTest() throws SQLException {
        Properties props = new Properties();
        props.put("tokenProvider", "tech.ydb.jdbc.settings.NonSupplierTokenProvider");
        YdbConnectionProperties cp = new YdbConnectionProperties(null, null, props);
        ExceptionAssert.sqlException(
                "tokenProvider tech.ydb.jdbc.settings.NonSupplierTokenProvider is not implement Supplier<String>",
                () -> cp.applyToGrpcTransport(GrpcTransport.forConnectionString(YDB_URL))
        );
    }

    @Test
    public void tokenProviderWrongObjectTest() throws SQLException {
        Properties props = new Properties();
        props.put("tokenProvider", new Object());
        YdbConnectionProperties cp = new YdbConnectionProperties(null, null, props);

        ExceptionAssert.sqlException(
                "Cannot parse tokenProvider java.lang.Object",
                () -> cp.applyToGrpcTransport(GrpcTransport.forConnectionString(YDB_URL))
        );
    }
}
