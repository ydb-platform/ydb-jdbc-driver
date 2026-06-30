package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class TxErrorsTest {

    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb)
            .withArg("channelInitializer", GrpcTestInterceptor.class.getCanonicalName());

    @BeforeEach
    public void resetInterceptor() {
        GrpcTestInterceptor.reset();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void transactionBadSessionTest(boolean useQueryService) throws SQLException {
        String url = jdbcURL.withArg("useQueryService", Boolean.toString(useQueryService)).build();
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);
            YdbConnection ydbConn = conn.unwrap(YdbConnection.class);

            Assertions.assertNull(ydbConn.getYdbTxId());

            try (Statement st = conn.createStatement()) {
                Assertions.assertTrue(st.execute("SELECT 1 + 2")); // tx is opened
            }

            String tx1 = ydbConn.getYdbTxId();
            Assertions.assertNotNull(tx1);

            try (PreparedStatement ps = conn.prepareStatement("SELECT 1 + 2")) {
                GrpcTestInterceptor.nextExecuteQuery(StatusCode.BAD_SESSION);
                ExceptionAssert.sqlRecoverable("with Status{code = BAD_SESSION(code=400100)", ps::execute);
            }

            Assertions.assertNull(ydbConn.getYdbTxId());
            try (PreparedStatement ps = conn.prepareStatement("SELECT 1 + 2")) {
                Assertions.assertTrue(ps.execute()); // tx is opened
            }

            String tx2 = ydbConn.getYdbTxId();
            Assertions.assertNotEquals(tx1, tx2);

            conn.rollback();
            Assertions.assertNull(ydbConn.getYdbTxId());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void transactionUnavailableTest(boolean useQueryService) throws SQLException {
        String url = jdbcURL.withArg("useQueryService", Boolean.toString(useQueryService)).build();
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);
            YdbConnection ydbConn = conn.unwrap(YdbConnection.class);

            Assertions.assertNull(ydbConn.getYdbTxId());

            try (Statement st = conn.createStatement()) {
                Assertions.assertTrue(st.execute("SELECT 1 + 2")); // tx is opened
            }

            String tx1 = ydbConn.getYdbTxId();
            Assertions.assertNotNull(tx1);

            try (PreparedStatement ps = conn.prepareStatement("SELECT 1 + 2")) {
                GrpcTestInterceptor.nextGrpcCall(io.grpc.Status.UNAVAILABLE);
                ExceptionAssert.sqlTransientConnection(
                        "with Status{code = TRANSPORT_UNAVAILABLE(code=401010), issues = [gRPC error: (UNAVAILABLE) on",
                        ps::execute
                );
            }

            Assertions.assertNull(ydbConn.getYdbTxId());
            try (PreparedStatement ps = conn.prepareStatement("SELECT 1 + 2")) {
                Assertions.assertTrue(ps.execute()); // tx is opened
            }

            String tx2 = ydbConn.getYdbTxId();
            Assertions.assertNotEquals(tx1, tx2);

            conn.rollback();
            Assertions.assertNull(ydbConn.getYdbTxId());
        }
    }
}
