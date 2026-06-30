package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class BadSessionRetryTest {

    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb)
            .withArg("channelInitializer", GrpcTestInterceptor.class.getCanonicalName());

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void badSessionRetryTest(boolean useQueryService) throws SQLException {
        GrpcTestInterceptor.reset();
//        String prefix = useQueryService ? "Cannot execute 'STREAM_QUERY >>\n" : "Cannot call 'DATA_QUERY >>\n";
        String prefix = "Cannot call 'DATA_QUERY >>\n";

        String url = jdbcURL.withArg("useQueryService", Boolean.toString(useQueryService)).build();
        try (Connection conn = DriverManager.getConnection(url)) {
            // BAD_SESSION will be retried
            GrpcTestInterceptor.nextExecuteQuery(StatusCode.BAD_SESSION, StatusCode.BAD_SESSION);
            try (Statement st = conn.createStatement()) {
                Assertions.assertTrue(st.execute("SELECT 1 + 2"));
            }

            // BAD_REQUEST will not be retried
            GrpcTestInterceptor.nextExecuteQuery(StatusCode.BAD_REQUEST);
            try (Statement st = conn.createStatement()) {
                ExceptionAssert.ydbException(""
                        + prefix
                        + "SELECT 1 + 2' with Status{code = BAD_REQUEST(code=400010)",
                        () -> st.execute("SELECT 1 + 2"));
            }

            conn.setAutoCommit(false);

            // BAD_SESSION will be retried
            GrpcTestInterceptor.nextExecuteQuery(StatusCode.BAD_SESSION, StatusCode.BAD_SESSION);
            try (Statement st = conn.createStatement()) {
                Assertions.assertTrue(st.execute("SELECT 1 + 2"));
            }

            // BAD_SESSION will not be retried, transaction is already started
            GrpcTestInterceptor.nextExecuteQuery(StatusCode.BAD_SESSION);
            try (Statement st = conn.createStatement()) {
                ExceptionAssert.sqlRecoverable(""
                        + prefix
                        + "SELECT 1 + 2' with Status{code = BAD_SESSION(code=400100)",
                        () -> st.execute("SELECT 1 + 2"));
            }
        }
    }
}
