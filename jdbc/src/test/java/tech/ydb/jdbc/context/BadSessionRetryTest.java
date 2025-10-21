package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.impl.YdbTracerImpl;
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
            .withArg("enableTxTracer", "true");

    @ParameterizedTest
    @ValueSource(strings = { "true", "false" })
    public void badSessionRetryTest(String useQueryService) throws SQLException {
        String url = jdbcURL.withArg("useQueryService", useQueryService).build();
        try (Connection conn = DriverManager.getConnection(url)) {
            ErrorTxTracer tracer = YdbTracerImpl.use(new ErrorTxTracer());

            // BAD_SESSION will be retried
            tracer.throwErrorOn(3, "<-- Status{code = SUCCESS}", Status.of(StatusCode.BAD_SESSION));
            try (Statement st = conn.createStatement()) {
                Assertions.assertTrue(st.execute("SELECT 1 + 2"));
            }

            // BAD_REQUEST will not be retried
            tracer.throwErrorOn(1, "<-- Status{code = SUCCESS}", Status.of(StatusCode.BAD_REQUEST));
            try (Statement st = conn.createStatement()) {
                ExceptionAssert.ydbException(""
                        + "Cannot call 'DATA_QUERY >>\n"
                        + "SELECT 1 + 2' with Status{code = BAD_REQUEST(code=400010)",
                        () -> st.execute("SELECT 1 + 2"));
            }

            conn.setAutoCommit(false);

            // BAD_SESSION will be retried
            tracer.throwErrorOn(3, "<-- Status{code = SUCCESS}", Status.of(StatusCode.BAD_SESSION));
            try (Statement st = conn.createStatement()) {
                Assertions.assertTrue(st.execute("SELECT 1 + 2"));
            }

            // BAD_SESSION will not be retried, transaction is already started
            tracer.throwErrorOn(1, "<-- Status{code = SUCCESS}", Status.of(StatusCode.BAD_SESSION));
            try (Statement st = conn.createStatement()) {
                ExceptionAssert.sqlRecoverable(""
                        + "Cannot call 'DATA_QUERY >>\n"
                        + "SELECT 1 + 2' with Status{code = BAD_SESSION(code=400100)",
                        () -> st.execute("SELECT 1 + 2"));
            }
        }
    }

    private class ErrorTxTracer extends YdbTracerImpl {
        private String traceMsg = null;
        private Status error = null;
        private int count = 0;

        public void throwErrorOn(int count, String traceMsg, Status error) {
            this.count = count;
            this.traceMsg = traceMsg;
            this.error = error;
        }

        @Override
        public void trace(String message) {
            super.trace(message);
            if (count > 0 && message.startsWith(traceMsg)) {
                Status status = error;
                count -= 1;
                throw new UnexpectedResultException("Test error", status);
            }
        }
    }
}
