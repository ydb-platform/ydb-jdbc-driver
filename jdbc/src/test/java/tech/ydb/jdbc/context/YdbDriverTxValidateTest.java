package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.exception.YdbConditionallyRetryableException;
import tech.ydb.jdbc.exception.YdbRetryableException;
import tech.ydb.jdbc.exception.YdbTimeoutException;
import tech.ydb.jdbc.impl.YdbTracerImpl;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbDriverTxValidateTest {

    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private static final JdbcConnectionExtention jdbc = new JdbcConnectionExtention(ydb)
            .withArg("usePrefixPath", "tx_validated");

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb)
            .withArg("enableTxTracer", "true")
            .withArg("usePrefixPath", "tx_validated");

    private void assertTxCount(String tableName, long count) throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            try (ResultSet rs = st.executeQuery("select count(*) from " + tableName)) {
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals(count, rs.getLong(1));
                Assertions.assertFalse(rs.next());
            }
        }
    }

    @Test
    public void basicUsageTest() throws SQLException {
        String url = jdbcURL.withArg("withTxValidationTable", "tx_store").build();
        try (Connection conn = DriverManager.getConnection(url)) {
            // table was created automatically
            assertTxCount("tx_store", 0);

            // statements with auto commit won't be validated
            try (Statement st = conn.createStatement()) {
                Assertions.assertTrue(st.execute("SELECT * FROM tx_store"));
                Assertions.assertFalse(st.execute("DELETE FROM tx_store"));
            }

            assertTxCount("tx_store", 0);

            conn.setAutoCommit(false);

            // scheme statements and read won't be validated
            Assertions.assertFalse(conn.createStatement()
                    .execute("CREATE TABLE tmp (id Int32, vv UInt64, PRIMARY KEY(id))"));
            conn.commit();
            assertTxCount("tx_store", 0);

            // read tx wont' be validated
            Assertions.assertTrue(conn.createStatement().execute("SELECT * FROM tmp;"));
            conn.commit();
            assertTxCount("tx_store", 0);

            // rollbacked tx wont' be validated
            Assertions.assertFalse(conn.createStatement().execute("INSERT INTO tmp(id, vv) VALUES (1, 1);"));
            conn.rollback();
            assertTxCount("tx_store", 0);

            Assertions.assertFalse(conn.createStatement().execute("INSERT INTO tmp(id, vv) VALUES (1, 1);"));
            conn.commit();
            assertTxCount("tx_store", 1);
        }

        try (Connection conn = DriverManager.getConnection(url)) {
            conn.createStatement().execute("DROP TABLE tmp");
            // reuse current table
            assertTxCount("tx_store", 1);
        } finally {
            jdbc.connection().createStatement().execute("DROP TABLE tx_store");
        }
    }

    @Test
    public void testContextCacheConncurrent() throws SQLException {
        String url = jdbcURL.withArg("withTxValidationTable", "tx2_store").build();
        List<CompletableFuture<YdbConnection>> list = new ArrayList<>();

        for (int idx = 0; idx < 20; idx++) {
            list.add(CompletableFuture.supplyAsync(() -> {
                try {
                    Connection connection = DriverManager.getConnection(url);
                    return connection.unwrap(YdbConnection.class);
                } catch (SQLException ex) {
                    throw new RuntimeException("Cannot connect", ex);
                }
            }));
        }

        YdbContext first = list.get(0).join().getCtx();

        for (CompletableFuture<YdbConnection> future: list) {
            Assertions.assertEquals(first, future.join().getCtx());
        }

        for (CompletableFuture<YdbConnection> future: list) {
            future.join().close();
        }
    }

    @Test
    public void commitedTxTest() throws SQLException {
        String url = jdbcURL.withArg("withTxValidationTable", "tx1_store").build();
        try (Connection conn = DriverManager.getConnection(url)) {
            ErrorTxTracer tracer = YdbTracerImpl.use(new ErrorTxTracer());
            // table was created automatically
            assertTxCount("tx1_store", 0);

            conn.setAutoCommit(false);

            conn.createStatement().execute("DELETE FROM tx1_store");
            // throw condintionally retryable exception AFTER commit
            tracer.throwErrorOn("<-- Status", Status.of(StatusCode.UNDETERMINED));
            conn.commit(); // no error, tx is validated successfully

            assertTxCount("tx1_store", 1);

            conn.createStatement().execute("DELETE FROM tx1_store");
            // throw condintionally retryable exception BEFORE commit
            tracer.throwErrorOn("--> commit-and-store-tx", Status.of(StatusCode.UNDETERMINED));
            ExceptionAssert.sqlRecoverable("Transaction wasn't committed", conn::commit);

            Assertions.assertNull(tracer.error);
        } finally {
            jdbc.connection().createStatement().execute("DROP TABLE tx1_store");
        }
    }

    @Test
    public void unavailableTxTest() throws SQLException {
        String url = jdbcURL.withArg("withTxValidationTable", "tx1_store").build();
        try (Connection conn = DriverManager.getConnection(url)) {
            ErrorTxTracer tracer = YdbTracerImpl.use(new ErrorTxTracer());
            // table was created automatically
            assertTxCount("tx1_store", 0);

            conn.setAutoCommit(false);

            conn.createStatement().execute("DELETE FROM tx1_store");
            // throw condintionally retryable exception AFTER commit
            tracer.throwErrorOn("<-- Status", Status.of(StatusCode.TRANSPORT_UNAVAILABLE));
            conn.commit(); // no error, tx is validated successfully

            assertTxCount("tx1_store", 1);

            conn.createStatement().execute("DELETE FROM tx1_store");
            // throw condintionally retryable exception BEFORE commit
            tracer.throwErrorOn("--> commit-and-store-tx", Status.of(StatusCode.TRANSPORT_UNAVAILABLE));
            ExceptionAssert.sqlRecoverable("Transaction wasn't committed", conn::commit);

            Assertions.assertNull(tracer.error);
        } finally {
            jdbc.connection().createStatement().execute("DROP TABLE tx1_store");
        }
    }

    @Test
    public void executeDataQueryTest() throws SQLException {
        String url = jdbcURL.withArg("withTxValidationTable", "tx1_store").build();
        try (Connection conn = DriverManager.getConnection(url)) {
            ErrorTxTracer tracer = YdbTracerImpl.use(new ErrorTxTracer());
            // table was created automatically
            assertTxCount("tx1_store", 0);

            conn.setAutoCommit(true);
            try (Statement st = conn.createStatement()) {
                st.execute("DELETE FROM tx1_store");

                tracer.throwErrorOn("<-- Status", Status.of(StatusCode.UNDETERMINED));
                YdbConditionallyRetryableException e = Assertions.assertThrows(YdbConditionallyRetryableException.class,
                        () -> st.execute("DELETE FROM tx1_store"));
                Assertions.assertEquals(Status.of(StatusCode.UNDETERMINED), e.getStatus());

                tracer.throwErrorOn("<-- Status", Status.of(StatusCode.ABORTED));
                YdbRetryableException e2 = Assertions.assertThrows(YdbRetryableException.class,
                        () -> st.execute("DELETE FROM tx1_store"));
                Assertions.assertEquals(Status.of(StatusCode.ABORTED), e2.getStatus());

                tracer.throwErrorOn("<-- Status", Status.of(StatusCode.CLIENT_DEADLINE_EXCEEDED));
                YdbTimeoutException e3 = Assertions.assertThrows(YdbTimeoutException.class,
                        () -> st.execute("DELETE FROM tx1_store"));
                Assertions.assertEquals(Status.of(StatusCode.CLIENT_DEADLINE_EXCEEDED), e3.getStatus());
            }


            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("DELETE FROM tx1_store");
                conn.commit();

                tracer.throwErrorOn("<-- Status", Status.of(StatusCode.UNDETERMINED));
                YdbRetryableException e = Assertions.assertThrows(YdbRetryableException.class,
                        () -> st.execute("DELETE FROM tx1_store"));
                Assertions.assertEquals(StatusCode.ABORTED, e.getStatus().getCode());
                Assertions.assertNotNull(e.getStatus().getCause());

                tracer.throwErrorOn("<-- Status", Status.of(StatusCode.ABORTED));
                YdbRetryableException e2 = Assertions.assertThrows(YdbRetryableException.class,
                        () -> st.execute("DELETE FROM tx1_store"));
                Assertions.assertEquals(Status.of(StatusCode.ABORTED), e2.getStatus());
            }
        } finally {
            jdbc.connection().createStatement().execute("DROP TABLE tx1_store");
        }
    }

    @Test
    public void invalididTxTableTest() throws SQLException {
        String url = jdbcURL.withArg("withTxValidationTable", "tx store").build();
        ExceptionAssert.ydbException(
                "Cannot initialize TableTxExecutor with tx table " + ydb.database() + "/tx_validated/tx store",
                () -> DriverManager.getConnection(url)
        );
    }

    private class ErrorTxTracer extends YdbTracerImpl {
        private String traceMsg = null;
        private Status error = null;

        public void throwErrorOn(String traceMsg, Status error) {
            this.traceMsg = traceMsg;
            this.error = error;
        }

        @Override
        public void trace(String message) {
            super.trace(message);
            if (traceMsg != null && error != null && message.startsWith(traceMsg)) {
                Status status = error;
                error = null;
                traceMsg = null;
                throw new UnexpectedResultException("Test error", status);
            }
        }
    }
}
