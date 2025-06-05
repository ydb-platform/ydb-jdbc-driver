package tech.ydb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
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

            Assert.assertNull(tracer.error);
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
            System.out.println("TRACE " + message);
            if (traceMsg != null && error != null && message.startsWith(traceMsg)) {
                Status status = error;
                error = null;
                traceMsg = null;
                throw new UnexpectedResultException("Test error", status);
            }
        }
    }
}
