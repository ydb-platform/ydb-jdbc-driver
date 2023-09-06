package tech.ydb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.impl.YdbConnectionImpl;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbDriverIntegrationTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb);

    @Test
    public void connect() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            Assertions.assertTrue(connection instanceof YdbConnectionImpl);

            YdbConnection unwrapped = connection.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());
            Assertions.assertNotNull(unwrapped.getCtx().getSchemeClient());
            Assertions.assertNotNull(unwrapped.getCtx().getTableClient());
        }
    }

    @Test
    public void testContextCache() throws SQLException {
        YdbContext ctx;
        try (Connection conn1 = DriverManager.getConnection(jdbcURL.build())) {
            Assertions.assertTrue(conn1.isValid(5000));

            YdbConnection unwrapped = conn1.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());

            ctx = unwrapped.getCtx();
        }

        try (Connection conn2 = DriverManager.getConnection(jdbcURL.build())) {
            Assertions.assertTrue(conn2.isValid(5000));

            YdbConnection unwrapped = conn2.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());
            Assertions.assertSame(ctx, unwrapped.getCtx());
        }

        Properties props = new Properties();
        try (Connection conn3 = DriverManager.getConnection(jdbcURL.build(), props)) {
            Assertions.assertTrue(conn3.isValid(5000));

            YdbConnection unwrapped = conn3.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());
            Assertions.assertSame(ctx, unwrapped.getCtx());
        }

        props.setProperty("TEST_KEY", "TEST_VALUE");
        try (Connection conn4 = DriverManager.getConnection(jdbcURL.build(), props)) {
            Assertions.assertTrue(conn4.isValid(5000));

            YdbConnection unwrapped = conn4.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());
            Assertions.assertNotSame(ctx, unwrapped.getCtx());
        }

        try (Connection conn5 = DriverManager.getConnection(jdbcURL.withArg("test", "false").build())) {
            Assertions.assertTrue(conn5.isValid(5000));

            YdbConnection unwrapped = conn5.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());
            Assertions.assertNotSame(ctx, unwrapped.getCtx());
        }

        if (YdbDriver.isRegistered()) {
            YdbDriver.deregister();
            YdbDriver.register();
        }

        try (Connection conn6 = DriverManager.getConnection(jdbcURL.build())) {
            Assertions.assertTrue(conn6.isValid(5000));

            YdbConnection unwrapped = conn6.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());
            Assertions.assertNotSame(ctx, unwrapped.getCtx());
        }
    }
}
