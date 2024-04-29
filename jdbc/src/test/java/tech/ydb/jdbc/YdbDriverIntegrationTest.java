package tech.ydb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.jdbc.impl.YdbConnectionImpl;
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
        Connection firstConnection = DriverManager.getConnection(jdbcURL.build());
        Assertions.assertTrue(firstConnection.isValid(5000));

        YdbConnection first = firstConnection.unwrap(YdbConnection.class);
        Assertions.assertNotNull(first.getCtx());

        YdbContext ctx = first.getCtx();

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

        firstConnection.close();

        try (Connection conn6 = DriverManager.getConnection(jdbcURL.build())) {
            Assertions.assertTrue(conn6.isValid(5000));

            YdbConnection unwrapped = conn6.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());
            Assertions.assertNotSame(ctx, unwrapped.getCtx());
        }
    }

    @Test
    public void testContextCacheConncurrent() throws SQLException {
        List<CompletableFuture<YdbConnection>> list = new ArrayList<>();

        for (int idx = 0; idx < 20; idx++) {
            list.add(CompletableFuture.supplyAsync(() -> {
                try {
                    Connection connection = DriverManager.getConnection(jdbcURL.build());
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
    public void testResizeSessionPool() throws SQLException {
        String url = jdbcURL.build();
        try (Connection conn = DriverManager.getConnection(url)) {
            YdbContext ctx = conn.unwrap(YdbConnection.class).getCtx();

            Assertions.assertEquals(1, ctx.getConnectionsCount());
            Assertions.assertEquals(50, ctx.getTableClient().sessionPoolStats().getMaxSize());

            Deque<Connection> connections = new ArrayDeque<>();
            for (int i = 0; i < 39; i++) {
                connections.offer(DriverManager.getConnection(url));
            }

            Assertions.assertEquals(40, ctx.getConnectionsCount());
            Assertions.assertEquals(50, ctx.getTableClient().sessionPoolStats().getMaxSize());

            connections.add(DriverManager.getConnection(url));

            Assertions.assertEquals(41, ctx.getConnectionsCount());
            Assertions.assertEquals(100, ctx.getTableClient().sessionPoolStats().getMaxSize());

            for (int i = 0; i < 11; i++) {
                connections.poll().close();
            }

            Assertions.assertEquals(30, ctx.getConnectionsCount());
            Assertions.assertEquals(100, ctx.getTableClient().sessionPoolStats().getMaxSize());

            connections.poll().close();

            Assertions.assertEquals(29, ctx.getConnectionsCount());
            Assertions.assertEquals(50, ctx.getTableClient().sessionPoolStats().getMaxSize());

            for (Connection c: connections) {
                c.close();
            }

            Assertions.assertEquals(1, ctx.getConnectionsCount());
            Assertions.assertEquals(50, ctx.getTableClient().sessionPoolStats().getMaxSize());
        }
    }

    @Test
    public void testFixedSessionPool() throws SQLException {
        assertFixedSessionPool("sessionPoolSizeMin", "0", 50);
        assertFixedSessionPool("sessionPoolSizeMin", "-1", 50);
        assertFixedSessionPool("sessionPoolSizeMax", "0", 1);
        assertFixedSessionPool("sessionPoolSizeMax", "5", 5);
    }

    private void assertFixedSessionPool(String arg, String value, int poolSize) throws SQLException {
        String url = jdbcURL.withArg(arg, value).build();
        try (Connection conn = DriverManager.getConnection(url)) {
            YdbContext ctx = conn.unwrap(YdbConnection.class).getCtx();

            Assertions.assertEquals(1, ctx.getConnectionsCount());
            Assertions.assertEquals(poolSize, ctx.getTableClient().sessionPoolStats().getMaxSize());

            Deque<Connection> connections = new ArrayDeque<>();
            for (int i = 0; i < poolSize * 2; i++) {
                connections.offer(DriverManager.getConnection(url));
            }

            Assertions.assertEquals(1 + poolSize * 2, ctx.getConnectionsCount());
            Assertions.assertEquals(poolSize, ctx.getTableClient().sessionPoolStats().getMaxSize());

            for (int i = 0; i < poolSize; i++) {
                connections.poll().close();
            }

            Assertions.assertEquals(1 + poolSize, ctx.getConnectionsCount());
            Assertions.assertEquals(poolSize, ctx.getTableClient().sessionPoolStats().getMaxSize());

            for (Connection c: connections) {
                c.close();
            }

            Assertions.assertEquals(1, ctx.getConnectionsCount());
            Assertions.assertEquals(poolSize, ctx.getTableClient().sessionPoolStats().getMaxSize());
        }
    }
}
