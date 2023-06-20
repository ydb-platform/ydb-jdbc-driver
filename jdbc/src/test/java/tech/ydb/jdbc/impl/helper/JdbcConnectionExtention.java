package tech.ydb.jdbc.impl.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.junit.Assert;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class JdbcConnectionExtention implements
        BeforeEachCallback, BeforeAllCallback, AfterEachCallback, AfterAllCallback {

    private final JdbcUrlHelper jdbcURL;

    private final Map<ExtensionContext, Connection> map = new HashMap<>();
    private final Stack<Connection> stack = new Stack<>();

    public JdbcConnectionExtention(YdbHelperExtension ydb, boolean autoCommit) {
        this.jdbcURL = new JdbcUrlHelper(ydb)
                .withArg("failOnTruncatedResult", "true")
                .withArg("autoCommit", String.valueOf(autoCommit));
    }

    public JdbcConnectionExtention(YdbHelperExtension ydb) {
        this(ydb, true);
    }

    private void register(ExtensionContext ctx) throws SQLException {
        Assert.assertFalse("Dublicate of context registration", map.containsKey(ctx));

        Connection connection = DriverManager.getConnection(jdbcURL());
        map.put(ctx, connection);
        stack.push(connection);
    }

    private void unregister(ExtensionContext ctx) throws SQLException {
        Assert.assertFalse("Extra unregister call", stack.isEmpty());
        Assert.assertEquals("Top connection must be unregistered first", stack.peek(), map.get(ctx));

        stack.pop().close();
        map.remove(ctx);
    }

    public String jdbcURL() {
        return jdbcURL.build();
    }

    public Connection connection() {
        Assert.assertFalse("Retrive connection before initialization", stack.isEmpty());
        return stack.peek();
    }

    @Override
    public void beforeEach(ExtensionContext ctx) throws Exception {
        register(ctx);
    }

    @Override
    public void afterEach(ExtensionContext ctx) throws Exception {
        unregister(ctx);
    }

    @Override
    public void beforeAll(ExtensionContext ctx) throws Exception {
        register(ctx);
    }

    @Override
    public void afterAll(ExtensionContext ctx) throws Exception {
        unregister(ctx);
    }
}
