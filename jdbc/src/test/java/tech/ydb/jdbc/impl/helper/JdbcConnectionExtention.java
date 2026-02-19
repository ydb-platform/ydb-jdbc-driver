package tech.ydb.jdbc.impl.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import tech.ydb.test.integration.YdbHelperFactory;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class JdbcConnectionExtention implements ExecutionCondition,
        BeforeEachCallback, BeforeAllCallback, AfterEachCallback, AfterAllCallback {

    private final JdbcUrlHelper jdbcURL;

    private final Map<ExtensionContext, Connection> map = new HashMap<>();
    private final Stack<Connection> stack = new Stack<>();

    private JdbcConnectionExtention(JdbcUrlHelper jdbcURL) {
        this.jdbcURL = jdbcURL;
    }

    public JdbcConnectionExtention(YdbHelperExtension ydb, boolean autoCommit) {
        this(new JdbcUrlHelper(ydb)
                .withArg("failOnTruncatedResult", "true")
                .withArg("autoCommit", String.valueOf(autoCommit))
//                .withArg("useQueryService", "false")

        );
    }

    public JdbcConnectionExtention(YdbHelperExtension ydb) {
        this(ydb, true);
    }

    public JdbcConnectionExtention withArg(String key, String value) {
        return new JdbcConnectionExtention(jdbcURL.withArg(key, value));
    }

    private void register(ExtensionContext ctx) throws SQLException {
        Assertions.assertFalse(map.containsKey(ctx), "Dublicate of context registration");

        Connection connection = DriverManager.getConnection(jdbcURL());
        map.put(ctx, connection);
        stack.push(connection);
    }

    private void unregister(ExtensionContext ctx) throws SQLException {
        Assertions.assertFalse(stack.isEmpty(), "Extra unregister call");
        Assertions.assertEquals(stack.peek(), map.get(ctx), "Top connection must be unregistered first");

        stack.pop().close();
        map.remove(ctx);
    }

    public String jdbcURL() {
        return jdbcURL.build();
    }

    public String database() {
        return jdbcURL.database();
    }

    public Connection connection() {
        Assertions.assertFalse(stack.isEmpty(), "Retrive connection before initialization");
        return stack.peek();
    }

    public Connection createCustomConnection(String arg, String value) throws SQLException {
        Properties props = new Properties();
        props.put(arg, value);
        return DriverManager.getConnection(jdbcURL(), props);
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (!YdbHelperFactory.getInstance().isEnabled()) {
            return ConditionEvaluationResult.disabled("Ydb helper is disabled " + context.getDisplayName());
        }

        return ConditionEvaluationResult.enabled("OK");
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
