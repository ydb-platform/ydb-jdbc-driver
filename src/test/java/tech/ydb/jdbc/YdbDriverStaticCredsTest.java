package tech.ydb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.common.QueryType;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbDriverStaticCredsTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb);

    @BeforeAll
    public static void createUsers() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(QueryType.SCHEME_QUERY.getPrefix() + "\n"
                        + "CREATE USER user1 PASSWORD NULL;"
                        + "CREATE USER user2 PASSWORD 'pwss';"
                );
            }
        }
    }

    @AfterAll
    public static void dropUsers() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(QueryType.SCHEME_QUERY.getPrefix() + "\n"
                        + "DROP USER IF EXISTS user1, user2;"
                );
            }
        }
    }

    private ConnectionSupplier connectByProperties(String username, String password) {
        final Properties props = new Properties();
        props.put("user", username);
        if (password != null) {
            props.put("password", password);
        }
        return () -> DriverManager.getConnection(jdbcURL.disableToken().build(), props);
    }

    private ConnectionSupplier connectByAuthority(String username, String password) {
        return () -> DriverManager.getConnection(jdbcURL.disableToken().withAutority(username, password).build());
    }

    private void testConnection(ConnectionSupplier connectionSupplier) throws SQLException {
        try (Connection connection = connectionSupplier.get()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1;");
            }
        }
    }

    @Test
    public void connectOK() throws SQLException {
        testConnection(connectByProperties("user1", "pwss"));
        testConnection(connectByAuthority("user1", "pwss"));

        testConnection(connectByProperties("user2", ""));
        testConnection(connectByAuthority("user2", ""));

        testConnection(connectByProperties("user2", null));
        testConnection(connectByAuthority("user2", null));
    }

    @Test
    public void connectWring() throws SQLException {
        ExceptionAssert.ydbConfiguration("can't connect", () -> testConnection(connectByProperties("user1", "")));
        ExceptionAssert.ydbConfiguration("can't connect", () -> testConnection(connectByProperties("user1", null)));
        ExceptionAssert.ydbConfiguration("can't connect", () -> testConnection(connectByProperties("user1", "pass")));

        ExceptionAssert.ydbConfiguration("can't connect", () -> testConnection(connectByProperties("user2", "a")));
    }

    interface ConnectionSupplier {
        Connection get() throws SQLException;
    }
}
