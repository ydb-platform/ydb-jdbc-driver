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
                statement.execute(""
                        + "CREATE USER user1 PASSWORD NULL;\n"
                        + "CREATE USER user2 PASSWORD 'pwss';\n"
                        + "CREATE USER user3 PASSWORD 'pw :ss;'\n;"
                );
            }
        }
    }

    @AfterAll
    public static void dropUsers() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP USER IF EXISTS user1, user2, user3;");
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

    private void wrongConnection(ConnectionSupplier connectionSupplier) {
        ExceptionAssert.ydbConfiguration("Cannot connect to YDB", () -> testConnection(connectionSupplier));
    }

    @Test
    public void connectOK() throws SQLException {
        testConnection(connectByProperties("user1", ""));
        testConnection(connectByAuthority("user1", ""));

        testConnection(connectByProperties("user1", null));
        testConnection(connectByAuthority("user1", null));

        testConnection(connectByProperties("user2", "pwss"));
        testConnection(connectByAuthority("user2", "pwss"));

        testConnection(connectByProperties("user3", "pw :ss;"));
        testConnection(connectByAuthority("user3", "pw :ss;"));
    }

    @Test
    public void connectWrong() throws SQLException {
        wrongConnection(connectByProperties("user1", "a"));
        wrongConnection(connectByAuthority("user1", "a"));

        wrongConnection(connectByProperties("user2", ""));
        wrongConnection(connectByProperties("user2", null));
        wrongConnection(connectByProperties("user2", "pass"));

        wrongConnection(connectByAuthority("user2", ""));
        wrongConnection(connectByAuthority("user2", null));
        wrongConnection(connectByAuthority("user2", "pass"));

        wrongConnection(connectByProperties("user3", ""));
        wrongConnection(connectByProperties("user3", null));
        wrongConnection(connectByProperties("user3", "pw:ss;"));

        wrongConnection(connectByAuthority("user3", ""));
        wrongConnection(connectByAuthority("user3", null));
        wrongConnection(connectByAuthority("user3", "pw:ss;"));
    }

    interface ConnectionSupplier {
        Connection get() throws SQLException;
    }
}
