package tech.ydb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbDriverStaticCredsTest {
    private static final String ERROR_PREFIX = "Cannot connect to YDB: Discovery failed";
    private static final String ERROR_SUFFIX = "Can't login, code: UNAUTHORIZED, "
            + "issues: [#400020 Invalid password (S_FATAL)]";

    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private static final JdbcConnectionExtention jdbc = new JdbcConnectionExtention(ydb);

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb);

    @BeforeAll
    public static void createUsers() throws SQLException {
        try (Statement statement = jdbc.connection().createStatement()) {
            statement.execute(""
                    + "CREATE USER user1 PASSWORD NULL;\n"
                    + "CREATE USER user2 PASSWORD 'pwss';\n"
                    + "CREATE USER user3 PASSWORD 'pwss';\n"
                    + "CREATE USER user4 PASSWORD 'pw@&ss'\n;"
                    + "CREATE USER user5 PASSWORD 'pw@&ss'\n;"
            );
        }
    }

    @AfterAll
    public static void dropUsers() throws SQLException {
        try (Statement statement = jdbc.connection().createStatement()) {
            statement.execute("DROP USER IF EXISTS user1, user2, user3, user4, user5;");
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

    private void testConnection(ConnectionSupplier connectionSupplier, String userName) throws SQLException {
        try (Connection connection = connectionSupplier.get()) {
            Assertions.assertEquals(userName, connection.getMetaData().getUserName());
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1;");
            }
        }
    }

    private void wrongConnection(ConnectionSupplier connectionSupplier) {
        SQLException ex = Assertions.assertThrows(SQLException.class, () -> testConnection(connectionSupplier, null));
        int len = ex.getMessage().length();
        Assertions.assertEquals(ERROR_PREFIX, ex.getMessage().substring(0, ERROR_PREFIX.length()));
        Assertions.assertEquals(ERROR_SUFFIX, ex.getMessage().substring(len - ERROR_SUFFIX.length(), len));
    }

    @Test
    public void connectOK() throws SQLException {
        testConnection(connectByProperties("user1", ""), "user1");
        testConnection(connectByAuthority("user1", ""), "user1");

        testConnection(connectByProperties("user1", null), "user1");
        testConnection(connectByAuthority("user1", null), "user1");

        testConnection(connectByProperties("user2", "pwss"), "user2");
        testConnection(connectByAuthority("user3", "pwss"), "user3");

        testConnection(connectByProperties("user4", "pw@&ss"), "user4");
        testConnection(connectByAuthority("user5", "pw@&ss"), "user5");
    }

    @Test
    public void connectWrong() throws SQLException {
        wrongConnection(connectByProperties("user1", "a"));
        wrongConnection(connectByAuthority("user1", "a"));

        wrongConnection(connectByProperties("user2", ""));
        wrongConnection(connectByProperties("user2", null));
        wrongConnection(connectByProperties("user2", "pass"));

        wrongConnection(connectByAuthority("user3", ""));
        wrongConnection(connectByAuthority("user3", null));
        wrongConnection(connectByAuthority("user3", "pass"));

        wrongConnection(connectByProperties("user4", ""));
        wrongConnection(connectByProperties("user4", null));
        wrongConnection(connectByProperties("user4", "pw:ss;"));

        wrongConnection(connectByAuthority("user5", ""));
        wrongConnection(connectByAuthority("user5", null));
        wrongConnection(connectByAuthority("user5", "pw:@&ss"));
    }

    interface ConnectionSupplier {
        Connection get() throws SQLException;
    }
}
