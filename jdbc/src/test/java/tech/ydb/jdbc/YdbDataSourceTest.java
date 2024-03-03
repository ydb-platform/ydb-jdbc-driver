package tech.ydb.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class YdbDataSourceTest {
    @RegisterExtension
    public static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcUrlBuilder = new JdbcUrlHelper(ydb);

    @Test
    void testConnection() throws SQLException {
        final DataSource dataSource = new YdbDataSource(jdbcUrlBuilder.build());

        try (Connection connection = dataSource.getConnection()) {
            Assertions.assertFalse(connection.isClosed());
            Assertions.assertTrue(connection.isValid(5000));
        }
    }
}
