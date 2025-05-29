package tech.ydb.jdbc;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbSerialTypesTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb)
            .withArg("usePrefixPath", "serial_types")
            .withArg("enableTxTracer", "true");

    @BeforeAll
    public static void prepareTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            try (Statement st = connection.createStatement()) {
                try {
                    st.execute("DROP TABLE serial_test");
                } catch (SQLException e) {
                    // ignore
                }
                st.execute("CREATE TABLE serial_test(id Serial NOT NULL, value Text, PRIMARY KEY(id))");
            }
        }
    }

    @BeforeEach
    public void cleanTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            try (Statement st = connection.createStatement()) {
                st.execute("DELETE FROM serial_test");
            }
        }
    }

    @Test
    public void singleOps() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            // single insert
            try (PreparedStatement ps = connection.prepareStatement(
                    "$ignored=select 1; INSERT INTO serial_test(value) VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, "first value");

                Assertions.assertEquals(1, ps.executeUpdate());
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("first value", rs.getString("value"));

                    Assertions.assertFalse(rs.next());
                }
            }

            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO serial_test(value) VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, "second value");

                Assertions.assertEquals(1, ps.executeUpdate());
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("second value", rs.getString("value"));

                    Assertions.assertFalse(rs.next());
                }
            }
        }
    }
}
