package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class UsePrefixPathTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb);

    @Test
    public void baseQueryTest() throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcURL.withArg("usePrefixPath", "p1").build())) {
            try (ResultSet rs = conn.createStatement().executeQuery("SELECT 1 + 2")) {
                Assertions.assertTrue(rs.next());
            }
        }
    }

    @Test
    public void listTablesTest() throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcURL.withArg("usePrefixPath", "p2").build())) {
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getTables(null, null, null, null)) {
                Assertions.assertFalse(rs.next());
            }
        }
    }

    @Test
    public void createTableTest() throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcURL.withArg("usePrefixPath", "p3").build())) {
            conn.createStatement().execute("CREATE TABLE test (id Int32, PRIMARY KEY(id))");
            try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM test;")) {
                Assertions.assertTrue(rs.next());
            }
        }

        try (Connection conn = DriverManager.getConnection(jdbcURL.build())) {
            try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM `p3/test`;")) {
                Assertions.assertTrue(rs.next());
            }
        }
    }
}
