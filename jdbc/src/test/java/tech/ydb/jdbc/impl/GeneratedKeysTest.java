package tech.ydb.jdbc.impl;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.jdbc.impl.helper.TestTxTracer;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class GeneratedKeysTest {
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
    public void inMemoryQueriesTest() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            TestTxTracer tracer = YdbTracerImpl.use(new TestTxTracer());

            try (PreparedStatement ps = connection.prepareStatement(
                    "$i=select 1; INSERT INTO serial_test(value) VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, "first value");

                Assertions.assertEquals(1, ps.executeUpdate());
                tracer.assertQueriesCount(1);
                tracer.assertLastQueryContains("RETURNING *");

                try (ResultSet rs = ps.getGeneratedKeys()) {
                    Assertions.assertEquals(2, rs.getMetaData().getColumnCount());
                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("first value", rs.getString("value"));

                    Assertions.assertFalse(rs.next());
                }
            }

            try (PreparedStatement ps = connection.prepareStatement(
                    "$i=select 1; INSERT INTO serial_test(value) VALUES (?)", new String[] { "id" })) {
                ps.setString(1, "first value");

                Assertions.assertEquals(1, ps.executeUpdate());
                tracer.assertQueriesCount(1);
                tracer.assertLastQueryContains("RETURNING `id`");

                try (ResultSet rs = ps.getGeneratedKeys()) {
                    Assertions.assertEquals(1, rs.getMetaData().getColumnCount());

                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);

                    Assertions.assertFalse(rs.next());
                }
            }

            try (PreparedStatement ps = connection.prepareStatement(
                    "$i=select 1; UPDATE serial_test SET value = ? WHERE value = ?", new String[] { "id", "value" })) {
                ps.setString(1, "second value");
                ps.setString(2, "first value");

                Assertions.assertEquals(1, ps.executeUpdate());
                tracer.assertQueriesCount(1);
                tracer.assertLastQueryContains("RETURNING `id`, `value`");

                try (ResultSet rs = ps.getGeneratedKeys()) {
                    Assertions.assertEquals(2, rs.getMetaData().getColumnCount());

                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("second value", rs.getString("value"));

                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("second value", rs.getString("value"));

                    Assertions.assertFalse(rs.next());
                }
            }

            try (PreparedStatement ps = connection.prepareStatement(
                    "$i=select 1; DELETE FROM serial_test", Statement.RETURN_GENERATED_KEYS)) {

                Assertions.assertEquals(1, ps.executeUpdate());
                tracer.assertQueriesCount(1);
                tracer.assertLastQueryContains("RETURNING *");

                try (ResultSet rs = ps.getGeneratedKeys()) {
                    Assertions.assertEquals(2, rs.getMetaData().getColumnCount());

                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("second value", rs.getString("value"));

                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("second value", rs.getString("value"));

                    Assertions.assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    @Disabled
    public void inMemoryBatchQueriesTest() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            TestTxTracer tracer = YdbTracerImpl.use(new TestTxTracer());

            try (PreparedStatement ps = connection.prepareStatement(
                    "$i=select 1; INSERT INTO serial_test(value) VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, "first value");
                ps.addBatch();

                ps.setString(1, "second value");
                ps.addBatch();

                Assertions.assertEquals(2, ps.executeBatch().length);

                tracer.assertQueriesCount(2);
                tracer.assertLastQueryContains("RETURNING *");

                try (ResultSet rs = ps.getGeneratedKeys()) {
                    Assertions.assertEquals(2, rs.getMetaData().getColumnCount());
                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("first value", rs.getString("value"));

                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("second value", rs.getString("value"));

                    Assertions.assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void standardQueriesTest() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            TestTxTracer tracer = YdbTracerImpl.use(new TestTxTracer());

            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO serial_test(value) VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, "first value");
                ps.addBatch();
                ps.setString(1, "second value");
                ps.addBatch();
                ps.setString(1, "third value");
                ps.addBatch();

                Assertions.assertEquals(3, ps.executeBatch().length);
                tracer.assertQueriesCount(1);
                tracer.assertLastQueryContains("FROM AS_TABLE($batch)");
                tracer.assertLastQueryContains("RETURNING *");

                try (ResultSet rs = ps.getGeneratedKeys()) {
                    Assertions.assertEquals(2, rs.getMetaData().getColumnCount());
                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("first value", rs.getString("value"));
                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("second value", rs.getString("value"));
                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("third value", rs.getString("value"));

                    Assertions.assertFalse(rs.next());
                }
            }

            try (PreparedStatement ps = connection.prepareStatement(
                    "UPDATE serial_test SET value = ? WHERE value = ?", new String[] { "id", "value" })) {
                ps.setString(1, "second value");
                ps.setString(2, "first value");

                Assertions.assertEquals(1, ps.executeUpdate());
                tracer.assertQueriesCount(1);
                tracer.assertLastQueryContains("RETURNING `id`, `value`");

                try (ResultSet rs = ps.getGeneratedKeys()) {
                    Assertions.assertEquals(2, rs.getMetaData().getColumnCount());

                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("second value", rs.getString("value"));

                    Assertions.assertFalse(rs.next());
                }
            }

            try (PreparedStatement ps = connection.prepareStatement(
                    "$i=select 1; DELETE FROM serial_test WHERE value=?", Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, "second value");

                Assertions.assertEquals(1, ps.executeUpdate());
                tracer.assertQueriesCount(1);
                tracer.assertLastQueryContains("RETURNING *");

                try (ResultSet rs = ps.getGeneratedKeys()) {
                    Assertions.assertEquals(2, rs.getMetaData().getColumnCount());

                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("second value", rs.getString("value"));

                    Assertions.assertTrue(rs.next());
                    Assertions.assertTrue(rs.getInt("id") != 0);
                    Assertions.assertEquals("second value", rs.getString("value"));

                    Assertions.assertFalse(rs.next());
                }
            }
        }
    }
}
