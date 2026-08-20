package tech.ydb.jdbc;


import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.Month;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbDriverTablesTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb)
            .withArg("queryTimeout", "30s")
            .withArg("enableTxTracer", "true")
            .withArg("usePrefixPath", "jdbc_oltp");

    private final static String ERROR_BULK_UNSUPPORTED =
            "BULK mode is available only for prepared statement with one UPSERT";

    private final static String CREATE_TABLE = ""
            + "CREATE TABLE table ("
            + "  id Int32 NOT NULL,"
            + "  value Text,"
            + "  date Date,"
            + "  PRIMARY KEY (id)"
            + ")";

    private final static String DROP_TABLE = "DROP TABLE IF EXISTS table";
    private final static String UPSERT_ROW = "UPSERT INTO table (id, value, date) VALUES (?, ?, ?)";
    private final static String INSERT_ROW = "INSERT INTO table (id, value, date) VALUES (?, ?, ?)";
    private final static String SELECT_ALL = "SELECT * FROM table ORDER BY id";
    private final static String SELECT_FIRST_100 = "SELECT * FROM table ORDER BY id LIMIT 100";
    private final static String SELECT_LAST_100 = "SELECT * FROM table ORDER BY id DESC LIMIT 100";
    private final static String UPDATE_ROW = "UPDATE table SET value = ? WHERE id = ?";
    private final static String DELETE_ROW = "DELETE FROM table WHERE id = ?";

    @BeforeEach
    public void dropTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            try (Statement st = connection.createStatement()) {
                st.execute(DROP_TABLE);
            }
        }
    }

    @Test
    public void defaultModeTest() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            connection.createStatement().execute(CREATE_TABLE);

            Assertions.assertTrue(connection.isValid(10));

            LocalDate ld = LocalDate.of(2017, 12, 3);
            String prefix = "text-value-";
            int idx = 0;

            // single upsert
            try (PreparedStatement ps = connection.prepareStatement(UPSERT_ROW)) {
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.executeUpdate();
            }

            // single insert
            try (PreparedStatement ps = connection.prepareStatement(INSERT_ROW)) {
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.executeUpdate();
            }

            // batch upsert
            try (PreparedStatement ps = connection.prepareStatement(UPSERT_ROW)) {
                for (int j = 0; j < 1000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // batch insert
            try (PreparedStatement ps = connection.prepareStatement(INSERT_ROW)) {
                for (int j = 0; j < 1000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // read all
            try (PreparedStatement ps = connection.prepareStatement(SELECT_ALL)) {
                int readed = 0;
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        readed++;
                        Assertions.assertEquals(readed, rs.getInt("id"));
                        Assertions.assertEquals(prefix + readed, rs.getString("value"));
                        Assertions.assertEquals(Date.valueOf(ld.plusDays(readed)), rs.getDate("date"));
                    }
                }
                Assertions.assertEquals(2002, readed);
            }

            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            // single update
            try (PreparedStatement ps = connection.prepareStatement(UPDATE_ROW)) {
                ps.setString(1, "updated-value");
                ps.setInt(2, 1);
                ps.executeUpdate();
            }

            // single delete
            try (PreparedStatement ps = connection.prepareStatement(DELETE_ROW)) {
                ps.setInt(1, 2);
                ps.executeUpdate();
            }
        }
    }

    @Test
    public void customQueriesTest() throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcURL.build())) {
            conn.createStatement().execute(CREATE_TABLE);

            Assertions.assertTrue(conn.isValid(10));

            LocalDate ld = LocalDate.of(2017, 12, 3);
            String prefix = "text-value-";
            int idx = 0;

            // single bulk upsert
            try (PreparedStatement ps = conn.prepareStatement("BULK " + UPSERT_ROW)) {
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.executeUpdate();
            }

            // single bulk insert
            ExceptionAssert.sqlException(ERROR_BULK_UNSUPPORTED, () -> conn.prepareStatement("BULK " + INSERT_ROW));

            // batch bulk upsert
            try (PreparedStatement ps = conn.prepareStatement("BULK " + UPSERT_ROW)) {
                for (int j = 0; j < 2000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();

                for (int j = 0; j < 2000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();

                // single row insert
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.execute();
            }

            // read all
            try (Statement st = conn.createStatement()) {
                int readed = 0;
                try (ResultSet rs = st.executeQuery("SCAN " + SELECT_ALL)) {
                    while (rs.next()) {
                        readed++;
                        Assertions.assertEquals(readed, rs.getInt("id"));
                        Assertions.assertEquals(prefix + readed, rs.getString("value"));
                        Assertions.assertEquals(Date.valueOf(ld.plusDays(readed)), rs.getDate("date"));
                    }
                }
                Assertions.assertEquals(4002, readed);
            }

            // single update
            ExceptionAssert.sqlException(ERROR_BULK_UNSUPPORTED, () -> conn.prepareStatement("BULK " + UPDATE_ROW));

            // single delete
            ExceptionAssert.sqlException(ERROR_BULK_UNSUPPORTED, () -> conn.prepareStatement("BULK " + DELETE_ROW));
        }
    }

    @Test
    public void forceScanAndBulkTest() throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcURL
                .withArg("replaceInsertByUpsert", "true")
                .withArg("forceBulkUpsert", "true")
                .withArg("forceScanSelect", "true")
                .build()
        )) {
            conn.createStatement().execute(CREATE_TABLE);

            Assertions.assertTrue(conn.isValid(10));

            LocalDate ld = LocalDate.of(2017, 12, 3);
            String prefix = "text-value-";
            int idx = 0;

            // single bulk upsert
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_ROW)) {
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.executeUpdate();
            }

            // single bulk insert
            try (PreparedStatement ps = conn.prepareStatement(INSERT_ROW)) {
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.executeUpdate();
            }

            // scan read
            try (Statement st = conn.createStatement()) {
                int readed = 0;
                try (ResultSet rs = st.executeQuery(SELECT_ALL)) {
                    while (rs.next()) {
                        readed++;
                        Assertions.assertEquals(readed, rs.getInt("id"));
                        Assertions.assertEquals(prefix + readed, rs.getString("value"));
                        Assertions.assertEquals(Date.valueOf(ld.plusDays(readed)), rs.getDate("date"));
                    }
                }
                Assertions.assertEquals(2, readed);
            }

            // batch bulk upsert
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_ROW)) {
                for (int j = 0; j < 2000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();

                // single row upsert
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.execute();
            }

            // batch bulk inserts
            try (PreparedStatement ps = conn.prepareStatement(INSERT_ROW)) {
                for (int j = 0; j < 2000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();

                // single row insert
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.execute();
            }

            // read all
            try (Statement st = conn.createStatement()) {
                int readed = 0;
                try (ResultSet rs = st.executeQuery(SELECT_ALL)) {
                    while (rs.next()) {
                        readed++;
                        Assertions.assertEquals(readed, rs.getInt("id"));
                        Assertions.assertEquals(prefix + readed, rs.getString("value"));
                        Assertions.assertEquals(Date.valueOf(ld.plusDays(readed)), rs.getDate("date"));
                    }
                }
                Assertions.assertEquals(4004, readed);
            }

            // single update
            try (PreparedStatement ps = conn.prepareStatement(UPDATE_ROW)) {
                ps.setString(1, "updated-value");
                ps.setInt(2, 1);
                ps.executeUpdate();
            }

            // single delete
            try (PreparedStatement ps = conn.prepareStatement(DELETE_ROW)) {
                ps.setInt(1, 2);
                ps.executeUpdate();
            }
        }
    }

    @Test
    public void streamResultsTest() throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcURL.withArg("useStreamResultSets", "true").build())) {
            conn.createStatement().execute(CREATE_TABLE);

            Assertions.assertTrue(conn.isValid(10));

            LocalDate ld = LocalDate.of(2017, 12, 3);
            String prefix = "text-value-";
            int idx = 0;

            // single batch upsert
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_ROW)) {
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.executeUpdate();
            }

            // single batch insert
            try (PreparedStatement ps = conn.prepareStatement(INSERT_ROW)) {
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.executeUpdate();
            }

            // stream read
            try (Statement st = conn.createStatement()) {
                int readed = 0;
                try (ResultSet rs = st.executeQuery(SELECT_ALL)) {
                    while (rs.next()) {
                        readed++;
                        Assertions.assertEquals(readed, rs.getInt("id"));
                        Assertions.assertEquals(prefix + readed, rs.getString("value"));
                        Assertions.assertEquals(Date.valueOf(ld.plusDays(readed)), rs.getDate("date"));
                    }
                }
                Assertions.assertEquals(2, readed);
            }

            // batch upsert
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_ROW)) {
                for (int j = 0; j < 2000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();

                // single row upsert
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.execute();

                for (int j = 0; j < 2000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // batch inserts
            try (PreparedStatement ps = conn.prepareStatement(INSERT_ROW)) {
                for (int j = 0; j < 2000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();

                // single row insert
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.execute();

                for (int j = 0; j < 2000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // read all
            try (Statement st = conn.createStatement()) {
                st.setFetchSize(1000);
                int readed = 0;
                try (ResultSet rs = st.executeQuery(SELECT_ALL)) {
                    while (rs.next()) {
                        readed++;
                        Assertions.assertEquals(readed, rs.getInt("id"));
                        Assertions.assertEquals(prefix + readed, rs.getString("value"));
                        Assertions.assertEquals(Date.valueOf(ld.plusDays(readed)), rs.getDate("date"));
                    }
                }
                Assertions.assertEquals(8004, readed);
            }

            // read multiple result sets
            try (Statement st = conn.createStatement()) {
                st.setFetchSize(100);
                Assertions.assertTrue(st.execute(SELECT_FIRST_100 + ";" + SELECT_LAST_100));
                try (ResultSet rs = st.getResultSet()) {
                    int readed = 0;
                    while (rs.next()) {
                        readed++;
                        Assertions.assertEquals(readed, rs.getInt("id"));
                        Assertions.assertEquals(prefix + readed, rs.getString("value"));
                        Assertions.assertEquals(Date.valueOf(ld.plusDays(readed)), rs.getDate("date"));
                    }
                    Assertions.assertEquals(100, readed);
                }
                Assertions.assertTrue(st.getMoreResults());
                try (ResultSet rs = st.getResultSet()) {
                    int readed = 0;
                    while (rs.next()) {
                        int id = 8004 - readed;
                        readed++;
                        Assertions.assertEquals(id, rs.getInt("id"));
                        Assertions.assertEquals(prefix + id, rs.getString("value"));
                        Assertions.assertEquals(Date.valueOf(ld.plusDays(id)), rs.getDate("date"));
                    }
                    Assertions.assertEquals(100, readed);
                }
                Assertions.assertFalse(st.getMoreResults());
            }

            // single update
            try (PreparedStatement ps = conn.prepareStatement(UPDATE_ROW)) {
                ps.setString(1, "updated-value");
                ps.setInt(2, 1);
                ps.executeUpdate();
            }

            // single delete
            try (PreparedStatement ps = conn.prepareStatement(DELETE_ROW)) {
                ps.setInt(1, 2);
                ps.executeUpdate();
            }
        }
    }

    @Test
    public void tableServiceModeTest() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.withArg("useQueryService", "false").build())) {
            connection.createStatement().execute(CREATE_TABLE);

            Assertions.assertTrue(connection.isValid(10));

            LocalDate ld = LocalDate.of(2017, 12, 3);
            String prefix = "text-value-";
            int idx = 0;

            // single upsert
            try (PreparedStatement ps = connection.prepareStatement(UPSERT_ROW)) {
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.executeUpdate();
            }

            // single insert
            try (PreparedStatement ps = connection.prepareStatement(INSERT_ROW)) {
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ps.executeUpdate();
            }

            // batch upsert
            try (PreparedStatement ps = connection.prepareStatement(UPSERT_ROW)) {
                for (int j = 0; j < 1000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // batch insert
            try (PreparedStatement ps = connection.prepareStatement(INSERT_ROW)) {
                for (int j = 0; j < 1000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // read all
            try (Statement st = connection.createStatement()) {
                int readed = 0;
                try (ResultSet rs = st.executeQuery(SELECT_ALL)) {
                    while (rs.next()) {
                        readed++;
                        Assertions.assertEquals(readed, rs.getInt("id"));
                        Assertions.assertEquals(prefix + readed, rs.getString("value"));
                        Assertions.assertEquals(Date.valueOf(ld.plusDays(readed)), rs.getDate("date"));
                    }
                }
                Assertions.assertEquals(1000, readed);
            }

            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            // single update
            try (PreparedStatement ps = connection.prepareStatement(UPDATE_ROW)) {
                ps.setString(1, "updated-value");
                ps.setInt(2, 1);
                ps.executeUpdate();
            }

            // single delete
            try (PreparedStatement ps = connection.prepareStatement(DELETE_ROW)) {
                ps.setInt(1, 2);
                ps.executeUpdate();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    public void errorsMappingTest(String useQS) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcURL.withArg("useQueryService", useQS).build())) {
            try (Statement st = conn.createStatement()) {
                SQLException ex = Assertions.assertThrows(SQLException.class, () -> st.execute(";NON SQL TEXT"));
                Assertions.assertEquals("42000", ex.getSQLState());
                Assertions.assertEquals(400080, ex.getErrorCode());
                Assertions.assertTrue(ex.getMessage().contains("no viable alternative at input"));
            }

            try (Statement st = conn.createStatement()) {
                SQLException ex = Assertions.assertThrows(SQLException.class, () -> st.execute(
                        "CREATE TABLE table (id Int32, value Text, PRIMARY KEY(id2))"
                ));
                Assertions.assertEquals("42000", ex.getSQLState());
                Assertions.assertEquals(400080, ex.getErrorCode());
                Assertions.assertTrue(ex.getMessage().contains("Undefined column: id2"));
            }

            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE table (id Int32, value Text, date Date, "
                        + "PRIMARY KEY(id), "
                        + "INDEX uniq GLOBAL UNIQUE ON (value))");
            }

            try (PreparedStatement ps = conn.prepareStatement(INSERT_ROW)) {
                ps.setInt(1, 1);
                ps.setString(2, "value");
                ps.setDate(3, Date.valueOf(LocalDate.of(2001, Month.MARCH, 4)));
                Assertions.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = conn.prepareStatement(INSERT_ROW)) {
                ps.setInt(1, 1);
                ps.setString(2, "value2");
                ps.setDate(3, Date.valueOf(LocalDate.of(2001, Month.MARCH, 4)));

                SQLException ex = Assertions.assertThrows(SQLException.class, ps::execute);
                Assertions.assertEquals("23505", ex.getSQLState());
                Assertions.assertEquals(400120, ex.getErrorCode());
                Assertions.assertTrue(ex.getMessage().contains("Conflict with existing key."));
            }

            try (PreparedStatement ps = conn.prepareStatement(INSERT_ROW)) {
                ps.setInt(1, 2);
                ps.setString(2, "value");
                ps.setDate(3, Date.valueOf(LocalDate.of(2001, Month.MARCH, 4)));

                SQLException ex = Assertions.assertThrows(SQLException.class, ps::execute);
                Assertions.assertEquals("23505", ex.getSQLState());
                Assertions.assertEquals(400120, ex.getErrorCode());
                Assertions.assertTrue(ex.getMessage().contains("Conflict with existing key."));
            }
        }
    }
}
