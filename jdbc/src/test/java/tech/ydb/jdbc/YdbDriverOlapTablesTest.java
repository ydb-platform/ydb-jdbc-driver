package tech.ydb.jdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbDriverOlapTablesTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb)
            .withArg("enableTxTracer", "true")
            .withArg("usePrefixPath", "jdbc_olap");

    private final static String ERROR_DATA_MANIPULATION =
            "Data manipulation queries do not support column shard tables. (S_ERROR)";

    private final static String ERROR_BULK_UNSUPPORTED =
            "BULK mode is available only for prepared statement with one UPSERT";

    private final static String CREATE_TABLE = ""
            + "CREATE TABLE table("
            + "  id Int32 NOT NULL,"
            + "  value Text,"
            + "  date Date,"
            + "  PRIMARY KEY (id)"
            + ") WITH (STORE = COLUMN)";

    private final static String DROP_TABLE = "DROP TABLE table";
    private final static String UPSERT_ROW = "UPSERT INTO table (id, value, date) VALUES (?, ?, ?)";
    private final static String INSERT_ROW = "INSERT INTO table (id, value, date) VALUES (?, ?, ?)";
    private final static String SELECT_ALL = "SELECT * FROM table ORDER BY id";
    private final static String UPDATE_ROW = "UPDATE table SET value = ? WHERE id = ?";
    private final static String DELETE_ROW = "DELETE FROM table WHERE id = ?";

    @Test
    public void defaultModeTest() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL.build())) {
            try {
                connection.createStatement().execute(DROP_TABLE);
            } catch (SQLException e) {
                // ignore
            }

            connection.createStatement().execute(CREATE_TABLE);

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
            try {
                conn.createStatement().execute(DROP_TABLE);
            } catch (SQLException e) {
                // ignore
            }

            conn.createStatement().execute(CREATE_TABLE);

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

            // scan read
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
                Assertions.assertEquals(1, readed);
            }

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
            try {
                conn.createStatement().execute(DROP_TABLE);
            } catch (SQLException e) {
                // ignore
            }

            conn.createStatement().execute(CREATE_TABLE);

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
        try (Connection conn = DriverManager.getConnection(jdbcURL
                .withArg("useStreamResultSets", "true")
                .build()
        )) {
            try {
                conn.createStatement().execute(DROP_TABLE);
            } catch (SQLException e) {
                // ignore
}

            conn.createStatement().execute(CREATE_TABLE);

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
            try {
                connection.createStatement().execute(DROP_TABLE);
            } catch (SQLException e) {
                // ignore
            }

            connection.createStatement().execute(CREATE_TABLE);

            LocalDate ld = LocalDate.of(2017, 12, 3);
            String prefix = "text-value-";
            int idx = 0;

            // single upsert
            try (PreparedStatement ps = connection.prepareStatement(UPSERT_ROW)) {
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ExceptionAssert.ydbException(ERROR_DATA_MANIPULATION, ps::executeUpdate);
            }

            // single insert
            try (PreparedStatement ps = connection.prepareStatement(INSERT_ROW)) {
                ps.setInt(1, ++idx);
                ps.setString(2, prefix + idx);
                ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                ExceptionAssert.ydbException(ERROR_DATA_MANIPULATION, ps::executeUpdate);
            }

            // batch upsert
            try (PreparedStatement ps = connection.prepareStatement(UPSERT_ROW)) {
                for (int j = 0; j < 1000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ExceptionAssert.ydbException(ERROR_DATA_MANIPULATION, ps::executeBatch);
            }

            // batch insert
            try (PreparedStatement ps = connection.prepareStatement(INSERT_ROW)) {
                for (int j = 0; j < 1000; j++) {
                    ps.setInt(1, ++idx);
                    ps.setString(2, prefix + idx);
                    ps.setDate(3, Date.valueOf(ld.plusDays(idx)));
                    ps.addBatch();
                }
                ExceptionAssert.ydbException(ERROR_DATA_MANIPULATION, ps::executeBatch);
            }

            // read all
            try (Statement st = connection.createStatement()) {
                ExceptionAssert.ydbException(ERROR_DATA_MANIPULATION, () -> st.executeQuery(SELECT_ALL));
            }

            // single update
            try (PreparedStatement ps = connection.prepareStatement(UPDATE_ROW)) {
                ps.setString(1, "updated-value");
                ps.setInt(2, 1);
                ExceptionAssert.ydbException(ERROR_DATA_MANIPULATION, ps::executeUpdate);
            }

            // single delete
            try (PreparedStatement ps = connection.prepareStatement(DELETE_ROW)) {
                ps.setInt(1, 2);
                ExceptionAssert.ydbException(ERROR_DATA_MANIPULATION, ps::executeUpdate);
            }
        }
    }
}
