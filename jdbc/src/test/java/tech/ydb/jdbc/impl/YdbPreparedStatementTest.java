package tech.ydb.jdbc.impl;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.SqlQueries;
import tech.ydb.jdbc.impl.helper.TextSelectAssert;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbPreparedStatementTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private static final JdbcConnectionExtention jdbc = new JdbcConnectionExtention(ydb);

    private static final SqlQueries TEST_TABLE = new SqlQueries("ydb_prepared_test");

    private static final Instant INSTANT = Instant.ofEpochMilli(1585932011123l); // Friday, April 3, 2020 4:40:11.123 PM

    // remove time part from instant in UTC
    private static Instant calcStartDayUTC(Instant instant) {
        return LocalDate.ofInstant(instant, ZoneOffset.UTC).atStartOfDay().toInstant(ZoneOffset.UTC);
    }

    @BeforeAll
    public static void createTable() throws SQLException {
        try (Statement statement = jdbc.connection().createStatement();) {
            // create test table
            statement.execute(TEST_TABLE.createTableSQL());
        }
    }

    @AfterAll
    public static void dropTable() throws SQLException {
//        try (Statement statement = jdbc.connection().createStatement();) {
//            statement.execute(TEST_TABLE.dropTableSQL());
//        }
    }

    @BeforeEach
    public void beforaEach() throws SQLException {
        try (Statement statement = jdbc.connection().createStatement();) {
            // clean table
            statement.execute(TEST_TABLE.deleteAllSQL());
        }
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void executeUpdateTest(SqlQueries.JdbcQuery query) throws SQLException {
        String sql = TEST_TABLE.upsertOne(query, "c_Text", "Text");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(sql)) {
            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.executeQuery("select 1 + 2"));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.execute("select 1 + 2"));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.execute("select 1 + 2", Statement.NO_GENERATED_KEYS));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.execute("select 1 + 2", new int[0]));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.execute("select 1 + 2", new String[0]));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.executeUpdate("select 1 + 2"));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.executeUpdate("select 1 + 2", Statement.NO_GENERATED_KEYS));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.executeUpdate("select 1 + 2", new int[0]));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.executeUpdate("select 1 + 2", new String[0]));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.addBatch("select 1 + 2"));
        }
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void executeWithMissingParameter(SqlQueries.JdbcQuery query) throws SQLException {
        String sql = TEST_TABLE.upsertOne(query, "c_Text", "Text");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(sql)) {
            statement.setInt(1, 1);
            ExceptionAssert.ydbNonRetryable("Missing value for parameter", statement::execute);
        }
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(value=SqlQueries.JdbcQuery.class)
    public void executeWithWrongType(SqlQueries.JdbcQuery query) throws SQLException {
        if (query == SqlQueries.JdbcQuery.STANDART) {
            // Standart mode doesn't support type checking
            return;
        }

        String sql = TEST_TABLE.upsertOne(query, "c_Text", "Text"); // Must be Optional<Text>

        try (PreparedStatement statement = jdbc.connection().prepareStatement(sql)) {
            statement.setInt(1, 1);
            ExceptionAssert.sqlException("Missing required value for parameter: $p2",
                    () -> statement.setObject(2, PrimitiveType.Text.makeOptional().emptyValue())
            );
        }
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void simpleUpsertTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Text", "Text");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(upsert)) {
            statement.setInt(1, 1);
            statement.setString(2, "value-1");
            statement.execute();

            statement.setInt(1, 2);
            statement.setString(2, "value-2");
            statement.execute();
        }

        String select = TEST_TABLE.selectColumn("c_Text");
        try (Statement statement = jdbc.connection().createStatement()) {
            TextSelectAssert.of(statement.executeQuery(select), "c_Text", "Text")
                    .nextRow(1, "value-1")
                    .nextRow(2, "value-2")
                    .noNextRows();
        }
    };

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void batchUpsertTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Text", "Text");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(upsert)) {
            // ----- base usage -----
            statement.setInt(1, 1);
            statement.setString(2, "value-1");
            statement.addBatch();

            statement.setInt(1, 2);
            statement.setString(2, "value-2");
            statement.addBatch();

            statement.executeBatch();

            // ----- executeBatch without addBatch -----
            statement.setInt(1, 3);
            statement.setString(2, "value-3");
            statement.addBatch();

            statement.setInt(1, 4);
            statement.setString(2, "value-4");

            statement.executeBatch();

            // ----- execute instead of executeBatch -----
            statement.setInt(1, 5);
            statement.setString(2, "value-5");
            statement.addBatch();

            statement.setInt(1, 6);
            statement.setString(2, "value-6");

            statement.execute();
        }

        String select = TEST_TABLE.selectColumn("c_Text");
        try (Statement statement = jdbc.connection().createStatement()) {
            TextSelectAssert.of(statement.executeQuery(select), "c_Text", "Text")
                    .nextRow(1, "value-1")
                    .nextRow(2, "value-2")
                    .nextRow(3, "value-3")
                    .nextRow(6, "value-6")
                    .noNextRows();
        }
    };

    private void fillRowValues(PreparedStatement statement, int id) throws SQLException {
        statement.setInt(1, id);                // id

        statement.setBoolean(2, id % 2 == 0);   // c_Bool

        statement.setByte(3, (byte)(id + 1));   // c_Int8
        statement.setShort(4, (short)(id + 2)); // c_Int16
        statement.setInt(5, id + 3);            // c_Int32
        statement.setLong(6, id + 4);           // c_Int64

        statement.setByte(7, (byte)(id + 5));   // c_Uint8
        statement.setShort(8, (short)(id + 6)); // c_Uint16
        statement.setInt(9, id + 7);            // c_Uint32
        statement.setLong(10, id + 8);          // c_Uint64

        statement.setFloat(11, 1.5f * id);      // c_Float
        statement.setDouble(12, 2.5d * id);     // c_Double

        statement.setBytes(13, new byte[] { (byte)id });      // c_Bytes
        statement.setString(14, "Text_" + id);                // c_Text
        statement.setString(15, "{\"json\": " + id + "}");    // c_Json
        statement.setString(16, "{\"jsonDoc\": " + id + "}"); // c_JsonDocument
        statement.setString(17, "{yson=" + id + "}");         // c_Yson


        Date sqlDate = new Date(calcStartDayUTC(INSTANT.plus(id, ChronoUnit.DAYS)).toEpochMilli());
        LocalDateTime dateTime = LocalDateTime.ofInstant(INSTANT, ZoneOffset.UTC).plusMinutes(id);
        Timestamp timestamp = Timestamp.from(INSTANT.plusSeconds(id));
        Duration duration = Duration.ofMinutes(id);

        statement.setDate(18, sqlDate);        // c_Date
        statement.setObject(19, dateTime);     // c_Datetime
        statement.setTimestamp(20, timestamp); // c_Timestamp
        statement.setObject(21, duration);     // c_Interval

        statement.setNull(22, Types.DECIMAL); // c_Decimal
    }

    private void assertRowValues(ResultSet rs, int id) throws SQLException {
        Assert.assertTrue(rs.next());

        Assert.assertEquals(id, rs.getInt("key"));

        Assert.assertEquals(id % 2 == 0, rs.getBoolean("c_Bool"));

        Assert.assertEquals(id + 1, rs.getByte("c_Int8"));
        Assert.assertEquals(id + 2, rs.getShort("c_Int16"));
        Assert.assertEquals(id + 3, rs.getInt("c_Int32"));
        Assert.assertEquals(id + 4, rs.getLong("c_Int64"));

        Assert.assertEquals(id + 5, rs.getByte("c_Uint8"));
        Assert.assertEquals(id + 6, rs.getShort("c_Uint16"));
        Assert.assertEquals(id + 7, rs.getInt("c_Uint32"));
        Assert.assertEquals(id + 8, rs.getLong("c_Uint64"));

        Assert.assertEquals(1.5f * id, rs.getFloat("c_Float"), 0.001f);
        Assert.assertEquals(2.5d * id, rs.getDouble("c_Double"), 0.001d);

        Assert.assertArrayEquals(new byte[] { (byte)id }, rs.getBytes("c_Bytes"));
        Assert.assertEquals("Text_" + id, rs.getString("c_Text"));
        Assert.assertEquals("{\"json\": " + id + "}", rs.getString("c_Json"));
        Assert.assertEquals("{\"jsonDoc\":" + id + "}", rs.getString("c_JsonDocument"));
        Assert.assertEquals("{yson=" + id + "}", rs.getString("c_Yson"));


        Date sqlDate = new Date(calcStartDayUTC(INSTANT.plus(id, ChronoUnit.DAYS)).toEpochMilli());
        LocalDateTime dateTime = LocalDateTime.ofInstant(INSTANT, ZoneOffset.UTC).plusMinutes(id)
                .truncatedTo(ChronoUnit.SECONDS);
        Timestamp timestamp = Timestamp.from(INSTANT.plusSeconds(id));
        Duration duration = Duration.ofMinutes(id);

        Date rsDate =  rs.getDate("c_Date");

        Assert.assertEquals(sqlDate, rsDate);
        Assert.assertEquals(dateTime, rs.getObject("c_Datetime"));
        Assert.assertEquals(timestamp, rs.getTimestamp("c_Timestamp"));
        Assert.assertEquals(duration, rs.getObject("c_Interval"));

        Assert.assertNull(rs.getString("c_Decimal"));
        Assert.assertTrue(rs.wasNull());
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void batchUpsertAllTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertAll(query);

        try (PreparedStatement statement = jdbc.connection().prepareStatement(upsert)) {
            // ----- base usage -----
            fillRowValues(statement, 1);
            statement.addBatch();

            fillRowValues(statement, 2);
            statement.addBatch();

            statement.executeBatch();

            // ----- executeBatch without addBatch -----
            fillRowValues(statement, 3);
            statement.addBatch();

            fillRowValues(statement, 4);
            statement.executeBatch();

            // ----- execute instead of executeBatch -----
            fillRowValues(statement, 5);
            statement.addBatch();

            fillRowValues(statement, 6);
            statement.execute();
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            try (ResultSet rs = statement.executeQuery(TEST_TABLE.selectSQL())) {
                assertRowValues(rs, 1);
                assertRowValues(rs, 2);
                assertRowValues(rs, 3);
                assertRowValues(rs, 6);

                Assert.assertFalse(rs.next());
            }
        }
    };
}
