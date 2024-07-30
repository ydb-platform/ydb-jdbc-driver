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
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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

    private static final Instant TEST_TS = Instant.ofEpochMilli(1583288311345l); // Friday, April 3, 2020 02:18:31.345

    @BeforeAll
    public static void setupTimeZone() throws SQLException {
        // Set non UTC timezone to test different cases
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.ofOffset("GMT", ZoneOffset.ofHours(-4))));
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
        try (Statement statement = jdbc.connection().createStatement();) {
            statement.execute(TEST_TABLE.dropTableSQL());
        }
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
    @EnumSource(value=SqlQueries.JdbcQuery.class, names = { "BATCHED", "TYPED" })
    public void executeWithMissingParameter(SqlQueries.JdbcQuery query) throws SQLException {
        String sql = TEST_TABLE.upsertOne(query, "c_Text", "Text");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(sql)) {
            statement.setInt(1, 1);
            ExceptionAssert.sqlDataException("Missing value for parameter", statement::execute);
        }
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(value=SqlQueries.JdbcQuery.class, names = { "BATCHED", "TYPED" })
    public void executeWithWrongType(SqlQueries.JdbcQuery query) throws SQLException {
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


        Date sqlDate = new Date(TEST_TS.toEpochMilli());
        LocalDateTime dateTime = LocalDateTime.ofInstant(TEST_TS, ZoneOffset.UTC).plusMinutes(id);
        Timestamp timestamp = Timestamp.from(TEST_TS.plusSeconds(id));
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


        Date sqlDate = new Date(TEST_TS.toEpochMilli());
        LocalDateTime dateTime = LocalDateTime.ofInstant(TEST_TS, ZoneOffset.UTC).plusMinutes(id)
                .truncatedTo(ChronoUnit.SECONDS);
        Timestamp timestamp = Timestamp.from(TEST_TS.plusSeconds(id));
        Duration duration = Duration.ofMinutes(id);

        Date rsDate = rs.getDate("c_Date");

        Assert.assertEquals(sqlDate.toLocalDate(), rsDate.toLocalDate());
        Assert.assertEquals(dateTime, rs.getObject("c_Datetime"));
//        Assert.assertEquals(timestamp, rs.getTimestamp("c_Timestamp"));
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

    @Disabled
    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void timestampTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Timestamp", "Timestamp");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(upsert)) {
            statement.setInt(1, 1);
            statement.setObject(2, LocalDate.of(2023, Month.MARCH, 3));
            statement.execute();

            statement.setInt(1, 2);
            statement.setObject(2, LocalDateTime.of(2023, Month.MARCH, 3, 14, 56, 59, 123456789));
            statement.execute();

            statement.setInt(1, 3);
            statement.setDate(2, new Date(TEST_TS.toEpochMilli())); // java.sql.Date will be truncated to days
            statement.execute();

            statement.setInt(1, 4);
            statement.setTimestamp(2, new Timestamp(TEST_TS.toEpochMilli()));
            statement.execute();

            if (query != SqlQueries.JdbcQuery.IN_MEMORY) { // IN MEMORY is not typed mode, casting is not supported
                statement.setInt(1, 5);
                statement.setLong(2, 1585932011123l);
                statement.execute();

                statement.setInt(1, 6);
                statement.setString(2, "2011-12-03T10:15:30.456789123Z");
                statement.execute();
            }
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            try (ResultSet rs = statement.executeQuery(TEST_TABLE.selectColumn("c_Timestamp"))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(1, rs.getInt("key"));
                // LocalDate.of(2023, Month.MARCH, 3) UTC == 1677801600000l;
                assertTimestamp(rs,
                        LocalDate.of(2023, Month.MARCH, 3),
                        LocalDateTime.of(2023, Month.MARCH, 3, 0, 0, 0),
                        Instant.ofEpochSecond(1677801600l, 0)
                );

                Assert.assertTrue(rs.next());
                Assert.assertEquals(2, rs.getInt("key"));
                // LocalDateTime.of(2023, Month.MARCH, 3, 14, 56, 59, 123456789) UTC == 1677855419123l;
                assertTimestamp(rs,
                        LocalDate.of(2023, Month.MARCH, 3),
                        LocalDateTime.of(2023, Month.MARCH, 3, 14, 56, 59), // Timestamp supports only micros
                        Instant.ofEpochSecond(1677855419l)
                );

                Assert.assertTrue(rs.next());
                Assert.assertEquals(3, rs.getInt("key"));
                assertTimestamp(rs,
                        LocalDate.of(2020, Month.APRIL, 3),
                        LocalDateTime.of(2020, Month.APRIL, 3, 0, 0, 0),
                        Instant.ofEpochSecond(1585872000l) // Friday, April 3, 2020 00:00:00 UTC
                );

                Assert.assertTrue(rs.next());
                Assert.assertEquals(4, rs.getInt("key"));
                //  Instant.ofEpochMilli(1585932011123l) == Friday, April 3, 2020 16:40:11.123 UTC
                assertTimestamp(rs,
                        LocalDate.of(2022, Month.APRIL, 3),
                        LocalDateTime.of(2022, Month.APRIL, 3, 16, 40, 11, 123000000), // Timestamp supports only micros
                        Instant.ofEpochSecond(1585932011l, 123000000)
                );

                if (query != SqlQueries.JdbcQuery.IN_MEMORY) { // IN MEMORY is not typed mode, casting is not supported
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(5, rs.getInt("key"));
                    //  Instant.ofEpochMilli(1585932011123l) == Friday, April 3, 2020 16:40:11.123 UTC
                    assertTimestamp(rs,
                            LocalDate.of(2022, Month.APRIL, 3),
                            LocalDateTime.of(2022, Month.APRIL, 3, 16, 40, 11, 123000000),
                            Instant.ofEpochSecond(1585932011l, 123000000)
                    );

                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(6, rs.getInt("key"));
                    // 2011-12-03T10:15:30.456789123Z
                    assertTimestamp(rs,
                            LocalDate.of(2011, Month.DECEMBER, 3),
                            LocalDateTime.of(2011, Month.DECEMBER, 3, 10, 15, 30, 456789000),
                            Instant.ofEpochSecond(1322907330l, 456789000)
                    );
                }

                Assert.assertFalse(rs.next());
            }
        }
    };

    private void assertTimestamp(ResultSet rs, LocalDate ld, LocalDateTime ldt, Instant instant) throws SQLException {
        // TODO: NOT SUPPORTED YET
//        Assert.assertEquals(ld, rs.getObject("c_Timestamp", LocalDate.class));
//        Assert.assertEquals(ldt, rs.getObject("c_Timestamp", LocalDateTime.class));
        Object obj = rs.getObject("c_Timestamp");
        Assert.assertTrue(obj instanceof Instant);
        Assert.assertEquals(instant, obj);

        Assert.assertEquals(new Date(instant.toEpochMilli()), rs.getDate("c_Timestamp"));
        Assert.assertEquals(new Timestamp(instant.toEpochMilli()), rs.getTimestamp("c_Timestamp"));
        Assert.assertEquals(instant.toEpochMilli(), rs.getLong("c_Timestamp"));
        Assert.assertEquals(instant.toString(), rs.getString("c_Timestamp"));
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void datetimeTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Datetime", "Datetime");

        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert)) {
            ps.setInt(1, 1);
            ps.setObject(2, LocalDateTime.of(2025, Month.AUGUST, 10, 23, 59, 59, 100));
            ps.execute();

            if (castingSupported) {
                ps.setInt(1, 2);
                ps.setTimestamp(2, new Timestamp(TEST_TS.toEpochMilli()));
                ps.execute();

                ps.setInt(1, 3);
                ps.setLong(2, 1585932011l);
                ps.execute();

                ps.setInt(1, 4);
                ps.setDate(2, new Date(TEST_TS.toEpochMilli()));
                ps.execute();

                ps.setInt(1, 5);
                ps.setObject(2, LocalDate.of(2021, Month.JULY, 21));
                ps.execute();

                ps.setInt(1, 6);
                ps.setString(2, "2011-12-03T10:15:30");
                ps.execute();
            }
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            Instant ts = TEST_TS.truncatedTo(ChronoUnit.SECONDS);
            try (ResultSet rs = statement.executeQuery(TEST_TABLE.selectColumn("c_Datetime"))) {
                assertNextDatetime(rs, 1, LocalDateTime.of(2025, Month.AUGUST, 10, 23, 59, 59));
                if (castingSupported) {
                    assertNextDatetime(rs, 2, ts.atZone(ZoneId.systemDefault()).toLocalDateTime());
                    assertNextDatetime(rs, 3, LocalDateTime.of(2020, Month.APRIL, 3, 16, 40, 11));
                    assertNextDatetime(rs, 4, ts.atZone(ZoneId.systemDefault()).toLocalDateTime());
                    assertNextDatetime(rs, 5, LocalDateTime.of(2021, Month.JULY, 21, 0, 0, 0));
                    assertNextDatetime(rs, 6, LocalDateTime.of(2011, Month.DECEMBER, 3, 10, 15, 30));
                }
                Assert.assertFalse(rs.next());
            }
        }
    };

    private void assertNextDatetime(ResultSet rs, int key, LocalDateTime ldt) throws SQLException {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_Datetime");
        Assert.assertTrue(obj instanceof LocalDateTime);
        Assert.assertEquals(ldt, obj);

        Assert.assertEquals(ldt.toEpochSecond(ZoneOffset.UTC), rs.getLong("c_Datetime"));

        Assert.assertEquals(Date.valueOf(ldt.toLocalDate()), rs.getDate("c_Datetime"));
        Assert.assertEquals(Timestamp.valueOf(ldt), rs.getTimestamp("c_Datetime"));
        Assert.assertEquals(ldt.toString(), rs.getString("c_Datetime"));

        Assert.assertEquals(Long.valueOf(ldt.toEpochSecond(ZoneOffset.UTC)), rs.getObject("c_Datetime", Long.class));
        Assert.assertEquals(ldt.toLocalDate(), rs.getObject("c_Datetime", LocalDate.class));
        Assert.assertEquals(ldt, rs.getObject("c_Datetime", LocalDateTime.class));
        Assert.assertEquals(ldt.atZone(ZoneId.systemDefault()).toInstant(), rs.getObject("c_Datetime", Instant.class));
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void dateTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Date", "Date");
        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert)) {
            ps.setInt(1, 1);
            ps.setObject(2, LocalDate.of(2025, Month.AUGUST, 10));
            ps.execute();

            ps.setInt(1, 2);
            ps.setDate(2, new Date(TEST_TS.toEpochMilli()));
            ps.execute();

            if (castingSupported) {
                ps.setInt(1, 3);
                ps.setTimestamp(2, new Timestamp(TEST_TS.toEpochMilli()));
                ps.execute();

                ps.setInt(1, 4);
                ps.setInt(2, 10); // Jan 11 1970
                ps.execute();

                ps.setInt(1, 5);
                ps.setLong(2, 12345); // Oct 20 2003
                ps.execute();

                ps.setInt(1, 6);
                ps.setObject(2, LocalDateTime.of(2023, Month.MARCH, 3, 14, 56, 59, 123456789));
                ps.execute();

                ps.setInt(1, 7);
                ps.setString(2, "2011-12-03");
                ps.execute();

                ps.setInt(1, 8);
                ps.setObject(2, TEST_TS);
                ps.execute();
            }
        }

        try (Statement st = jdbc.connection().createStatement()) {
            try (ResultSet rs = st.executeQuery(TEST_TABLE.selectColumn("c_Date"))) {
                assertNextDate(rs, 1, LocalDate.of(2025, Month.AUGUST, 10));
                assertNextDate(rs, 2, TEST_TS.atZone(ZoneId.systemDefault()).toLocalDate());

                if (castingSupported) {
                    assertNextDate(rs, 3, TEST_TS.atZone(ZoneId.systemDefault()).toLocalDate());
                    assertNextDate(rs, 4, LocalDate.of(1970, Month.JANUARY, 11));
                    assertNextDate(rs, 5, LocalDate.of(2003, Month.OCTOBER, 20));
                    assertNextDate(rs, 6, LocalDate.of(2023, Month.MARCH, 3));
                    assertNextDate(rs, 7, LocalDate.of(2011, Month.DECEMBER, 3));
                    assertNextDate(rs, 8, TEST_TS.atZone(ZoneId.systemDefault()).toLocalDate());
                }

                Assert.assertFalse(rs.next());
            }
        }
    };

    private void assertNextDate(ResultSet rs, int key, LocalDate ld) throws SQLException {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_Date");
        Assert.assertTrue(obj instanceof LocalDate);
        Assert.assertEquals(ld, obj);

        Assert.assertEquals(ld.toEpochDay(), rs.getInt("c_Date"));
        Assert.assertEquals(ld.toEpochDay(), rs.getLong("c_Date"));

        Assert.assertEquals(Date.valueOf(ld), rs.getDate("c_Date"));
        Assert.assertEquals(Timestamp.valueOf(LocalDateTime.of(ld, LocalTime.MIN)), rs.getTimestamp("c_Date"));
        Assert.assertEquals(ld.toString(), rs.getString("c_Date"));

        Assert.assertEquals(Long.valueOf(ld.toEpochDay()), rs.getObject("c_Date", Long.class));
        Assert.assertEquals(ld, rs.getObject("c_Date", LocalDate.class));
        Assert.assertEquals(ld.atTime(LocalTime.MIN), rs.getObject("c_Date", LocalDateTime.class));
        Assert.assertEquals(ld.atTime(LocalTime.MIN)
                .atZone(ZoneId.systemDefault())
                .toInstant(),
                rs.getObject("c_Date", Instant.class));
    }
}
