package tech.ydb.jdbc.impl;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
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
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.SqlQueries;
import tech.ydb.jdbc.impl.helper.TextSelectAssert;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;
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

    /**
     * Mar 04 2020 02:18:31.123456789 UTC
     */
    private static final Instant TEST_TS = Instant.ofEpochSecond(1583288311l, 123456789);

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

    private static Instant truncToMicros(Instant instant) {
        return instant.with(ChronoField.MICRO_OF_SECOND, instant.get(ChronoField.MICRO_OF_SECOND));
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
        Assertions.assertTrue(rs.next());

        Assertions.assertEquals(id, rs.getInt("key"));

        Assertions.assertEquals(id % 2 == 0, rs.getBoolean("c_Bool"));

        Assertions.assertEquals(id + 1, rs.getByte("c_Int8"));
        Assertions.assertEquals(id + 2, rs.getShort("c_Int16"));
        Assertions.assertEquals(id + 3, rs.getInt("c_Int32"));
        Assertions.assertEquals(id + 4, rs.getLong("c_Int64"));

        Assertions.assertEquals(id + 5, rs.getByte("c_Uint8"));
        Assertions.assertEquals(id + 6, rs.getShort("c_Uint16"));
        Assertions.assertEquals(id + 7, rs.getInt("c_Uint32"));
        Assertions.assertEquals(id + 8, rs.getLong("c_Uint64"));

        Assertions.assertEquals(1.5f * id, rs.getFloat("c_Float"), 0.001f);
        Assertions.assertEquals(2.5d * id, rs.getDouble("c_Double"), 0.001d);

        Assertions.assertArrayEquals(new byte[] { (byte)id }, rs.getBytes("c_Bytes"));
        Assertions.assertEquals("Text_" + id, rs.getString("c_Text"));
        Assertions.assertEquals("{\"json\": " + id + "}", rs.getString("c_Json"));
        Assertions.assertEquals("{\"jsonDoc\":" + id + "}", rs.getString("c_JsonDocument"));
        Assertions.assertEquals("{yson=" + id + "}", rs.getString("c_Yson"));


        Date sqlDate = new Date(TEST_TS.toEpochMilli());
        LocalDateTime dateTime = LocalDateTime.ofInstant(TEST_TS, ZoneOffset.UTC).plusMinutes(id)
                .truncatedTo(ChronoUnit.SECONDS);
        Timestamp timestamp = Timestamp.from(truncToMicros(TEST_TS.plusSeconds(id)));

        Assertions.assertEquals(sqlDate.toLocalDate(), rs.getDate("c_Date").toLocalDate());
        Assertions.assertEquals(dateTime, rs.getObject("c_Datetime"));
        Assertions.assertEquals(timestamp, rs.getTimestamp("c_Timestamp"));
        Assertions.assertEquals(Duration.ofMinutes(id), rs.getObject("c_Interval"));

        Assertions.assertNull(rs.getString("c_Decimal"));
        Assertions.assertTrue(rs.wasNull());
    }

    private void assertStructMember(PrimitiveValue value, StructValue sv, String name) {
        int index = sv.getType().getMemberIndex(name);
        Assertions.assertTrue(index >= 0);
        Value<?> member = sv.getMemberValue(index);

        switch (member.getType().getKind()) {
            case OPTIONAL:
                if (value == null) {
                    Assertions.assertFalse(member.asOptional().isPresent());
                } else {
                    Assertions.assertEquals(value, (PrimitiveValue) member.asOptional().get());
                }
                break;
            case PRIMITIVE:
                Assertions.assertEquals(value, member.asData());
                break;
            default:
                throw new AssertionError("Unsupported type " + member.getType());
        }
    }

    private void assertTableRow(ResultSet rs, int id) throws SQLException {
        Assertions.assertTrue(rs.next());

        Object obj = rs.getObject(1);
        Assertions.assertNotNull(obj);
        Assertions.assertFalse(rs.wasNull());
        Assertions.assertEquals(obj, rs.getObject("column0")); // default name of column

        Assertions.assertTrue(obj instanceof StructValue);
        StructValue sv = (StructValue) obj;

        Assertions.assertEquals(22, sv.getType().getMembersCount());

        assertStructMember(PrimitiveValue.newInt32(id), sv, "key");
        assertStructMember(PrimitiveValue.newBool(id % 2 == 0), sv, "c_Bool");

        assertStructMember(PrimitiveValue.newInt8((byte)(id + 1)), sv, "c_Int8");
        assertStructMember(PrimitiveValue.newInt16((short)(id + 2)), sv, "c_Int16");
        assertStructMember(PrimitiveValue.newInt32(id + 3), sv, "c_Int32");
        assertStructMember(PrimitiveValue.newInt64(id + 4), sv, "c_Int64");

        assertStructMember(PrimitiveValue.newUint8(id + 5), sv, "c_Uint8");
        assertStructMember(PrimitiveValue.newUint16(id + 6), sv, "c_Uint16");
        assertStructMember(PrimitiveValue.newUint32(id + 7), sv, "c_Uint32");
        assertStructMember(PrimitiveValue.newUint64(id + 8), sv, "c_Uint64");

        assertStructMember(PrimitiveValue.newFloat(1.5f * id), sv, "c_Float");
        assertStructMember(PrimitiveValue.newDouble(2.5d * id), sv, "c_Double");

        assertStructMember(PrimitiveValue.newBytes(new byte[] { (byte)id }), sv, "c_Bytes");
        assertStructMember(PrimitiveValue.newText("Text_" + id), sv, "c_Text");
        assertStructMember(PrimitiveValue.newJson("{\"json\": " + id + "}"), sv, "c_Json");
        assertStructMember(PrimitiveValue.newJsonDocument("{\"jsonDoc\":" + id + "}"), sv, "c_JsonDocument");
        assertStructMember(PrimitiveValue.newYson(("{yson=" + id + "}").getBytes()), sv, "c_Yson");


        Date sqlDate = new Date(TEST_TS.toEpochMilli());
        LocalDateTime dateTime = LocalDateTime.ofInstant(TEST_TS, ZoneOffset.UTC).plusMinutes(id)
                .truncatedTo(ChronoUnit.SECONDS);

        assertStructMember(PrimitiveValue.newDate(sqlDate.toLocalDate()), sv, "c_Date");
        assertStructMember(PrimitiveValue.newDatetime(dateTime), sv, "c_Datetime");
        assertStructMember(PrimitiveValue.newTimestamp(truncToMicros(TEST_TS.plusSeconds(id))), sv, "c_Timestamp");
        assertStructMember(PrimitiveValue.newInterval(Duration.ofMinutes(id)), sv, "c_Interval");

        assertStructMember(null, sv, "c_Decimal");
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

                Assertions.assertFalse(rs.next());
            }
        }
    };

    @Test
    public void tableRowTest() throws SQLException {
        String upsert = TEST_TABLE.upsertAll(SqlQueries.JdbcQuery.BATCHED);
        String selectTableRow = TEST_TABLE.withTableName("select TableRow() from #tableName");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(upsert)) {
            fillRowValues(statement, 1);
            statement.addBatch();
            fillRowValues(statement, 2);
            statement.addBatch();
            statement.executeBatch();
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            try (ResultSet rs = statement.executeQuery(selectTableRow)) {
                assertTableRow(rs, 1);
                assertTableRow(rs, 2);
                Assertions.assertFalse(rs.next());
            }
        }
    };

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void int32Test(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Int32", "Int32");
        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;
        int ydbSqlType = YdbConst.SQL_KIND_PRIMITIVE + PrimitiveType.Int32.ordinal();

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert)) {
            ps.setInt(1, 1);
            ps.setByte(2, (byte) -120);
            ps.execute();

            ps.setInt(1, 2);
            ps.setShort(2, (short) 1234);
            ps.execute();

            ps.setInt(1, 3);
            ps.setInt(2, 1234567);
            ps.execute();

            ps.setInt(1, 4);
            ps.setTime(2, Time.valueOf(LocalTime.of(11, 23, 59))); // will be stored as 11 * 3600 + 23 * 60 + 59
            ps.execute();

            ps.setInt(1, 5);
            ps.setTime(2, new Time(TEST_TS.toEpochMilli())); // will be stored local time of timestamp
            ps.execute();

            if (castingSupported) {
                ps.setInt(1, 6);
                ps.setBoolean(2, true);
                ps.execute();

                ps.setInt(1, 7);
                ps.setDate(2, Date.valueOf(LocalDate.of(2020, Month.MARCH, 3))); // will be store as day of epoch
                ps.execute();

                ps.setInt(1, 8);
                ps.setDate(2, new Date(TEST_TS.toEpochMilli())); // will be store as day of the year
                ps.execute();

                ps.setInt(1, 9);
                ps.setTimestamp(2, new Timestamp(TEST_TS.toEpochMilli()));
                ps.execute();
            } else {
                ps.setInt(1, 6);
                ps.setObject(2, Boolean.TRUE, ydbSqlType);
                ps.execute();

                ps.setInt(1, 7);
                ps.setObject(2, Date.valueOf(LocalDate.of(2020, Month.MARCH, 3)), ydbSqlType);
                ps.execute();

                ps.setInt(1, 8);
                ps.setObject(2, new Date(TEST_TS.toEpochMilli()), ydbSqlType);
                ps.execute();

                ps.setInt(1, 9);
                ps.setObject(2, new Timestamp(TEST_TS.toEpochMilli()), ydbSqlType);
                ps.execute();
            }
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            try (ResultSet rs = statement.executeQuery(TEST_TABLE.selectColumn("c_Int32"))) {
                assertNextInt32(rs, 1, -120);
                assertNextInt32(rs, 2, 1234);
                assertNextInt32(rs, 3, 1234567);
                assertNextInt32(rs, 4, 11 * 3600 + 23 * 60 + 59);
                assertNextInt32(rs, 5, TEST_TS.atZone(ZoneId.systemDefault()).toLocalTime().toSecondOfDay());

                assertNextInt32(rs, 6, 1);
                assertNextInt32(rs, 7, (int) LocalDate.of(2020, Month.MARCH, 3).toEpochDay());
                assertNextInt32(rs, 8, (int) TEST_TS.atZone(ZoneId.systemDefault()).toLocalDate().toEpochDay());
                assertNextInt32(rs, 9, (int) TEST_TS.toEpochMilli());

                Assertions.assertFalse(rs.next());
            }
        }
    };

    private void assertNextInt32(ResultSet rs, int key, Integer value) throws SQLException {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_Int32");
        Assertions.assertTrue(obj instanceof Integer);
        Assertions.assertEquals(value, obj);

        if (value.byteValue() == value.intValue()) {
            Assertions.assertEquals(value.intValue(), rs.getByte("c_Int32"));
        } else {
            String msg = String.format("Cannot cast [Int32] with value [%s] to [byte]", value);
            ExceptionAssert.sqlException(msg, () -> rs.getByte("c_Int32"));
        }

        if (value.shortValue() == value.intValue()) {
            Assertions.assertEquals(value.intValue(), rs.getShort("c_Int32"));
        } else {
            String msg = String.format("Cannot cast [Int32] with value [%s] to [short]", value);
            ExceptionAssert.sqlException(msg, () -> rs.getShort("c_Int32"));
        }

        Assertions.assertEquals(value.intValue(), rs.getInt("c_Int32"));
        Assertions.assertEquals(value.longValue(), rs.getLong("c_Int32"));

        if (value >= 0 && value < 24 * 3600) {
            Assertions.assertEquals(Time.valueOf(LocalTime.ofSecondOfDay(value)), rs.getTime("c_Int32"));
        } else {
            String msg = String.format("Cannot cast [Int32] with value [%s] to [class java.sql.Time]", value);
            ExceptionAssert.sqlException(msg, () -> rs.getTime("c_Int32"));
        }

        Assertions.assertEquals(Date.valueOf(LocalDate.ofEpochDay(value)), rs.getDate("c_Int32"));
        Assertions.assertEquals(new Timestamp(value), rs.getTimestamp("c_Int32"));
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void int64Test(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Int64", "Int64");
        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;
        int ydbSqlType = YdbConst.SQL_KIND_PRIMITIVE + PrimitiveType.Int64.ordinal();

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert)) {
            ps.setInt(1, 1);
            ps.setByte(2, (byte) -120);
            ps.execute();

            ps.setInt(1, 2);
            ps.setShort(2, (short) 1234);
            ps.execute();

            ps.setInt(1, 3);
            ps.setInt(2, 1234567);
            ps.execute();

            ps.setInt(1, 4);
            ps.setLong(2, 54211234567l);
            ps.execute();

            ps.setInt(1, 5);
            ps.setTime(2, Time.valueOf(LocalTime.of(11, 23, 59))); // will be stored as 11 * 3600 + 23 * 60 + 59
            ps.execute();

            ps.setInt(1, 6);
            ps.setTime(2, new Time(TEST_TS.toEpochMilli())); // will be stored local time of timestamp
            ps.execute();

            if (castingSupported) {
                ps.setInt(1, 7);
                ps.setBoolean(2, true);
                ps.execute();

                ps.setInt(1, 8);
                ps.setDate(2, Date.valueOf(LocalDate.of(2020, Month.MARCH, 3))); // will be store as day of epoch
                ps.execute();

                ps.setInt(1, 9);
                ps.setDate(2, new Date(TEST_TS.toEpochMilli())); // will be store as day of the year
                ps.execute();

                ps.setInt(1, 10);
                ps.setTimestamp(2, new Timestamp(TEST_TS.toEpochMilli()));
                ps.execute();
            } else {
                ps.setInt(1, 7);
                ps.setObject(2, Boolean.TRUE, ydbSqlType);
                ps.execute();

                ps.setInt(1, 8);
                ps.setObject(2, Date.valueOf(LocalDate.of(2020, Month.MARCH, 3)), ydbSqlType);
                ps.execute();

                ps.setInt(1, 9);
                ps.setObject(2, new Date(TEST_TS.toEpochMilli()), ydbSqlType);
                ps.execute();

                ps.setInt(1, 10);
                ps.setObject(2, new Timestamp(TEST_TS.toEpochMilli()), ydbSqlType);
                ps.execute();
            }
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            try (ResultSet rs = statement.executeQuery(TEST_TABLE.selectColumn("c_Int64"))) {
                assertNextInt64(rs, 1, -120l);
                assertNextInt64(rs, 2, 1234l);
                assertNextInt64(rs, 3, 1234567l);
                assertNextInt64(rs, 4, 54211234567l);
                assertNextInt64(rs, 5, 11 * 3600l + 23 * 60 + 59);
                assertNextInt64(rs, 6, Long.valueOf(TEST_TS.atZone(ZoneId.systemDefault()).toLocalTime().toSecondOfDay()));

                assertNextInt64(rs, 7, 1l);
                assertNextInt64(rs, 8, LocalDate.of(2020, Month.MARCH, 3).toEpochDay());
                assertNextInt64(rs, 9, TEST_TS.atZone(ZoneId.systemDefault()).toLocalDate().toEpochDay());
                assertNextInt64(rs, 10, TEST_TS.toEpochMilli());

                Assertions.assertFalse(rs.next());
            }
        }
    };

    private void assertNextInt64(ResultSet rs, int key, Long value) throws SQLException {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_Int64");
        Assertions.assertTrue(obj instanceof Long);
        Assertions.assertEquals(value, obj);

        if (value.byteValue() == value.intValue()) {
            Assertions.assertEquals(value.intValue(), rs.getByte("c_Int64"));
        } else {
            String msg = String.format("Cannot cast [Int64] with value [%s] to [byte]", value);
            ExceptionAssert.sqlException(msg, () -> rs.getByte("c_Int64"));
        }

        if (value.shortValue() == value.intValue()) {
            Assertions.assertEquals(value.intValue(), rs.getShort("c_Int64"));
        } else {
            String msg = String.format("Cannot cast [Int64] with value [%s] to [short]", value);
            ExceptionAssert.sqlException(msg, () -> rs.getShort("c_Int64"));
        }

        if (value.intValue() == value.longValue()) {
            Assertions.assertEquals(value.intValue(), rs.getInt("c_Int64"));
        } else {
            String msg = String.format("Cannot cast [Int64] with value [%s] to [int]", value);
            ExceptionAssert.sqlException(msg, () -> rs.getInt("c_Int64"));
        }

        Assertions.assertEquals(value.longValue(), rs.getLong("c_Int64"));

        if (value >= 0 && value < 24 * 3600) {
            Assertions.assertEquals(Time.valueOf(LocalTime.ofSecondOfDay(value)), rs.getTime("c_Int64"));
        } else {
            String msg = String.format("Cannot cast [Int64] with value [%s] to [class java.sql.Time]", value);
            ExceptionAssert.sqlException(msg, () -> rs.getTime("c_Int64"));
        }

        if (ChronoField.EPOCH_DAY.range().isValidValue(value)) {
            Assertions.assertEquals(Date.valueOf(LocalDate.ofEpochDay(value)), rs.getDate("c_Int64"));
        } else {
            String msg = String.format("Cannot cast [Int64] with value [%s] to [class java.sql.Date]", value);
            ExceptionAssert.sqlException(msg, () -> rs.getDate("c_Int64"));
        }

        Assertions.assertEquals(new Timestamp(value), rs.getTimestamp("c_Int64"));
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void timestampTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Timestamp", "Timestamp");
        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;
        int ydbSqlType = YdbConst.SQL_KIND_PRIMITIVE + PrimitiveType.Timestamp.ordinal();

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert)) {
            ps.setInt(1, 1);
            ps.setObject(2, TEST_TS);
            ps.execute();

            ps.setInt(1, 2);
            ps.setTimestamp(2, Timestamp.from(TEST_TS));
            ps.execute();

            ps.setInt(1, 3);
            ps.setDate(2, new Date(TEST_TS.toEpochMilli()));
            ps.execute();

            ps.setInt(1, 4);
            ps.setObject(2, LocalDate.of(2021, Month.MAY, 22));
            ps.execute();

            ps.setInt(1, 5);
            ps.setObject(2, LocalDateTime.of(2023, Month.MAY, 29, 14, 56, 59, 123456789));
            ps.execute();

            if (castingSupported) {
                ps.setInt(1, 6);
                ps.setLong(2, 1585932011123l);
                ps.execute();

                ps.setInt(1, 7);
                ps.setString(2, "2011-12-03T10:15:30.456789123Z");
                ps.execute();
            } else {
                ps.setInt(1, 6);
                ps.setObject(2, 1585932011123l, ydbSqlType);
                ps.execute();

                ps.setInt(1, 7);
                ps.setObject(2, "2011-12-03T10:15:30.456789123Z", ydbSqlType);
                ps.execute();
            }
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            try (ResultSet rs = statement.executeQuery(TEST_TABLE.selectColumn("c_Timestamp"))) {
                assertNextTimestamp(rs, 1, truncToMicros(TEST_TS));
                assertNextTimestamp(rs, 2, truncToMicros(TEST_TS));
                assertNextTimestamp(rs, 3, LocalDate.of(2020, Month.MARCH, 3).atStartOfDay().toInstant(ZoneOffset.UTC));
                assertNextTimestamp(rs, 4, LocalDate.of(2021, Month.MAY, 22).atStartOfDay().toInstant(ZoneOffset.UTC));
                assertNextTimestamp(rs, 5, LocalDateTime.of(2023, Month.MAY, 29, 14, 56, 59).toInstant(ZoneOffset.UTC));
                assertNextTimestamp(rs, 6, Instant.ofEpochMilli(1585932011123l));
                assertNextTimestamp(rs, 7, Instant.parse("2011-12-03T10:15:30.456789000Z"));

                Assertions.assertFalse(rs.next());
            }
        }
    };

    private void assertNextTimestamp(ResultSet rs, int key, Instant ts) throws SQLException {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_Timestamp");
        Assertions.assertTrue(obj instanceof Instant);
        Assertions.assertEquals(ts, obj);

        Assertions.assertEquals(ts.toEpochMilli(), rs.getLong("c_Timestamp"));

        Assertions.assertEquals(new Date(ts.toEpochMilli()), rs.getDate("c_Timestamp"));
        Assertions.assertEquals(Timestamp.from(ts), rs.getTimestamp("c_Timestamp"));
        Assertions.assertEquals(ts.toString(), rs.getString("c_Timestamp"));

        LocalDateTime ldt = LocalDateTime.ofInstant(ts, ZoneId.systemDefault());
        Assertions.assertEquals(Long.valueOf(ts.toEpochMilli()), rs.getObject("c_Timestamp", Long.class));
        Assertions.assertEquals(ldt.toLocalDate(), rs.getObject("c_Timestamp", LocalDate.class));
        Assertions.assertEquals(ldt, rs.getObject("c_Timestamp", LocalDateTime.class));
        Assertions.assertEquals(ts, rs.getObject("c_Timestamp", Instant.class));
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void datetimeTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Datetime", "Datetime");
        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;
        int ydbSqlType = YdbConst.SQL_KIND_PRIMITIVE + PrimitiveType.Datetime.ordinal();

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert)) {
            ps.setInt(1, 1);
            ps.setObject(2, LocalDateTime.of(2025, Month.AUGUST, 10, 23, 59, 59, 100));
            ps.execute();

            ps.setInt(1, 2);
            ps.setDate(2, new Date(TEST_TS.toEpochMilli()));
            ps.execute();

            if (castingSupported) {
                ps.setInt(1, 3);
                ps.setTimestamp(2, new Timestamp(TEST_TS.toEpochMilli()));
                ps.execute();

                ps.setInt(1, 4);
                ps.setLong(2, 1585932011l);
                ps.execute();

                ps.setInt(1, 5);
                ps.setObject(2, LocalDate.of(2021, Month.JULY, 21));
                ps.execute();

                ps.setInt(1, 6);
                ps.setString(2, "2011-12-03T10:15:30");
                ps.execute();

                ps.setInt(1, 7);
                ps.setObject(2, TEST_TS);
                ps.execute();
            } else {
                ps.setInt(1, 3);
                ps.setObject(2, new Timestamp(TEST_TS.toEpochMilli()), ydbSqlType);
                ps.execute();

                ps.setInt(1, 4);
                ps.setObject(2, 1585932011l, ydbSqlType);
                ps.execute();

                ps.setInt(1, 5);
                ps.setObject(2, LocalDate.of(2021, Month.JULY, 21), ydbSqlType);
                ps.execute();

                ps.setInt(1, 6);
                ps.setObject(2, "2011-12-03T10:15:30", ydbSqlType);
                ps.execute();

                ps.setInt(1, 7);
                ps.setObject(2, TEST_TS, ydbSqlType);
                ps.execute();
            }
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            LocalDateTime ts = TEST_TS.truncatedTo(ChronoUnit.SECONDS).atZone(ZoneId.systemDefault()).toLocalDateTime();
            try (ResultSet rs = statement.executeQuery(TEST_TABLE.selectColumn("c_Datetime"))) {
                assertNextDatetime(rs, 1, LocalDateTime.of(2025, Month.AUGUST, 10, 23, 59, 59));
                assertNextDatetime(rs, 2, LocalDateTime.of(2020, Month.MARCH, 3, 0, 0, 0));
                assertNextDatetime(rs, 3, ts);
                assertNextDatetime(rs, 4, LocalDateTime.of(2020, Month.APRIL, 3, 16, 40, 11));
                assertNextDatetime(rs, 5, LocalDateTime.of(2021, Month.JULY, 21, 0, 0, 0));
                assertNextDatetime(rs, 6, LocalDateTime.of(2011, Month.DECEMBER, 3, 10, 15, 30));
                assertNextDatetime(rs, 7, ts);
                Assertions.assertFalse(rs.next());
            }
        }
    };

    private void assertNextDatetime(ResultSet rs, int key, LocalDateTime ldt) throws SQLException {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_Datetime");
        Assertions.assertTrue(obj instanceof LocalDateTime);
        Assertions.assertEquals(ldt, obj);

        Assertions.assertEquals(ldt.toEpochSecond(ZoneOffset.UTC), rs.getLong("c_Datetime"));

        Assertions.assertEquals(Date.valueOf(ldt.toLocalDate()), rs.getDate("c_Datetime"));
        Assertions.assertEquals(Timestamp.valueOf(ldt), rs.getTimestamp("c_Datetime"));
        Assertions.assertEquals(ldt.toString(), rs.getString("c_Datetime"));

        Assertions.assertEquals(Long.valueOf(ldt.toEpochSecond(ZoneOffset.UTC)), rs.getObject("c_Datetime", Long.class));
        Assertions.assertEquals(ldt.toLocalDate(), rs.getObject("c_Datetime", LocalDate.class));
        Assertions.assertEquals(ldt, rs.getObject("c_Datetime", LocalDateTime.class));
        Assertions.assertEquals(ldt.atZone(ZoneId.systemDefault()).toInstant(), rs.getObject("c_Datetime", Instant.class));
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void dateTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Date", "Date");
        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;
        int ydbSqlType = YdbConst.SQL_KIND_PRIMITIVE + PrimitiveType.Date.ordinal();

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
                ps.setObject(2, LocalDateTime.of(2021, Month.JULY, 23, 14, 56, 59, 123456789));
                ps.execute();

                ps.setInt(1, 7);
                ps.setString(2, "2011-12-03");
                ps.execute();

                ps.setInt(1, 8);
                ps.setObject(2, TEST_TS);
                ps.execute();
            } else {
                ps.setInt(1, 3);
                ps.setObject(2, new Timestamp(TEST_TS.toEpochMilli()), ydbSqlType);
                ps.execute();

                ps.setInt(1, 4);
                ps.setObject(2, 10, ydbSqlType); // Jan 11 1970
                ps.execute();

                ps.setInt(1, 5);
                ps.setObject(2, 12345l, ydbSqlType); // Oct 20 2003
                ps.execute();

                ps.setInt(1, 6);
                ps.setObject(2, LocalDateTime.of(2021, Month.JULY, 23, 14, 56, 59, 123456789), ydbSqlType);
                ps.execute();

                ps.setInt(1, 7);
                ps.setObject(2, "2011-12-03", ydbSqlType);
                ps.execute();

                ps.setInt(1, 8);
                ps.setObject(2, TEST_TS, ydbSqlType);
                ps.execute();
            }
        }

        try (Statement st = jdbc.connection().createStatement()) {
            try (ResultSet rs = st.executeQuery(TEST_TABLE.selectColumn("c_Date"))) {
                assertNextDate(rs, 1, LocalDate.of(2025, Month.AUGUST, 10));
                assertNextDate(rs, 2, LocalDate.of(2020, Month.MARCH, 3));
                assertNextDate(rs, 3, LocalDate.of(2020, Month.MARCH, 3));
                assertNextDate(rs, 4, LocalDate.of(1970, Month.JANUARY, 11));
                assertNextDate(rs, 5, LocalDate.of(2003, Month.OCTOBER, 20));
                assertNextDate(rs, 6, LocalDate.of(2021, Month.JULY, 23));
                assertNextDate(rs, 7, LocalDate.of(2011, Month.DECEMBER, 3));
                assertNextDate(rs, 8, LocalDate.of(2020, Month.MARCH, 3));

                Assertions.assertFalse(rs.next());
            }
        }
    };

    private void assertNextDate(ResultSet rs, int key, LocalDate ld) throws SQLException {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_Date");
        Assertions.assertTrue(obj instanceof LocalDate);
        Assertions.assertEquals(ld, obj);

        Assertions.assertEquals(ld.toEpochDay(), rs.getInt("c_Date"));
        Assertions.assertEquals(ld.toEpochDay(), rs.getLong("c_Date"));

        Assertions.assertEquals(Date.valueOf(ld), rs.getDate("c_Date"));
        Assertions.assertEquals(Timestamp.valueOf(ld.atStartOfDay()), rs.getTimestamp("c_Date"));
        Assertions.assertEquals(ld.toString(), rs.getString("c_Date"));

        Assertions.assertEquals(Long.valueOf(ld.toEpochDay()), rs.getObject("c_Date", Long.class));
        Assertions.assertEquals(ld, rs.getObject("c_Date", LocalDate.class));
        Assertions.assertEquals(ld.atStartOfDay(), rs.getObject("c_Date", LocalDateTime.class));
        Assertions.assertEquals(ld.atStartOfDay(ZoneId.systemDefault()).toInstant(), rs.getObject("c_Date", Instant.class));
    }
}
