package tech.ydb.jdbc.impl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
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
import java.util.UUID;
import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.SqlQueries;
import tech.ydb.jdbc.impl.helper.TestTxTracer;
import tech.ydb.jdbc.impl.helper.TextSelectAssert;
import tech.ydb.table.values.DecimalType;
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
    private static final JdbcConnectionExtention jdbc = new JdbcConnectionExtention(ydb)
            .withArg("enableTxTracer", "true");

    private static final SqlQueries TEST_TABLE = new SqlQueries("ydb_prepared_test");

    /**
     * Mar 04 2020 02:18:31.123456789 UTC
     */
    private static final Instant TEST_TS = Instant.ofEpochSecond(1583288311l, 123456789);
    /**
     * May 22 1969 02:21:34.123456789 UTC
     */
    private static final Instant TEST_NTS = Instant.ofEpochSecond(-19345106l, -876543210);

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

    @Test
    public void executeWithMissingParameter() throws SQLException {
        Function<SqlQueries.JdbcQuery, String> upsert = mode -> TEST_TABLE.upsertOne(mode, "c_Text", "Text");

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert.apply(SqlQueries.JdbcQuery.STANDARD))) {
            ps.setInt(1, 1);
            ExceptionAssert.sqlDataException("Missing value for parameter: c_Text", ps::execute);
        }

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert.apply(SqlQueries.JdbcQuery.IN_MEMORY))) {
            ps.setInt(1, 1);
            ExceptionAssert.sqlDataException("Missing value for parameter: $jp2", ps::execute);
        }

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert.apply(SqlQueries.JdbcQuery.TYPED))) {
            ps.setInt(1, 1);
            ExceptionAssert.sqlDataException("Missing value for parameter: $p2", ps::execute);
        }

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert.apply(SqlQueries.JdbcQuery.BATCHED))) {
            ps.setInt(1, 1);
            ExceptionAssert.sqlDataException("Missing value for parameter: $p2", ps::execute);
        }

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert.apply(SqlQueries.JdbcQuery.BULK))) {
            ps.setInt(1, 1);
            ExceptionAssert.sqlDataException("Missing value for parameter: $c_Text", ps::execute);
        }
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(value = SqlQueries.JdbcQuery.class, names = {"BATCHED", "TYPED"})
    public void executeWithWrongType(SqlQueries.JdbcQuery query) throws SQLException {
        String sql = TEST_TABLE.upsertOne(query, "c_Text", "Text");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(sql)) {
            statement.setInt(1, 1);
            ExceptionAssert.sqlException("Missing required value for parameter: $p2",
                    () -> statement.setObject(2, PrimitiveType.Text.makeOptional().emptyValue())
            );
        }
    }

    @ParameterizedTest(name = "with {0}")
    @ValueSource(strings = {"c_NotText", "C_TEXT", "c_text"})
    public void executeWithWrongColumnName(String columnName) throws SQLException {
        String errorMessage = "No such column: " + columnName;

        String standard = TEST_TABLE.upsertOne(SqlQueries.JdbcQuery.STANDARD, columnName, "Text");
        String inMemory = TEST_TABLE.upsertOne(SqlQueries.JdbcQuery.IN_MEMORY, columnName, "Text");
        String typed = TEST_TABLE.upsertOne(SqlQueries.JdbcQuery.TYPED, columnName, "Text");
        String batched = TEST_TABLE.upsertOne(SqlQueries.JdbcQuery.BATCHED, columnName, "Text");
        String bulk = TEST_TABLE.upsertOne(SqlQueries.JdbcQuery.BULK, columnName, "Text");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(standard)) {
            statement.setInt(1, 1);
            statement.setString(2, "value-1");
            ExceptionAssert.ydbException(errorMessage, statement::execute);
        }

        try (PreparedStatement statement = jdbc.connection().prepareStatement(inMemory)) {
            statement.setInt(1, 1);
            statement.setString(2, "value-1");
            ExceptionAssert.ydbException(errorMessage, statement::execute);
        }

        ExceptionAssert.ydbException(errorMessage, () -> jdbc.connection().prepareStatement(typed));
        ExceptionAssert.ydbException(errorMessage, () -> jdbc.connection().prepareStatement(batched));
        ExceptionAssert.sqlException("Cannot parse BULK upsert: column " + columnName + " not found",
                () -> jdbc.connection().prepareStatement(bulk));
    }

    @ParameterizedTest(name = "with {0}")
    @ValueSource(strings = {"unknown_table"/*, "YDB_PREPARED_TEST", "ydD_prepared_test"*/})
    public void executeWithWrongTableName(String tableName) throws SQLException {
        String errorMessage = "Cannot find table 'db.[" + jdbc.database() + "/" + tableName + "]";
        SqlQueries queries = new SqlQueries(tableName);

        String standard = queries.upsertOne(SqlQueries.JdbcQuery.STANDARD, "c_Text", "Text");
        String inMemory = queries.upsertOne(SqlQueries.JdbcQuery.IN_MEMORY, "c_Text", "Text");
        String typed = queries.upsertOne(SqlQueries.JdbcQuery.TYPED, "c_Text", "Text");
        String batched = queries.upsertOne(SqlQueries.JdbcQuery.BATCHED, "c_Text", "Text");
        String bulk = queries.upsertOne(SqlQueries.JdbcQuery.BULK, "c_Text", "Text");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(standard)) {
            statement.setInt(1, 1);
            statement.setString(2, "value-1");
            ExceptionAssert.ydbException(errorMessage, statement::execute);
        }

        try (PreparedStatement statement = jdbc.connection().prepareStatement(inMemory)) {
            statement.setInt(1, 1);
            statement.setString(2, "value-1");
            ExceptionAssert.ydbException(errorMessage, statement::execute);
        }

        ExceptionAssert.ydbException(errorMessage, () -> jdbc.connection().prepareStatement(typed));
        ExceptionAssert.ydbException(errorMessage, () -> jdbc.connection().prepareStatement(batched));
        ExceptionAssert.sqlException("Cannot parse BULK upsert: Status{code = SCHEME_ERROR(code=400070)}",
                () -> jdbc.connection().prepareStatement(bulk));
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
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void batchUpsertTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Text", "Text");
        boolean batched = query != SqlQueries.JdbcQuery.TYPED && query != SqlQueries.JdbcQuery.IN_MEMORY;
        try (PreparedStatement statement = jdbc.connection().prepareStatement(upsert)) {
            TestTxTracer tracer = YdbTracerImpl.use(new TestTxTracer());

            // ----- base usage -----
            statement.setInt(1, 1);
            statement.setString(2, "value-1");
            statement.addBatch();

            statement.setInt(1, 2);
            statement.setString(2, "value-2");
            statement.addBatch();

            statement.executeBatch();
            tracer.assertQueriesCount(batched ? 1 : 2);

            // ----- executeBatch without addBatch -----
            statement.setInt(1, 3);
            statement.setString(2, "value-3");
            statement.addBatch();

            statement.setInt(1, 4);
            statement.setString(2, "value-4");

            statement.executeBatch();
            tracer.assertQueriesCount(1);

            // ----- execute instead of executeBatch -----
            statement.setInt(1, 5);
            statement.setString(2, "value-5");
            statement.addBatch();

            statement.setInt(1, 6);
            statement.setString(2, "value-6");

            statement.execute();
            tracer.assertQueriesCount(1);
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
    }

    private int ydbType(PrimitiveType type) {
        return YdbConst.SQL_KIND_PRIMITIVE + type.ordinal();
    }

    private int ydbType(DecimalType type) {
        return YdbConst.SQL_KIND_DECIMAL + (type.getPrecision() << 6) + type.getScale();
    }

    private void fillRowValues(PreparedStatement statement, int id, boolean castingSupported) throws SQLException {
        statement.setInt(1, id);                // id

        statement.setBoolean(2, id % 2 == 0);   // c_Bool

        statement.setByte(3, (byte) (id + 1));   // c_Int8
        statement.setShort(4, (short) (id + 2)); // c_Int16
        statement.setInt(5, id + 3);             // c_Int32
        statement.setLong(6, id + 4);            // c_Int64

        if (castingSupported) {
            statement.setByte(7, (byte) (id + 5));   // c_Uint8
            statement.setShort(8, (short) (id + 6)); // c_Uint16
            statement.setInt(9, id + 7);             // c_Uint32
            statement.setLong(10, id + 8);           // c_Uint64
        } else {
            statement.setObject(7, id + 5, ydbType(PrimitiveType.Uint8));   // c_Uint8
            statement.setObject(8, id + 6, ydbType(PrimitiveType.Uint16));  // c_Uint16
            statement.setObject(9, id + 7, ydbType(PrimitiveType.Uint32));  // c_Uint32
            statement.setObject(10, id + 8, ydbType(PrimitiveType.Uint64)); // c_Uint64
        }

        statement.setFloat(11, 1.5f * id);      // c_Float
        statement.setDouble(12, 2.5d * id);     // c_Double

        statement.setBytes(13, ("bytes" + id).getBytes()); // c_Bytes
        statement.setString(14, "Text_" + id);             // c_Text

        UUID uuid = UUID.nameUUIDFromBytes(("uuid" + id).getBytes());
        if (castingSupported) {
            statement.setString(15, "{\"json\": " + id + "}");    // c_Json
            statement.setString(16, "{\"jsonDoc\": " + id + "}"); // c_JsonDocument
            statement.setString(17, "{yson=" + id + "}");         // c_Yson
            statement.setString(18, uuid.toString()); // c_Uuid
        } else {
            statement.setObject(15, "{\"json\": " + id + "}", ydbType(PrimitiveType.Json));            // c_Json
            statement.setObject(16, "{\"jsonDoc\": " + id + "}", ydbType(PrimitiveType.JsonDocument)); // c_JsonDocument
            statement.setObject(17, "{yson=" + id + "}", ydbType(PrimitiveType.Yson));                 // c_Yson
            statement.setObject(18, uuid, ydbType(PrimitiveType.Uuid));                                   // c_Uuid
        }

        Date sqlDate = new Date(TEST_TS.toEpochMilli());
        LocalDateTime dateTime = LocalDateTime.ofInstant(TEST_TS, ZoneOffset.UTC).plusMinutes(id);
        Timestamp timestamp = Timestamp.from(TEST_TS.plusSeconds(id));
        Duration duration = Duration.ofMinutes(id);

        statement.setDate(19, sqlDate);        // c_Date
        statement.setObject(20, dateTime);     // c_Datetime
        statement.setTimestamp(21, timestamp); // c_Timestamp
        statement.setObject(22, duration);     // c_Interval

        Date sqlDate32 = new Date(TEST_NTS.toEpochMilli());
        LocalDateTime dateTime64 = LocalDateTime.ofInstant(TEST_NTS, ZoneOffset.UTC).plusMinutes(id);
        Timestamp timestamp64 = Timestamp.from(TEST_NTS.plusSeconds(id));

        if (castingSupported) {
            statement.setDate(23, sqlDate32);            // c_Date32
            statement.setObject(24, dateTime64);         // c_Datetime64
            statement.setTimestamp(25, timestamp64);     // c_Timestamp64
            statement.setObject(26, duration.negated()); // c_Interval64
        } else {
            statement.setObject(23, sqlDate32, ydbType(PrimitiveType.Date32));              // c_Date32
            statement.setObject(24, dateTime64, ydbType(PrimitiveType.Datetime64));         // c_Datetime64
            statement.setObject(25, timestamp64, ydbType(PrimitiveType.Timestamp64));       // c_Timestamp64
            statement.setObject(26, duration.negated(), ydbType(PrimitiveType.Interval64)); // c_Interval64
        }

        statement.setBigDecimal(27, BigDecimal.valueOf(10000 + id, 3)); // c_Decimal
        if (castingSupported) {
            statement.setBigDecimal(28, BigDecimal.valueOf(20000 + id, 0)); // c_BigDecimal
            statement.setBigDecimal(29, BigDecimal.valueOf(30000 + id, 6)); // c_BankDecimal
        } else {
            statement.setObject(28, BigDecimal.valueOf(20000 + id, 0), ydbType(DecimalType.of(35, 0))); // c_BigDecimal
            statement.setObject(29, BigDecimal.valueOf(30000 + id, 6), ydbType(DecimalType.of(31, 9))); // c_BankDecimal
        }
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

        Assertions.assertArrayEquals(("bytes" + id).getBytes(), rs.getBytes("c_Bytes"));
        Assertions.assertEquals("Text_" + id, rs.getString("c_Text"));
        Assertions.assertEquals("{\"json\": " + id + "}", rs.getString("c_Json"));
        Assertions.assertEquals("{\"jsonDoc\":" + id + "}", rs.getString("c_JsonDocument"));
        Assertions.assertEquals("{yson=" + id + "}", rs.getString("c_Yson"));
        Assertions.assertEquals(UUID.nameUUIDFromBytes(("uuid" + id).getBytes()).toString(), rs.getString("c_Uuid"));

        Date sqlDate = new Date(TEST_TS.toEpochMilli());
        LocalDateTime dateTime = LocalDateTime.ofInstant(TEST_TS, ZoneOffset.UTC).plusMinutes(id)
                .truncatedTo(ChronoUnit.SECONDS);
        Timestamp timestamp = Timestamp.from(truncToMicros(TEST_TS.plusSeconds(id)));

        Assertions.assertEquals(sqlDate.toLocalDate(), rs.getDate("c_Date").toLocalDate());
        Assertions.assertEquals(dateTime, rs.getObject("c_Datetime"));
        Assertions.assertEquals(timestamp, rs.getTimestamp("c_Timestamp"));
        Assertions.assertEquals(Duration.ofMinutes(id), rs.getObject("c_Interval"));

        Date sqlDate32 = new Date(TEST_NTS.toEpochMilli());
        LocalDateTime dateTime64 = LocalDateTime.ofInstant(TEST_NTS, ZoneOffset.UTC).plusMinutes(id)
                .truncatedTo(ChronoUnit.SECONDS);
        Timestamp timestamp64 = Timestamp.from(truncToMicros(TEST_NTS.plusSeconds(id)));

        Assertions.assertEquals(sqlDate32.toLocalDate(), rs.getDate("c_Date32").toLocalDate());
        Assertions.assertEquals(dateTime64, rs.getObject("c_Datetime64"));
        Assertions.assertEquals(timestamp64, rs.getTimestamp("c_Timestamp64"));
        Assertions.assertEquals(Duration.ofMinutes(-id), rs.getObject("c_Interval64"));

        Assertions.assertEquals(BigDecimal.valueOf(1000000l * (10000 + id), 9), rs.getBigDecimal("c_Decimal"));
        Assertions.assertEquals(BigDecimal.valueOf(20000 + id), rs.getBigDecimal("c_BigDecimal"));
        Assertions.assertEquals(BigDecimal.valueOf(1000l * (30000 + id), 9), rs.getBigDecimal("c_BankDecimal"));
    }

    private void assertStructMember(Value<?> value, StructValue sv, String name) {
        int index = sv.getType().getMemberIndex(name);
        Assertions.assertTrue(index >= 0);
        Value<?> member = sv.getMemberValue(index);

        switch (member.getType().getKind()) {
            case OPTIONAL:
                if (value == null) {
                    Assertions.assertFalse(member.asOptional().isPresent());
                } else {
                    Assertions.assertTrue(value.equals(member.asOptional().get()));
                }
                break;
            case PRIMITIVE:
            case DECIMAL:
                Assertions.assertTrue(value.equals(member.asOptional().get()));
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

        Assertions.assertEquals(30, sv.getType().getMembersCount());

        assertStructMember(PrimitiveValue.newInt32(id), sv, "key");
        assertStructMember(PrimitiveValue.newBool(id % 2 == 0), sv, "c_Bool");

        assertStructMember(PrimitiveValue.newInt8((byte) (id + 1)), sv, "c_Int8");
        assertStructMember(PrimitiveValue.newInt16((short) (id + 2)), sv, "c_Int16");
        assertStructMember(PrimitiveValue.newInt32(id + 3), sv, "c_Int32");
        assertStructMember(PrimitiveValue.newInt64(id + 4), sv, "c_Int64");

        assertStructMember(PrimitiveValue.newUint8(id + 5), sv, "c_Uint8");
        assertStructMember(PrimitiveValue.newUint16(id + 6), sv, "c_Uint16");
        assertStructMember(PrimitiveValue.newUint32(id + 7), sv, "c_Uint32");
        assertStructMember(PrimitiveValue.newUint64(id + 8), sv, "c_Uint64");

        assertStructMember(PrimitiveValue.newFloat(1.5f * id), sv, "c_Float");
        assertStructMember(PrimitiveValue.newDouble(2.5d * id), sv, "c_Double");

        assertStructMember(PrimitiveValue.newBytes(("bytes" + id).getBytes()), sv, "c_Bytes");
        assertStructMember(PrimitiveValue.newText("Text_" + id), sv, "c_Text");
        assertStructMember(PrimitiveValue.newJson("{\"json\": " + id + "}"), sv, "c_Json");
        assertStructMember(PrimitiveValue.newJsonDocument("{\"jsonDoc\":" + id + "}"), sv, "c_JsonDocument");
        assertStructMember(PrimitiveValue.newYson(("{yson=" + id + "}").getBytes()), sv, "c_Yson");

        assertStructMember(PrimitiveValue.newUuid(UUID.nameUUIDFromBytes(("uuid" + id).getBytes())), sv, "c_Uuid");

        Date sqlDate = new Date(TEST_TS.toEpochMilli());
        LocalDateTime dateTime = LocalDateTime.ofInstant(TEST_TS, ZoneOffset.UTC).plusMinutes(id)
                .truncatedTo(ChronoUnit.SECONDS);

        assertStructMember(PrimitiveValue.newDate(sqlDate.toLocalDate()), sv, "c_Date");
        assertStructMember(PrimitiveValue.newDatetime(dateTime), sv, "c_Datetime");
        assertStructMember(PrimitiveValue.newTimestamp(truncToMicros(TEST_TS.plusSeconds(id))), sv, "c_Timestamp");
        assertStructMember(PrimitiveValue.newInterval(Duration.ofMinutes(id)), sv, "c_Interval");

        Date sqlDate32 = new Date(TEST_NTS.toEpochMilli());
        LocalDateTime dateTime64 = LocalDateTime.ofInstant(TEST_NTS, ZoneOffset.UTC).plusMinutes(id)
                .truncatedTo(ChronoUnit.SECONDS);

        assertStructMember(PrimitiveValue.newDate32(sqlDate32.toLocalDate()), sv, "c_Date32");
        assertStructMember(PrimitiveValue.newDatetime64(dateTime64), sv, "c_Datetime64");
        assertStructMember(PrimitiveValue.newTimestamp64(truncToMicros(TEST_NTS.plusSeconds(id))), sv, "c_Timestamp64");
        assertStructMember(PrimitiveValue.newInterval64(Duration.ofMinutes(-id)), sv, "c_Interval64");

        assertStructMember(DecimalType.getDefault().newValue(BigDecimal.valueOf(10000 + id, 3)), sv, "c_Decimal");
        assertStructMember(DecimalType.of(35, 0).newValue(BigDecimal.valueOf(20000 + id, 0)), sv, "c_BigDecimal");
        assertStructMember(DecimalType.of(31, 9).newValue(BigDecimal.valueOf(30000 + id, 6)), sv, "c_BankDecimal");
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void batchUpsertAllTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertAll(query);
        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;

        try (PreparedStatement statement = jdbc.connection().prepareStatement(upsert)) {
            // ----- base usage -----
            fillRowValues(statement, 1, castingSupported);
            statement.addBatch();

            fillRowValues(statement, 2, castingSupported);
            statement.addBatch();

            statement.executeBatch();

            // ----- executeBatch without addBatch -----
            fillRowValues(statement, 3, castingSupported);
            statement.addBatch();

            fillRowValues(statement, 4, castingSupported);
            statement.executeBatch();

            // ----- execute instead of executeBatch -----
            fillRowValues(statement, 5, castingSupported);
            statement.addBatch();

            fillRowValues(statement, 6, castingSupported);
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
    }

    @Test
    public void setStringTest() throws SQLException {
        String upsert = TEST_TABLE.upsertAll(SqlQueries.JdbcQuery.STANDARD);
        int id = 120;
        UUID uuid = UUID.nameUUIDFromBytes(("uuid" + id).getBytes());

        try (PreparedStatement statement = jdbc.connection().prepareStatement(upsert)) {
            statement.setString(1, String.valueOf(id)); // id
            statement.setString(2, String.valueOf(id % 2 == 0));   // c_Bool
            statement.setString(3, String.valueOf(id + 1)); // c_Int8
            statement.setString(4, String.valueOf(id + 2)); // c_Int16
            statement.setString(5, String.valueOf(id + 3)); // c_Int32
            statement.setString(6, String.valueOf(id + 4)); // c_Int64
            statement.setString(7, String.valueOf(id + 5)); // c_Uint8
            statement.setString(8, String.valueOf(id + 6)); // c_Uint16
            statement.setString(9, String.valueOf(id + 7)); // c_Uint32
            statement.setString(10, String.valueOf(id + 8));    // c_Uint64
            statement.setString(11, String.valueOf(1.5f * id)); // c_Float
            statement.setString(12, String.valueOf(2.5d * id)); // c_Double
            statement.setString(13, "bytes" + id);              // c_Bytes
            statement.setString(14, "Text_" + id);              // c_Text
            statement.setString(15, "{\"json\": " + id + "}");  // c_Json
            statement.setString(16, "{\"jsonDoc\": " + id + "}"); // c_JsonDocument
            statement.setString(17, "{yson=" + id + "}");         // c_Yson
            statement.setString(18, uuid.toString()); // c_Uuid

            Date sqlDate = new Date(TEST_TS.toEpochMilli());
            LocalDateTime dateTime = LocalDateTime.ofInstant(TEST_TS, ZoneOffset.UTC).plusMinutes(id);

            statement.setString(19, sqlDate.toString());   // c_Date
            statement.setString(20, dateTime.toString());  // c_Datetime
            statement.setString(21, TEST_TS.plusSeconds(id).toString()); // c_Timestamp
            statement.setString(22, Duration.ofMinutes(id).toString());  // c_Interval

            Date sqlDate32 = new Date(TEST_NTS.toEpochMilli());
            LocalDateTime dateTime64 = LocalDateTime.ofInstant(TEST_NTS, ZoneOffset.UTC).plusMinutes(id);

            statement.setString(23, sqlDate32.toString());   // c_Date32
            statement.setString(24, dateTime64.toString());  // c_Datetime64
            statement.setString(25, TEST_NTS.plusSeconds(id).toString()); // c_Timestamp64
            statement.setString(26, Duration.ofMinutes(-id).toString());     // c_Interval64

            statement.setString(27, BigDecimal.valueOf(10000 + id, 3).toString()); // c_Decimal
            statement.setString(28, BigDecimal.valueOf(20000 + id, 0).toString()); // c_BigDecimal
            statement.setString(29, BigDecimal.valueOf(30000 + id, 6).toString()); // c_BankDecimal

            statement.execute();
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            try (ResultSet rs = statement.executeQuery(TEST_TABLE.selectSQL())) {
                assertRowValues(rs, id);
                Assertions.assertFalse(rs.next());
            }
        }
    }

    @Test
    public void tableRowTest() throws SQLException {
        String upsert = TEST_TABLE.upsertAll(SqlQueries.JdbcQuery.BATCHED);
        String selectTableRow = TEST_TABLE.withTableName("select TableRow() from #tableName");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(upsert)) {
            fillRowValues(statement, 1, true);
            statement.addBatch();
            fillRowValues(statement, 2, true);
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
    }

    private void assertResultSetCount(ResultSet rs, int count) throws SQLException {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(count, rs.getInt(1));
        Assertions.assertFalse(rs.next());
        rs.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    public void inListTest(boolean convertInToList) throws SQLException {
        String option = String.valueOf(convertInToList);
        String arg1Name = convertInToList ? "$jp1[0]" : "$jp1";
        String arg2Name = convertInToList ? "$jp1[1]" : "$jp2";
        try (Connection conn = jdbc.createCustomConnection("replaceJdbcInByYqlList", option)) {
            String upsert = TEST_TABLE.upsertOne(SqlQueries.JdbcQuery.STANDARD, "c_Text", "Text");
            String selectPrefix = TEST_TABLE.withTableName("select count(*) from #tableName ");
            String selectByIds = selectPrefix + "where key in (?, ?)";
            String selectByValue = selectPrefix + "where c_Text in (?, ?)";
            String selectByTuple = selectPrefix + "where (key, c_Text) in ((?, ?), (?, ?))";

            try (PreparedStatement ps = conn.prepareStatement(upsert)) {
                ps.setInt(1, 1);
                ps.setString(2, "1");
                ps.addBatch();

                ps.setInt(1, 2);
                ps.setString(2, null);
                ps.addBatch();

                ps.setInt(1, 3);
                ps.setString(2, "3");
                ps.addBatch();

                ps.setInt(1, 4);
                ps.setString(2, "null");
                ps.addBatch();

                ps.executeBatch();
            }

            try (PreparedStatement ps = conn.prepareStatement(selectByIds)) {
                ps.setInt(1, 1);
                ExceptionAssert.sqlException("Missing value for parameter: " + arg2Name, ps::executeQuery);

                ps.setInt(1, 1);
                ps.setInt(2, 2);
                assertResultSetCount(ps.executeQuery(), 2);

                ps.setInt(1, 1);
                ps.setInt(2, 5);
                assertResultSetCount(ps.executeQuery(), 1);

                ps.setInt(1, 1);
                if (convertInToList) {
                    ExceptionAssert.sqlException("Cannot cast [class java.lang.String: text] to [Int32]", () -> {
                        ps.setString(2, "text");
                    });
                } else {
                    ExceptionAssert.ydbException("Can't compare Optional<Int32> with Utf8 (S_ERROR)", () -> {
                        ps.setString(2, "text");
                        ps.executeQuery();
                    });
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(selectByValue)) {
                ExceptionAssert.sqlException("Missing value for parameter: " + arg1Name, ps::executeQuery);

                ps.setString(1, null);
                ExceptionAssert.sqlException("Missing value for parameter: " + arg2Name, ps::executeQuery);

                ps.setString(1, null);
                ps.setString(2, null);
                assertResultSetCount(ps.executeQuery(), 0);

                ps.setString(1, "1");
                ps.setString(2, null);
                assertResultSetCount(ps.executeQuery(), 1);

                ps.setString(1, null);
                ps.setString(2, "2");
                assertResultSetCount(ps.executeQuery(), 0);

                ps.setString(1, "1");
                ps.setString(2, "1");
                assertResultSetCount(ps.executeQuery(), 1);

                ps.setString(1, "1");
                ps.setString(2, "2");
                assertResultSetCount(ps.executeQuery(), 1);

                ps.setString(1, "1");
                ps.setString(2, "3");
                assertResultSetCount(ps.executeQuery(), 2);
            }

            try (PreparedStatement ps = conn.prepareStatement(selectByTuple)) {
                ExceptionAssert.sqlException("Missing value for parameter: " + arg1Name, ps::executeQuery);

                ps.setInt(1, 1);
                ExceptionAssert.sqlException("Missing value for parameter: " + arg2Name, ps::executeQuery);

                ps.setInt(1, 1);
                ps.setInt(3, 2);
                ps.setString(4, "3");
                ExceptionAssert.sqlException("Missing value for parameter: " + arg2Name, ps::executeQuery);

                ps.setInt(1, 1);
                ps.setString(2, null);
                ps.setInt(3, 2);
                ps.setString(4, "3");
                assertResultSetCount(ps.executeQuery(), 0);

                ps.setInt(1, 1);
                ps.setString(2, "1");
                ps.setInt(3, 3);
                ps.setString(4, "3");
                assertResultSetCount(ps.executeQuery(), 2);
            }
        }
    }

    @Test
    public void jdbcTableListTest() throws SQLException {
        String upsert = TEST_TABLE.upsertOne(SqlQueries.JdbcQuery.STANDARD, "c_Text", "Text");
        String selectByIds = TEST_TABLE.withTableName(
                "select count(*) from jdbc_table(?,?) as j join #tableName t on t.key=j.x"
        );
        String selectByValue = TEST_TABLE.withTableName(
                "select count(*) from jdbc_table(?,?) as j join #tableName t on t.c_Text=j.x"
        );

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert)) {
            ps.setInt(1, 1);
            ps.setString(2, "1");
            ps.addBatch();

            ps.setInt(1, 2);
            ps.setString(2, null);
            ps.addBatch();

            ps.setInt(1, 3);
            ps.setString(2, "3");
            ps.addBatch();

            ps.setInt(1, 4);
            ps.setString(2, "null");
            ps.addBatch();

            ps.executeBatch();
        }

        try (PreparedStatement ps = jdbc.connection().prepareStatement(selectByIds)) {
            ExceptionAssert.sqlException("Missing value for parameter: $jp1[0]", ps::executeQuery);

            ps.setInt(1, 1);
            ExceptionAssert.sqlException("Missing value for parameter: $jp1[1]", ps::executeQuery);

            ps.setInt(1, 1);
            ps.setInt(2, 2);
            assertResultSetCount(ps.executeQuery(), 2);

            ps.setInt(1, 1);
            ps.setInt(2, 5);
            assertResultSetCount(ps.executeQuery(), 1);

            ps.setInt(1, 1);
            ExceptionAssert.sqlException("Cannot cast [class java.lang.String: text] to [Int32]", () -> {
                ps.setString(2, "text");
            });
        }

        try (PreparedStatement ps = jdbc.connection().prepareStatement(selectByValue)) {
            ps.setString(1, null);
            ExceptionAssert.sqlException("Missing value for parameter: $jp1[1]", ps::executeQuery);

            ps.setString(1, null);
            ps.setString(2, null);
            assertResultSetCount(ps.executeQuery(), 0);

            ps.setString(1, "1");
            ps.setString(2, null);
            assertResultSetCount(ps.executeQuery(), 1);

            ps.setString(1, null);
            ps.setString(2, "2");
            assertResultSetCount(ps.executeQuery(), 0);

            ps.setString(1, "1");
            ps.setString(2, "1");
            assertResultSetCount(ps.executeQuery(), 2);

            ps.setString(1, "1");
            ps.setString(2, "2");
            assertResultSetCount(ps.executeQuery(), 1);

            ps.setString(1, "1");
            ps.setString(2, "3");
            assertResultSetCount(ps.executeQuery(), 2);
        }
    }

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
    }

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
    }

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

            // Wrong values
            ps.setInt(1, 8);
            ExceptionAssert.sqlDataException(
                    "Instant value is before minimum timestamp(1970-01-01 00:00:00.000000): 1969-12-31T23:59:59.999Z",
                    () -> ps.setTimestamp(2, Timestamp.from(Instant.EPOCH.minusMillis(1)))
            );

            ExceptionAssert.sqlDataException(
                    "Instant value is after maximum timestamp(2105-12-31 23:59:59.999999): 2106-01-01T00:00:00Z",
                    () -> ps.setTimestamp(2, Timestamp.from(Instant.ofEpochSecond(4291747200l)))
            );
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
    }

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
    public void timestamp64Test(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Timestamp64", "Timestamp64");

        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;
        int ydbSqlType = YdbConst.SQL_KIND_PRIMITIVE + PrimitiveType.Timestamp64.ordinal();

        try (Connection conn = jdbc.createCustomConnection("forceSignedDatetimes", "true")) {
            try (PreparedStatement ps = conn.prepareStatement(upsert)) {
                ps.setInt(1, 1);
                ps.setObject(2, TEST_NTS);
                ps.execute();

                ps.setInt(1, 2);
                ps.setTimestamp(2, Timestamp.from(TEST_NTS));
                ps.execute();

                ps.setInt(1, 3);
                ps.setDate(2, new Date(TEST_NTS.toEpochMilli()));
                ps.execute();

                ps.setInt(1, 4);
                ps.setObject(2, LocalDate.of(1955, Month.MAY, 5));
                ps.execute();

                ps.setInt(1, 5);
                ps.setObject(2, LocalDateTime.of(1955, Month.MAY, 6, 14, 56, 59, 123456789));
                ps.execute();

                if (castingSupported) {
                    ps.setInt(1, 6);
                    ps.setLong(2, -1585932011123l);
                    ps.execute();

                    ps.setInt(1, 7);
                    ps.setString(2, "1955-12-03T10:15:30.456789123Z");
                    ps.execute();
                } else {
                    ps.setInt(1, 6);
                    ps.setObject(2, -1585932011123l, ydbSqlType);
                    ps.execute();

                    ps.setInt(1, 7);
                    ps.setObject(2, "1955-12-03T10:15:30.456789123Z", ydbSqlType);
                    ps.execute();
                }
            }

            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery(TEST_TABLE.selectColumn("c_Timestamp64"))) {
                    assertNextTimestamp64(rs, 1, truncToMicros(TEST_NTS));
                    assertNextTimestamp64(rs, 2, truncToMicros(TEST_NTS));
                    assertNextTimestamp64(rs, 3, LocalDate.of(1969, Month.MAY, 21).atStartOfDay().toInstant(ZoneOffset.UTC));
                    assertNextTimestamp64(rs, 4, LocalDate.of(1955, Month.MAY, 5).atStartOfDay().toInstant(ZoneOffset.UTC));
                    assertNextTimestamp64(rs, 5, LocalDateTime.of(1955, Month.MAY, 6, 14, 56, 59).toInstant(ZoneOffset.UTC));
                    assertNextTimestamp64(rs, 6, Instant.ofEpochMilli(-1585932011123l));
                    assertNextTimestamp64(rs, 7, Instant.parse("1955-12-03T10:15:30.456789Z"));

                    Assertions.assertFalse(rs.next());
                }
            }
        }
    }

    private void assertNextTimestamp64(ResultSet rs, int key, Instant ts) throws SQLException {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_Timestamp64");
        Assertions.assertTrue(obj instanceof Instant);
        Assertions.assertEquals(ts, obj);

        Assertions.assertEquals(ts.toEpochMilli(), rs.getLong("c_Timestamp64"));

        Assertions.assertEquals(new Date(ts.toEpochMilli()), rs.getDate("c_Timestamp64"));
        Assertions.assertEquals(Timestamp.from(ts), rs.getTimestamp("c_Timestamp64"));
        Assertions.assertEquals(ts.toString(), rs.getString("c_Timestamp64"));

        LocalDateTime ldt = LocalDateTime.ofInstant(ts, ZoneId.systemDefault());
        Assertions.assertEquals(Long.valueOf(ts.toEpochMilli()), rs.getObject("c_Timestamp64", Long.class));
        Assertions.assertEquals(ldt.toLocalDate(), rs.getObject("c_Timestamp64", LocalDate.class));
        Assertions.assertEquals(ldt, rs.getObject("c_Timestamp64", LocalDateTime.class));
        Assertions.assertEquals(ts, rs.getObject("c_Timestamp64", Instant.class));
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
    }

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
    public void datetime64Test(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Datetime64", "Datetime64");
        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;
        int ydbSqlType = YdbConst.SQL_KIND_PRIMITIVE + PrimitiveType.Datetime64.ordinal();

        try (Connection conn = jdbc.createCustomConnection("forceSignedDatetimes", "true")) {
            try (PreparedStatement ps = conn.prepareStatement(upsert)) {
                ps.setInt(1, 1);
                ps.setObject(2, LocalDateTime.of(1932, Month.AUGUST, 10, 23, 59, 59, 100));
                ps.execute();

                ps.setInt(1, 2);
                ps.setDate(2, new Date(TEST_NTS.toEpochMilli()));
                ps.execute();

                if (castingSupported) {
                    ps.setInt(1, 3);
                    ps.setTimestamp(2, new Timestamp(TEST_NTS.toEpochMilli()));
                    ps.execute();

                    ps.setInt(1, 4);
                    ps.setLong(2, -1585932011l);
                    ps.execute();

                    ps.setInt(1, 5);
                    ps.setObject(2, LocalDate.of(1812, Month.JULY, 21));
                    ps.execute();

                    ps.setInt(1, 6);
                    ps.setString(2, "1761-12-03T10:15:30");
                    ps.execute();

                    ps.setInt(1, 7);
                    ps.setObject(2, TEST_NTS);
                    ps.execute();
                } else {
                    ps.setInt(1, 3);
                    ps.setObject(2, new Timestamp(TEST_NTS.toEpochMilli()), ydbSqlType);
                    ps.execute();

                    ps.setInt(1, 4);
                    ps.setObject(2, -1585932011l, ydbSqlType);
                    ps.execute();

                    ps.setInt(1, 5);
                    ps.setObject(2, LocalDate.of(1812, Month.JULY, 21), ydbSqlType);
                    ps.execute();

                    ps.setInt(1, 6);
                    ps.setObject(2, "1761-12-03T10:15:30", ydbSqlType);
                    ps.execute();

                    ps.setInt(1, 7);
                    ps.setObject(2, TEST_NTS, ydbSqlType);
                    ps.execute();
                }
            }

            try (Statement statement = conn.createStatement()) {
                LocalDateTime ts = TEST_NTS.atZone(ZoneId.systemDefault()).toLocalDateTime().truncatedTo(ChronoUnit.SECONDS);
                try (ResultSet rs = statement.executeQuery(TEST_TABLE.selectColumn("c_Datetime64"))) {
                    assertNextDatetime64(rs, 1, LocalDateTime.of(1932, Month.AUGUST, 10, 23, 59, 59));
                    assertNextDatetime64(rs, 2, LocalDateTime.of(1969, Month.MAY, 21, 0, 0, 0));
                    assertNextDatetime64(rs, 3, ts);
                    assertNextDatetime64(rs, 4, LocalDateTime.of(1919, Month.SEPTEMBER, 30, 7, 19, 49));
                    assertNextDatetime64(rs, 5, LocalDateTime.of(1812, Month.JULY, 21, 0, 0, 0));
                    assertNextDatetime64(rs, 6, LocalDateTime.of(1761, Month.DECEMBER, 3, 10, 15, 30));
                    assertNextDatetime64(rs, 7, ts);
                    Assertions.assertFalse(rs.next());
                }
            }
        }
    }

    private void assertNextDatetime64(ResultSet rs, int key, LocalDateTime ldt) throws SQLException {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_Datetime64");
        Assertions.assertTrue(obj instanceof LocalDateTime);
        Assertions.assertEquals(ldt, obj);

        Assertions.assertEquals(ldt.toEpochSecond(ZoneOffset.UTC), rs.getLong("c_Datetime64"));

        Assertions.assertEquals(Date.valueOf(ldt.toLocalDate()), rs.getDate("c_Datetime64"));
        Assertions.assertEquals(Timestamp.valueOf(ldt), rs.getTimestamp("c_Datetime64"));
        Assertions.assertEquals(ldt.toString(), rs.getString("c_Datetime64"));

        Assertions.assertEquals(Long.valueOf(ldt.toEpochSecond(ZoneOffset.UTC)), rs.getObject("c_Datetime64", Long.class));
        Assertions.assertEquals(ldt.toLocalDate(), rs.getObject("c_Datetime64", LocalDate.class));
        Assertions.assertEquals(ldt, rs.getObject("c_Datetime64", LocalDateTime.class));
        Assertions.assertEquals(ldt.atZone(ZoneId.systemDefault()).toInstant(), rs.getObject("c_Datetime64", Instant.class));
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
    }

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

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void date32Test(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Date32", "Date32");
        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;
        int ydbSqlType = YdbConst.SQL_KIND_PRIMITIVE + PrimitiveType.Date32.ordinal();

        try (Connection conn = jdbc.createCustomConnection("forceSignedDatetimes", "true")) {
            try (PreparedStatement ps = conn.prepareStatement(upsert)) {
                ps.setInt(1, 1);
                ps.setObject(2, LocalDate.of(2025, Month.AUGUST, 10));
                ps.execute();

                ps.setInt(1, 2);
                ps.setDate(2, new Date(TEST_NTS.toEpochMilli()));
                ps.execute();

                if (castingSupported) {
                    ps.setInt(1, 3);
                    ps.setTimestamp(2, new Timestamp(TEST_NTS.toEpochMilli()));
                    ps.execute();

                    ps.setInt(1, 4);
                    ps.setInt(2, -10); // Dec 22 1969
                    ps.execute();

                    ps.setInt(1, 5);
                    ps.setLong(2, -12345); // Oct 20 2003
                    ps.execute();

                    ps.setInt(1, 6);
                    ps.setObject(2, LocalDateTime.of(1491, Month.JULY, 23, 14, 56, 59, 123456789));
                    ps.execute();

                    ps.setInt(1, 7);
                    ps.setString(2, "1091-12-03");
                    ps.execute();

                    ps.setInt(1, 8);
                    ps.setObject(2, TEST_NTS);
                    ps.execute();
                } else {
                    ps.setInt(1, 3);
                    ps.setObject(2, new Timestamp(TEST_NTS.toEpochMilli()), ydbSqlType);
                    ps.execute();

                    ps.setInt(1, 4);
                    ps.setObject(2, -10, ydbSqlType); // Dec 22 1969
                    ps.execute();

                    ps.setInt(1, 5);
                    ps.setObject(2, -12345l, ydbSqlType); // Oct 20 2003
                    ps.execute();

                    ps.setInt(1, 6);
                    ps.setObject(2, LocalDateTime.of(1491, Month.JULY, 23, 14, 56, 59, 123456789), ydbSqlType);
                    ps.execute();

                    ps.setInt(1, 7);
                    ps.setObject(2, "1091-12-03", ydbSqlType);
                    ps.execute();

                    ps.setInt(1, 8);
                    ps.setObject(2, TEST_NTS, ydbSqlType);
                    ps.execute();
                }
            }

            try (Statement st = conn.createStatement()) {
                try (ResultSet rs = st.executeQuery(TEST_TABLE.selectColumn("c_Date32"))) {
                    assertNextDate32(rs, 1, LocalDate.of(2025, Month.AUGUST, 10));
                    assertNextDate32(rs, 2, LocalDate.of(1969, Month.MAY, 21));
                    assertNextDate32(rs, 3, LocalDate.of(1969, Month.MAY, 21));
                    assertNextDate32(rs, 4, LocalDate.of(1969, Month.DECEMBER, 22));
                    assertNextDate32(rs, 5, LocalDate.of(1936, Month.MARCH, 15));
                    assertNextDate32(rs, 6, LocalDate.of(1491, Month.JULY, 23));
                    assertNextDate32(rs, 7, LocalDate.of(1091, Month.DECEMBER, 3));
                    assertNextDate32(rs, 8, LocalDate.of(1969, Month.MAY, 21));

                    Assertions.assertFalse(rs.next());
                }
            }
        }
    }

    private void assertNextDate32(ResultSet rs, int key, LocalDate ld) throws SQLException {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_Date32");
        Assertions.assertTrue(obj instanceof LocalDate);
        Assertions.assertEquals(ld, obj);

        Assertions.assertEquals(ld.toEpochDay(), rs.getInt("c_Date32"));
        Assertions.assertEquals(ld.toEpochDay(), rs.getLong("c_Date32"));

        Assertions.assertEquals(Date.valueOf(ld), rs.getDate("c_Date32"));
        Assertions.assertEquals(Timestamp.valueOf(ld.atStartOfDay()), rs.getTimestamp("c_Date32"));
        Assertions.assertEquals(ld.toString(), rs.getString("c_Date32"));

        Assertions.assertEquals(Long.valueOf(ld.toEpochDay()), rs.getObject("c_Date32", Long.class));
        Assertions.assertEquals(ld, rs.getObject("c_Date32", LocalDate.class));
        Assertions.assertEquals(ld.atStartOfDay(), rs.getObject("c_Date32", LocalDateTime.class));
        Assertions.assertEquals(ld.atStartOfDay(ZoneId.systemDefault()).toInstant(), rs.getObject("c_Date32", Instant.class));
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void decimalTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_Decimal", "Decimal(22, 9)");

        BigDecimal closeToInf = new BigDecimal("9999999999999.999999999");
        BigDecimal closeToNegInf = new BigDecimal("-9999999999999.999999999");

        BigDecimal inf = closeToInf.add(BigDecimal.valueOf(1, 9));
        BigDecimal negInf = closeToNegInf.subtract(BigDecimal.valueOf(1, 9));
        BigDecimal nan = new BigDecimal("100000000000000000000000000.000000001");

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert)) {
            ps.setInt(1, 1);
            ps.setBigDecimal(2, BigDecimal.valueOf(1.5d));
            ps.execute();

            ps.setInt(1, 2);
            ps.setBigDecimal(2, BigDecimal.valueOf(-12345, 10)); // will be rounded to -0.000001234
            ps.execute();

            ps.setInt(1, 3);
            ps.setBigDecimal(2, closeToInf);
            ps.execute();

            ps.setInt(1, 4);
            ps.setBigDecimal(2, closeToNegInf);
            ps.execute();

            ps.setInt(1, 5);
            ExceptionAssert.sqlException(""
                            + "Cannot cast to decimal type Decimal(22, 9): "
                            + "[class java.math.BigDecimal: " + inf + "] is Infinite",
                    () -> ps.setBigDecimal(2, inf)
            );

            ExceptionAssert.sqlException(""
                            + "Cannot cast to decimal type Decimal(22, 9): "
                            + "[class java.math.BigDecimal: " + negInf + "] is -Infinite",
                    () -> ps.setBigDecimal(2, negInf)
            );

            ExceptionAssert.sqlException(""
                            + "Cannot cast to decimal type Decimal(22, 9): "
                            + "[class java.math.BigDecimal: " + nan + "] is NaN",
                    () -> ps.setBigDecimal(2, nan)
            );
        }

        try (Statement st = jdbc.connection().createStatement()) {
            try (ResultSet rs = st.executeQuery(TEST_TABLE.selectColumn("c_Decimal"))) {
                assertNextDecimal(rs, 1, BigDecimal.valueOf(1.5d).setScale(9));
                assertNextDecimal(rs, 2, BigDecimal.valueOf(-1234, 9));
                assertNextDecimal(rs, 3, closeToInf);
                assertNextDecimal(rs, 4, closeToNegInf);

                Assertions.assertFalse(rs.next());
            }
        }
    }

    private void assertNextDecimal(ResultSet rs, int key, BigDecimal bg) throws SQLException {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_Decimal");
        Assertions.assertTrue(obj instanceof BigDecimal);
        Assertions.assertEquals(bg, obj);

        BigDecimal decimal = rs.getBigDecimal("c_Decimal");
        Assertions.assertEquals(bg, decimal);
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void bankDecimalTest(SqlQueries.JdbcQuery query) throws SQLException {
        String upsert = TEST_TABLE.upsertOne(query, "c_BankDecimal", "Decimal(31, 9)");
        boolean castingSupported = query != SqlQueries.JdbcQuery.IN_MEMORY;

        BigDecimal closeToInf = new BigDecimal("9999999999999999999999.999999999");
        BigDecimal closeToNegInf = new BigDecimal("-9999999999999999999999.999999999");

        BigDecimal inf = closeToInf.add(BigDecimal.valueOf(1, 9));
        BigDecimal negInf = closeToNegInf.subtract(BigDecimal.valueOf(1, 9));
        BigDecimal nan = new BigDecimal("100000000000000000000000000.000000001");

        try (PreparedStatement ps = jdbc.connection().prepareStatement(upsert)) {
            if (castingSupported) {
                ps.setInt(1, 1);
                ps.setBigDecimal(2, BigDecimal.valueOf(1.5d));
                ps.execute();

                ps.setInt(1, 2);
                ps.setBigDecimal(2, BigDecimal.valueOf(-12345, 10)); // will be rounded to -0.000001234
                ps.execute();

                ps.setInt(1, 3);
                ps.setBigDecimal(2, closeToInf);
                ps.execute();

                ps.setInt(1, 4);
                ps.setBigDecimal(2, closeToNegInf);
                ps.execute();

                ps.setInt(1, 5);
                ExceptionAssert.sqlException(""
                                + "Cannot cast to decimal type Decimal(31, 9): "
                                + "[class java.math.BigDecimal: " + inf + "] is Infinite",
                        () -> ps.setBigDecimal(2, inf)
                );

                ExceptionAssert.sqlException(""
                                + "Cannot cast to decimal type Decimal(31, 9): "
                                + "[class java.math.BigDecimal: " + negInf + "] is -Infinite",
                        () -> ps.setBigDecimal(2, negInf)
                );

                ExceptionAssert.sqlException(""
                                + "Cannot cast to decimal type Decimal(31, 9): "
                                + "[class java.math.BigDecimal: " + nan + "] is NaN",
                        () -> ps.setBigDecimal(2, nan)
                );
            } else {
                int sqlType = ydbType(DecimalType.of(31, 9));
                ps.setInt(1, 1);
                ps.setObject(2, BigDecimal.valueOf(1.5d), sqlType);
                ps.execute();

                ps.setInt(1, 2);
                ps.setObject(2, BigDecimal.valueOf(-12345, 10), sqlType); // will be rounded to -0.000001234
                ps.execute();

                ps.setInt(1, 3);
                ps.setObject(2, closeToInf, sqlType);
                ps.execute();

                ps.setInt(1, 4);
                ps.setObject(2, closeToNegInf, sqlType);
                ps.execute();

                ps.setInt(1, 5);
                ExceptionAssert.sqlException(""
                                + "Cannot cast to decimal type Decimal(31, 9): "
                                + "[class java.math.BigDecimal: " + inf + "] is Infinite",
                        () -> ps.setObject(2, inf, sqlType)
                );

                ExceptionAssert.sqlException(""
                                + "Cannot cast to decimal type Decimal(31, 9): "
                                + "[class java.math.BigDecimal: " + negInf + "] is -Infinite",
                        () -> ps.setObject(2, negInf, sqlType)
                );

                ExceptionAssert.sqlException(""
                                + "Cannot cast to decimal type Decimal(31, 9): "
                                + "[class java.math.BigDecimal: " + nan + "] is NaN",
                        () -> ps.setObject(2, nan, sqlType)
                );
            }
        }

        try (Statement st = jdbc.connection().createStatement()) {
            try (ResultSet rs = st.executeQuery(TEST_TABLE.selectColumn("c_BankDecimal"))) {
                assertNextBankDecimal(rs, 1, BigDecimal.valueOf(1.5d).setScale(9));
                assertNextBankDecimal(rs, 2, BigDecimal.valueOf(-1234, 9));
                assertNextBankDecimal(rs, 3, closeToInf);
                assertNextBankDecimal(rs, 4, closeToNegInf);

                Assertions.assertFalse(rs.next());
            }
        }
    }

    private void assertNextBankDecimal(ResultSet rs, int key, BigDecimal bg) throws SQLException {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(key, rs.getInt("key"));

        Object obj = rs.getObject("c_BankDecimal");
        Assertions.assertTrue(obj instanceof BigDecimal);
        Assertions.assertEquals(bg, obj);

        BigDecimal decimal = rs.getBigDecimal("c_BankDecimal");
        Assertions.assertEquals(bg, decimal);
    }
}
