package tech.ydb.jdbc.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

class MappingSettersTest {
    private final YdbTypes types = new YdbTypes(false);
    private final TypeDescription uint8 = types.find(PrimitiveType.Uint8);
    private final TypeDescription uint16 = types.find(PrimitiveType.Uint16);
    private final TypeDescription uint32 = types.find(PrimitiveType.Uint32);
    private final TypeDescription uint64 = types.find(PrimitiveType.Uint64);

    @Test
    void castUint8FromStringMax() throws SQLException {
        Assertions.assertEquals(PrimitiveValue.newUint8((byte) 0xFF), uint8.toYdbValue("255"));
    }

    @Test
    void castUint8FromBigIntegerMax() throws SQLException {
        Assertions.assertEquals(PrimitiveValue.newUint8((byte) 0xFF), uint8.toYdbValue(BigInteger.valueOf(255)));
    }

    @Test
    void rejectUint8Overflow() {
        Assertions.assertThrows(SQLException.class, () -> uint8.toYdbValue("256"));
        Assertions.assertThrows(SQLException.class, () -> uint8.toYdbValue(BigInteger.valueOf(256)));
        Assertions.assertThrows(SQLException.class, () -> uint8.toYdbValue("-1"));
        Assertions.assertThrows(SQLException.class, () -> uint8.toYdbValue(BigInteger.valueOf(-1)));
    }

    @Test
    void castUint16FromStringMax() throws SQLException {
        Assertions.assertEquals(PrimitiveValue.newUint16((short) 0xFFFF), uint16.toYdbValue("65535"));
    }

    @Test
    void castUint16FromBigIntegerMax() throws SQLException {
        Assertions.assertEquals(PrimitiveValue.newUint16((short) 0xFFFF), uint16.toYdbValue(BigInteger.valueOf(65535)));
    }

    @Test
    void rejectUint16Overflow() {
        Assertions.assertThrows(SQLException.class, () -> uint16.toYdbValue("65536"));
        Assertions.assertThrows(SQLException.class, () -> uint16.toYdbValue(BigInteger.valueOf(65536)));
        Assertions.assertThrows(SQLException.class, () -> uint16.toYdbValue("-1"));
        Assertions.assertThrows(SQLException.class, () -> uint16.toYdbValue(BigInteger.valueOf(-1)));
    }

    @Test
    void castUint32FromStringMax() throws SQLException {
        Assertions.assertEquals(PrimitiveValue.newUint32(0xFFFFFFFF), uint32.toYdbValue("4294967295"));
    }

    @Test
    void castUint32FromBigIntegerMax() throws SQLException {
        Assertions.assertEquals(PrimitiveValue.newUint32(0xFFFFFFFF), uint32.toYdbValue(new BigInteger("4294967295")));
    }

    @Test
    void rejectUint32Overflow() {
        Assertions.assertThrows(SQLException.class, () -> uint32.toYdbValue("4294967296"));
        Assertions.assertThrows(SQLException.class, () -> uint32.toYdbValue(new BigInteger("4294967296")));
        Assertions.assertThrows(SQLException.class, () -> uint32.toYdbValue("-1"));
        Assertions.assertThrows(SQLException.class, () -> uint32.toYdbValue(BigInteger.valueOf(-1)));
    }

    @Test
    void castUint64FromBigIntegerAboveLongMax() throws SQLException {
        BigInteger input = new BigInteger("9223372036854775808"); // Long.MAX_VALUE + 1
        Value<?> value = uint64.toYdbValue(input);

        Assertions.assertEquals(PrimitiveValue.newUint64(Long.parseUnsignedLong("9223372036854775808")), value);
    }

    @Test
    void castUint64FromBigIntegerMax() throws SQLException {
        BigInteger input = new BigInteger("18446744073709551615"); // 2^64 - 1
        Value<?> value = uint64.toYdbValue(input);

        Assertions.assertEquals(PrimitiveValue.newUint64(Long.parseUnsignedLong("18446744073709551615")), value);
    }

    @Test
    void castUint64FromStringMax() throws SQLException {
        Value<?> value = uint64.toYdbValue("18446744073709551615");

        Assertions.assertEquals(PrimitiveValue.newUint64(Long.parseUnsignedLong("18446744073709551615")), value);
    }

    @Test
    void rejectUint64BigIntegerOverflow() {
        BigInteger overflow = new BigInteger("18446744073709551616"); // 2^64

        Assertions.assertThrows(SQLException.class, () -> uint64.toYdbValue(overflow));
    }

    @Test
    void rejectUint64BigIntegerNegative() {
        Assertions.assertThrows(SQLException.class, () -> uint64.toYdbValue(new BigInteger("-1")));
    }

    @Test
    void rejectUint64StringNegative() {
        Assertions.assertThrows(SQLException.class, () -> uint64.toYdbValue("-1"));
    }

    @Test
    void castUintFromBigDecimal() throws SQLException {
        Assertions.assertEquals(PrimitiveValue.newUint8((byte) 0xFF), uint8.toYdbValue(new BigDecimal("255")));
        Assertions.assertEquals(PrimitiveValue.newUint16((short) 0xFFFF), uint16.toYdbValue(new BigDecimal("65535.0")));
        Assertions.assertEquals(PrimitiveValue.newUint32(0xFFFFFFFF), uint32.toYdbValue(new BigDecimal("4294967295.000")));
        Assertions.assertEquals(
                PrimitiveValue.newUint64(Long.parseUnsignedLong("18446744073709551615")),
                uint64.toYdbValue(new BigDecimal("18446744073709551615"))
        );
    }

    @Test
    void rejectUintBigDecimalWithFraction() {
        Assertions.assertThrows(SQLException.class, () -> uint8.toYdbValue(new BigDecimal("1.5")));
        Assertions.assertThrows(SQLException.class, () -> uint16.toYdbValue(new BigDecimal("100.001")));
        Assertions.assertThrows(SQLException.class, () -> uint32.toYdbValue(new BigDecimal("0.5")));
        Assertions.assertThrows(SQLException.class, () -> uint64.toYdbValue(new BigDecimal("1.0000000001")));
    }

    @Test
    void rejectUintBigDecimalOverflow() {
        Assertions.assertThrows(SQLException.class, () -> uint8.toYdbValue(new BigDecimal("256")));
        Assertions.assertThrows(SQLException.class, () -> uint8.toYdbValue(new BigDecimal("-1")));
        Assertions.assertThrows(SQLException.class, () -> uint64.toYdbValue(new BigDecimal("18446744073709551616")));
    }

    @Test
    void castUintFromFloatAndDouble() throws SQLException {
        Assertions.assertEquals(PrimitiveValue.newUint8((byte) 200), uint8.toYdbValue(200.0f));
        Assertions.assertEquals(PrimitiveValue.newUint8((byte) 200), uint8.toYdbValue(200.0d));
        Assertions.assertEquals(PrimitiveValue.newUint32((int) 4_000_000_000L), uint32.toYdbValue(4_000_000_000.0d));
    }

    @Test
    void rejectUintFloatDoubleWithFraction() {
        Assertions.assertThrows(SQLException.class, () -> uint8.toYdbValue(1.5f));
        Assertions.assertThrows(SQLException.class, () -> uint16.toYdbValue(100.5d));
    }

    @Test
    void rejectUintFloatDoubleNonFinite() {
        Assertions.assertThrows(SQLException.class, () -> uint8.toYdbValue(Double.NaN));
        Assertions.assertThrows(SQLException.class, () -> uint8.toYdbValue(Double.POSITIVE_INFINITY));
        Assertions.assertThrows(SQLException.class, () -> uint64.toYdbValue(Float.NEGATIVE_INFINITY));
    }
}
