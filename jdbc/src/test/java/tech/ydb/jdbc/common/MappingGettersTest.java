package tech.ydb.jdbc.common;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.SQLException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.PrimitiveType;

class MappingGettersTest {
    private final YdbTypes types = new YdbTypes(false);
    private final TypeDescription uint64 = types.find(PrimitiveType.Uint64);

    private static ValueReader uint64Reader(long rawBits) {
        return (ValueReader) Proxy.newProxyInstance(
                ValueReader.class.getClassLoader(),
                new Class[] {ValueReader.class},
                (proxy, method, args) -> {
                    if ("getUint64".equals(method.getName())) {
                        return rawBits;
                    }
                    throw new UnsupportedOperationException("Stub does not implement " + method);
                }
        );
    }

    @Test
    void uint64StringPreservesUnsignedRange() throws SQLException {
        Assertions.assertEquals("0", uint64.getters().readString(uint64Reader(0L)));
        Assertions.assertEquals(
                String.valueOf(Long.MAX_VALUE),
                uint64.getters().readString(uint64Reader(Long.MAX_VALUE))
        );
        // 2^63 — first value above signed long, raw bits = Long.MIN_VALUE.
        Assertions.assertEquals(
                "9223372036854775808",
                uint64.getters().readString(uint64Reader(Long.MIN_VALUE))
        );
        // 2^64 - 1 — Uint64 max, raw bits = -1L.
        Assertions.assertEquals(
                "18446744073709551615",
                uint64.getters().readString(uint64Reader(-1L))
        );
    }

    @Test
    void uint64BigDecimalPreservesUnsignedRange() throws SQLException {
        Assertions.assertEquals(
                BigDecimal.ZERO,
                uint64.getters().readBigDecimal(uint64Reader(0L))
        );
        Assertions.assertEquals(
                new BigDecimal("18446744073709551615"),
                uint64.getters().readBigDecimal(uint64Reader(-1L))
        );
        Assertions.assertEquals(
                new BigDecimal("9223372036854775808"),
                uint64.getters().readBigDecimal(uint64Reader(Long.MIN_VALUE))
        );
    }

    @Test
    void uint64ByteRejectsValuesAbove127() {
        Assertions.assertThrows(SQLException.class, () -> uint64.getters().readByte(uint64Reader(128L)));
        // 2^64 - 1 must NOT silently come back as -1 (the pre-fix behavior).
        Assertions.assertThrows(SQLException.class, () -> uint64.getters().readByte(uint64Reader(-1L)));
    }

    @Test
    void uint64ByteAcceptsSmallValues() throws SQLException {
        Assertions.assertEquals((byte) 0, uint64.getters().readByte(uint64Reader(0L)));
        Assertions.assertEquals((byte) 127, uint64.getters().readByte(uint64Reader(127L)));
    }

    @Test
    void uint64ShortRejectsOutOfRange() {
        Assertions.assertThrows(SQLException.class, () -> uint64.getters().readShort(uint64Reader(32768L)));
        Assertions.assertThrows(SQLException.class, () -> uint64.getters().readShort(uint64Reader(-1L)));
    }

    @Test
    void uint64ShortAcceptsSmallValues() throws SQLException {
        Assertions.assertEquals((short) 32767, uint64.getters().readShort(uint64Reader(32767L)));
    }

    @Test
    void uint64IntRejectsOutOfRange() {
        Assertions.assertThrows(SQLException.class,
                () -> uint64.getters().readInt(uint64Reader((long) Integer.MAX_VALUE + 1)));
        Assertions.assertThrows(SQLException.class, () -> uint64.getters().readInt(uint64Reader(-1L)));
    }

    @Test
    void uint64IntAcceptsSmallValues() throws SQLException {
        Assertions.assertEquals(Integer.MAX_VALUE, uint64.getters().readInt(uint64Reader(Integer.MAX_VALUE)));
    }

    @Test
    void uint64LongReturnsRawBits() throws SQLException {
        // Contract: getLong on Uint64 returns the 64-bit pattern as signed long.
        // Values above Long.MAX_VALUE come back as their two's-complement representation.
        Assertions.assertEquals(0L, uint64.getters().readLong(uint64Reader(0L)));
        Assertions.assertEquals(Long.MAX_VALUE, uint64.getters().readLong(uint64Reader(Long.MAX_VALUE)));
        Assertions.assertEquals(-1L, uint64.getters().readLong(uint64Reader(-1L)));
    }

    @Test
    void uint64FloatPreservesUnsignedRange() throws SQLException {
        // For values above Long.MAX_VALUE the raw long is negative; pre-fix (float) cast
        // would produce a negative float. Now goes through unsigned BigInteger.
        Assertions.assertEquals(0.0f, uint64.getters().readFloat(uint64Reader(0L)));
        Assertions.assertEquals(1.8446744E19f, uint64.getters().readFloat(uint64Reader(-1L)), 1e13f);
        Assertions.assertTrue(uint64.getters().readFloat(uint64Reader(-1L)) > 0,
                "Uint64 max must come back as a positive float");
    }

    @Test
    void uint64DoublePreservesUnsignedRange() throws SQLException {
        Assertions.assertEquals(0.0d, uint64.getters().readDouble(uint64Reader(0L)));
        Assertions.assertEquals(1.8446744073709552E19d, uint64.getters().readDouble(uint64Reader(-1L)), 1.0d);
        Assertions.assertTrue(uint64.getters().readDouble(uint64Reader(-1L)) > 0,
                "Uint64 max must come back as a positive double");
    }
}
