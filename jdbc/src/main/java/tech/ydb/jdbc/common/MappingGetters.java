package tech.ydb.jdbc.common;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.table.result.PrimitiveReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

public class MappingGetters {
    private MappingGetters() { }

    @SuppressWarnings("Convert2Lambda")
    static Getters buildGetters(Type type) {
        Type.Kind kind = type.getKind();
        String clazz = kind.toString();
        switch (kind) {
            case PRIMITIVE:
                PrimitiveType id = (PrimitiveType) type;
                return new Getters(
                        valueToString(id),
                        valueToBoolean(id),
                        valueToByte(id),
                        valueToShort(id),
                        valueToInt(id),
                        valueToLong(id),
                        valueToFloat(id),
                        valueToDouble(id),
                        valueToBytes(id),
                        valueToObject(id),
                        valueToClass(id),
                        valueToInstant(id),
                        valueToNString(id),
                        valueToURL(id),
                        valueToBigDecimal(id),
                        valueToReader(id)
                );
            case DECIMAL:
                return new Getters(
                        value -> String.valueOf(value.getDecimal()),
                        castToBooleanNotSupported(clazz),
                        castToByteNotSupported(clazz),
                        castToShortNotSupported(clazz),
                        value -> safeDecimalInt(value.getDecimal()),
                        value -> safeDecimalLong(value.getDecimal()),
                        value -> safeDecimal(value.getDecimal()).floatValue(),
                        value -> safeDecimal(value.getDecimal()).doubleValue(),
                        castToBytesNotSupported(clazz),
                        value -> safeDecimal(value.getDecimal()),
                        castToClassNotSupported(clazz),
                        castToInstantNotSupported(clazz),
                        castToNStringNotSupported(clazz),
                        castToUrlNotSupported(clazz),
                        value -> safeDecimal(value.getDecimal()),
                        castToReaderNotSupported(clazz)
                );
            case VOID:
            case NULL:
                return new Getters(
                        value -> null,
                        value -> false,
                        value -> 0,
                        value -> 0,
                        value -> 0,
                        value -> 0,
                        value -> 0,
                        value -> 0,
                        value -> null,
                        value -> null,
                        new ValueToClass() {
                            @Override
                            public <T> T fromValue(ValueReader reader, Class<T> clazz) throws SQLException {
                                return null;
                            }
                        },
                        value -> Instant.ofEpochSecond(0),
                        value -> null,
                        value -> null,
                        value -> null,
                        value -> null
                );
            default:
                return new Getters(
                        value -> String.valueOf(value.getValue()),
                        castToBooleanNotSupported(clazz),
                        castToByteNotSupported(clazz),
                        castToShortNotSupported(clazz),
                        castToIntNotSupported(clazz),
                        castToLongNotSupported(clazz),
                        castToFloatNotSupported(clazz),
                        castToDoubleNotSupported(clazz),
                        castToBytesNotSupported(clazz),
                        ValueReader::getValue,
                        castToClassNotSupported(clazz),
                        castToInstantNotSupported(clazz),
                        castToNStringNotSupported(clazz),
                        castToUrlNotSupported(clazz),
                        castToBigDecimalNotSupported(clazz),
                        castToReaderNotSupported(clazz)
                );
        }
    }

    private static ValueToString valueToString(PrimitiveType id) {
        switch (id) {
            case Bytes:
                return value -> new String(value.getBytes());
            case Text:
                return PrimitiveReader::getText;
            case Json:
                return PrimitiveReader::getJson;
            case JsonDocument:
                return PrimitiveReader::getJsonDocument;
            case Yson:
                return value -> new String(value.getYson());
            case Uuid:
                return value -> String.valueOf(value.getUuid());
            case Bool:
                return value -> String.valueOf(value.getBool());
            case Int8:
                return value -> String.valueOf(value.getInt8());
            case Uint8:
                return value -> String.valueOf(value.getUint8());
            case Int16:
                return value -> String.valueOf(value.getInt16());
            case Uint16:
                return value -> String.valueOf(value.getUint16());
            case Int32:
                return value -> String.valueOf(value.getInt32());
            case Uint32:
                return value -> String.valueOf(value.getUint32());
            case Int64:
                return value -> String.valueOf(value.getInt64());
            case Uint64:
                return value -> String.valueOf(value.getUint64());
            case Float:
                return value -> String.valueOf(value.getFloat());
            case Double:
                return value -> String.valueOf(value.getDouble());
            case Date:
                return value -> String.valueOf(value.getDate());
            case Datetime:
                return value -> String.valueOf(value.getDatetime());
            case Timestamp:
                return value -> String.valueOf(value.getTimestamp());
            case Interval:
                return value -> String.valueOf(value.getInterval());
            case TzDate:
                return value -> String.valueOf(value.getTzDate());
            case TzDatetime:
                return value -> String.valueOf(value.getTzDatetime());
            case TzTimestamp:
                return value -> String.valueOf(value.getTzTimestamp());
            default:
                // DyNumber
                return value -> {
                    throw dataTypeNotSupported(id, String.class);
                };
        }
    }

    private static ValueToBoolean valueToBoolean(PrimitiveType id) {
        switch (id) {
            case Bool:
                return PrimitiveReader::getBool;
            case Int8:
                return value -> value.getInt8() != 0;
            case Uint8:
                return value -> value.getUint8() != 0;
            case Int16:
                return value -> value.getInt16() != 0;
            case Uint16:
                return value -> value.getUint16() != 0;
            case Int32:
                return value -> value.getInt32() != 0;
            case Uint32:
                return value -> value.getUint32() != 0;
            case Int64:
                return value -> value.getInt64() != 0;
            case Uint64:
                return value -> value.getUint64() != 0;
            case Bytes:
                return value -> {
                    byte[] stringValue = value.getBytes();
                    if (stringValue.length == 0) {
                        return false;
                    } else if (stringValue.length == 1) {
                        if (stringValue[0] == '0') {
                            return false;
                        } else if (stringValue[0] == '1') {
                            return true;
                        }
                    }
                    throw cannotConvert(id, boolean.class, new String(stringValue));
                };
            case Text:
                return value -> {
                    String utfValue = value.getText();
                    if (utfValue.isEmpty()) {
                        return false;
                    } else if (utfValue.length() == 1) {
                        if (utfValue.charAt(0) == '0') {
                            return false;
                        } else if (utfValue.charAt(0) == '1') {
                            return true;
                        }
                    }
                    throw cannotConvert(id, boolean.class, utfValue);
                };
            default:
                return castToBooleanNotSupported(id.name());
        }
    }

    private static BigDecimal safeDecimal(DecimalValue value) throws SQLException {
        if (value.isInf() || value.isNegativeInf() || value.isNan()) {
            throw cannotConvert(value.getType(), BigDecimal.class, value.toString());
        }
        return value.toBigDecimal();
    }

    private static int safeDecimalInt(DecimalValue value) throws SQLException {
        if (value.isInf() || value.isNegativeInf() || value.isNan()) {
            throw cannotConvert(value.getType(), int.class, value.toString());
        }
        try {
            return value.toBigDecimal().intValueExact();
        } catch (ArithmeticException ex) {
            throw cannotConvert(value.getType(), int.class, value.toString());
        }
    }

    private static long safeDecimalLong(DecimalValue value) throws SQLException {
        if (value.isInf() || value.isNegativeInf() || value.isNan()) {
            throw cannotConvert(value.getType(), long.class, value.toString());
        }
        try {
            return value.toBigDecimal().longValueExact();
        } catch (ArithmeticException ex) {
            throw cannotConvert(value.getType(), long.class, value.toString());
        }
    }

    private static byte checkByteValue(PrimitiveType id, int value) throws SQLException {
        int ch = value >= 0 ? value : ~value;
        if ((ch & 0x7F) != ch) {
            throw cannotConvert(id, byte.class, value);
        }
        return (byte) value;
    }

    private static byte checkByteValue(PrimitiveType id, long value) throws SQLException {
        long ch = value >= 0 ? value : ~value;
        if ((ch & 0x7FL) != ch) {
            throw cannotConvert(id, byte.class, value);
        }
        return (byte) value;
    }

    private static ValueToByte valueToByte(PrimitiveType id) {
        switch (id) {
            case Bool:
                return value -> checkByteValue(id, value.getBool() ? 1 : 0);
            case Int8:
                return PrimitiveReader::getInt8;
            case Int16:
                return value -> checkByteValue(id, value.getInt16());
            case Int32:
                return value -> checkByteValue(id, value.getInt32());
            case Int64:
                return value -> checkByteValue(id, value.getInt64());
            case Uint8:
                return value -> checkByteValue(id, value.getUint8());
            case Uint16:
                return value -> checkByteValue(id, value.getUint16());
            case Uint32:
                return value -> checkByteValue(id, value.getUint32());
            case Uint64:
                return value -> checkByteValue(id, value.getUint64());
            default:
                return castToByteNotSupported(id.name());
        }
    }

    private static short checkShortValue(PrimitiveType id, int value) throws SQLException {
        int ch = value >= 0 ? value : ~value;
        if ((ch & 0x7FFF) != ch) {
            throw cannotConvert(id, short.class, value);
        }
        return (short) value;
    }

    private static short checkShortValue(PrimitiveType id, long value) throws SQLException {
        long ch = value >= 0 ? value : ~value;
        if ((ch & 0x7FFFL) != ch) {
            throw cannotConvert(id, short.class, value);
        }
        return (short) value;
    }

    private static ValueToShort valueToShort(PrimitiveType id) {
        switch (id) {
            case Bool:
                return value -> checkShortValue(id, value.getBool() ? 1 : 0);
            case Int8:
                return PrimitiveReader::getInt8;
            case Int16:
                return PrimitiveReader::getInt16;
            case Int32:
                return value -> checkShortValue(id, value.getInt32());
            case Int64:
                return value -> checkShortValue(id, value.getInt64());
            case Uint8:
                return value -> checkShortValue(id, value.getUint8());
            case Uint16:
                return value -> checkShortValue(id, value.getUint16());
            case Uint32:
                return value -> checkShortValue(id, value.getUint32());
            case Uint64:
                return value -> checkShortValue(id, value.getUint64());
            default:
                return castToShortNotSupported(id.name());
        }
    }

    private static int checkIntValue(PrimitiveType id, long value) throws SQLException {
        long ch = value >= 0 ? value : ~value;
        if ((ch & 0x7FFFFFFFL) != ch) {
            throw cannotConvert(id, int.class, value);
        }
        return (int) value;
    }

    private static ValueToInt valueToInt(PrimitiveType id) {
        switch (id) {
            case Bool:
                return value -> value.getBool() ? 1 : 0;
            case Int8:
                return PrimitiveReader::getInt8;
            case Int16:
                return PrimitiveReader::getInt16;
            case Int32:
                return PrimitiveReader::getInt32;
            case Int64:
                return value -> checkIntValue(id, value.getInt64());
            case Uint8:
                return PrimitiveReader::getUint8;
            case Uint16:
                return PrimitiveReader::getUint16;
            case Uint32:
                return value -> checkIntValue(id, value.getUint32());
            case Uint64:
                return value -> checkIntValue(id, value.getUint64());
            case Date:
                return value -> checkIntValue(id, value.getDate().toEpochDay());
            default:
                return castToIntNotSupported(id.name());
        }
    }

    private static ValueToLong valueToLong(PrimitiveType id) {
        switch (id) {
            case Bool:
                return value -> value.getBool() ? 1 : 0;
            case Int8:
                return PrimitiveReader::getInt8;
            case Uint8:
                return PrimitiveReader::getUint8;
            case Int16:
                return PrimitiveReader::getInt16;
            case Uint16:
                return PrimitiveReader::getUint16;
            case Int32:
                return PrimitiveReader::getInt32;
            case Uint32:
                return PrimitiveReader::getUint32;
            case Int64:
                return PrimitiveReader::getInt64;
            case Uint64:
                return PrimitiveReader::getUint64;
            case Date:
                return value -> value.getDate().toEpochDay();
            case Datetime:
                return value -> value.getDatetime().toEpochSecond(ZoneOffset.UTC);
            case Timestamp:
                return value -> value.getTimestamp().toEpochMilli();
            case TzDate:
            case TzDatetime:
            case TzTimestamp:
                ValueToInstant delegate = valueToInstant(id);
                return value -> delegate.fromValue(value).toEpochMilli();
            case Interval:
                return value -> TimeUnit.NANOSECONDS.toMicros(value.getInterval().toNanos());
            default:
                return castToLongNotSupported(id.name());
        }
    }

    private static ValueToFloat valueToFloat(PrimitiveType id) {
        switch (id) {
            case Bool:
                return value -> value.getBool() ? 1 : 0;
            case Int8:
                return PrimitiveReader::getInt8;
            case Int16:
                return PrimitiveReader::getInt16;
            case Int32:
                return PrimitiveReader::getInt32;
            case Int64:
                return PrimitiveReader::getInt64;
            case Uint8:
                return PrimitiveReader::getUint8;
            case Uint16:
                return PrimitiveReader::getUint16;
            case Uint32:
                return PrimitiveReader::getUint32;
            case Uint64:
                return PrimitiveReader::getUint64;
            case Float:
                return PrimitiveReader::getFloat;
            case Double:
                return value -> (float) value.getDouble();
            default:
                return castToFloatNotSupported(id.name());
        }
    }

    private static ValueToDouble valueToDouble(PrimitiveType id) {
        switch (id) {
            case Bool:
                return value -> value.getBool() ? 1 : 0;
            case Int8:
                return PrimitiveReader::getInt8;
            case Uint8:
                return PrimitiveReader::getUint8;
            case Int16:
                return PrimitiveReader::getInt16;
            case Uint16:
                return PrimitiveReader::getUint16;
            case Int32:
                return PrimitiveReader::getInt32;
            case Uint32:
                return PrimitiveReader::getUint32;
            case Int64:
                return PrimitiveReader::getInt64;
            case Uint64:
                return PrimitiveReader::getUint64;
            case Float:
                return PrimitiveReader::getFloat;
            case Double:
                return PrimitiveReader::getDouble;
            default:
                return castToDoubleNotSupported(id.name());
        }
    }

    private static ValueToBytes valueToBytes(PrimitiveType id) {
        switch (id) {
            case Bytes:
                return PrimitiveReader::getBytes;
            case Text:
                // TODO: pretty ineffective conversion (bytes -> string -> bytes)
                return value -> value.getText().getBytes();
            case Json:
                return value -> value.getJson().getBytes();
            case JsonDocument:
                return value -> value.getJsonDocument().getBytes();
            case Yson:
                return PrimitiveReader::getYson;
            case Uuid:
                return value -> value.getUuid().toString().getBytes();
            default:
                return castToBytesNotSupported(id.name());
        }
    }

    private static ValueToInstant valueToInstant(PrimitiveType id) {
        switch (id) {
            case Int64:
                return v -> Instant.ofEpochMilli(v.getInt64());
            case Uint64:
                return v -> Instant.ofEpochMilli(v.getUint64());
            case Date:
                return v -> Instant.ofEpochSecond(v.getDate().toEpochDay() * 24 * 60 * 60);
            case Datetime:
                return v -> Instant.ofEpochSecond(v.getDatetime().toEpochSecond(ZoneOffset.UTC));
            case TzDate:
                return v -> Instant.ofEpochSecond(v.getTzDate().toEpochSecond());
            case TzDatetime:
                return v -> Instant.ofEpochSecond(v.getTzDatetime().toEpochSecond());
            case Timestamp:
                return ValueReader::getTimestamp;
            case TzTimestamp:
                return v -> v.getTzTimestamp().toInstant();
            default:
                return castToInstantNotSupported(id.name());
        }
    }

    private static ValueToNString valueToNString(PrimitiveType id) {
        switch (id) {
            case Bytes:
                return value -> new String(value.getBytes());
            case Text:
                return PrimitiveReader::getText;
            case Json:
                return PrimitiveReader::getJson;
            case JsonDocument:
                return PrimitiveReader::getJsonDocument;
            case Yson:
                return value -> new String(value.getYson());
            case Uuid:
                return value -> String.valueOf(value.getUuid());
            default:
                return castToNStringNotSupported(id.name());
        }
    }

    private static ValueToURL valueToURL(PrimitiveType id) {
        switch (id) {
            case Bytes:
                return value -> new String(value.getBytes());
            case Text:
                return PrimitiveReader::getText;
            default:
                return castToUrlNotSupported(id.name());
        }
    }

    private static ValueToBigDecimal valueToBigDecimal(PrimitiveType id) {
        switch (id) {
            case Bool:
                return value -> BigDecimal.valueOf(value.getBool() ? 1 : 0);
            case Int8:
                return value -> BigDecimal.valueOf(value.getInt8());
            case Uint8:
                return value -> BigDecimal.valueOf(value.getUint8());
            case Int16:
                return value -> BigDecimal.valueOf(value.getInt16());
            case Uint16:
                return value -> BigDecimal.valueOf(value.getUint16());
            case Int32:
                return value -> BigDecimal.valueOf(value.getInt32());
            case Uint32:
                return value -> BigDecimal.valueOf(value.getUint32());
            case Int64:
                return value -> BigDecimal.valueOf(value.getInt64());
            case Uint64:
                return value -> BigDecimal.valueOf(value.getUint64());
            case Float:
                return value -> BigDecimal.valueOf(value.getFloat());
            case Double:
                return value -> BigDecimal.valueOf(value.getDouble());
            default:
                return castToBigDecimalNotSupported(id.name());
        }
    }

    private static ValueToReader valueToReader(PrimitiveType id) {
        switch (id) {
            case Bytes:
                return value -> new InputStreamReader(new ByteArrayInputStream(value.getBytes()));
            case Text:
                return value -> new StringReader(value.getText());
            case Json:
                return value -> new StringReader(value.getJson());
            case JsonDocument:
                return value -> new StringReader(value.getJsonDocument());
            case Yson:
                return value -> new InputStreamReader(new ByteArrayInputStream(value.getYson()));
            case Uuid:
                return value -> new StringReader(value.getUuid().toString());
            default:
                return castToReaderNotSupported(id.name());
        }
    }

    private static SqlType buildPrimitiveType(int sqlType, PrimitiveType id) {
        switch (id) {
            case Text:
            case Json:
            case JsonDocument:
                return new SqlType(sqlType, String.class);
            case Uuid:
                return new SqlType(sqlType, UUID.class);
            case Bytes:
            case Yson:
                return new SqlType(sqlType, byte[].class);
            case Bool:
                return new SqlType(sqlType, Boolean.class);
            case Int8:
                return new SqlType(sqlType, Byte.class);
            case Uint8:
            case Int32:
            case Uint16:
                return new SqlType(sqlType, Integer.class);
            case Int16:
                return new SqlType(sqlType, Short.class);
            case Uint32:
            case Int64:
            case Uint64:
                return new SqlType(sqlType, Long.class);
            case Float:
                return new SqlType(sqlType, Float.class);
            case Double:
                return new SqlType(sqlType, Double.class);
            case Date:
                return new SqlType(sqlType, LocalDate.class);
            case Datetime:
                return new SqlType(sqlType, LocalDateTime.class);
            case Timestamp:
                return new SqlType(sqlType, Instant.class);
            case Interval:
                return new SqlType(sqlType, Duration.class);
            case TzDate:
            case TzDatetime:
            case TzTimestamp:
                return new SqlType(sqlType, ZonedDateTime.class);
            default:
                return new SqlType(sqlType, Value.class);
        }
    }

    static SqlType buildDataType(int sqlType, Type type) {
        switch (type.getKind()) {
            case PRIMITIVE:
                return buildPrimitiveType(sqlType, (PrimitiveType) type);
            case DECIMAL:
                return new SqlType(sqlType, DecimalValue.class);
            default:
                return new SqlType(sqlType, Value.class);
        }
    }

    private static ValueToObject valueToObject(PrimitiveType id) {
        switch (id) {
            case Bytes:
                return PrimitiveReader::getBytes;
            case Text:
                return PrimitiveReader::getText;
            case Json:
                return PrimitiveReader::getJson;
            case JsonDocument:
                return PrimitiveReader::getJsonDocument;
            case Yson:
                return PrimitiveReader::getYson;
            case Uuid:
                return PrimitiveReader::getUuid;
            case Bool:
                return PrimitiveReader::getBool;
            case Int8:
                return PrimitiveReader::getInt8;
            case Uint8:
                return PrimitiveReader::getUint8;
            case Int16:
                return PrimitiveReader::getInt16;
            case Uint16:
                return PrimitiveReader::getUint16;
            case Int32:
                return PrimitiveReader::getInt32;
            case Uint32:
                return PrimitiveReader::getUint32;
            case Int64:
                return PrimitiveReader::getInt64;
            case Uint64:
                return PrimitiveReader::getUint64;
            case Float:
                return PrimitiveReader::getFloat;
            case Double:
                return PrimitiveReader::getDouble;
            case Date:
                return PrimitiveReader::getDate;
            case Datetime:
                return PrimitiveReader::getDatetime;
            case Timestamp:
                return PrimitiveReader::getTimestamp;
            case Interval:
                return PrimitiveReader::getInterval;
            case TzDate:
                return PrimitiveReader::getTzDate;
            case TzDatetime:
                return PrimitiveReader::getTzDatetime;
            case TzTimestamp:
                return PrimitiveReader::getTzTimestamp;
            default:
                // DyNumber
                return value -> {
                    throw dataTypeNotSupported(id, Object.class);
                };
        }
    }

    private static class ValueToClassBuilder {
        private final Type type;
        private final Map<Class<?>, Function<ValueReader, ?>> map = new HashMap<>();

        ValueToClassBuilder(Type type) {
            this.type = type;
        }

        public <T> ValueToClassBuilder register(Class<T> clazz, Function<ValueReader, T> func) {
            map.put(clazz, func);
            return this;
        }

        @SuppressWarnings("Convert2Lambda")
        public ValueToClass build() {
            return new ValueToClass() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> T fromValue(ValueReader reader, Class<T> clazz) throws SQLException {
                    Function<ValueReader, ?> f = map.get(clazz);
                    if (f != null) {
                        return (T) f.apply(reader);
                    }
                    throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST_TO_CLASS, type, clazz));
                }
            };
        }
    }


    private static ValueToClass valueToClass(PrimitiveType id) {
        ValueToClassBuilder builder = new ValueToClassBuilder(id);
        switch (id) {
            case Bytes:
                return builder
                        .register(byte[].class, ValueReader::getBytes)
                        .build();
            case Text:
                return builder
                        .register(String.class, ValueReader::getText)
                        .build();
            case Json:
                return builder
                        .register(String.class, ValueReader::getJson)
                        .build();
            case JsonDocument:
                return builder
                        .register(String.class, ValueReader::getJsonDocument)
                        .build();
            case Yson:
                return builder
                        .register(byte[].class, ValueReader::getYson)
                        .build();
            case Uuid:
                return builder
                        .register(UUID.class, ValueReader::getUuid)
                        .build();
            case Bool:
                return builder
                        .register(boolean.class, ValueReader::getBool)
                        .register(Boolean.class, ValueReader::getBool)
                        .build();
            case Int8:
                return builder
                        .register(byte.class, ValueReader::getInt8)
                        .register(Byte.class, ValueReader::getInt8)
                        .build();
            case Uint8:
                return builder
                        .register(int.class, ValueReader::getUint8)
                        .register(Integer.class, ValueReader::getUint8)
                        .build();
            case Int16:
                return builder
                        .register(short.class, ValueReader::getInt16)
                        .register(Short.class, ValueReader::getInt16)
                        .build();
            case Uint16:
                return builder
                        .register(int.class, ValueReader::getUint16)
                        .register(Integer.class, ValueReader::getUint16)
                        .build();
            case Int32:
                return builder
                        .register(int.class, ValueReader::getInt32)
                        .register(Integer.class, ValueReader::getInt32)
                        .build();
            case Uint32:
                return builder
                        .register(long.class, ValueReader::getUint32)
                        .register(Long.class, ValueReader::getUint32)
                        .build();
            case Int64:
                return builder
                        .register(long.class, ValueReader::getInt64)
                        .register(Long.class, ValueReader::getInt64)
                        .build();
            case Uint64:
                return builder
                        .register(long.class, ValueReader::getUint64)
                        .register(Long.class, ValueReader::getUint64)
                        .build();
            case Float:
                return builder
                        .register(float.class, ValueReader::getFloat)
                        .register(Float.class, ValueReader::getFloat)
                        .build();
            case Double:
                return builder
                        .register(double.class, ValueReader::getDouble)
                        .register(Double.class, ValueReader::getDouble)
                        .build();
            case Date:
                return builder
                        .register(long.class, v -> v.getDate().toEpochDay())
                        .register(Long.class, v -> v.getDate().toEpochDay())
                        .register(LocalDate.class, ValueReader::getDate)
                        .register(LocalDateTime.class, v -> v.getDate().atStartOfDay())
                        .register(java.sql.Date.class, v -> java.sql.Date.valueOf(v.getDate()))
                        .register(java.sql.Timestamp.class, v -> java.sql.Timestamp.valueOf(v.getDate().atStartOfDay()))
                        .register(Instant.class, v -> v.getDate().atStartOfDay(ZoneId.systemDefault()).toInstant())
                        .register(java.util.Date.class, v -> java.util.Date.from(
                                v.getDate().atStartOfDay(ZoneId.systemDefault()).toInstant()))
                        .build();
            case Datetime:
                return builder
                        .register(long.class, v -> v.getDatetime().toEpochSecond(ZoneOffset.UTC))
                        .register(Long.class, v -> v.getDatetime().toEpochSecond(ZoneOffset.UTC))
                        .register(LocalDate.class, v -> v.getDatetime().toLocalDate())
                        .register(LocalDateTime.class, ValueReader::getDatetime)
                        .register(java.sql.Date.class, v -> java.sql.Date.valueOf(v.getDatetime().toLocalDate()))
                        .register(java.sql.Timestamp.class, v -> java.sql.Timestamp.valueOf(v.getDatetime()))
                        .register(Instant.class, v -> v.getDatetime().atZone(ZoneId.systemDefault()).toInstant())
                        .register(java.util.Date.class, v -> java.util.Date.from(
                                v.getDatetime().atZone(ZoneId.systemDefault()).toInstant()))
                        .build();
            case Timestamp:
                return builder
                        .register(long.class, v -> v.getTimestamp().toEpochMilli())
                        .register(Long.class, v -> v.getTimestamp().toEpochMilli())
                        .register(LocalDate.class, v -> v.getTimestamp().atZone(ZoneId.systemDefault()).toLocalDate())
                        .register(LocalDateTime.class, v -> v.getTimestamp()
                                .atZone(ZoneId.systemDefault()).toLocalDateTime())
                        .register(java.sql.Date.class, v -> java.sql.Date
                                .valueOf(v.getTimestamp().atZone(ZoneId.systemDefault()).toLocalDate()))
                        .register(java.sql.Timestamp.class, v -> java.sql.Timestamp
                                .valueOf(v.getTimestamp().atZone(ZoneId.systemDefault()).toLocalDateTime()))
                        .register(Instant.class, ValueReader::getTimestamp)
                        .register(java.util.Date.class, v -> java.util.Date.from(v.getTimestamp()))
                        .build();
            case Interval:
                return builder
                        .register(Duration.class, ValueReader::getInterval)
                        .build();
            case TzDate:
                return builder
                        .register(ZonedDateTime.class, ValueReader::getTzDate)
                        .build();
            case TzDatetime:
                return builder
                        .register(ZonedDateTime.class, ValueReader::getTzDatetime)
                        .build();
            case TzTimestamp:
                return builder
                        .register(ZonedDateTime.class, ValueReader::getTzTimestamp)
                        .build();
            default:
                return castToClassNotSupported(id.toString());
        }
    }

    private static SQLException cannotConvert(Type type, Class<?> javaType, Object value) {
        return new SQLException(String.format(YdbConst.UNABLE_TO_CONVERT, type, value, javaType));
    }

    private static SQLException dataTypeNotSupported(PrimitiveType type, Class<?> javaType) {
        return new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, javaType));
    }

    private static ValueToBoolean castToBooleanNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, boolean.class));
        };
    }

    private static ValueToByte castToByteNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, byte.class));
        };
    }

    private static ValueToShort castToShortNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, short.class));
        };
    }

    private static ValueToInt castToIntNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, int.class));
        };
    }

    private static ValueToLong castToLongNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, long.class));
        };
    }

    private static ValueToFloat castToFloatNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, float.class));
        };
    }

    private static ValueToDouble castToDoubleNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, double.class));
        };
    }

    private static ValueToBytes castToBytesNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, byte[].class));
        };
    }

    private static ValueToInstant castToInstantNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, long.class));
        };
    }

    private static ValueToNString castToNStringNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, String.class));
        };
    }

    private static ValueToURL castToUrlNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, URL.class));
        };
    }

    private static ValueToBigDecimal castToBigDecimalNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, BigDecimal.class));
        };
    }

    private static ValueToReader castToReaderNotSupported(String type) {
        return value -> {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST, type, Reader.class));
        };
    }

    @SuppressWarnings("Convert2Lambda")
    private static ValueToClass castToClassNotSupported(String type) {
        return new ValueToClass() {
            @Override
            public <T> T fromValue(ValueReader reader, Class<T> clazz) throws SQLException {
                throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST_TO_CLASS, type, clazz));
            }
        };
    }

    public static class Getters {
        private final ValueToString toString;
        private final ValueToBoolean toBoolean;
        private final ValueToByte toByte;
        private final ValueToShort toShort;
        private final ValueToInt toInt;
        private final ValueToLong toLong;
        private final ValueToFloat toFloat;
        private final ValueToDouble toDouble;
        private final ValueToBytes toBytes;
        private final ValueToObject toObject;
        private final ValueToClass toClass;
        private final ValueToInstant toInstant;
        private final ValueToNString toNString;
        private final ValueToURL toURL;
        private final ValueToBigDecimal toBigDecimal;
        private final ValueToReader toReader;

        @SuppressWarnings("ParameterNumber")
        Getters(ValueToString toString,
                ValueToBoolean toBoolean,
                ValueToByte toByte,
                ValueToShort toShort,
                ValueToInt toInt,
                ValueToLong toLong,
                ValueToFloat toFloat,
                ValueToDouble toDouble,
                ValueToBytes toBytes,
                ValueToObject toObject,
                ValueToClass toClass,
                ValueToInstant toInstant,
                ValueToNString toNString,
                ValueToURL toURL,
                ValueToBigDecimal toBigDecimal,
                ValueToReader toReader) {
            this.toString = toString;
            this.toBoolean = toBoolean;
            this.toByte = toByte;
            this.toShort = toShort;
            this.toInt = toInt;
            this.toLong = toLong;
            this.toFloat = toFloat;
            this.toDouble = toDouble;
            this.toBytes = toBytes;
            this.toObject = toObject;
            this.toClass = toClass;
            this.toInstant = toInstant;
            this.toNString = toNString;
            this.toURL = toURL;
            this.toBigDecimal = toBigDecimal;
            this.toReader = toReader;
        }

        public String readString(ValueReader reader) throws SQLException {
            return toString.fromValue(reader);
        }

        public boolean readBoolean(ValueReader reader) throws SQLException {
            return toBoolean.fromValue(reader);
        }

        public byte readByte(ValueReader reader) throws SQLException {
            return toByte.fromValue(reader);
        }

        public short readShort(ValueReader reader) throws SQLException {
            return toShort.fromValue(reader);
        }

        public int readInt(ValueReader reader) throws SQLException {
            return toInt.fromValue(reader);
        }

        public long readLong(ValueReader reader) throws SQLException {
            return toLong.fromValue(reader);
        }

        public float readFloat(ValueReader reader) throws SQLException {
            return toFloat.fromValue(reader);
        }

        public double readDouble(ValueReader reader) throws SQLException {
            return toDouble.fromValue(reader);
        }

        public byte[] readBytes(ValueReader reader) throws SQLException {
            return toBytes.fromValue(reader);
        }

        public Object readObject(ValueReader reader) throws SQLException {
            return toObject.fromValue(reader);
        }

        public <T> T readClass(ValueReader reader, Class<T> clazz) throws SQLException {
            return toClass.fromValue(reader, clazz);
        }

        public Instant readInstant(ValueReader reader) throws SQLException {
            return toInstant.fromValue(reader);
        }

        public String readNString(ValueReader reader) throws SQLException {
            return toNString.fromValue(reader);
        }

        public String readURL(ValueReader reader) throws SQLException {
            return toURL.fromValue(reader);
        }

        public BigDecimal readBigDecimal(ValueReader reader) throws SQLException {
            return toBigDecimal.fromValue(reader);
        }

        public Reader readReader(ValueReader reader) throws SQLException {
            return toReader.fromValue(reader);
        }
    }

    private interface ValueToString {
        String fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToBoolean {
        boolean fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToByte {
        byte fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToShort {
        short fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToInt {
        int fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToLong {
        long fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToFloat {
        float fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToDouble {
        double fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToBytes {
        byte[] fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToObject {
        Object fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToClass {
        <T> T fromValue(ValueReader reader, Class<T> clazz) throws SQLException;
    }

    private interface ValueToInstant {
        Instant fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToNString {
        String fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToURL {
        String fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToBigDecimal {
        BigDecimal fromValue(ValueReader reader) throws SQLException;
    }

    private interface ValueToReader {
        Reader fromValue(ValueReader reader) throws SQLException;
    }

    //

    public static class SqlType {
        private final int sqlType;
        private final Class<?> javaType;

        SqlType(int sqlType, Class<?> javaType) {
            this.sqlType = sqlType;
            this.javaType = Objects.requireNonNull(javaType);
        }

        public int getSqlType() {
            return sqlType;
        }

        public Class<?> getJavaType() {
            return javaType;
        }
    }

}
