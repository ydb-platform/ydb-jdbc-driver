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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.impl.YdbTypesImpl;
import tech.ydb.table.result.PrimitiveReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

import static tech.ydb.table.values.PrimitiveType.Bool;
import static tech.ydb.table.values.PrimitiveType.Bytes;
import static tech.ydb.table.values.PrimitiveType.Date;
import static tech.ydb.table.values.PrimitiveType.Datetime;
import static tech.ydb.table.values.PrimitiveType.Double;
import static tech.ydb.table.values.PrimitiveType.Float;
import static tech.ydb.table.values.PrimitiveType.Int16;
import static tech.ydb.table.values.PrimitiveType.Int32;
import static tech.ydb.table.values.PrimitiveType.Int64;
import static tech.ydb.table.values.PrimitiveType.Int8;
import static tech.ydb.table.values.PrimitiveType.Interval;
import static tech.ydb.table.values.PrimitiveType.Json;
import static tech.ydb.table.values.PrimitiveType.JsonDocument;
import static tech.ydb.table.values.PrimitiveType.Text;
import static tech.ydb.table.values.PrimitiveType.Timestamp;
import static tech.ydb.table.values.PrimitiveType.TzDate;
import static tech.ydb.table.values.PrimitiveType.TzDatetime;
import static tech.ydb.table.values.PrimitiveType.TzTimestamp;
import static tech.ydb.table.values.PrimitiveType.Uint16;
import static tech.ydb.table.values.PrimitiveType.Uint32;
import static tech.ydb.table.values.PrimitiveType.Uint64;
import static tech.ydb.table.values.PrimitiveType.Uint8;
import static tech.ydb.table.values.PrimitiveType.Uuid;
import static tech.ydb.table.values.PrimitiveType.Yson;
import static tech.ydb.table.values.Type.Kind.PRIMITIVE;

public class MappingGetters {
    private MappingGetters() { }

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
                        valueToDateMillis(id),
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
                        value -> value.getDecimal().toBigInteger().intValue(),
                        value -> value.getDecimal().toBigInteger().longValue(),
                        value -> value.getDecimal().toBigDecimal().floatValue(),
                        value -> value.getDecimal().toBigDecimal().doubleValue(),
                        castToBytesNotSupported(clazz),
                        PrimitiveReader::getDecimal,
                        castToDateMillisNotSupported(clazz),
                        castToNStringNotSupported(clazz),
                        castToUrlNotSupported(clazz),
                        value -> value.getDecimal().toBigDecimal(),
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
                        value -> 0,
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
                        castToDateMillisNotSupported(clazz),
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
            case Datetime:
            case TzDate:
            case TzDatetime:
            case Timestamp:
            case TzTimestamp:
                ValueToDateMillis delegate = valueToDateMillis(id);
                return delegate::fromValue;
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

    private static ValueToDateMillis valueToDateMillis(PrimitiveType id) {
        switch (id) {
            case Int64:
                return PrimitiveReader::getInt64;
            case Uint64:
                return PrimitiveReader::getUint64;
            case Date:
                return value -> TimeUnit.DAYS.toMillis(value.getDate().toEpochDay());
            case Datetime:
                return value -> TimeUnit.SECONDS.toMillis(value.getDatetime().toEpochSecond(ZoneOffset.UTC));
            case TzDate:
                return value -> TimeUnit.SECONDS.toMillis(value.getTzDate().toEpochSecond());
            case TzDatetime:
                return value -> TimeUnit.SECONDS.toMillis(value.getTzDatetime().toEpochSecond());
            case Timestamp:
                return value -> value.getTimestamp().toEpochMilli();
            case TzTimestamp:
                return value -> TimeUnit.SECONDS.toMillis(value.getTzTimestamp().toEpochSecond());
            default:
                return castToDateMillisNotSupported(id.name());
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
            case Uuid:
                return new SqlType(sqlType, String.class);
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

    static SqlType buildDataType(Type type) {
        // All types must be the same as for #valueToObject
        int sqlType = YdbTypesImpl.getInstance().toSqlType(type);

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

    private static SQLException cannotConvert(PrimitiveType type, Class<?> javaType, Object value) {
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

    private static ValueToDateMillis castToDateMillisNotSupported(String type) {
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
        private final ValueToDateMillis toDateMillis;
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
                ValueToDateMillis toDateMillis,
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
            this.toDateMillis = toDateMillis;
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

        public long readDateMillis(ValueReader reader) throws SQLException {
            return toDateMillis.fromValue(reader);
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

    private interface ValueToDateMillis {
        long fromValue(ValueReader reader) throws SQLException;
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
