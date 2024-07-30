package tech.ydb.jdbc.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

import static tech.ydb.jdbc.YdbConst.CANNOT_LOAD_DATA_FROM_IS;
import static tech.ydb.jdbc.YdbConst.CANNOT_LOAD_DATA_FROM_READER;
import static tech.ydb.jdbc.YdbConst.UNABLE_TO_CAST;

public class MappingSetters {
    private MappingSetters() { }

    static Setters buildSetters(Type type) {
        return buildToValueImpl(type);
    }

    private static Setters buildToValueImpl(Type type) {
        Type.Kind kind = type.getKind();
        // TODO: Separate setters for primitive values?
        if (kind == Type.Kind.PRIMITIVE) {
            PrimitiveType id = (PrimitiveType) type;
            switch (id) {
                case Bytes:
                    return x -> PrimitiveValue.newBytesOwn(castAsBytes(id, x));
                case Text:
                    return x -> PrimitiveValue.newText(castAsString(id, x));
                case Json:
                    return x -> PrimitiveValue.newJson(castAsJson(id, x));
                case JsonDocument:
                    return x -> PrimitiveValue.newJsonDocument(castAsJson(id, x));
                case Yson:
                    return x -> PrimitiveValue.newYsonOwn(castAsYson(id, x));
                case Uuid:
                    return x -> castAsUuid(id, x);
                case Bool:
                    return x -> PrimitiveValue.newBool(castAsBoolean(id, x));
                case Int8:
                    return x -> PrimitiveValue.newInt8(castAsByte(id, x));
                case Uint8:
                    return x -> PrimitiveValue.newUint8(castAsByte(id, x));
                case Int16:
                    return x -> PrimitiveValue.newInt16(castAsShort(id, x));
                case Uint16:
                    return x -> PrimitiveValue.newUint16(castAsShort(id, x));
                case Int32:
                    return x -> PrimitiveValue.newInt32(castAsInt(id, x));
                case Uint32:
                    return x -> PrimitiveValue.newUint32(castAsInt(id, x));
                case Int64:
                    return x -> PrimitiveValue.newInt64(castAsLong(id, x));
                case Uint64:
                    return x -> PrimitiveValue.newUint64(castAsLong(id, x));
                case Float:
                    return x -> PrimitiveValue.newFloat(castAsFloat(id, x));
                case Double:
                    return x -> PrimitiveValue.newDouble(castAsDouble(id, x));
                case Date:
                    return x -> castToDate(id, x);
                case Datetime:
                    return x -> castToDateTime(id, x);
                case Timestamp:
                    return x -> castToTimestamp(id, x);
                case Interval:
                    return x -> castToInterval(id, x);
                case TzDate:
                    return x -> PrimitiveValue.newTzDate(castAsZonedDateTime(id, x));
                case TzDatetime:
                    return x -> PrimitiveValue.newTzDatetime(castAsZonedDateTime(id, x));
                case TzTimestamp:
                    return x -> PrimitiveValue.newTzTimestamp(castAsZonedDateTime(id, x));
                default:
                    return x -> {
                        throw castNotSupported(id, x);
                    };
            }
        } else if (kind == Type.Kind.DECIMAL) {
            return x -> castToDecimalValue((DecimalType) type, x);
        } else if (kind == Type.Kind.LIST) {
            ListType listType = (ListType) type;
            Setters itemSetter = buildToValueImpl(listType.getItemType());
            return x -> castAsList(listType, itemSetter, x);
        } else if (kind == Type.Kind.OPTIONAL) {
            return buildToValueImpl(((OptionalType) type).getItemType());
        } else {
            return x -> {
                throw castNotSupported(kind, x);
            };
        }
    }

    private static String toString(Object x) {
        return x == null ? "null" : (x.getClass() + ": " + x);
    }

    private static SQLException castNotSupported(PrimitiveType type, Object x, Exception cause) {
        return new SQLException(String.format(UNABLE_TO_CAST, toString(x), type), cause);
    }

    private static SQLException castNotSupported(PrimitiveType type, Object x) {
        return new SQLException(String.format(UNABLE_TO_CAST, toString(x), type));
    }

    private static SQLException castNotSupported(Type.Kind kind, Object x) {
        return new SQLException(String.format(UNABLE_TO_CAST, toString(x), kind));
    }

    private static ListValue castAsList(ListType type, Setters itemSetter, Object x) throws SQLException {
        if (x instanceof Collection<?>) {
            Collection<?> values = (Collection<?>) x;
            int len = values.size();
            Value<?>[] result = new Value<?>[len];
            int index = 0;
            for (Object value : values) {
                if (value != null) {
                    if (value instanceof Value<?>) {
                        result[index++] = (Value<?>) value;
                    } else {
                        result[index++] = itemSetter.toValue(value);
                    }
                }
            }
            if (index < result.length) {
                result = Arrays.copyOf(result, index); // Some values are null
            }
            return type.newValueOwn(result);
        } else {
            throw castNotSupported(type.getKind(), x);
        }
    }

    private static byte[] castAsBytes(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof byte[]) {
            return (byte[]) x;
        } else if (x instanceof String) {
            return ((String) x).getBytes();
        } else if (x instanceof InputStream) {
            return ByteStream.fromInputStream((InputStream) x, -1).asByteArray();
        } else if (x instanceof Reader) {
            return CharStream.fromReader((Reader) x, -1).asString().getBytes();
        } else if (x instanceof ByteStream) {
            return ((ByteStream) x).asByteArray();
        } else if (x instanceof CharStream) {
            return ((CharStream) x).asString().getBytes();
        } else {
            return castAsString(type, x).getBytes();
        }
    }

    private static byte[] castAsYson(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof byte[]) {
            return (byte[]) x;
        } else if (x instanceof String) {
            return ((String) x).getBytes();
        } else if (x instanceof InputStream) {
            return ByteStream.fromInputStream((InputStream) x, -1).asByteArray();
        } else if (x instanceof Reader) {
            return CharStream.fromReader((Reader) x, -1).asString().getBytes();
        } else if (x instanceof ByteStream) {
            return ((ByteStream) x).asByteArray();
        } else if (x instanceof CharStream) {
            return ((CharStream) x).asString().getBytes();
        }
        throw castNotSupported(type, x);
    }

    @SuppressWarnings("unused")
    private static String castAsString(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof String) {
            return (String) x;
        } else if (x instanceof byte[]) {
            return new String((byte[]) x);
        } else if (x instanceof InputStream) {
            return new String(ByteStream.fromInputStream((InputStream) x, -1).asByteArray());
        } else if (x instanceof Reader) {
            return CharStream.fromReader((Reader) x, -1).asString();
        } else if (x instanceof ByteStream) {
            return new String(((ByteStream) x).asByteArray());
        } else if (x instanceof CharStream) {
            return ((CharStream) x).asString();
        } else {
            return String.valueOf(x);
        }
    }

    private static String castAsJson(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof String) {
            return (String) x;
        } else if (x instanceof byte[]) {
            return new String((byte[]) x);
        } else if (x instanceof InputStream) {
            return new String(ByteStream.fromInputStream((InputStream) x, -1).asByteArray());
        } else if (x instanceof Reader) {
            return CharStream.fromReader((Reader) x, -1).asString();
        } else if (x instanceof ByteStream) {
            return new String(((ByteStream) x).asByteArray());
        } else if (x instanceof CharStream) {
            return ((CharStream) x).asString();
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castAsUuid(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof String) {
            return PrimitiveValue.newUuid((String) x);
        } else if (x instanceof byte[]) {
            return PrimitiveValue.newUuid(new String((byte[]) x));
        } else if (x instanceof UUID) {
            return PrimitiveValue.newUuid((UUID) x);
        }
        throw castNotSupported(type, x);
    }

    private static byte castAsByte(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Byte) {
            return (Byte) x;
        } else if (x instanceof Boolean) {
            return (byte) (((Boolean) x) ? 1 : 0);
        }
        throw castNotSupported(type, x);
    }

    private static short castAsShort(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Short) {
            return (Short) x;
        } else if (x instanceof Byte) {
            return (Byte) x;
        } else if (x instanceof Boolean) {
            return (short) (((Boolean) x) ? 1 : 0);
        }
        throw castNotSupported(type, x);
    }

    private static int castAsInt(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Integer) {
            return (Integer) x;
        } else if (x instanceof Short) {
            return (Short) x;
        } else if (x instanceof Byte) {
            return (Byte) x;
        } else if (x instanceof Boolean) {
            return ((Boolean) x) ? 1 : 0;
        }
        throw castNotSupported(type, x);
    }

    private static long castAsLong(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Long) {
            return (Long) x;
        } else if (x instanceof Integer) {
            return (Integer) x;
        } else if (x instanceof Short) {
            return (Short) x;
        } else if (x instanceof Byte) {
            return (Byte) x;
        } else if (x instanceof Boolean) {
            return ((Boolean) x) ? 1L : 0L;
        } else if (x instanceof BigInteger) {
            return ((BigInteger) x).longValue();
        }
        throw castNotSupported(type, x);
    }

    private static float castAsFloat(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Float) {
            return (Float) x;
        } else if (x instanceof Integer) {
            return (Integer) x;
        } else if (x instanceof Short) {
            return (Short) x;
        } else if (x instanceof Byte) {
            return (Byte) x;
        } else if (x instanceof Boolean) {
            return ((Boolean) x) ? 1f : 0f;
        }
        throw castNotSupported(type, x);
    }

    private static double castAsDouble(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Double) {
            return (Double) x;
        } else if (x instanceof Float) {
            return (Float) x;
        } else if (x instanceof Long) {
            return (Long) x;
        } else if (x instanceof Integer) {
            return (Integer) x;
        } else if (x instanceof Short) {
            return (Short) x;
        } else if (x instanceof Byte) {
            return (Byte) x;
        } else if (x instanceof Boolean) {
            return ((Boolean) x) ? 1d : 0d;
        }
        throw castNotSupported(type, x);
    }

    private static boolean castAsBoolean(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Boolean) {
            return (boolean) x;
        } else if (x instanceof Number) {
            long lValue = ((Number) x).longValue();
            return lValue > 0;
        }
        throw castNotSupported(type, x);
    }

    private static ZonedDateTime castAsZonedDateTime(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof ZonedDateTime) {
            return (ZonedDateTime) x;
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castToInterval(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Duration) {
            return PrimitiveValue.newInterval((Duration) x);
        } else if (x instanceof Long) {
            return PrimitiveValue.newInterval((Long) x);
        } else if (x instanceof String) {
            Duration parsed;
            try {
                parsed = Duration.parse((String) x);
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
            return PrimitiveValue.newInterval(parsed);
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castToDate(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Instant) {
            return PrimitiveValue.newDate((Instant) x);
        } else if (x instanceof LocalDateTime) {
            return PrimitiveValue.newDate(((LocalDateTime) x).toLocalDate());
        } else if (x instanceof LocalDate) {
            return PrimitiveValue.newDate((LocalDate) x);
        } else if (x instanceof Long) {
            return PrimitiveValue.newDate(TimeUnit.MILLISECONDS.toDays((Long) x));
        } else if (x instanceof Timestamp) {
            // Normalize date - use system timezone to detect correct date
            Instant instant = Instant.ofEpochMilli(((Timestamp) x).getTime());
            LocalDateTime ldt = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
            return PrimitiveValue.newDate(ldt.toLocalDate());
        } else if (x instanceof Date) {
            // Normalize date - use system timezone to detect correct date
            Instant instant = Instant.ofEpochMilli(((Date) x).getTime());
            LocalDate ld = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return PrimitiveValue.newDate(ld);
        } else if (x instanceof String) {
            Instant parsed;
            try {
                parsed = Instant.parse((String) x);
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
            return PrimitiveValue.newDate(parsed);
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castToDateTime(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Instant) {
            return PrimitiveValue.newDatetime((Instant) x);
        } else if (x instanceof Long) {
            return PrimitiveValue.newDatetime(TimeUnit.MILLISECONDS.toSeconds((Long) x));
        } else if (x instanceof Timestamp) {
            // Normalize date - use system timezone to detect correct datetime
            Instant instant = Instant.ofEpochMilli(((Timestamp) x).getTime());
            LocalDateTime ldt = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
            return PrimitiveValue.newDatetime(ldt);
        } else if (x instanceof Date) {
            // Normalize date - use system timezone to detect correct datetime
            Instant instant = Instant.ofEpochMilli(((Date) x).getTime());
            LocalDate ld = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return PrimitiveValue.newDatetime(ld.toEpochDay() * 24 * 60 * 60);
        } else if (x instanceof LocalDate) {
            return PrimitiveValue.newDatetime(((LocalDate) x).toEpochDay() * 24 * 60 * 60);
        } else if (x instanceof LocalDateTime) {
            return PrimitiveValue.newDatetime((LocalDateTime) x);
        } else if (x instanceof String) {
            Instant parsed;
            try {
                parsed = Instant.parse((String) x);
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
            return PrimitiveValue.newDatetime(parsed);
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castToTimestamp(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Instant) {
            return PrimitiveValue.newTimestamp((Instant) x);
        } else if (x instanceof Long) {
            return PrimitiveValue.newTimestamp(TimeUnit.MILLISECONDS.toMicros((Long) x));
        } else if (x instanceof LocalDate) {
            return PrimitiveValue.newTimestamp(((LocalDate) x).toEpochDay() * 24 * 60 * 60 * 1000000);
        } else if (x instanceof LocalDateTime) {
            // LocalDateTime is usually used as Datetime analog, so truncate it to seconds
            long seconds = ((LocalDateTime) x).toInstant(ZoneOffset.UTC).getEpochSecond();
            return PrimitiveValue.newTimestamp(seconds * 1000000);
        } else if (x instanceof Timestamp) {
            return PrimitiveValue.newTimestamp(TimeUnit.MILLISECONDS.toMicros(((Timestamp) x).getTime()));
        } else if (x instanceof Date) {
            // Normalize date to UTC
            // Normalize date - use system timezone to detect correct datetime
            Instant instant = Instant.ofEpochMilli(((Date) x).getTime());
            LocalDate ld = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return PrimitiveValue.newTimestamp(ld.toEpochDay() * 24 * 60 * 60 * 1000000);
        } else if (x instanceof String) {
            Instant parsed;
            try {
                parsed = Instant.parse((String) x);
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
            return PrimitiveValue.newTimestamp(parsed);
        }
        throw castNotSupported(type, x);
    }

    private static DecimalValue castToDecimalValue(DecimalType type, Object x) throws SQLException {
        if (x instanceof DecimalValue) {
            return (DecimalValue) x;
        } else if (x instanceof BigDecimal) {
            return type.newValue((BigDecimal) x);
        } else if (x instanceof BigInteger) {
            return type.newValue((BigInteger) x);
        } else if (x instanceof Long) {
            return type.newValue((Long) x);
        } else if (x instanceof Integer) {
            return type.newValue((Integer) x);
        } else if (x instanceof Short) {
            return type.newValue((Short) x);
        } else if (x instanceof Byte) {
            return type.newValue((Byte) x);
        } else if (x instanceof String) {
            return type.newValue((String) x);
        }
        throw castNotSupported(type.getKind(), x);
    }

    public interface Setters {
        Value<?> toValue(Object value) throws SQLException;
    }

    public interface CharStream {
        String asString() throws SQLException;

        static CharStream fromReader(Reader reader, long length) {
            return () -> {
                try {
                    if (length >= 0) {
                        return CharStreams.toString(new LimitedReader(reader, length));
                    } else {
                        return CharStreams.toString(reader);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(CANNOT_LOAD_DATA_FROM_READER + e.getMessage(), e);
                }
            };
        }
    }

    public interface ByteStream {
        byte[] asByteArray() throws SQLException;

        @SuppressWarnings("UnstableApiUsage")
        static ByteStream fromInputStream(InputStream stream, long length) {
            return () -> {
                try {
                    if (length >= 0) {
                        return ByteStreams.toByteArray(ByteStreams.limit(stream, length));
                    } else {
                        return ByteStreams.toByteArray(stream);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(CANNOT_LOAD_DATA_FROM_IS + e.getMessage(), e);
                }
            };
        }
    }
}
