package tech.ydb.jdbc.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
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
import java.util.UUID;

import com.google.common.io.ByteStreams;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

public class MappingSetters {
    private static final int DEFAULT_BUF_SIZE = 0x800;

    private MappingSetters() {
    }

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
                case Date32:
                    return x -> castToDate32(id, x);
                case Datetime64:
                    return x -> castToDateTime64(id, x);
                case Timestamp64:
                    return x -> castToTimestamp64(id, x);
                case Interval64:
                    return x -> castToInterval64(id, x);
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
        return new SQLException(String.format(YdbConst.UNABLE_TO_CAST, toString(x), type), cause);
    }

    private static SQLException castNotSupported(PrimitiveType type, Object x) {
        return new SQLException(String.format(YdbConst.UNABLE_TO_CAST, toString(x), type));
    }

    private static SQLException castNotSupported(Type.Kind kind, Object x) {
        return new SQLException(String.format(YdbConst.UNABLE_TO_CAST, toString(x), kind));
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
        } else if (x instanceof Short) {
            return ((Short) x).byteValue();
        } else if (x instanceof Integer) {
            return ((Integer) x).byteValue();
        } else if (x instanceof Long) {
            return ((Long) x).byteValue();
        } else if (x instanceof Boolean) {
            return (byte) (((Boolean) x) ? 1 : 0);
        } else if (x instanceof String) {
            try {
                return Byte.parseByte((String) x);
            } catch (NumberFormatException e) {
                throw castNotSupported(type, x, e);
            }
        }
        throw castNotSupported(type, x);
    }

    private static short castAsShort(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Short) {
            return (Short) x;
        } else if (x instanceof Byte) {
            return (Byte) x;
        } else if (x instanceof Integer) {
            return ((Integer) x).shortValue();
        } else if (x instanceof Long) {
            return ((Long) x).shortValue();
        } else if (x instanceof Boolean) {
            return (short) (((Boolean) x) ? 1 : 0);
        } else if (x instanceof String) {
            try {
                return Short.parseShort((String) x);
            } catch (NumberFormatException e) {
                throw castNotSupported(type, x, e);
            }
        }
        throw castNotSupported(type, x);
    }

    private static int castAsInt(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Integer) {
            return (Integer) x;
        } else if (x instanceof Long) {
            return ((Long) x).intValue();
        } else if (x instanceof Short) {
            return (Short) x;
        } else if (x instanceof Byte) {
            return (Byte) x;
        } else if (x instanceof Boolean) {
            return ((Boolean) x) ? 1 : 0;
        } else if (x instanceof Time) {
            return ((Time) x).toLocalTime().toSecondOfDay();
        } else if (x instanceof Date) {
            return (int) ((Date) x).toLocalDate().toEpochDay();
        } else if (x instanceof Timestamp) {
            return (int) ((Timestamp) x).getTime();
        } else if (x instanceof String) {
            try {
                return Integer.parseInt((String) x);
            } catch (NumberFormatException e) {
                throw castNotSupported(type, x, e);
            }
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
        } else if (x instanceof Time) {
            return ((Time) x).toLocalTime().toSecondOfDay();
        } else if (x instanceof Date) {
            return ((Date) x).toLocalDate().toEpochDay();
        } else if (x instanceof Timestamp) {
            return ((Timestamp) x).getTime();
        } else if (x instanceof String) {
            try {
                return Long.parseLong((String) x);
            } catch (NumberFormatException e) {
                throw castNotSupported(type, x, e);
            }
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
        } else if (x instanceof String) {
            try {
                return Float.parseFloat((String) x);
            } catch (NumberFormatException e) {
                throw castNotSupported(type, x, e);
            }
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
        } else if (x instanceof String) {
            try {
                return Double.parseDouble((String) x);
            } catch (NumberFormatException e) {
                throw castNotSupported(type, x, e);
            }
        }
        throw castNotSupported(type, x);
    }

    private static boolean castAsBoolean(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Boolean) {
            return (boolean) x;
        } else if (x instanceof Number) {
            long lValue = ((Number) x).longValue();
            return lValue > 0;
        } else if (x instanceof String) {
            return Boolean.parseBoolean((String) x);
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
            try {
                return PrimitiveValue.newInterval(Duration.parse((String) x));
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castToDate(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Instant) {
            return PrimitiveValue.newDate(((Instant) x).atZone(ZoneId.systemDefault()).toLocalDate());
        } else if (x instanceof LocalDateTime) {
            return PrimitiveValue.newDate(((LocalDateTime) x).toLocalDate());
        } else if (x instanceof LocalDate) {
            return PrimitiveValue.newDate((LocalDate) x);
        } else if (x instanceof Integer) {
            return PrimitiveValue.newDate(LocalDate.ofEpochDay((Integer) x));
        } else if (x instanceof Long) {
            return PrimitiveValue.newDate(LocalDate.ofEpochDay((Long) x));
        } else if (x instanceof Timestamp) {
            // Normalize date - use system timezone to detect correct date
            Instant instant = Instant.ofEpochMilli(((Timestamp) x).getTime());
            LocalDate ld = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return PrimitiveValue.newDate(ld);
        } else if (x instanceof Date) {
            // Normalize date - use system timezone to detect correct date
            Instant instant = Instant.ofEpochMilli(((Date) x).getTime());
            LocalDate ld = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return PrimitiveValue.newDate(ld);
        } else if (x instanceof String) {
            try {
                return PrimitiveValue.newDate(LocalDate.parse((String) x));
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castToDateTime(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Instant) {
            return PrimitiveValue.newDatetime(((Instant) x).atZone(ZoneId.systemDefault()).toLocalDateTime());
        } else if (x instanceof LocalDateTime) {
            return PrimitiveValue.newDatetime(((LocalDateTime) x));
        } else if (x instanceof LocalDate) {
            return PrimitiveValue.newDatetime(((LocalDate) x).atStartOfDay());
        } else if (x instanceof Long) {
            return PrimitiveValue.newDatetime(LocalDateTime.ofEpochSecond((Long) x, 0, ZoneOffset.UTC));
        } else if (x instanceof Timestamp) {
            // Normalize date - use system timezone to detect correct date
            Instant instant = Instant.ofEpochMilli(((Timestamp) x).getTime());
            LocalDateTime ldt = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
            return PrimitiveValue.newDatetime(ldt);
        } else if (x instanceof Date) {
            // Normalize date - use system timezone to detect correct date
            Instant instant = Instant.ofEpochMilli(((Date) x).getTime());
            LocalDate ld = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return PrimitiveValue.newDatetime(ld.atStartOfDay());
        } else if (x instanceof String) {
            try {
                return PrimitiveValue.newDatetime(LocalDateTime.parse((String) x));
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castToTimestamp(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Instant) {
            return PrimitiveValue.newTimestamp((Instant) x);
        } else if (x instanceof Long) {
            return PrimitiveValue.newTimestamp(Instant.ofEpochMilli((Long) x));
        } else if (x instanceof LocalDate) {
            return PrimitiveValue.newTimestamp(((LocalDate) x).atStartOfDay().toInstant(ZoneOffset.UTC));
        } else if (x instanceof LocalDateTime) {
            long epochSeconds = ((LocalDateTime) x).toEpochSecond(ZoneOffset.UTC);
            return PrimitiveValue.newTimestamp(Instant.ofEpochSecond(epochSeconds));
        } else if (x instanceof Timestamp) {
            return PrimitiveValue.newTimestamp(((Timestamp) x).toInstant());
        } else if (x instanceof Date) {
            Instant instant = ((Date) x).toLocalDate().atStartOfDay().toInstant(ZoneOffset.UTC);
            return PrimitiveValue.newTimestamp(instant);
        } else if (x instanceof String) {
            try {
                return PrimitiveValue.newTimestamp(Instant.parse((String) x));
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castToInterval64(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Duration) {
            return PrimitiveValue.newInterval64((Duration) x);
        } else if (x instanceof Long) {
            return PrimitiveValue.newInterval64((Long) x);
        } else if (x instanceof String) {
            Duration parsed;
            try {
                parsed = Duration.parse((String) x);
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
            return PrimitiveValue.newInterval64(parsed);
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castToDate32(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Instant) {
            return PrimitiveValue.newDate32(((Instant) x).atZone(ZoneId.systemDefault()).toLocalDate());
        } else if (x instanceof LocalDateTime) {
            return PrimitiveValue.newDate32(((LocalDateTime) x).toLocalDate());
        } else if (x instanceof LocalDate) {
            return PrimitiveValue.newDate32((LocalDate) x);
        } else if (x instanceof Integer) {
            return PrimitiveValue.newDate32(LocalDate.ofEpochDay((Integer) x));
        } else if (x instanceof Long) {
            return PrimitiveValue.newDate32(LocalDate.ofEpochDay((Long) x));
        } else if (x instanceof Timestamp) {
            // Normalize date - use system timezone to detect correct date
            Instant instant = Instant.ofEpochMilli(((Timestamp) x).getTime());
            LocalDate ld = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return PrimitiveValue.newDate32(ld);
        } else if (x instanceof Date) {
            // Normalize date - use system timezone to detect correct date
            Instant instant = Instant.ofEpochMilli(((Date) x).getTime());
            LocalDate ld = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return PrimitiveValue.newDate32(ld);
        } else if (x instanceof String) {
            try {
                return PrimitiveValue.newDate32(LocalDate.parse((String) x));
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castToDateTime64(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Instant) {
            return PrimitiveValue.newDatetime64(((Instant) x).atZone(ZoneId.systemDefault()).toLocalDateTime());
        } else if (x instanceof LocalDateTime) {
            return PrimitiveValue.newDatetime64(((LocalDateTime) x));
        } else if (x instanceof LocalDate) {
            return PrimitiveValue.newDatetime64(((LocalDate) x).atStartOfDay());
        } else if (x instanceof Long) {
            return PrimitiveValue.newDatetime64(LocalDateTime.ofEpochSecond((Long) x, 0, ZoneOffset.UTC));
        } else if (x instanceof Timestamp) {
            // Normalize date - use system timezone to detect correct date
            Instant instant = Instant.ofEpochMilli(((Timestamp) x).getTime());
            LocalDateTime ldt = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
            return PrimitiveValue.newDatetime64(ldt);
        } else if (x instanceof Date) {
            // Normalize date - use system timezone to detect correct date
            Instant instant = Instant.ofEpochMilli(((Date) x).getTime());
            LocalDate ld = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return PrimitiveValue.newDatetime64(ld.atStartOfDay());
        } else if (x instanceof String) {
            try {
                return PrimitiveValue.newDatetime64(LocalDateTime.parse((String) x));
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
        }
        throw castNotSupported(type, x);
    }

    private static PrimitiveValue castToTimestamp64(PrimitiveType type, Object x) throws SQLException {
        if (x instanceof Instant) {
            return PrimitiveValue.newTimestamp64((Instant) x);
        } else if (x instanceof Long) {
            return PrimitiveValue.newTimestamp64(Instant.ofEpochMilli((Long) x));
        } else if (x instanceof LocalDate) {
            return PrimitiveValue.newTimestamp64(((LocalDate) x).atStartOfDay().toInstant(ZoneOffset.UTC));
        } else if (x instanceof LocalDateTime) {
            long epochSeconds = ((LocalDateTime) x).toEpochSecond(ZoneOffset.UTC);
            return PrimitiveValue.newTimestamp64(Instant.ofEpochSecond(epochSeconds));
        } else if (x instanceof Timestamp) {
            return PrimitiveValue.newTimestamp64(((Timestamp) x).toInstant());
        } else if (x instanceof Date) {
            Instant instant = ((Date) x).toLocalDate().atStartOfDay().toInstant(ZoneOffset.UTC);
            return PrimitiveValue.newTimestamp64(instant);
        } else if (x instanceof String) {
            try {
                return PrimitiveValue.newTimestamp64(Instant.parse((String) x));
            } catch (DateTimeParseException e) {
                throw castNotSupported(type, x, e);
            }
        }
        throw castNotSupported(type, x);
    }

    private static DecimalValue validateValue(DecimalType type, DecimalValue value, Object x) throws SQLException {
        if (value.isNan()) {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST_TO_DECIMAL, type, toString(x), "NaN"));
        }
        if (value.isInf()) {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST_TO_DECIMAL, type, toString(x), "Infinite"));
        }
        if (value.isNegativeInf()) {
            throw new SQLException(String.format(YdbConst.UNABLE_TO_CAST_TO_DECIMAL, type, toString(x), "-Infinite"));
        }
        return value;
    }

    private static DecimalValue castToDecimalValue(DecimalType type, Object x) throws SQLException {
        if (x instanceof DecimalValue) {
            return validateValue(type, (DecimalValue) x, x);
        } else if (x instanceof BigDecimal) {
            return validateValue(type, type.newValue((BigDecimal) x), x);
        } else if (x instanceof BigInteger) {
            return validateValue(type, type.newValue((BigInteger) x), x);
        } else if (x instanceof Long) {
            return validateValue(type, type.newValue((Long) x), x);
        } else if (x instanceof Integer) {
            return validateValue(type, type.newValue((Integer) x), x);
        } else if (x instanceof Short) {
            return validateValue(type, type.newValue((Short) x), x);
        } else if (x instanceof Byte) {
            return validateValue(type, type.newValue((Byte) x), x);
        } else if (x instanceof String) {
            return validateValue(type, type.newValue((String) x), x);
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
                    char[] buf = new char[DEFAULT_BUF_SIZE];
                    StringBuilder sb = new StringBuilder();
                    int nRead;
                    long total = 0;
                    while ((nRead = reader.read(buf)) != -1) {
                        total += nRead;
                        if (length < 0 || total < length) {
                            sb.append(buf, 0, nRead);
                        } else {
                            sb.append(buf, 0, nRead - (int) (total - length));
                            break;
                        }
                    }
                    return sb.toString();
                } catch (IOException e) {
                    throw new RuntimeException(YdbConst.CANNOT_LOAD_DATA_FROM_READER + e.getMessage(), e);
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
                    throw new RuntimeException(YdbConst.CANNOT_LOAD_DATA_FROM_IS + e.getMessage(), e);
                }
            };
        }
    }
}
