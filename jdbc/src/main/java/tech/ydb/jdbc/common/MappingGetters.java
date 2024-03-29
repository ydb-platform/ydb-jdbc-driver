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

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import tech.ydb.jdbc.impl.YdbTypesImpl;
import tech.ydb.table.result.PrimitiveReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

import static tech.ydb.jdbc.YdbConst.UNABLE_TO_CAST;
import static tech.ydb.jdbc.YdbConst.UNABLE_TO_CONVERT;
import static tech.ydb.table.values.PrimitiveType.Int16;
import static tech.ydb.table.values.PrimitiveType.Int32;
import static tech.ydb.table.values.PrimitiveType.Uint32;

public class MappingGetters {

    static Getters buildGetters(Type type) {
        Type.Kind kind = type.getKind();
        @Nullable PrimitiveType id = type.getKind() == Type.Kind.PRIMITIVE ? ((PrimitiveType) type) : null;
        return new Getters(
                valueToString(kind, id),
                valueToBoolean(kind, id),
                valueToByte(kind, id),
                valueToShort(kind, id),
                valueToInt(kind, id),
                valueToLong(kind, id),
                valueToFloat(kind, id),
                valueToDouble(kind, id),
                valueToBytes(kind, id),
                valueToObject(kind, id),
                valueToDateMillis(kind, id),
                valueToNString(kind, id),
                valueToURL(kind, id),
                valueToBigDecimal(kind, id),
                valueToReader(kind, id));
    }

    private static ValueToString valueToString(Type.Kind kind, @Nullable PrimitiveType id) {
        Class<?> javaType = String.class;
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                        throw dataTypeNotSupported(id, javaType);
                    };
            }
        } else if (kind == Type.Kind.DECIMAL) {
            return value -> String.valueOf(value.getDecimal());
        } else {
            return value -> String.valueOf(value.getValue());
        }
    }

    private static ValueToBoolean valueToBoolean(Type.Kind kind, @Nullable PrimitiveType id) {
        Class<?> javaType = boolean.class;
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                        throw cannotConvert(id, javaType, new String(stringValue));
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
                        throw cannotConvert(id, javaType, utfValue);
                    };
                default:
                    return value -> {
                        throw dataTypeNotSupported(id, javaType);
                    };
            }
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, javaType);
            };
        }
    }

    private static byte checkByteValue(PrimitiveType id, int value) throws SQLException {
        int ch = value >= 0 ? value : ~value;
        if ((ch & 0x7F) != ch) {
            throw cannotConvert(id, byte.class, value);
        }
        return (byte)value;
    }

    private static byte checkByteValue(PrimitiveType id, long value) throws SQLException {
        long ch = value >= 0 ? value : ~value;
        if ((ch & 0x7Fl) != ch) {
            throw cannotConvert(id, byte.class, value);
        }
        return (byte)value;
    }

    private static ValueToByte valueToByte(Type.Kind kind, PrimitiveType id) {
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                    return value -> {
                        throw dataTypeNotSupported(id, byte.class);
                    };
            }
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, byte.class);
            };
        }
    }

    private static short checkShortValue(PrimitiveType id, int value) throws SQLException {
        int ch = value >= 0 ? value : ~value;
        if ((ch & 0x7FFF) != ch) {
            throw cannotConvert(id, short.class, value);
        }
        return (short)value;
    }

    private static short checkShortValue(PrimitiveType id, long value) throws SQLException {
        long ch = value >= 0 ? value : ~value;
        if ((ch & 0x7FFFl) != ch) {
            throw cannotConvert(id, short.class, value);
        }
        return (short)value;
    }

    private static ValueToShort valueToShort(Type.Kind kind, PrimitiveType id) {
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                    return value -> {
                        throw dataTypeNotSupported(id, short.class);
                    };
            }
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, short.class);
            };
        }
    }

    private static int checkIntValue(PrimitiveType id, long value) throws SQLException {
        long ch = value >= 0 ? value : ~value;
        if ((ch & 0x7FFFFFFFl) != ch) {
            throw cannotConvert(id, int.class, value);
        }
        return (int)value;
    }

    private static ValueToInt valueToInt(Type.Kind kind, PrimitiveType id) {
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                    return value -> {
                        throw dataTypeNotSupported(id, int.class);
                    };
            }
        } else if (kind == Type.Kind.DECIMAL) {
            return value -> value.getDecimal().toBigInteger().intValue(); // TODO: Improve performance
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, int.class);
            };
        }
    }

    private static ValueToLong valueToLong(Type.Kind kind, @Nullable PrimitiveType id) {
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                    ValueToDateMillis delegate = valueToDateMillis(kind, id);
                    return delegate::fromValue;
                case Interval:
                    return value -> TimeUnit.NANOSECONDS.toMicros(value.getInterval().toNanos());
                default:
                    return value -> {
                        throw dataTypeNotSupported(id, long.class);
                    };
            }
        } else if (kind == Type.Kind.DECIMAL) {
            return value -> value.getDecimal().toBigInteger().longValue(); // TODO: Improve performance
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, long.class);
            };
        }
    }

    private static ValueToFloat valueToFloat(Type.Kind kind, @Nullable PrimitiveType id) {
        Class<?> javaType = float.class;
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                    return value -> {
                        throw dataTypeNotSupported(id, javaType);
                    };
            }
        } else if (kind == Type.Kind.DECIMAL) {
            return value -> value.getDecimal().toBigDecimal().floatValue();
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, javaType);
            };
        }
    }

    private static ValueToDouble valueToDouble(Type.Kind kind, @Nullable PrimitiveType id) {
        Class<?> javaType = double.class;
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                    return value -> {
                        throw dataTypeNotSupported(id, javaType);
                    };
            }
        } else if (kind == Type.Kind.DECIMAL) {
            return value -> value.getDecimal().toBigDecimal().doubleValue();
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, javaType);
            };
        }
    }

    private static ValueToBytes valueToBytes(Type.Kind kind, @Nullable PrimitiveType id) {
        Class<?> javaType = byte[].class;
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                    return value -> {
                        throw dataTypeNotSupported(id, javaType);
                    };
            }
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, javaType);
            };
        }
    }

    private static ValueToDateMillis valueToDateMillis(Type.Kind kind, @Nullable PrimitiveType id) {
        Class<?> javaType = long.class;
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                    return value -> {
                        throw dataTypeNotSupported(id, javaType);
                    };
            }
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, javaType);
            };
        }
    }

    private static ValueToNString valueToNString(Type.Kind kind, @Nullable PrimitiveType id) {
        Class<?> javaType = String.class;
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                    return value -> {
                        throw dataTypeNotSupported(id, javaType);
                    };
            }
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, javaType);
            };
        }
    }

    private static ValueToURL valueToURL(Type.Kind kind, @Nullable PrimitiveType id) {
        Class<?> javaType = URL.class;
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
            switch (id) {
                case Bytes:
                    return value -> new String(value.getBytes());
                case Text:
                    return PrimitiveReader::getText;
                default:
                    return value -> {
                        throw dataTypeNotSupported(id, javaType);
                    };
            }
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, javaType);
            };
        }
    }

    private static ValueToBigDecimal valueToBigDecimal(Type.Kind kind, @Nullable PrimitiveType id) {
        Class<?> javaType = BigDecimal.class;
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                    return value -> {
                        throw dataTypeNotSupported(id, javaType);
                    };
            }
        } else if (kind == Type.Kind.DECIMAL) {
            return value -> value.getDecimal().toBigDecimal();
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, javaType);
            };
        }
    }

    private static ValueToReader valueToReader(Type.Kind kind, @Nullable PrimitiveType id) {
        Class<?> javaType = Reader.class;
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                    return value -> {
                        throw dataTypeNotSupported(id, javaType);
                    };
            }
        } else {
            return value -> {
                throw dataTypeNotSupported(kind, javaType);
            };
        }
    }


    static SqlType buildDataType(Type type) {
        Type.Kind kind = type.getKind();
        // All types must be the same as for #valueToObject
        int sqlType = YdbTypesImpl.getInstance().toSqlType(type);

        if (kind == Type.Kind.PRIMITIVE) {
            PrimitiveType id = (PrimitiveType) type;
            final Class<?> javaType;
            switch (id) {
                case Text:
                case Json:
                case JsonDocument:
                case Uuid:
                    javaType = String.class;
                    break;
                case Bytes:
                case Yson:
                    javaType = byte[].class;
                    break;
                case Bool:
                    javaType = Boolean.class;
                    break;
                case Int8:
                    javaType = Byte.class;
                    break;
                case Uint8:
                case Int32:
                case Uint16:
                    javaType = Integer.class;
                    break;
                case Int16:
                    javaType = Short.class;
                    break;
                case Uint32:
                case Int64:
                case Uint64:
                    javaType = Long.class;
                    break;
                case Float:
                    javaType = Float.class;
                    break;
                case Double:
                    javaType = Double.class;
                    break;
                case Date:
                    javaType = LocalDate.class;
                    break;
                case Datetime:
                    javaType = LocalDateTime.class;
                    break;
                case Timestamp:
                    javaType = Instant.class;
                    break;
                case Interval:
                    javaType = Duration.class;
                    break;
                case TzDate:
                case TzDatetime:
                case TzTimestamp:
                    javaType = ZonedDateTime.class;
                    break;
                default:
                    javaType = Value.class;
            }
            return new SqlType(sqlType, javaType);
        } else if (kind == Type.Kind.DECIMAL) {
            return new SqlType(sqlType, DecimalValue.class);
        } else {
            return new SqlType(sqlType, Value.class);
        }
    }

    private static ValueToObject valueToObject(Type.Kind kind, @Nullable PrimitiveType id) {
        Class<?> javaType = Object.class;
        if (kind == Type.Kind.PRIMITIVE) {
            Preconditions.checkState(id != null, "Primitive type must not be null when kind is %s", kind);
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
                        throw dataTypeNotSupported(id, javaType);
                    };
            }
        } else if (kind == Type.Kind.DECIMAL) {
            return PrimitiveReader::getDecimal;
        } else {
            return ValueReader::getValue;
        }
    }


    private static SQLException cannotConvert(PrimitiveType type, Class<?> javaType, Object value) {
        return new SQLException(String.format(UNABLE_TO_CONVERT, type, value, javaType));
    }

    private static SQLException dataTypeNotSupported(PrimitiveType type, Class<?> javaType) {
        return new SQLException(String.format(UNABLE_TO_CAST, type, javaType));
    }

    private static SQLException dataTypeNotSupported(Type.Kind kind, Class<?> javaType) {
        return new SQLException(String.format(UNABLE_TO_CAST, kind, javaType));
    }

    public static class Getters {
        public final ValueToString toString;
        public final ValueToBoolean toBoolean;
        public final ValueToByte toByte;
        public final ValueToShort toShort;
        public final ValueToInt toInt;
        public final ValueToLong toLong;
        public final ValueToFloat toFloat;
        public final ValueToDouble toDouble;
        public final ValueToBytes toBytes;
        public final ValueToObject toObject;
        public final ValueToDateMillis toDateMillis;
        public final ValueToNString toNString;
        public final ValueToURL toURL;
        public final ValueToBigDecimal toBigDecimal;
        public final ValueToReader toReader;

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
    }

    public interface ValueToString {
        String fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToBoolean {
        boolean fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToByte {
        byte fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToShort {
        short fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToInt {
        int fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToLong {
        long fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToFloat {
        float fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToDouble {
        double fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToBytes {
        byte[] fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToObject {
        Object fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToDateMillis {
        long fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToNString {
        String fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToURL {
        String fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToBigDecimal {
        BigDecimal fromValue(ValueReader reader) throws SQLException;
    }

    public interface ValueToReader {
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
