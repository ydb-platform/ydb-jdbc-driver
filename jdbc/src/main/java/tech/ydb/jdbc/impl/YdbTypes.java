package tech.ydb.jdbc.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbJdbcCode;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.VoidType;

public class YdbTypes {
    private static final YdbTypes INSTANCE = new YdbTypes();

    private final Map<Integer, Type> typeBySqlType;
    private final Map<Class<?>, Type> typeByClass;

    private YdbTypes() {
        typeBySqlType = new HashMap<>();

        // Store custom type ids to use it for PrepaparedStatement.setObject
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 0, PrimitiveType.Bool);

        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 1, PrimitiveType.Int8);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 2, PrimitiveType.Uint8);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 3, PrimitiveType.Int16);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 4, PrimitiveType.Uint16);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 5, PrimitiveType.Int32);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 6, PrimitiveType.Uint32);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 7, PrimitiveType.Int64);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 8, PrimitiveType.Uint64);

        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 9, PrimitiveType.Float);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 10, PrimitiveType.Double);

        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 11, PrimitiveType.Bytes);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 12, PrimitiveType.Text);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 13, PrimitiveType.Yson);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 14, PrimitiveType.Json);

        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 15, PrimitiveType.Uuid);

        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 16, PrimitiveType.Date);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 17, PrimitiveType.Datetime);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 18, PrimitiveType.Timestamp);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 19, PrimitiveType.Interval);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 20, PrimitiveType.TzDate);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 21, PrimitiveType.TzDatetime);
        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 22, PrimitiveType.TzTimestamp);

        typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + 23, PrimitiveType.JsonDocument);

        typeBySqlType.put(YdbJdbcCode.DATE_32, PrimitiveType.Date32);
        typeBySqlType.put(YdbJdbcCode.DATETIME_64, PrimitiveType.Datetime64);
        typeBySqlType.put(YdbJdbcCode.TIMESTAMP_64, PrimitiveType.Timestamp64);
        typeBySqlType.put(YdbJdbcCode.INTERVAL_64, PrimitiveType.Interval64);

        typeBySqlType.put(Types.VARCHAR, PrimitiveType.Text);
        typeBySqlType.put(Types.BIGINT, PrimitiveType.Int64);
        typeBySqlType.put(Types.TINYINT, PrimitiveType.Int8);
        typeBySqlType.put(Types.SMALLINT, PrimitiveType.Int16);

        typeBySqlType.put(Types.INTEGER, PrimitiveType.Int32);
        typeBySqlType.put(Types.REAL, PrimitiveType.Float);
        typeBySqlType.put(Types.FLOAT, PrimitiveType.Float);
        typeBySqlType.put(Types.DOUBLE, PrimitiveType.Double);

        typeBySqlType.put(Types.BIT, PrimitiveType.Bool);
        typeBySqlType.put(Types.BOOLEAN, PrimitiveType.Bool);
        typeBySqlType.put(Types.BINARY, PrimitiveType.Bytes);
        typeBySqlType.put(Types.VARBINARY, PrimitiveType.Bytes);

        typeBySqlType.put(Types.DATE, PrimitiveType.Date);
        typeBySqlType.put(Types.TIMESTAMP, PrimitiveType.Timestamp);
        typeBySqlType.put(Types.TIME, PrimitiveType.Int32); // YDB doesn't support TIME

        typeBySqlType.put(Types.TIMESTAMP_WITH_TIMEZONE, PrimitiveType.TzTimestamp);
        typeBySqlType.put(Types.DECIMAL, DecimalType.getDefault());
        typeBySqlType.put(Types.NUMERIC, DecimalType.getDefault());

        typeByClass = new HashMap<>();

        typeByClass.put(boolean.class, PrimitiveType.Bool);
        typeByClass.put(Boolean.class, PrimitiveType.Bool);

        typeByClass.put(byte.class, PrimitiveType.Int8);
        typeByClass.put(Byte.class, PrimitiveType.Int8);
        typeByClass.put(short.class, PrimitiveType.Int16);
        typeByClass.put(Short.class, PrimitiveType.Int16);
        typeByClass.put(int.class, PrimitiveType.Int32);
        typeByClass.put(Integer.class, PrimitiveType.Int32);

        typeByClass.put(long.class, PrimitiveType.Int64);
        typeByClass.put(Long.class, PrimitiveType.Int64);

        typeByClass.put(BigInteger.class, PrimitiveType.Int64);

        typeByClass.put(float.class, PrimitiveType.Float);
        typeByClass.put(Float.class, PrimitiveType.Float);
        typeByClass.put(double.class, PrimitiveType.Double);
        typeByClass.put(Double.class, PrimitiveType.Double);

        typeByClass.put(String.class, PrimitiveType.Text);
        typeByClass.put(byte[].class, PrimitiveType.Bytes);

        typeByClass.put(UUID.class, PrimitiveType.Uuid);
        typeByClass.put(java.sql.Date.class, PrimitiveType.Date);
        typeByClass.put(LocalDate.class, PrimitiveType.Date);
        typeByClass.put(LocalDateTime.class, PrimitiveType.Datetime);

        typeByClass.put(java.util.Date.class, PrimitiveType.Timestamp);
        typeByClass.put(Timestamp.class, PrimitiveType.Timestamp);
        typeByClass.put(Instant.class, PrimitiveType.Timestamp);

        typeByClass.put(LocalTime.class, PrimitiveType.Int32);
        typeByClass.put(Time.class, PrimitiveType.Int32);

        typeByClass.put(DecimalValue.class, DecimalType.getDefault());
        typeByClass.put(BigDecimal.class, DecimalType.getDefault());
        typeByClass.put(Duration.class, PrimitiveType.Interval);
    }

    public static Type findType(Object obj, int sqlType) {
        return INSTANCE.findTypeImpl(obj, sqlType);
    }

    private Type findTypeImpl(Object obj, int sqlType) {
        if ((sqlType & YdbConst.SQL_KIND_DECIMAL) != 0) {
            int precision = ((sqlType - YdbConst.SQL_KIND_DECIMAL) >> 6);
            int scale = ((sqlType - YdbConst.SQL_KIND_DECIMAL) & 0b111111);
            if (precision > 0 && precision < 36 && scale >= 0 && scale <= precision) {
                return DecimalType.of(precision, scale);
            }
        }

        if (typeBySqlType.containsKey(sqlType)) {
            return typeBySqlType.get(sqlType);
        }

        if (obj == null) {
            return VoidType.of();
        }
        if (obj instanceof Value<?>) {
            return ((Value<?>) obj).getType();
        }

        return typeByClass.get(obj.getClass());
    }

    /**
     * Converts given YDB type to standard SQL type
     *
     * @param type YDB type to convert
     * @return sqlType
     */
    public static int toSqlType(Type type) {
        return INSTANCE.toSqlTypeImpl(type);
    }

    private int toSqlTypeImpl(Type type) {
        switch (type.getKind()) {
            case PRIMITIVE:
                switch ((PrimitiveType) type) {
                    case Text:
                    case Json:
                    case JsonDocument:
                    case Uuid:
                        return Types.VARCHAR;
                    case Bytes:
                    case Yson:
                        return Types.BINARY;
                    case Bool:
                        return Types.BOOLEAN;
                    case Int8:
                    case Int16:
                        return Types.SMALLINT;
                    case Uint8:
                    case Int32:
                    case Uint16:
                        return Types.INTEGER;
                    case Uint32:
                    case Int64:
                    case Uint64:
                    case Interval:
                    case Interval64:
                        return Types.BIGINT;
                    case Float:
                        return Types.FLOAT;
                    case Double:
                        return Types.DOUBLE;
                    case Date:
                    case Date32:
                        return Types.DATE;
                    case Datetime:
                    case Timestamp:
                    case Datetime64:
                    case Timestamp64:
                        return Types.TIMESTAMP;
                    case TzDate:
                    case TzDatetime:
                    case TzTimestamp:
                        return Types.TIMESTAMP_WITH_TIMEZONE;
                    default:
                        return Types.JAVA_OBJECT;
                }
            case OPTIONAL:
                return toSqlTypeImpl(type.unwrapOptional());
            case DECIMAL:
                return Types.DECIMAL;
            case STRUCT:
                return Types.STRUCT;
            case LIST:
                return Types.ARRAY;
            case NULL:
            case VOID:
                return Types.NULL;
            case PG_TYPE:
            case TUPLE:
            case DICT:
            case VARIANT:
                return Types.OTHER;
            default:
                throw new RuntimeException("Internal error. Unsupported YDB kind: " + type);
        }
    }

    /**
     * Returns sql precision for given YDB type (or 0 if not applicable)
     *
     * @param type YDB type
     * @return precision
     */
    public static int getSqlPrecision(Type type) {
        return INSTANCE.getSqlPrecisionImpl(type);
    }

    private int getSqlPrecisionImpl(Type type) {
        // The <...> column specifies the column size for the given column.
        // For numeric data, this is the maximum precision.
        // For character data, this is the length in characters.
        // For datetime datatypes, this is the length in characters of the String representation
        // (assuming the maximum allowed precision of the fractional seconds component).
        // For binary data, this is the length in bytes.
        // For the ROWID datatype, this is the length in bytes.
        // Null is returned for data types where the column size is not applicable.

        switch (type.getKind()) {
            case OPTIONAL:
                return getSqlPrecisionImpl(type.unwrapOptional());
            case DECIMAL:
                return ((DecimalType) type).getPrecision();
            case PRIMITIVE:
                return getSqlPrecisionImpl((PrimitiveType) type);
            default:
                return 0; // unsupported?
        }
    }

    /**
     * Returns all types supported by database
     *
     * @return list of YDB types that supported by database (could be stored in columns)
     */
    public static List<Type> getAllDatabaseTypes() {
        return INSTANCE.getAllDatabaseTypesImpl();
    }

    private List<Type> getAllDatabaseTypesImpl() {
        return Arrays.asList(
                PrimitiveType.Bool,
                PrimitiveType.Int8,
                PrimitiveType.Int16,
                PrimitiveType.Int32,
                PrimitiveType.Int64,
                PrimitiveType.Uint8,
                PrimitiveType.Uint16,
                PrimitiveType.Uint32,
                PrimitiveType.Uint64,
                PrimitiveType.Float,
                PrimitiveType.Double,
                PrimitiveType.Bytes,
                PrimitiveType.Text,
                PrimitiveType.Json,
                PrimitiveType.JsonDocument,
                PrimitiveType.Yson,
                PrimitiveType.Date,
                PrimitiveType.Datetime,
                PrimitiveType.Timestamp,
                PrimitiveType.Interval,
                DecimalType.getDefault(),
                PrimitiveType.Date32,
                PrimitiveType.Datetime64,
                PrimitiveType.Timestamp64,
                PrimitiveType.Interval64);
    }

    private int getSqlPrecisionImpl(PrimitiveType type) {
        switch (type) {
            case Bool:
            case Int8:
            case Uint8:
                return 1;
            case Int16:
            case Uint16:
                return 2;
            case Int32:
            case Uint32:
            case Float:
                return 4;
            case Int64:
            case Uint64:
            case Double:
            case Interval:
            case Interval64:
                return 8;
            case Bytes:
            case Text:
            case Yson:
            case Json:
            case JsonDocument:
                return YdbConst.MAX_COLUMN_SIZE;
            case Uuid:
                return 8 + 8;
            case Date:
            case Date32:
                return "0000-00-00".length();
            case Datetime:
            case Datetime64:
                return "0000-00-00 00:00:00".length();
            case Timestamp:
            case Timestamp64:
                return "0000-00-00T00:00:00.000000".length();
            case TzDate:
                return "0000-00-00+00:00".length();
            case TzDatetime:
                return "0000-00-00 00:00:00+00:00".length();
            case TzTimestamp:
                return "0000-00-00T00:00:00.000000+00:00".length();
            default:
                return 0;
        }
    }
}
