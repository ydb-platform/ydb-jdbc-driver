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

import io.grpc.netty.shaded.io.netty.util.collection.IntObjectHashMap;
import io.grpc.netty.shaded.io.netty.util.collection.IntObjectMap;

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

    private final IntObjectMap<Type> typeBySqlType;
    private final Map<Class<?>, Type> typeByClass;

    private final Map<Type, Integer> sqlTypeByPrimitiveNumId;

    private YdbTypes() {
        typeBySqlType = new IntObjectHashMap<>(18 + PrimitiveType.values().length);

        // Store custom type ids to use it for PreparedStatement.setObject
        for (PrimitiveType type : PrimitiveType.values()) {
            typeBySqlType.put(YdbConst.SQL_KIND_PRIMITIVE + type.ordinal(), type);
        }

        typeBySqlType.put(YdbJdbcCode.DECIMAL_22_9, DecimalType.of(22, 9));
        typeBySqlType.put(YdbJdbcCode.DECIMAL_31_9, DecimalType.of(31, 9));
        typeBySqlType.put(YdbJdbcCode.DECIMAL_35_9, DecimalType.of(35, 9));

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

        sqlTypeByPrimitiveNumId = new HashMap<>(PrimitiveType.values().length);
        for (PrimitiveType id : PrimitiveType.values()) {
            final int sqlType;
            switch (id) {
                case Text:
                case Json:
                case JsonDocument:
                case Uuid:
                    sqlType = Types.VARCHAR;
                    break;
                case Bytes:
                case Yson:
                    sqlType = Types.BINARY;
                    break;
                case Bool:
                    sqlType = Types.BOOLEAN;
                    break;
                case Int8:
                case Int16:
                    sqlType = Types.SMALLINT;
                    break;
                case Uint8:
                case Int32:
                case Uint16:
                    sqlType = Types.INTEGER;
                    break;
                case Uint32:
                case Int64:
                case Uint64:
                case Interval:
                    sqlType = Types.BIGINT;
                    break;
                case Float:
                    sqlType = Types.FLOAT;
                    break;
                case Double:
                    sqlType = Types.DOUBLE;
                    break;
                case Date:
                    sqlType = Types.DATE;
                    break;
                case Datetime:
                    sqlType = Types.TIMESTAMP;
                    break;
                case Timestamp:
                    sqlType = Types.TIMESTAMP;
                    break;
                case TzDate:
                case TzDatetime:
                case TzTimestamp:
                    sqlType = Types.TIMESTAMP_WITH_TIMEZONE;
                    break;
                default:
                    sqlType = Types.JAVA_OBJECT;
            }
            sqlTypeByPrimitiveNumId.put(id, sqlType);
        }
    }

    public static Type findType(Object obj, int sqlType) {
        return INSTANCE.findTypeImpl(obj, sqlType);
    }

    private Type findTypeImpl(Object obj, int sqlType) {
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
                if (!sqlTypeByPrimitiveNumId.containsKey(type)) {
                    throw new RuntimeException("Internal error. Unsupported YDB type: " + type);
                }
                return sqlTypeByPrimitiveNumId.get(type);
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
                return 8 + 8;
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
                DecimalType.getDefault());
    }

    //

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
                return "0000-00-00".length();
            case Datetime:
                return "0000-00-00 00:00:00".length();
            case Timestamp:
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
