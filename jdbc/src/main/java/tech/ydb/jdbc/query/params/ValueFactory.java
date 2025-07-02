package tech.ydb.jdbc.query.params;

import java.sql.SQLException;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ValueFactory {
    private ValueFactory() { }

    public static Value<?> readValue(String name, Object value, TypeDescription type) throws SQLException {
        if (value == null) {
            if (type.nullValue() != null) {
                return type.nullValue();
            } else {
                return type.ydbType().makeOptional().emptyValue();
            }
        }

        if (value instanceof Value<?>) {
            // For all external values (passed 'as is') we have to check data types
            Value<?> ydbValue = (Value<?>) value;
            if (type.isOptional()) {
                if (ydbValue instanceof OptionalValue) {
                    checkType(name, type.ydbType(), ydbValue.asOptional().getType().getItemType());
                    return ydbValue; // Could be null
                } else {
                    checkType(name, type.ydbType(), ydbValue.getType());
                    return ydbValue.makeOptional();
                }
            } else {
                if (ydbValue instanceof OptionalValue) {
                    OptionalValue optional = ydbValue.asOptional();
                    if (!optional.isPresent()) {
                        throw new SQLException(YdbConst.MISSING_REQUIRED_VALUE + name);
                    }
                    checkType(name, type.ydbType(), optional.getType().getItemType());
                    return optional.get();
                } else {
                    checkType(name, type.ydbType(), ydbValue.getType());
                    return ydbValue;
                }
            }
        } else {
            Value<?> targetValue = type.toYdbValue(value);
            if (type.isOptional()) {
                return targetValue.makeOptional();
            } else {
                return targetValue;
            }
        }
    }

    private static void checkType(String name, Type type, Type objectType) throws SQLException {
        if (!type.equals(objectType)) {
            String msg = String.format(YdbConst.INVALID_PARAMETER_TYPE, name, objectType, type);
            throw new SQLException(msg);
        }
    }
}
