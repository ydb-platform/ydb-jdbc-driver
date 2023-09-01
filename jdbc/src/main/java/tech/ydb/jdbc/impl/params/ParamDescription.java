package tech.ydb.jdbc.impl.params;

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
public class ParamDescription {
    private final int index;
    private final String name;
    private final String displayName;
    private final TypeDescription type;

    public ParamDescription(int index, String name, String displayName, TypeDescription type) {
        this.index = index;
        this.name = name;
        this.displayName = displayName;
        this.type = type;
    }

    public ParamDescription(int index, String name, TypeDescription type) {
        this(index, name, name, type);
    }

    public int index() {
        return index;
    }

    public String name() {
        return name;
    }

    public String displayName() {
        return displayName;
    }

    public TypeDescription type() {
        return type;
    }

    protected Value<?> getValue(Object value) throws SQLException {
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
                    checkType(ydbValue.asOptional().getType().getItemType());
                    return ydbValue; // Could be null
                } else {
                    checkType(ydbValue.getType());
                    return ydbValue.makeOptional();
                }
            } else {
                if (ydbValue instanceof OptionalValue) {
                    OptionalValue optional = ydbValue.asOptional();
                    if (!optional.isPresent()) {
                        throw new SQLException(YdbConst.MISSING_REQUIRED_VALUE + displayName);
                    }
                    checkType(optional.getType().getItemType());
                    return optional.get();
                } else {
                    checkType(ydbValue.getType());
                    return ydbValue;
                }
            }
        } else {
            Value<?> targetValue = type.setters().toValue(value);
            if (type.isOptional()) {
                return targetValue.makeOptional();
            } else {
                return targetValue;
            }
        }
    }

    private void checkType(Type objectType) throws SQLException {
        if (!type.ydbType().equals(objectType)) {
            String msg = String.format(YdbConst.INVALID_PARAMETER_TYPE, displayName, objectType, type.ydbType());
            throw new SQLException(msg);
        }
    }
}
