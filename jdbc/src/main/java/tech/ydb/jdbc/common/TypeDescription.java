package tech.ydb.jdbc.common;

import java.util.Objects;

import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

public class TypeDescription {
    private final Type type;

    private final boolean isTimestamp;
    private final boolean isNumber;
    private final boolean isNull;

    private final boolean optional;
    private final OptionalValue optionalValue;

    private final MappingGetters.Getters getters;
    private final MappingSetters.Setters setters;

    private final MappingGetters.SqlType sqlType;

    private TypeDescription(Type type,
                            OptionalValue optionalValue,
                            MappingGetters.Getters getters,
                            MappingSetters.Setters setters,
                            MappingGetters.SqlType sqlType) {
        this.type = Objects.requireNonNull(type);
        this.optional = optionalValue != null;
        this.optionalValue = optionalValue;
        this.getters = Objects.requireNonNull(getters);
        this.setters = Objects.requireNonNull(setters);
        this.sqlType = Objects.requireNonNull(sqlType);

        this.isTimestamp = type == PrimitiveType.Timestamp;
        this.isNumber = type == PrimitiveType.Int8 || type == PrimitiveType.Uint8
                || type == PrimitiveType.Int16 || type == PrimitiveType.Uint16
                || type == PrimitiveType.Int32 || type == PrimitiveType.Uint32
                || type == PrimitiveType.Int64 || type == PrimitiveType.Uint64;
        this.isNull = type.getKind() == Type.Kind.NULL || type.getKind() == Type.Kind.VOID;
    }

    public boolean isNull() {
        return isNull;
    }

    public boolean isTimestamp() {
        return isTimestamp;
    }

    public boolean isNumber() {
        return isNumber;
    }

    public boolean isOptional() {
        return optional;
    }

    public OptionalValue nullValue() {
        return optionalValue;
    }

    public MappingGetters.SqlType sqlType() {
        return sqlType;
    }

    public MappingGetters.Getters getters() {
        return getters;
    }

    public MappingSetters.Setters setters() {
        return setters;
    }

    public Type ydbType() {
        return type;
    }

    static TypeDescription buildType(YdbTypes types, Type origType) {
        Type type;
        OptionalValue optionalValue;
        if (origType.getKind() == Type.Kind.OPTIONAL) {
            OptionalType optionalType = (OptionalType) origType;
            type = optionalType.getItemType();
            optionalValue = optionalType.emptyValue();
        } else {
            type = origType;
            optionalValue = null;
        }

        // All types must be the same as for #valueToObject
        int sqlType = types.toSqlType(type);

        MappingGetters.Getters getters = MappingGetters.buildGetters(type);
        MappingSetters.Setters setters = MappingSetters.buildSetters(type);
        MappingGetters.SqlType sqlTypes = MappingGetters.buildDataType(sqlType, type);

        return new TypeDescription(type, optionalValue, getters, setters, sqlTypes);
    }
}
