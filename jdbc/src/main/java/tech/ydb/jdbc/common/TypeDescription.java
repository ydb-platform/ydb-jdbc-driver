package tech.ydb.jdbc.common;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

public class TypeDescription {
    private static final Map<Type, TypeDescription> TYPES = new ConcurrentHashMap<>();

    static {
        ofInternal(DecimalType.of(DecimalType.MAX_PRECISION)); // max
        ofInternal(DecimalType.of(22, 9)); // default for database
        for (PrimitiveType type : PrimitiveType.values()) {
            ofInternal(type); // All primitive values
        }
    }

    private final Type type;
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

    private static void ofInternal(Type type) {
        of(type);
        of(type.makeOptional()); // Register both normal and optional types
    }

    private static TypeDescription buildType(Type origType) {
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

        MappingGetters.Getters getters = MappingGetters.buildGetters(type);
        MappingSetters.Setters setters = MappingSetters.buildSetters(type);
        MappingGetters.SqlType sqlTypes = MappingGetters.buildDataType(type);

        return new TypeDescription(type, optionalValue, getters, setters, sqlTypes);
    }

    public static TypeDescription of(Type type) {
        // TODO: check for cache poisoning?
        return TYPES.computeIfAbsent(type, TypeDescription::buildType);
    }
}
