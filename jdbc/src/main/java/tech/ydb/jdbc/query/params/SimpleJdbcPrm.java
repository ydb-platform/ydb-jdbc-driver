package tech.ydb.jdbc.query.params;

import java.sql.SQLException;
import java.util.function.Supplier;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.impl.YdbTypes;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class SimpleJdbcPrm implements JdbcPrm {
    private final String name;
    private Value<?> value;

    private SimpleJdbcPrm(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void reset() {
        value = null;
    }

    @Override
    public void copyToParams(Params params) throws SQLException {
        if (value == null) {
            throw new SQLException(YdbConst.MISSING_VALUE_FOR_PARAMETER + name);
        }
        params.put(name, value);
    }

    @Override
    public TypeDescription getType() {
        if (value == null) {
            return null;
        }
        return TypeDescription.of(value.getType());
    }

    @Override
    public void setValue(Object obj, int sqlType) throws SQLException {
        if (obj instanceof Value<?>) {
            value = (Value<?>) obj;
            return;
        }

        Type type = YdbTypes.findType(obj, sqlType);
        if (type == null) {
            throw new SQLException(String.format(YdbConst.PARAMETER_TYPE_UNKNOWN, sqlType, obj));
        }

        if (obj == null) {
            value = type.makeOptional().emptyValue();
            return;
        }

        value = TypeDescription.of(type).setters().toValue(obj);
    }

    public static Supplier<JdbcPrm> withName(String name) {
        return () -> new SimpleJdbcPrm(name);
    }
}
