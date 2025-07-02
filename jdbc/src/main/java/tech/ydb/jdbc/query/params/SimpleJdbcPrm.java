package tech.ydb.jdbc.query.params;

import java.sql.SQLDataException;
import java.sql.SQLException;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
class SimpleJdbcPrm implements JdbcPrm {
    private final YdbTypes types;
    private final String name;
    private Value<?> value;

    SimpleJdbcPrm(YdbTypes types, String name) {
        this.types = types;
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
            throw new SQLDataException(YdbConst.MISSING_VALUE_FOR_PARAMETER + name);
        }
        params.put(name, value);
    }

    @Override
    public TypeDescription getType() {
        if (value == null) {
            return null;
        }
        return types.find(value.getType());
    }

    @Override
    public void setValue(Object obj, int sqlType) throws SQLException {
        if (obj instanceof Value<?>) {
            value = (Value<?>) obj;
            return;
        }

        Type type = types.findType(obj, sqlType);
        if (type == null) {
            throw new SQLDataException(String.format(YdbConst.PARAMETER_TYPE_UNKNOWN, sqlType, obj));
        }

        if (obj == null) {
            value = type.makeOptional().emptyValue();
            return;
        }

        value = types.find(type).toYdbValue(obj);
    }
}
