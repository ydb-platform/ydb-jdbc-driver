package tech.ydb.jdbc.query.params;

import java.sql.SQLException;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
class UInt64JdbcPrm implements JdbcPrm {
    private final TypeDescription desc;
    private final String name;
    private Value<?> value;

    UInt64JdbcPrm(YdbTypes types, String name) {
        this.desc = types.find(PrimitiveType.Uint64);
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
        return desc;
    }

    @Override
    public void setValue(Object obj, int sqlType) throws SQLException {
        value = desc.toYdbValue(obj);
    }
}
