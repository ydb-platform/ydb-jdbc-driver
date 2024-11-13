package tech.ydb.jdbc.query.params;

import java.sql.SQLException;
import java.util.function.Supplier;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class UInt64JdbcPrm implements JdbcPrm {
    private static final TypeDescription DESC = TypeDescription.of(PrimitiveType.Uint64);
    private final String name;
    private Value<?> value;

    private UInt64JdbcPrm(String name) {
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
        return DESC;
    }

    @Override
    public void setValue(Object obj, int sqlType) throws SQLException {
        value = DESC.setters().toValue(obj);
    }

    public static Supplier<JdbcPrm> withName(String name) {
        return () -> new UInt64JdbcPrm(name);
    }

}
