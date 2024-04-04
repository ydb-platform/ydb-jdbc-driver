package tech.ydb.jdbc.impl;



import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbParameterMetaData;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.query.JdbcParams;

public class YdbParameterMetaDataImpl implements YdbParameterMetaData {
    private final JdbcParams params;

    public YdbParameterMetaDataImpl(JdbcParams params) {
        this.params = params;
    }

    @Override
    public int getParameterCount() {
        return params.parametersCount();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        TypeDescription description = params.getDescription(param);
        if (description == null) {
            return ParameterMetaData.parameterNullableUnknown;
        }
        return description.isOptional() ? ParameterMetaData.parameterNullable : ParameterMetaData.parameterNoNulls;
    }

    @Override
    public boolean isSigned(int param) {
        return false; // TODO: support?
    }

    @Override
    public int getPrecision(int param) {
        return 0; // TODO: support?
    }

    @Override
    public int getScale(int param) {
        return 0; // TODO: support?
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        TypeDescription description = params.getDescription(param);
        if (description == null) {
            return Types.OTHER;
        }
        return description.sqlType().getSqlType();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        TypeDescription description = params.getDescription(param);
        if (description == null) {
            return null;
        }
        return description.ydbType().toString();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        TypeDescription description = params.getDescription(param);
        if (description == null) {
            return null;
        }
        return description.sqlType().getJavaType().getName();
    }

    @Override
    public int getParameterMode(int param) {
        return parameterModeIn; // Only in is supported
    }

    @Override
    public String getParameterName(int param) throws SQLException {
        return params.getNameByIndex(param);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException(YdbConst.CANNOT_UNWRAP_TO + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }
}
