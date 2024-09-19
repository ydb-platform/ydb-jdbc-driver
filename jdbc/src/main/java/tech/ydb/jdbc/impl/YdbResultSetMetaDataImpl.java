package tech.ydb.jdbc.impl;

import java.sql.SQLException;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSetMetaData;
import tech.ydb.jdbc.common.ColumnInfo;
import tech.ydb.table.values.Type;

public class YdbResultSetMetaDataImpl implements YdbResultSetMetaData {
    private final BaseYdbResultSet rs;

    public YdbResultSetMetaDataImpl(BaseYdbResultSet rs) {
        this.rs = rs;
    }

    @Override
    public int getColumnCount() {
        return rs.getColumnsLength();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return true;
    }

    @Override
    public boolean isSearchable(int column) {
        return false;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        ColumnInfo info = rs.getColumnInfo(column);
        return info.isOptional() || info.isNull() ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) {
        return false; // TODO: support?
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return 0; // TODO: support?
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return rs.getColumnInfo(column).getName();
    }

    @Override
    public String getSchemaName(int column) {
        return ""; // not applicable
    }

    @Override
    public int getPrecision(int column) {
        return 0; // TODO: support?
    }

    @Override
    public int getScale(int column) {
        return 0; // TODO: support?
    }

    @Override
    public String getTableName(int column) {
        return ""; // unknown
    }

    @Override
    public String getCatalogName(int column) {
        return ""; // unknown
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return rs.getColumnInfo(column).getSqlType().getSqlType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return rs.getColumnInfo(column).getYdbType().toString();
    }

    @Override
    public boolean isReadOnly(int column) {
        return true;
    }

    @Override
    public boolean isWritable(int column) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return rs.getColumnInfo(column).getSqlType().getJavaType().getName();
    }

    @Override
    public Type getYdbType(int column) throws SQLException {
        return rs.getColumnInfo(column).getYdbType();
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
