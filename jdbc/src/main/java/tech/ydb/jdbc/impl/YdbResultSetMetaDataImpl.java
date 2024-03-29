package tech.ydb.jdbc.impl;

import java.sql.SQLException;
import java.util.Objects;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSetMetaData;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.Type;

public class YdbResultSetMetaDataImpl implements YdbResultSetMetaData {
    private final ResultSetReader result;
    private final TypeDescription[] descriptions;
    private final String[] names;

    YdbResultSetMetaDataImpl(ResultSetReader result) {
        this.result = Objects.requireNonNull(result);
        this.descriptions = new TypeDescription[result.getColumnCount()];
        this.names = new String[result.getColumnCount()];

        for (int i = 0; i < result.getColumnCount(); i++) {
            descriptions[i] = TypeDescription.of(result.getColumnType(i));
            names[i] = result.getColumnName(i);
        }
    }

    @Override
    public int getColumnCount() {
        return descriptions.length;
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
        return getDescription(column).isOptional() ?
                columnNullable :
                columnNoNulls;
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
        return names[getIndex(column)];
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
        return getDescription(column).sqlType().getSqlType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getDescription(column).ydbType().toString();
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
        return getDescription(column).sqlType().getJavaType().getName();
    }

    @Override
    public Type getYdbType(int column) throws SQLException {
        return getDescription(column).ydbType();
    }

    @Override
    public int getColumnIndex(String columnName) throws SQLException {
        int index = result.getColumnIndex(columnName);
        if (index >= 0) {
            return index + 1;
        } else {
            throw new SQLException(YdbConst.COLUMN_NOT_FOUND + columnName);
        }
    }


    //

    private int getIndex(int column) throws SQLException {
        if (column <= 0 || column > descriptions.length) {
            throw new SQLException(YdbConst.COLUMN_NUMBER_NOT_FOUND + column);
        }
        return column - 1;
    }

    private TypeDescription getDescription(int column) throws SQLException {
        return descriptions[getIndex(column)];
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
