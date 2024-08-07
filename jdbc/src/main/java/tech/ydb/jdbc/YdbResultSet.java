package tech.ydb.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import tech.ydb.table.values.Value;

public interface YdbResultSet extends ResultSet {
    /**
     * Returns native YDB value, extracted from optional value.
     * Please note that this method will create value object for each method call.
     *
     * @param columnIndex columnIndex column index
     * @return value if available; return empty if value is optional and no value provided
     * @throws SQLException if column cannot be read
     */
    Value<?> getNativeColumn(int columnIndex) throws SQLException;

    /**
     * Return native YDB value.
     * Sett {@link #getNativeColumn(int)}
     *
     * @param columnLabel column label
     * @return value if available
     * @throws SQLException if column cannot be read
     */
    Value<?> getNativeColumn(String columnLabel) throws SQLException;

    //

    @Override
    YdbResultSetMetaData getMetaData() throws SQLException;

    @Override
    YdbStatement getStatement() throws SQLException;
}
