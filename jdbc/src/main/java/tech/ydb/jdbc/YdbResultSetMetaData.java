package tech.ydb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import tech.ydb.table.values.Type;

public interface YdbResultSetMetaData extends ResultSetMetaData {

    /**
     * Returns native YDB type for column
     *
     * @param column column, 1..N
     * @return YDB type
     * @throws java.sql.SQLException
     */
    Type getYdbType(int column) throws SQLException;
}
