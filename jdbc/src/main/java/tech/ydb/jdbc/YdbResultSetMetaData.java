package tech.ydb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import tech.ydb.table.values.Type;

public interface YdbResultSetMetaData extends ResultSetMetaData {

    /**
     * Returns native YDB type for column
     *
     * @param column column index, 1..N
     * @return YDB type
     * @throws java.sql.SQLException if result set doesn't have this index
     */
    Type getYdbType(int column) throws SQLException;
}
