package tech.ydb.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;


public interface YdbParameterMetaData extends ParameterMetaData {
    /**
     * Returns parameter name by it's index
     *
     * @param param parameter (1..N)
     * @return parameter name
     * @throws SQLException if parameter index is invalid
     */
    String getParameterName(int param) throws SQLException;
}
