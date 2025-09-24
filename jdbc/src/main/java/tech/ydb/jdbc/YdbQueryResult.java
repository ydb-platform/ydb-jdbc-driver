package tech.ydb.jdbc;

import java.sql.SQLException;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbQueryResult {
    int getUpdateCount() throws SQLException;
    YdbResultSet getCurrentResultSet() throws SQLException;
    YdbResultSet getGeneratedKeys() throws SQLException;

    boolean hasResultSets() throws SQLException;
    boolean getMoreResults(int current) throws SQLException;

    void close() throws SQLException;
}
