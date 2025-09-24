package tech.ydb.jdbc.impl;

import java.sql.SQLException;

import tech.ydb.jdbc.YdbQueryResult;
import tech.ydb.jdbc.YdbResultSet;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryResultEmpty implements YdbQueryResult {
    @Override
    public int getUpdateCount() throws SQLException {
        return -1;
    }

    @Override
    public YdbResultSet getCurrentResultSet() throws SQLException {
        return null;
    }

    @Override
    public YdbResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public boolean hasResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public void close() throws SQLException {
        // nothing
    }
}
