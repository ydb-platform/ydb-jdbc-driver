package tech.ydb.jdbc.impl;

import java.sql.SQLException;

import tech.ydb.jdbc.YdbResultSet;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbQueryResult {
    YdbQueryResult EMPTY = new YdbQueryResult() {
        @Override
        public int getUpdateCount() throws SQLException {
            return -1;
        }

        @Override
        public YdbResultSet getCurrentResultSet() throws SQLException {
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
        public void close() throws SQLException { }
    };

    int getUpdateCount() throws SQLException;
    YdbResultSet getCurrentResultSet() throws SQLException;

    boolean hasResultSets() throws SQLException;
    boolean getMoreResults(int current) throws SQLException;

    void close() throws SQLException;
}
