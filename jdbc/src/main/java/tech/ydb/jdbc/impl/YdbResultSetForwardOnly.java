package tech.ydb.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.common.ColumnInfo;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public abstract class YdbResultSetForwardOnly extends YdbResultSetBase {
    private ResultSetReader current = null;
    private boolean isClosed = false;

    private int currentIndex = 0;
    private int rowIndex = 0;

    public YdbResultSetForwardOnly(YdbStatement statement, ColumnInfo[] columns) {
        super(statement, columns);
    }

    protected abstract boolean hasNext() throws SQLException;
    protected abstract ResultSetReader readNext() throws SQLException;

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    protected ValueReader getValue(int columnIndex) throws SQLException {
        if (isClosed) {
            throw new SQLException(YdbConst.RESULT_SET_IS_CLOSED);
        }

        if (rowIndex <= 0 || current == null) {
            throw new SQLException(YdbConst.INVALID_ROW + rowIndex);
        }

        return current.getColumn(columnIndex);
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed) {
            return false;
        }

        if (current != null && current.next()) {
            rowIndex++;
            currentIndex++;
            return true;
        }

        while (hasNext()) {
            current = readNext();
            currentIndex = 0;

            if (current.next()) {
                rowIndex++;
                currentIndex++;
                return true;
            }
        }

        // nothing to read, reset index like Postgres
        rowIndex = 0;
        currentIndex = current != null ? current.getRowCount() + 1 : 1;
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return rowIndex;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (current == null) {
            return hasNext();
        }

        return rowIndex == 0 && currentIndex == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (current == null) {
            return false;
        }

        return !hasNext() && currentIndex > current.getRowCount();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return current != null && rowIndex == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        if (hasNext()) {
            return false;
        }

        return current != null && currentIndex > 0 && currentIndex == current.getRowCount();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException(YdbConst.FORWARD_ONLY_MODE);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }
}
