package tech.ydb.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.common.ColumnInfo;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;

public class YdbStaticResultSet extends BaseYdbResultSet {
    private final ResultSetReader rsReader;
    private final int rowCount;

    private int fetchDirection = ResultSet.FETCH_UNKNOWN;
    private int rowIndex = 0;
    private boolean isClosed = false;

    public YdbStaticResultSet(YdbStatement statement, ResultSetReader result) {
        super(statement, readColumns(Objects.requireNonNull(result)));
        this.rsReader = result;
        this.rowCount = result.getRowCount();
    }

    @Override
    protected ValueReader getValue(int columnIndex) throws SQLException {
        try {
            return rsReader.getColumn(columnIndex);
        } catch (IllegalStateException ex) {
            throw new SQLException(YdbConst.INVALID_ROW + rowIndex);
        }
    }

    private static ColumnInfo[] readColumns(ResultSetReader rsr) {
        ColumnInfo[] columns = new ColumnInfo[rsr.getColumnCount()];
        for (int idx = 0; idx < rsr.getColumnCount(); idx += 1) {
            columns[idx] = new ColumnInfo(rsr.getColumnName(idx), rsr.getColumnType(idx));
        }
        return columns;
    }

    @Override
    public boolean next() {
        setRowIndex(rowIndex + 1);
        return isRowIndexValid();
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isBeforeFirst() {
        return rowCount != 0 && rowIndex <= 0;
    }

    @Override
    public boolean isAfterLast() {
        return rowCount != 0 && rowIndex > rowCount;
    }

    @Override
    public boolean isFirst() {
        return rowCount != 0 && rowIndex == 1;
    }

    @Override
    public boolean isLast() {
        return rowCount != 0 && rowIndex == rowCount;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkScroll();
        setRowIndex(0);
    }

    @Override
    public void afterLast() throws SQLException {
        checkScroll();
        setRowIndex(rowCount + 1);
    }

    @Override
    public boolean first() throws SQLException {
        checkScroll();
        setRowIndex(1);
        return isRowIndexValid();
    }

    @Override
    public boolean last() throws SQLException {
        checkScroll();
        setRowIndex(rowCount);
        return isRowIndexValid();
    }

    @Override
    public int getRow() {
        return rowIndex;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkScroll();
        if (row >= 0) {
            setRowIndex(row);
        } else {
            setRowIndex(rowCount + 1 + row);
        }
        return isRowIndexValid();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkScroll();
        if (rows != 0) {
            setRowIndex(rowIndex + rows);
        }
        return isRowIndexValid();
    }

    @Override
    public boolean previous() throws SQLException {
        checkScroll();
        setRowIndex(rowIndex - 1);
        return isRowIndexValid();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.fetchDirection = direction;
    }

    @Override
    public int getFetchDirection() {
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) {
        // do nothing
    }

    @Override
    public int getFetchSize() throws SQLException {
        return statement.getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return statement.getResultSetType();
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private void checkScroll() throws SQLException {
        if (getType() == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException(YdbConst.FORWARD_ONLY_MODE);
        }
    }

    private void setRowIndex(int rowIndex) {
        if (rowCount > 0) {
            int actualIndex = Math.max(Math.min(rowIndex, rowCount + 1), 0);
            this.rowIndex = actualIndex;
            rsReader.setRowIndex(actualIndex - 1);
        }
    }

    private boolean isRowIndexValid() {
        return rowIndex > 0 && rowIndex <= rowCount;
    }
}
