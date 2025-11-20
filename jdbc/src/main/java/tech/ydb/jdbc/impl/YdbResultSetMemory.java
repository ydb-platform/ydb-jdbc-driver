package tech.ydb.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.common.ColumnInfo;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;

public class YdbResultSetMemory extends YdbResultSetBase {
    private final ResultSetReader[] rs;
    private final int totalCount;

    private int fetchDirection;
    private int globalRowIndex = 0; // before start

    private int rsIndex = 0;
    private int rowIndex = 0;

    private boolean isClosed = false;

    public YdbResultSetMemory(YdbTypes types, YdbStatement statement, ResultSetReader... rs) {
        super(statement, ColumnInfo.fromResultSetReader(types, Objects.requireNonNull(rs[0])));
        this.fetchDirection = statement.getFetchDirection();
        this.rs = rs;
        int total = 0;
        for (int idx = 0; idx < rs.length; idx += 1) {
            total += rs[idx].getRowCount();
        }
        this.totalCount = total;
    }

    public ResultSetReader[] getResultSets() {
        return rs;
    }

    @Override
    protected ValueReader getValue(int columnIndex) throws SQLException {
        if (!isRowIndexValid()) {
            throw new SQLException(YdbConst.INVALID_ROW + globalRowIndex);
        }
        return rs[rsIndex].getColumn(columnIndex);
    }

    @Override
    public boolean next() {
        while (true) {
            if (rsIndex >= rs.length) {
                rsIndex = totalCount;
                globalRowIndex = totalCount + 1;
                rowIndex = 0;
                return false;
            }

            if (rowIndex < rs[rsIndex].getRowCount()) {
                rs[rsIndex].setRowIndex(rowIndex);
                globalRowIndex++;
                rowIndex++;
                return true;
            }

            rsIndex++;
            rowIndex = 0;
        }
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isBeforeFirst() {
        return totalCount > 0 && globalRowIndex == 0;
    }

    @Override
    public boolean isAfterLast() {
        return totalCount > 0 && globalRowIndex > totalCount;
    }

    @Override
    public boolean isFirst() {
        return totalCount > 0 && globalRowIndex == 1;
    }

    @Override
    public boolean isLast() {
        return totalCount > 0 && globalRowIndex == totalCount;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkScroll();
        setRowIndex(0);
    }

    @Override
    public void afterLast() throws SQLException {
        checkScroll();
        setRowIndex(totalCount + 1);
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
        setRowIndex(totalCount);
        return isRowIndexValid();
    }

    @Override
    public int getRow() {
        return isRowIndexValid() ? globalRowIndex : 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkScroll();
        if (row >= 0) {
            setRowIndex(row);
        } else {
            setRowIndex(totalCount + 1 + row);
        }
        return isRowIndexValid();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkScroll();
        if (rows != 0) {
            setRowIndex(globalRowIndex + rows);
        }
        return isRowIndexValid();
    }

    @Override
    public boolean previous() throws SQLException {
        checkScroll();
        setRowIndex(globalRowIndex - 1);
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

    private void setRowIndex(int index) {
        if (index <= 0) { // before first
            globalRowIndex = 0;
            rsIndex = 0;
            rowIndex = 0;
            return;
        }

        if (index > totalCount) { // after last
            globalRowIndex = totalCount + 1;
            rsIndex = rs.length;
            rowIndex = 0;
            return;
        }

        globalRowIndex = index;
        rsIndex = 0;
        rowIndex = index;
        int currentSize = rs[rsIndex].getRowCount();
        while (rowIndex > currentSize) {
            rsIndex++;
            rowIndex -= currentSize;
            currentSize = rs[rsIndex].getRowCount();
        }

        rs[rsIndex].setRowIndex(rowIndex - 1);
    }

    private boolean isRowIndexValid() {
        return rsIndex >= 0 && rsIndex < rs.length && globalRowIndex > 0 && globalRowIndex <= totalCount;
    }
}
