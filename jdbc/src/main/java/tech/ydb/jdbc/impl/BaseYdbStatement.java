package tech.ydb.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;
import java.util.Objects;
import java.util.logging.Logger;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.context.QueryStat;
import tech.ydb.jdbc.context.StaticQueryResult;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.jdbc.context.YdbValidator;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.settings.FakeTxMode;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.ListValue;

/**
 *
 * @author Aleksandr Gorshenin
 */
public abstract class BaseYdbStatement implements YdbStatement {
    private final YdbConnection connection;
    private final YdbValidator validator;
    private final int resultSetType;
    private final int maxRows;
    private final FakeTxMode scanQueryTxMode;
    private final FakeTxMode schemeQueryTxMode;
    private final FakeTxMode bulkQueryTxMode;

    private YdbQueryResult state = YdbQueryResult.EMPTY;
    private int queryTimeout;
    private boolean isPoolable;
    private boolean isClosed = false;

    public BaseYdbStatement(Logger logger, YdbConnection connection, int resultSetType, boolean isPoolable) {
        this.connection = Objects.requireNonNull(connection);
        this.validator = new YdbValidator();
        this.resultSetType = resultSetType;
        this.isPoolable = isPoolable;

        YdbOperationProperties props = connection.getCtx().getOperationProperties();
        this.queryTimeout = (int) props.getQueryTimeout().getSeconds();
        this.maxRows = props.getMaxRows();
        this.scanQueryTxMode = props.getScanQueryTxMode();
        this.schemeQueryTxMode = props.getSchemeQueryTxMode();
        this.bulkQueryTxMode = props.getBulkQueryTxMode();
    }

    private void ensureOpened() throws SQLException {
        connection.getExecutor().ensureOpened();
    }

    @Override
    public YdbValidator getValidator() {
        return validator;
    }

    @Override
    public YdbConnection getConnection() {
        return connection;
    }

    @Override
    public void close() throws SQLException {
        clearBatch();
        state = YdbQueryResult.EMPTY;
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public int getResultSetType() {
        return resultSetType;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        ensureOpened();
        return validator.toSQLWarnings();
    }

    @Override
    public void clearWarnings() {
        validator.clearWarnings();
    }

    @Override
    public int getQueryTimeout() {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        queryTimeout = seconds;
    }

    @Override
    public void setPoolable(boolean poolable) {
        isPoolable = poolable;
    }

    @Override
    public boolean isPoolable() {
        return isPoolable;
    }

    @Override
    public int getMaxRows() {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) {
        // has not effect
    }

    @Override
    public YdbResultSet getResultSet() throws SQLException {
        return state.getCurrentResultSet();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return state.getMoreResults(current);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return state.getUpdateCount();
    }

    protected void cleanState() throws SQLException {
        state = YdbQueryResult.EMPTY;
        clearWarnings();
    }

    protected boolean updateState(YdbQueryResult result) throws SQLException {
        state = result == null ? YdbQueryResult.EMPTY : result;
        return state.hasResultSets();
    }

    protected YdbQueryResult executeBulkUpsert(YdbQuery query, String tablePath, ListValue rows)
            throws SQLException {
        ensureOpened();

        if (connection.getExecutor().isInsideTransaction()) {
            switch (bulkQueryTxMode) {
                case FAKE_TX:
                    break;
                case SHADOW_COMMIT:
                    connection.commit();
                    break;
                case ERROR:
                default:
                    throw new SQLException(YdbConst.BULK_QUERY_INSIDE_TRANSACTION);
            }
        }

        return connection.getExecutor().executeBulkUpsert(this, query, tablePath, rows);
    }

    protected YdbQueryResult executeExplainQuery(YdbQuery query) throws SQLException {
        ensureOpened();
        return connection.getExecutor().executeExplainQuery(this, query);
    }

    protected YdbQueryResult executeDataQuery(YdbQuery query, String yql, Params params) throws SQLException {
        ensureOpened();

        YdbContext ctx = connection.getCtx();
        if (ctx.queryStatsEnabled()) {
            if (QueryStat.isPrint(yql)) {
                ResultSetReader rsr = QueryStat.toResultSetReader(ctx.getQueryStats());
                YdbResultSet rs = new YdbStaticResultSet(ctx.getTypes(), this, rsr);
                return new StaticQueryResult(query, Collections.singletonList(rs));
            }
            if (QueryStat.isReset(yql)) {
                getConnection().getCtx().resetQueryStats();
                return null;
            }
        }

        ctx.traceQuery(query, yql);
        return connection.getExecutor().executeDataQuery(this, query, yql, params, getQueryTimeout(), isPoolable());
    }

    protected YdbQueryResult executeSchemeQuery(YdbQuery query) throws SQLException {
        ensureOpened();

        if (connection.getExecutor().isInsideTransaction()) {
            switch (schemeQueryTxMode) {
                case FAKE_TX:
                    break;
                case SHADOW_COMMIT:
                    connection.commit();
                    break;
                case ERROR:
                default:
                    throw new SQLException(YdbConst.SCHEME_QUERY_INSIDE_TRANSACTION);
            }
        }

        return connection.getExecutor().executeSchemeQuery(this, query);
    }

    protected YdbQueryResult executeScanQuery(YdbQuery query, String yql, Params params) throws SQLException {
        ensureOpened();

        if (connection.getExecutor().isInsideTransaction()) {
            switch (scanQueryTxMode) {
                case FAKE_TX:
                    break;
                case SHADOW_COMMIT:
                    connection.commit();
                    break;
                case ERROR:
                default:
                    throw new SQLException(YdbConst.SCAN_QUERY_INSIDE_TRANSACTION);
            }
        }

        return connection.getExecutor().executeScanQuery(this, query, yql, params);
    }

    // UNSUPPORTED
    @Override
    public void setCursorName(String name) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(YdbConst.NAMED_CURSORS_UNSUPPORTED);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return state.getGeneratedKeys();
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void closeOnCompletion() {
        // do nothing
    }

    @Override
    public boolean isCloseOnCompletion() {
        return false;
    }

    @Override
    public int getMaxFieldSize() {
        return 0; // not supported
    }

    @Override
    public void setMaxFieldSize(int max) {
        // not supported
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
        // has not effect
    }

    @Override
    public void cancel() {
        // has not effect
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(Statement.KEEP_CURRENT_RESULT);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD && direction != ResultSet.FETCH_UNKNOWN) {
            throw new SQLException(YdbConst.DIRECTION_UNSUPPORTED + direction);
        }
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) {
        // has not effect
    }

    @Override
    public int getFetchSize() {
        return getMaxRows();
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }
}
