package tech.ydb.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.logging.Logger;

import tech.ydb.core.Issue;
import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbQueryResult;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.jdbc.context.QueryStat;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.jdbc.context.YdbExecutor;
import tech.ydb.jdbc.context.YdbValidator;
import tech.ydb.jdbc.exception.YdbRetryableException;
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
public abstract class YdbStatementBase implements YdbStatement {
    private static final YdbQueryResult EMPTY_RESULT = new YdbQueryResultEmpty();

    private final YdbConnection connection;
    private final YdbValidator validator;
    private final int resultSetType;
    private final FakeTxMode scanQueryTxMode;
    private final FakeTxMode schemeQueryTxMode;
    private final FakeTxMode bulkQueryTxMode;

    private YdbQueryResult state = EMPTY_RESULT;
    private int queryTimeout;
    private boolean isPoolable;
    private boolean isClosed = false;

    /** @see Statement#getMaxRows() */
    private int maxRows = 0; // no limit
    private int fetchSize = 0;
    private int fetchDirection = ResultSet.FETCH_UNKNOWN;

    public YdbStatementBase(Logger logger, YdbConnection connection, int resultSetType, boolean isPoolable) {
        this.connection = Objects.requireNonNull(connection);
        this.validator = new YdbValidator();
        this.resultSetType = resultSetType;
        this.isPoolable = isPoolable;

        YdbOperationProperties props = connection.getCtx().getOperationProperties();
        this.queryTimeout = (int) props.getQueryTimeout().getSeconds();
        this.scanQueryTxMode = props.getScanQueryTxMode();
        this.schemeQueryTxMode = props.getSchemeQueryTxMode();
        this.bulkQueryTxMode = props.getBulkQueryTxMode();
    }

    private void prepareNewExecution() throws SQLException {
        if (fetchSize > 0 && (fetchDirection != ResultSet.FETCH_FORWARD && fetchDirection != ResultSet.FETCH_UNKNOWN)) {
            throw new SQLException(YdbConst.RESULT_IS_NOT_SCROLLABLE);
        }
        connection.getExecutor().ensureOpened();
        connection.getExecutor().clearState();
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
        connection.getExecutor().clearState();
        state = EMPTY_RESULT;
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
        prepareNewExecution();
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
        this.maxRows = max;
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
        state = EMPTY_RESULT;
        clearWarnings();
    }

    protected boolean updateState(YdbQueryResult result) throws SQLException {
        state = result == null ? EMPTY_RESULT : result;
        return state.hasResultSets();
    }

    protected YdbQueryResult executeBulkUpsert(YdbQuery query, String tablePath, ListValue rows) throws SQLException {
        prepareNewExecution();

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
        prepareNewExecution();
        return connection.getExecutor().executeExplainQuery(this, query);
    }

    protected YdbQueryResult executeDataQuery(YdbQuery query, String yql, Params params) throws SQLException {
        prepareNewExecution();

        YdbContext ctx = connection.getCtx();
        YdbExecutor executor = connection.getExecutor();

        if (ctx.isFullScanDetectorEnabled()) {
            if (QueryStat.isPrint(yql)) {
                ResultSetReader rsr = QueryStat.toResultSetReader(ctx.getFullScanDetectorStats());
                YdbResultSet rs = new YdbResultSetMemory(ctx.getTypes(), this, rsr);
                return new YdbQueryResultStatic(query, rs);
            }
            if (QueryStat.isReset(yql)) {
                ctx.resetFullScanDetector();
                return null;
            }
        }
        ctx.traceQueryByFullScanDetector(query, yql);

        boolean isInsideTx = executor.isInsideTransaction();
        while (true) {
            try {
                return executor.executeDataQuery(this, query, yql, params);
            } catch (YdbRetryableException ex) {
                if (isInsideTx || ex.getStatus().getCode() != StatusCode.BAD_SESSION) {
                    throw ex;
                }
                // TODO: Move this logic to YdbValidator
                Issue warning = Issue.of("Operation retried because of of BAD_SESSION", Issue.Severity.INFO);
                validator.addStatusIssues(Arrays.asList(warning));
            }
        }
    }

    protected YdbQueryResult executeBatchQuery(YdbQuery query, Function<Params, String> queryFunc, List<Params> params)
            throws SQLException {
        prepareNewExecution();

        if (params.isEmpty()) {
            return new YdbQueryResultEmpty();
        }

        YdbExecutor executor = connection.getExecutor();
        YdbTypes types = connection.getCtx().getTypes();
        List<YdbResultSetMemory[]> batchResults = new ArrayList<>();
        int count = 0;

        boolean autoCommit = executor.isAutoCommit();
        try {
            if (autoCommit) {
                executor.setAutoCommit(false);
            }
            for (Params prm: params) {
                YdbResultSetMemory[] res = executor.executeInMemoryQuery(this, queryFunc.apply(prm), prm);
                count = Math.max(count, res.length);
                batchResults.add(res);
            }
            if (autoCommit) {
                executor.commit(connection.getCtx(), validator);
            }
        } finally {
            executor.setAutoCommit(autoCommit);
        }

        YdbResultSetMemory[] merged = new YdbResultSetMemory[count];
        for (int idx = 0; idx < count; idx += 1) {
            List<ResultSetReader> expressionResults = new ArrayList<>();
            for (YdbResultSetMemory[] res: batchResults) {
                if (idx < res.length) {
                    expressionResults.addAll(Arrays.asList(res[idx].getResultSets()));
                }
            }
            merged[idx] = new YdbResultSetMemory(types, this, expressionResults.toArray(new ResultSetReader[0]));
        }

        return new YdbQueryResultStatic(query, merged);
    }

    protected YdbQueryResult executeSchemeQuery(YdbQuery query) throws SQLException {
        prepareNewExecution();

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
        prepareNewExecution();

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
        this.fetchDirection = direction;
    }

    @Override
    public int getFetchDirection() {
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) {
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }
}
