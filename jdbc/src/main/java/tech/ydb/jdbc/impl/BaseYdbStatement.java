package tech.ydb.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.common.FixedResultSetFactory;
import tech.ydb.jdbc.context.YdbExecutor;
import tech.ydb.jdbc.query.YdbExpression;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.query.YdbQueryOptions;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.ExplainDataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ExecuteDataQuerySettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public abstract class BaseYdbStatement implements YdbStatement {
    private static final ResultState EMPTY_STATE = new ResultState(null);
    private static final YdbResult NO_UPDATED = new YdbResult(0);

    // TODO: YDB doesn't return the count of affected rows, so we use little hach to return always 1
    private static final YdbResult HAS_UPDATED = new YdbResult(1);

    private static FixedResultSetFactory EXPLAIN_RS_FACTORY = FixedResultSetFactory.newBuilder()
            .addTextColumn(YdbConst.EXPLAIN_COLUMN_AST)
            .addTextColumn(YdbConst.EXPLAIN_COLUMN_PLAN)
            .build();

    private final YdbConnection connection;
    private final YdbExecutor executor;
    private final YdbQueryOptions queryOptions;
    private final int resultSetType;
    private final int maxRows;
    private final boolean failOnTruncatedResult;

    private ResultState state = EMPTY_STATE;
    private int queryTimeout;
    private boolean isPoolable;
    private boolean isClosed = false;

    public BaseYdbStatement(Logger logger, YdbConnection connection, int resultSetType, boolean isPoolable) {
        this.connection = Objects.requireNonNull(connection);
        this.executor = new YdbExecutor(logger);
        this.resultSetType = resultSetType;
        this.queryOptions = connection.getCtx().getQueryOptions();
        this.isPoolable = isPoolable;

        YdbOperationProperties props = connection.getCtx().getOperationProperties();
        this.queryTimeout = (int) props.getQueryTimeout().getSeconds();
        this.maxRows = props.getMaxRows();
        this.failOnTruncatedResult = props.isFailOnTruncatedResult();
    }

    @Override
    public YdbConnection getConnection() {
        return connection;
    }

    public YdbQuery createYdbQuery(String sql) throws SQLException {
        return YdbQuery.from(queryOptions, sql);
    }

    @Override
    public void close() {
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
    public SQLWarning getWarnings() {
        return executor.toSQLWarnings();
    }

    @Override
    public void clearWarnings() {
        executor.clearWarnings();
    }

    @Override
    public int getQueryTimeout() {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        ensureOpened();
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
        ensureOpened();
        return state.getCurrentResultSet();
    }

    @Override
    public YdbResultSet getResultSetAt(int resultSetIndex) throws SQLException {
        ensureOpened();
        return state.getResultSet(resultSetIndex);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        ensureOpened();
        return state.getMoreResults(current);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        ensureOpened();
        return state.getUpdateCount();
    }

    private void ensureOpened() throws SQLException {
        if (isClosed) {
            throw new SQLException(YdbConst.CLOSED_CONNECTION);
        }
    }

    protected void cleanState() throws SQLException {
        ensureOpened();
        clearWarnings();
        state = EMPTY_STATE;
    }

    protected boolean updateState(List<YdbResult> results) {
        state = results == null ? EMPTY_STATE : new ResultState(results);
        return state.hasResultSets();
    }

    protected List<YdbResult> executeSchemeQuery(YdbQuery query) throws SQLException {
        connection.executeSchemeQuery(query, executor);

        int expressionsCount = query.getExpressions().isEmpty() ? 1 : query.getExpressions().size();
        List<YdbResult> results = new ArrayList<>();
        for (int i = 0; i < expressionsCount; i++) {
            results.add(NO_UPDATED);
        }
        return results;
    }

    protected List<YdbResult> executeExplainQuery(YdbQuery query) throws SQLException {
        ExplainDataQueryResult explainDataQuery = connection.executeExplainQuery(query, executor);

        ResultSetReader result = EXPLAIN_RS_FACTORY.createResultSet()
                .newRow()
                .withTextValue(YdbConst.EXPLAIN_COLUMN_AST, explainDataQuery.getQueryAst())
                .withTextValue(YdbConst.EXPLAIN_COLUMN_PLAN, explainDataQuery.getQueryPlan())
                .build()
                .build();

        return Collections.singletonList(new YdbResult(new YdbResultSetImpl(this, result)));
    }

    protected List<YdbResult> executeScanQuery(YdbQuery query, Params params) throws SQLException {
        ResultSetReader result = connection.executeScanQuery(query, executor, params);
        return Collections.singletonList(new YdbResult(new YdbResultSetImpl(this, result)));
    }

    protected List<YdbResult> executeDataQuery(YdbQuery query, Params params) throws SQLException {
        int timeout = getQueryTimeout();
        ExecuteDataQuerySettings settings = new ExecuteDataQuerySettings()
                .setOperationTimeout(Duration.ofSeconds(timeout))
                .setTimeout(Duration.ofSeconds(timeout + 1));
        if (!isPoolable()) {
            settings = settings.disableQueryCache();
        }

        DataQueryResult result = connection.executeDataQuery(query, executor, settings, params);

        List<YdbResult> results = new ArrayList<>();
        int idx = 0;
        for (YdbExpression exp: query.getExpressions()) {
            if (exp.isDDL()) {
                results.add(NO_UPDATED);
                continue;
            }
            if (!exp.isSelect()) {
                results.add(HAS_UPDATED);
                continue;
            }

            if (idx < result.getResultSetCount())  {
                ResultSetReader rs = result.getResultSet(idx);
                if (failOnTruncatedResult && rs.isTruncated()) {
                    String msg = String.format(YdbConst.RESULT_IS_TRUNCATED, idx, rs.getRowCount());
                    throw new SQLException(msg);
                }
                results.add(new YdbResult(new YdbResultSetImpl(this, rs)));
                idx++;
            }
        }

        while (idx < result.getResultSetCount())  {
            ResultSetReader rs = result.getResultSet(idx);
            if (failOnTruncatedResult && rs.isTruncated()) {
                String msg = String.format(YdbConst.RESULT_IS_TRUNCATED, idx, rs.getRowCount());
                throw new SQLException(msg);
            }
            results.add(new YdbResult(new YdbResultSetImpl(this, rs)));
            idx++;
        }

        return results;
    }

    // UNSUPPORTED
    @Override
    public void setCursorName(String name) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(YdbConst.NAMED_CURSORS_UNSUPPORTED);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null; // --
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
        ensureOpened();
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

    protected static class YdbResult {
        private final int updateCount;
        private final YdbResultSet resultSet;

        YdbResult(int updateCount) {
            this.updateCount = updateCount;
            this.resultSet = null;
        }

        YdbResult(YdbResultSet resultSet) {
            this.updateCount = -1;
            this.resultSet = resultSet;
        }
    }

    private static class ResultState {
        private final List<YdbResult> results;
        private int resultIndex;

        ResultState(List<YdbResult> results) {
            this.results = results;
            this.resultIndex = 0;
        }

        boolean hasResultSets() {
            if (results == null || resultIndex >= results.size()) {
                return false;
            }

            return results.get(resultIndex).resultSet != null;
        }

        int getUpdateCount() {
            if (results == null || resultIndex >= results.size()) {
                return -1;
            }

            return results.get(resultIndex).updateCount;
        }

        YdbResultSet getCurrentResultSet()  {
            if (results == null || resultIndex >= results.size()) {
                return null;
            }
            return results.get(resultIndex).resultSet;
        }

        YdbResultSet getResultSet(int index) {
            if (results == null || index < 0 || index >= results.size()) {
                return null;
            }
            return results.get(index).resultSet;
        }

        boolean getMoreResults(int current) throws SQLException {
            if (results == null || resultIndex >= results.size()) {
                return false;
            }

            switch (current) {
                case Statement.KEEP_CURRENT_RESULT:
                    break;
                case Statement.CLOSE_CURRENT_RESULT:
                    results.get(resultIndex).resultSet.close();
                    break;
                case Statement.CLOSE_ALL_RESULTS:
                    for (int idx = 0; idx <= resultIndex; idx += 1) {
                        results.get(idx).resultSet.close();
                    }
                    break;
                default:
                    throw new SQLException(YdbConst.RESULT_SET_MODE_UNSUPPORTED + current);
            }

            resultIndex += 1;
            return hasResultSets();
        }
    }
}
