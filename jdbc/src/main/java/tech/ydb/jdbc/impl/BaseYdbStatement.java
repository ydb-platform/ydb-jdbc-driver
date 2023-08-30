package tech.ydb.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.context.YdbQuery;
import tech.ydb.jdbc.context.YdbExecutor;
import tech.ydb.jdbc.exception.YdbResultTruncatedException;
import tech.ydb.jdbc.impl.MappingResultSets;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.ExplainDataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ExecuteDataQuerySettings;

import static tech.ydb.jdbc.YdbConst.NAMED_CURSORS_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.RESULT_SET_MODE_UNSUPPORTED;

/**
 *
 * @author Aleksandr Gorshenin
 */
public abstract class BaseYdbStatement implements YdbStatement {
    protected static final ResultState EMPTY_STATE = new ResultState();

    private final YdbConnection connection;
    private final YdbExecutor executor;
    private final YdbOperationProperties properties;
    private final int resultSetType;

    private ResultState state = EMPTY_STATE;
    private int queryTimeout;
    private boolean isPoolable;
    private boolean isClosed = false;

    public BaseYdbStatement(Logger logger, YdbConnection connection, int resultSetType, boolean isPoolable) {
        this.connection = Objects.requireNonNull(connection);
        this.executor = new YdbExecutor(logger);
        this.properties = connection.getCtx().getOperationProperties();
        this.resultSetType = resultSetType;
        this.queryTimeout = (int) connection.getCtx().getOperationProperties().getQueryTimeout().getSeconds();
        this.isPoolable = isPoolable;
    }

    @Override
    public YdbConnection getConnection() {
        return connection;
    }

    protected YdbQuery parseYdbQuery(String sql) {
        return YdbQuery.from(properties, sql);
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
        return properties.getMaxRows();
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

    protected boolean updateState(ResultState results) {
        state = results;
        return results.hasResultSets();
    }

    protected void executeSchemeQuery(YdbQuery query) throws SQLException {
        connection.executeSchemeQuery(query, executor);
    }

    protected ResultState executeExplainQuery(YdbQuery query) throws SQLException {
        ExplainDataQueryResult explainDataQuery = connection.executeExplainQuery(query, executor);

        Map<String, Object> row = new HashMap<>();
        row.put(YdbConst.EXPLAIN_COLUMN_AST, explainDataQuery.getQueryAst());
        row.put(YdbConst.EXPLAIN_COLUMN_PLAN, explainDataQuery.getQueryPlan());
        ResultSetReader result = MappingResultSets.readerFromMap(row);

        List<YdbResultSet> list = Collections.singletonList(new YdbResultSetImpl(this, result));
        return new ResultState(list);
    }

    protected ResultState executeScanQuery(YdbQuery query, Params params) throws SQLException {
        ResultSetReader result = connection.executeScanQuery(query, executor, params);
        List<YdbResultSet> list = Collections.singletonList(new YdbResultSetImpl(this, result));
        return new ResultState(list);
    }

    protected ResultState executeDataQuery(YdbQuery query, Params params) throws SQLException {
        int timeout = getQueryTimeout();
        ExecuteDataQuerySettings settings = new ExecuteDataQuerySettings()
                .setOperationTimeout(Duration.ofSeconds(timeout))
                .setTimeout(Duration.ofSeconds(timeout + 1));
        if (!isPoolable()) {
            settings = settings.disableQueryCache();
        }

        DataQueryResult result = connection.executeDataQuery(query, executor, settings, params);
        List<YdbResultSet> list = new ArrayList<>();
        for (int idx = 0; idx < result.getResultSetCount(); idx += 1) {
            ResultSetReader rs = result.getResultSet(idx);
            if (properties.isFailOnTruncatedResult() && rs.isTruncated()) {
                String msg = String.format(YdbConst.RESULT_IS_TRUNCATED, idx, rs.getRowCount());
                throw new YdbResultTruncatedException(msg);
            }
            list.add(new YdbResultSetImpl(this, rs));
        }

        return new ResultState(list);
    }

    // UNSUPPORTED
    @Override
    public void setCursorName(String name) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(NAMED_CURSORS_UNSUPPORTED);
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

    protected static class ResultState {
        private final List<YdbResultSet> results;
        private int resultSetIndex;
        private final int updateCount;

        private ResultState() {
            results = null;
            resultSetIndex = -1;
            updateCount = -1;
        }

        private ResultState(List<YdbResultSet> list) {
            updateCount = 0;
            results = list;
            resultSetIndex = 0;
        }

        public boolean hasResultSets() {
            return results != null && !results.isEmpty();
        }

        public int getUpdateCount() {
            return updateCount;
        }

        public YdbResultSet getCurrentResultSet() throws SQLException {
            return getResultSet(resultSetIndex);
        }

        public YdbResultSet getResultSet(int index) throws SQLException {
            if (results == null) {
                return null;
            }
            if (index < 0 || index >= results.size()) {
                return null;
            }

            YdbResultSet resultSet = results.get(index);
            if (resultSet.isClosed()) {
                throw new SQLException(YdbConst.RESULT_SET_UNAVAILABLE + index);
            }
            return resultSet;
        }

        public boolean getMoreResults(int current) throws SQLException {
            if (results == null || results.isEmpty()) {
                return false;
            }

            switch (current) {
                case Statement.KEEP_CURRENT_RESULT:
                    break;
                case Statement.CLOSE_CURRENT_RESULT:
                    results.get(resultSetIndex).close();
                    break;
                case Statement.CLOSE_ALL_RESULTS:
                    for (int idx = 0; idx <= resultSetIndex; idx += 1) {
                        results.get(idx).close();
                    }
                    break;
                default:
                    throw new SQLException(RESULT_SET_MODE_UNSUPPORTED + current);
            }

            resultSetIndex += 1;
            return resultSetIndex >= 0 && resultSetIndex < results.size();
        }
    }
}
