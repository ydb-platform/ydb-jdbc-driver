package tech.ydb.jdbc.statement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.common.YdbQuery;
import tech.ydb.jdbc.connection.YdbExecutor;
import tech.ydb.jdbc.exception.YdbResultTruncatedException;
import tech.ydb.jdbc.impl.MappingResultSets;
import tech.ydb.jdbc.impl.YdbResultSetImpl;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.ExplainDataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;

import static tech.ydb.jdbc.YdbConst.AUTO_GENERATED_KEYS_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.CANNOT_UNWRAP_TO;
import static tech.ydb.jdbc.YdbConst.CLOSED_CONNECTION;
import static tech.ydb.jdbc.YdbConst.DIRECTION_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.NAMED_CURSORS_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.QUERY_EXPECT_RESULT_SET;
import static tech.ydb.jdbc.YdbConst.QUERY_EXPECT_UPDATE;
import static tech.ydb.jdbc.YdbConst.RESULT_SET_MODE_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.RESULT_SET_UNAVAILABLE;

public class YdbStatementImpl implements YdbStatement {
    private static final Logger LOGGER = Logger.getLogger(YdbStatementImpl.class.getName());

    private final MutableState state = new MutableState();

    private final List<String> batch = new ArrayList<>();
    private final YdbConnection connection;
    private final YdbExecutor executor;
    private final YdbOperationProperties properties;
    private final int resultSetType;

    public YdbStatementImpl(YdbConnection connection, int resultSetType) {
        this.connection = Objects.requireNonNull(connection);
        this.resultSetType = resultSetType;

        this.properties = connection.getCtx().getOperationProperties();
        state.queryTimeout = properties.getQueryTimeout();
        state.keepInQueryCache = properties.isKeepInQueryCache();

        this.executor = new YdbExecutor(LOGGER, state.queryTimeout);
    }

    @Override
    public void executeSchemeQuery(String sql) throws SQLException {
        YdbQuery query = new YdbQuery(properties, sql, false);
        if (executeSchemeQueryImpl(query)) {
            throw new SQLException(YdbConst.QUERY_EXPECT_UPDATE);
        }
    }

    @Override
    public YdbResultSet executeScanQuery(String sql) throws SQLException {
        YdbQuery query = new YdbQuery(properties, sql, false);
        if (!executeScanQueryImpl(query, Params.create())) {
            throw new SQLException(YdbConst.QUERY_EXPECT_RESULT_SET);
        }
        return getResultSet(0);
    }

    @Override
    public YdbResultSet executeExplainQuery(String sql) throws SQLException {
        YdbQuery query = new YdbQuery(properties, sql, false);

        if (!executeExplainQueryImpl(query)) {
            throw new SQLException(QUERY_EXPECT_RESULT_SET);
        }
        return getResultSet(0);
    }

    @Override
    public YdbResultSet executeQuery(String sql) throws SQLException {
        boolean result = this.execute(sql);
        if (!result) {
            throw new SQLException(QUERY_EXPECT_RESULT_SET);
        }
        return getResultSet(0);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        boolean result = this.execute(sql);
        if (result) {
            throw new SQLException(QUERY_EXPECT_UPDATE);
        }
        return state.updateCount;
    }

    @Override
    public void close() {
        // do nothing
        state.closed = true;
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
    public int getMaxRows() {
        return properties.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) {
        // has not effect
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
        // has not effect
    }

    @Override
    public int getQueryTimeout() {
        return (int) state.queryTimeout.getSeconds();
    }

    @Override
    public void setQueryTimeout(int seconds) {
        state.queryTimeout = Duration.ofSeconds(seconds);
    }

    @Override
    public void cancel() {
        // has not effect
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
    public void setCursorName(String name) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(NAMED_CURSORS_UNSUPPORTED);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return executeImpl(new YdbQuery(properties, sql, false));
    }

    @Override
    public YdbResultSet getResultSet() throws SQLException {
        ensureOpened();

        if (state.lastResultSets != null && state.resultSetIndex < state.lastResultSets.size()) {
            return getResultSet(state.resultSetIndex);
        } else {
            return null;
        }
    }

    @Override
    public YdbResultSet getResultSetAt(int resultSetIndex) throws SQLException {
        ensureOpened();

        if (resultSetIndex >= 0 && resultSetIndex < state.lastResultSets.size()) {
            return state.lastResultSets.get(resultSetIndex);
        } else {
            return null;
        }
    }

    @Override
    public int getUpdateCount() {
        return state.updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        ensureOpened();

        return getMoreResults(Statement.KEEP_CURRENT_RESULT);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD && direction != ResultSet.FETCH_UNKNOWN) {
            throw new SQLException(DIRECTION_UNSUPPORTED + direction);
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

    @Override
    public int getResultSetType() {
        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        ensureOpened();

        batch.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        ensureOpened();

        if (batch.isEmpty()) {
            LOGGER.log(Level.FINE, "Batch is empty, nothing to execute");
            return new int[0];
        }

        try {
            LOGGER.log(Level.FINE, "Executing batch of {0} item(s)", batch.size());

            String sql = String.join(";\n", batch);
            execute(sql);

            int[] ret = new int[batch.size()];
            Arrays.fill(ret, SUCCESS_NO_INFO);
            return ret;
        } finally {
            clearBatch();
        }
    }

    @Override
    public YdbConnection getConnection() {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        ensureOpened();

        List<YdbResultSet> lastResultSets = state.lastResultSets;
        int resultSetIndex = state.resultSetIndex;
        switch (current) {
            case Statement.KEEP_CURRENT_RESULT:
                // do nothing
                break;
            case Statement.CLOSE_CURRENT_RESULT:
                if (lastResultSets != null && resultSetIndex < lastResultSets.size()) {
                    lastResultSets.get(resultSetIndex).close();
                }
                break;
            case Statement.CLOSE_ALL_RESULTS:
                if (lastResultSets != null && resultSetIndex < lastResultSets.size()) {
                    for (int idx = 0; idx <= resultSetIndex; idx += 1) {
                        lastResultSets.get(idx).close();
                    }
                }
                break;
            default:
                throw new SQLException(RESULT_SET_MODE_UNSUPPORTED + current);
        }
        if (lastResultSets != null) {
            state.resultSetIndex++;
            return state.resultSetIndex < lastResultSets.size();
        } else {
            return false;
        }
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_UNSUPPORTED);
        }
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_UNSUPPORTED);
        }
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return state.closed;
    }

    @Override
    public void setPoolable(boolean poolable) {
        state.keepInQueryCache = poolable;
    }

    @Override
    public boolean isPoolable() {
        return state.keepInQueryCache;
    }

    @Override
    public void closeOnCompletion() {
        // do nothing
    }

    @Override
    public boolean isCloseOnCompletion() {
        return false;
    }

    protected boolean executeImpl(YdbQuery query) throws SQLException {
        ensureOpened();

        Preconditions.checkState(query != null, "queryType cannot be null");
        this.clearState();
        switch (query.type()) {
            case SCHEME_QUERY:
                return executeSchemeQueryImpl(query);
            case DATA_QUERY:
                return executeDataQueryImpl(query, Params.create());
            case SCAN_QUERY:
                return executeScanQueryImpl(query, Params.create());
            case EXPLAIN_QUERY:
                return executeExplainQueryImpl(query);
            default:
                throw new IllegalStateException("Internal error. Unsupported query type " + query.type());
        }
    }

    private boolean executeSchemeQueryImpl(YdbQuery query) throws SQLException {
        ensureOpened();
        connection.executeSchemeQuery(query, executor);
        return false;
    }

    protected boolean executeDataQueryImpl(YdbQuery query, Params params) throws SQLException {
        ensureOpened();
        updateStateWithDataQueryResult(connection.executeDataQuery(query, params, executor));
        return state.lastResultSets != null && !state.lastResultSets.isEmpty();
    }

    protected boolean executeScanQueryImpl(YdbQuery query, Params params) throws SQLException {
        ensureOpened();
        updateStateWithResultSet(connection.executeScanQuery(query, params, executor));
        return state.lastResultSets != null && !state.lastResultSets.isEmpty();
    }

    protected boolean executeExplainQueryImpl(YdbQuery query) throws SQLException {
        ensureOpened();

        ExplainDataQueryResult explainDataQuery = connection.executeExplainQuery(query, executor);

        Map<String, Object> result = new HashMap<>();
        result.put(YdbConst.EXPLAIN_COLUMN_AST, explainDataQuery.getQueryAst());
        result.put(YdbConst.EXPLAIN_COLUMN_PLAN, explainDataQuery.getQueryPlan());
        ResultSetReader resultSetReader = MappingResultSets.readerFromMap(result);

        updateStateWithResultSet(resultSetReader);
        return true;
    }

    private void clearState() {
        this.clearWarnings();
        state.lastResultSets = null;
        state.resultSetIndex = 0;
        state.updateCount = -1;
    }

    protected YdbResultSet getResultSet(int index) throws SQLException {
        List<YdbResultSet> lastResultSets = state.lastResultSets;
        if (lastResultSets == null) {
            throw new IllegalStateException("Internal error, not result to use");
        }
        if (index < 0 || index >= lastResultSets.size()) {
            throw new IllegalStateException("Internal error, no result at position: " + index);
        }
        YdbResultSet resultSet = lastResultSets.get(index);
        if (resultSet.isClosed()) {
            throw new SQLException(RESULT_SET_UNAVAILABLE + index);
        }
        return resultSet;
    }

    private void updateStateWithDataQueryResult(DataQueryResult result) throws YdbResultTruncatedException {
        if (result == null) {
            updateStateWithResultList(null);
            return;
        }
        List<ResultSetReader> list = new ArrayList<>(result.getResultSetCount());
        for (int idx = 0; idx < result.getResultSetCount(); idx += 1) {
            list.add(result.getResultSet(idx));
        }
        updateStateWithResultList(list);
    }

    private void updateStateWithResultSet(ResultSetReader reader) throws YdbResultTruncatedException {
        if (reader == null) {
            updateStateWithResultList(null);
            return;
        }
        updateStateWithResultList(Collections.singletonList(reader));
    }

    private void updateStateWithResultList(List<ResultSetReader> list) throws YdbResultTruncatedException {
        if (list == null || list.isEmpty()) {
            state.resultSetIndex = 0;
            state.updateCount = 1;
            state.lastResultSets = null;
            return;
        }

        if (properties.isFailOnTruncatedResult()) {
            for (int idx = 0; idx < list.size(); idx++) {
                if (list.get(idx).isTruncated()) {
                    String msg = String.format(YdbConst.RESULT_IS_TRUNCATED, idx, list.get(idx).getRowCount());
                    throw new YdbResultTruncatedException(msg);
                }
            }
        }

        state.resultSetIndex = 0;
        state.lastResultSets = list.stream().map(rs -> new YdbResultSetImpl(this, rs)).collect(Collectors.toList());
    }

    protected void ensureOpened() throws SQLException {
        if (state.closed) {
            throw new SQLException(CLOSED_CONNECTION);
        }
    }

    private static class MutableState {
        private List<YdbResultSet> lastResultSets = null;
        private int resultSetIndex;
        private int updateCount = -1; // TODO: figure out how to get update count from DML

        private Duration queryTimeout;
        private boolean keepInQueryCache;

        private boolean closed;
    }

    // UNSUPPORTED
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null; // --
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_UNSUPPORTED);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_UNSUPPORTED);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_UNSUPPORTED);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_UNSUPPORTED);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException(CANNOT_UNWRAP_TO + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }
}
