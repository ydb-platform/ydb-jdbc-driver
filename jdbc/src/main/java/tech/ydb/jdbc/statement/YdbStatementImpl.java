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
import tech.ydb.table.settings.ExecuteDataQuerySettings;

import static tech.ydb.jdbc.YdbConst.AUTO_GENERATED_KEYS_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.CANNOT_UNWRAP_TO;
import static tech.ydb.jdbc.YdbConst.CLOSED_CONNECTION;
import static tech.ydb.jdbc.YdbConst.DIRECTION_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.NAMED_CURSORS_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.QUERY_EXPECT_RESULT_SET;
import static tech.ydb.jdbc.YdbConst.QUERY_EXPECT_UPDATE;

public class YdbStatementImpl implements YdbStatement {
    private static final Logger LOGGER = Logger.getLogger(YdbStatementImpl.class.getName());

    private final List<String> batch = new ArrayList<>();
    private final YdbConnection connection;
    private final YdbExecutor executor;
    private final YdbOperationProperties properties;
    private final int resultSetType;

    private YdbStatementResultSets results = YdbStatementResultSets.EMPTY;
    private int queryTimeout;
    private boolean isPoolable;
    private boolean isClosed = false;

    public YdbStatementImpl(YdbConnection connection, int resultSetType) {
        this.connection = Objects.requireNonNull(connection);
        this.resultSetType = resultSetType;
        this.properties = connection.getCtx().getOperationProperties();
        this.executor = new YdbExecutor(LOGGER);
        this.queryTimeout = (int) properties.getQueryTimeout().toSeconds();
        this.isPoolable = false;
    }

    @Override
    public void executeSchemeQuery(String sql) throws SQLException {
        executeSchemeQuery(YdbQuery.from(properties, sql));
    }

    @Override
    public YdbResultSet executeScanQuery(String sql) throws SQLException {
        ensureOpened();
        clearWarnings();

        YdbQuery query = YdbQuery.from(properties, sql);
        results = executeScanQueryImpl(query);
        if (!results.hasResultSets()) {
            throw new SQLException(YdbConst.QUERY_EXPECT_RESULT_SET);
        }
        return getResultSet();
    }

    @Override
    public YdbResultSet executeExplainQuery(String sql) throws SQLException {
        ensureOpened();
        clearWarnings();

        YdbQuery query = YdbQuery.from(properties, sql);
        results = executeExplainQueryImpl(query);
        if (!results.hasResultSets()) {
            throw new SQLException(YdbConst.QUERY_EXPECT_RESULT_SET);
        }
        return getResultSet();
    }

    @Override
    public YdbResultSet executeQuery(String sql) throws SQLException {
        if (!execute(sql)) {
            throw new SQLException(QUERY_EXPECT_RESULT_SET);
        }
        return getResultSet();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (execute(sql)) {
            throw new SQLException(QUERY_EXPECT_UPDATE);
        }
        return getUpdateCount();
    }

    @Override
    public void close() {
        // do nothing
        isClosed = true;
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
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) {
        queryTimeout = seconds;
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
        ensureOpened();
        clearWarnings();

        YdbQuery query = YdbQuery.from(properties, sql);
        results = YdbStatementResultSets.EMPTY;

        switch (query.type()) {
            case SCHEME_QUERY:
                executeSchemeQuery(query);
                break;
            case DATA_QUERY:
                results = executeDataQueryImpl(query);
                break;
            case SCAN_QUERY:
                results = executeScanQueryImpl(query);
                break;
            case EXPLAIN_QUERY:
                results = executeExplainQueryImpl(query);
                break;
            default:
                throw new IllegalStateException("Internal error. Unsupported query type " + query.type());
        }

        return results.hasResultSets();
    }

    @Override
    public YdbResultSet getResultSet() throws SQLException {
        ensureOpened();
        return results.getCurrentResultSet();
    }

    @Override
    public YdbResultSet getResultSetAt(int resultSetIndex) throws SQLException {
        ensureOpened();
        return results.getResultSet(resultSetIndex);
    }

    @Override
    public int getUpdateCount() {
        return results.getUpdateCount();
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
        return results.getMoreResults(current);
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
        return isClosed;
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
    public void closeOnCompletion() {
        // do nothing
    }

    @Override
    public boolean isCloseOnCompletion() {
        return false;
    }

//    protected boolean executeImpl(YdbQuery query) throws SQLException {
//    }

    private void executeSchemeQuery(YdbQuery query) throws SQLException {
        connection.executeSchemeQuery(query, executor);
    }

    private YdbStatementResultSets executeDataQueryImpl(YdbQuery query) throws SQLException {
        ensureOpened();

        ExecuteDataQuerySettings settings = new ExecuteDataQuerySettings()
                .setOperationTimeout(Duration.ofSeconds(queryTimeout))
                .setTimeout(Duration.ofSeconds(queryTimeout + 1));
        if (!isPoolable) {
            settings = settings.disableQueryCache();
        }

        DataQueryResult result = connection.executeDataQuery(query, executor, settings, Params.empty());
        List<YdbResultSet> list = new ArrayList<>();
        for (int idx = 0; idx < result.getResultSetCount(); idx += 1) {
            ResultSetReader rs = result.getResultSet(idx);
            if (properties.isFailOnTruncatedResult() && rs.isTruncated()) {
                String msg = String.format(YdbConst.RESULT_IS_TRUNCATED, idx, rs.getRowCount());
                throw new YdbResultTruncatedException(msg);
            }
            list.add(new YdbResultSetImpl(this, rs));
        }

        return new YdbStatementResultSets(list);
    }

    protected YdbStatementResultSets executeScanQueryImpl(YdbQuery query) throws SQLException {
        ensureOpened();
        ResultSetReader result = connection.executeScanQuery(query, executor, Params.empty());
        List<YdbResultSet> list = Collections.singletonList(new YdbResultSetImpl(this, result));
        return new YdbStatementResultSets(list);
    }

    protected YdbStatementResultSets executeExplainQueryImpl(YdbQuery query) throws SQLException {
        ensureOpened();

        ExplainDataQueryResult explainDataQuery = connection.executeExplainQuery(query, executor);

        Map<String, Object> row = new HashMap<>();
        row.put(YdbConst.EXPLAIN_COLUMN_AST, explainDataQuery.getQueryAst());
        row.put(YdbConst.EXPLAIN_COLUMN_PLAN, explainDataQuery.getQueryPlan());
        ResultSetReader result = MappingResultSets.readerFromMap(row);

        List<YdbResultSet> list = Collections.singletonList(new YdbResultSetImpl(this, result));
        return new YdbStatementResultSets(list);
    }

    protected void ensureOpened() throws SQLException {
        if (isClosed) {
            throw new SQLException(CLOSED_CONNECTION);
        }
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
