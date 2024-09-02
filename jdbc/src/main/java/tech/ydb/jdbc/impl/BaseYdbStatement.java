package tech.ydb.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
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
import tech.ydb.jdbc.context.QueryStat;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.jdbc.context.YdbValidator;
import tech.ydb.jdbc.query.ExplainedQuery;
import tech.ydb.jdbc.query.QueryStatement;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public abstract class BaseYdbStatement implements YdbStatement {
    private static final ResultState EMPTY_STATE = new ResultState(null);
    private static final YdbResult NO_UPDATED = new YdbResult(0);

    // TODO: YDB doesn't return the count of affected rows, so we use little hach to return always 1
    private static final YdbResult HAS_UPDATED = new YdbResult(1);

    private static final FixedResultSetFactory EXPLAIN_RS_FACTORY = FixedResultSetFactory.newBuilder()
            .addTextColumn(YdbConst.EXPLAIN_COLUMN_AST)
            .addTextColumn(YdbConst.EXPLAIN_COLUMN_PLAN)
            .build();

    private final YdbConnection connection;
    private final YdbValidator validator;
    private final int resultSetType;
    private final int maxRows;
    private final boolean failOnTruncatedResult;

    private ResultState state = EMPTY_STATE;
    private int queryTimeout;
    private boolean isPoolable;
    private boolean isClosed = false;

    public BaseYdbStatement(Logger logger, YdbConnection connection, int resultSetType, boolean isPoolable) {
        this.connection = Objects.requireNonNull(connection);
        this.validator = new YdbValidator(logger);
        this.resultSetType = resultSetType;
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
        connection.executeSchemeQuery(query.getPreparedYql(), validator);

        int expressionsCount = query.getStatements().isEmpty() ? 1 : query.getStatements().size();
        List<YdbResult> results = new ArrayList<>();
        for (int i = 0; i < expressionsCount; i++) {
            results.add(NO_UPDATED);
        }
        return results;
    }

    protected List<YdbResult> executeExplainQuery(YdbQuery query) throws SQLException {
        ExplainedQuery explainedQuery = connection.executeExplainQuery(query.getPreparedYql(), validator);

        ResultSetReader result = EXPLAIN_RS_FACTORY.createResultSet()
                .newRow()
                .withTextValue(YdbConst.EXPLAIN_COLUMN_AST, explainedQuery.getAst())
                .withTextValue(YdbConst.EXPLAIN_COLUMN_PLAN, explainedQuery.getPlan())
                .build()
                .build();

        return Collections.singletonList(new YdbResult(new YdbResultSetImpl(this, result)));
    }

    protected List<YdbResult> executeScanQuery(YdbQuery query, String yql, Params params) throws SQLException {
        connection.getCtx().traceQuery(query, yql);
        ResultSetReader result = connection.executeScanQuery(query, yql, validator, params);
        return Collections.singletonList(new YdbResult(new YdbResultSetImpl(this, result)));
    }

    protected List<YdbResult> executeDataQuery(YdbQuery query, String yql, Params params) throws SQLException {
        YdbContext ctx = connection.getCtx();

        if (ctx.queryStatsEnabled()) {
            if (QueryStat.isPrint(yql)) {
                YdbResultSet rs = new YdbResultSetImpl(this, QueryStat.toResultSetReader(ctx.getQueryStats()));
                return Collections.singletonList(new YdbResult(rs));
            }
            if (QueryStat.isReset(yql)) {
                getConnection().getCtx().resetQueryStats();
                return null;
            }
        }

        ctx.traceQuery(query, yql);
        List<ResultSetReader> resultSets = connection
                .executeDataQuery(query, yql, validator, getQueryTimeout(), isPoolable(), params);

        List<YdbResult> results = new ArrayList<>();
        int idx = 0;
        for (QueryStatement exp: query.getStatements()) {
            if (exp.isDDL()) {
                results.add(NO_UPDATED);
                continue;
            }
            if (exp.hasUpdateCount()) {
                results.add(HAS_UPDATED);
                continue;
            }

            if (exp.hasResults() && idx < resultSets.size()) {
                ResultSetReader rs = resultSets.get(idx);
                if (failOnTruncatedResult && rs.isTruncated()) {
                    String msg = String.format(YdbConst.RESULT_IS_TRUNCATED, idx, rs.getRowCount());
                    throw new SQLException(msg);
                }
                results.add(new YdbResult(new YdbResultSetImpl(this, rs)));
                idx++;
            }
        }

        while (idx < resultSets.size())  {
            ResultSetReader rs = resultSets.get(idx);
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
