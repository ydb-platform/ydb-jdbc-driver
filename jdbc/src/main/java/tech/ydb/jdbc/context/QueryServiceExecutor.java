package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.impl.YdbQueryResult;
import tech.ydb.jdbc.impl.YdbStaticResultSet;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.result.QueryInfo;
import tech.ydb.query.result.QueryResultPart;
import tech.ydb.query.settings.CommitTransactionSettings;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.settings.QueryExecMode;
import tech.ydb.query.settings.RollbackTransactionSettings;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class QueryServiceExecutor extends BaseYdbExecutor {
    private final Duration sessionTimeout;
    private final QueryClient queryClient;
    private final boolean useStreamResultSet;

    private int transactionLevel;
    private boolean isReadOnly;
    private boolean isAutoCommit;
    private TxMode txMode;

    private final AtomicReference<QueryTransaction> tx = new AtomicReference<>();
    private volatile boolean isClosed;

    public QueryServiceExecutor(YdbContext ctx, int transactionLevel, boolean autoCommit) throws SQLException {
        super(ctx);
        this.sessionTimeout = ctx.getOperationProperties().getSessionTimeout();
        this.queryClient = ctx.getQueryClient();
        this.useStreamResultSet = ctx.getOperationProperties().getUseStreamResultSets();

        this.transactionLevel = transactionLevel;
        this.isReadOnly = transactionLevel != Connection.TRANSACTION_SERIALIZABLE;
        this.isAutoCommit = autoCommit;
        this.txMode = txMode(transactionLevel, isReadOnly);
        this.isClosed = false;
    }

    protected QuerySession createNewQuerySession(YdbValidator validator) throws SQLException {
        try {
            Result<QuerySession> result = queryClient.createSession(sessionTimeout).join();
            validator.addStatusIssues(result.getStatus());
            QuerySession session = result.getValue();
            return session;
        } catch (UnexpectedResultException ex) {
            throw ExceptionFactory.createException("Cannot create session with " + ex.getStatus(), ex);
        }
    }

    @Override
    public void close() throws SQLException {
        closeCurrentResult();
        isClosed = true;
        QueryTransaction old = tx.getAndSet(null);
        if (old != null) {
            old.getSession().close();
        }
    }

    @Override
    public void setTransactionLevel(int level) throws SQLException {
        ensureOpened();

        if (level == transactionLevel) {
            return;
        }

        QueryTransaction localTx = tx.get();
        if (localTx != null && localTx.isActive()) {
            throw new SQLFeatureNotSupportedException(YdbConst.CHANGE_ISOLATION_INSIDE_TX);
        }

        isReadOnly = isReadOnly || level != Connection.TRANSACTION_SERIALIZABLE;
        transactionLevel = level;
        txMode = txMode(transactionLevel, isReadOnly);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        ensureOpened();

        if (readOnly == isReadOnly) {
            return;
        }

        QueryTransaction localTx = tx.get();
        if (localTx != null && localTx.isActive()) {
            throw new SQLFeatureNotSupportedException(YdbConst.READONLY_INSIDE_TRANSACTION);
        }

        isReadOnly = readOnly;
        txMode = txMode(transactionLevel, isReadOnly);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        ensureOpened();

        if (autoCommit == isAutoCommit) {
            return;
        }

        QueryTransaction localTx = tx.get();
        if (localTx != null && localTx.isActive()) {
            throw new SQLFeatureNotSupportedException(YdbConst.CHANGE_ISOLATION_INSIDE_TX);
        }

        isAutoCommit = autoCommit;
    }

    @Override
    public boolean isClosed() throws SQLException {
        closeCurrentResult();
        return isClosed;
    }

    @Override
    public String txID() throws SQLException {
        closeCurrentResult();
        QueryTransaction localTx = tx.get();
        return localTx != null ? localTx.getId() : null;
    }

    @Override
    public boolean isInsideTransaction() throws SQLException {
        ensureOpened();
        QueryTransaction localTx = tx.get();
        return localTx != null && localTx.isActive();
    }

    @Override
    public boolean isAutoCommit() throws SQLException {
        ensureOpened();
        return isAutoCommit;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        ensureOpened();
        return isReadOnly;
    }

    @Override
    public int transactionLevel() throws SQLException {
        ensureOpened();
        return transactionLevel;
    }

    @Override
    public void commit(YdbContext ctx, YdbValidator validator) throws SQLException {
        ensureOpened();

        QueryTransaction localTx = tx.get();
        if (localTx == null || !localTx.isActive()) {
            return;
        }

        CommitTransactionSettings settings = ctx.withRequestTimeout(CommitTransactionSettings.newBuilder()).build();
        try {
            validator.clearWarnings();
            validator.call("Commit TxId: " + localTx.getId(), () -> localTx.commit(settings));
        } finally {
            if (tx.compareAndSet(localTx, null)) {
                localTx.getSession().close();
            }
        }
    }

    @Override
    public void rollback(YdbContext ctx, YdbValidator validator) throws SQLException {
        ensureOpened();

        QueryTransaction localTx = tx.get();
        if (localTx == null || !localTx.isActive()) {
            return;
        }

        RollbackTransactionSettings settings = ctx.withRequestTimeout(RollbackTransactionSettings.newBuilder())
            .build();

        try {
            validator.clearWarnings();
            validator.execute("Rollback TxId: " + localTx.getId(), () -> localTx.rollback(settings));
        } finally {
            if (tx.compareAndSet(localTx, null)) {
                localTx.getSession().close();
            }
        }
    }

    @Override
    public YdbQueryResult executeDataQuery(
            YdbStatement statement, YdbQuery query, String yql, Params params, long timeout, boolean keepInCache
    ) throws SQLException {
        ensureOpened();

        YdbValidator validator = statement.getValidator();

        ExecuteQuerySettings.Builder builder = ExecuteQuerySettings.newBuilder();
        if (timeout > 0) {
            builder = builder.withRequestTimeout(timeout, TimeUnit.SECONDS);
        }
        final ExecuteQuerySettings settings = builder.build();

        QueryTransaction nextTx = tx.get();
        while (nextTx == null) {
            nextTx = createNewQuerySession(validator).createNewTransaction(txMode);
            if (!tx.compareAndSet(null, nextTx)) {
                nextTx.getSession().close();
                nextTx = tx.get();
            }
        }

        final QueryTransaction localTx = nextTx;

        if (useStreamResultSet) {
            String msg = "STREAM_QUERY >>\n" + yql;
            StreamQueryResult lazy = validator.call(msg, () -> {
                final CompletableFuture<Result<StreamQueryResult>> future = new CompletableFuture<>();
                final QueryStream stream = localTx.createQuery(yql, isAutoCommit, params, settings);
                final StreamQueryResult result = new StreamQueryResult(msg, statement, query, stream::cancel);


                stream.execute(new QueryStream.PartsHandler() {
                    @Override
                    public void onIssues(Issue[] issues) {
                        validator.addStatusIssues(Arrays.asList(issues));
                    }

                    @Override
                    public void onNextPart(QueryResultPart part) {
                        result.onStreamResultSet((int) part.getResultSetIndex(), part.getResultSetReader());
                        future.complete(Result.success(result));
                    }
                }).whenComplete((res, th) -> {
                    if (!localTx.isActive()) {
                        if (tx.compareAndSet(localTx, null)) {
                            localTx.getSession().close();
                        }
                    }

                    if (th != null) {
                        future.completeExceptionally(th);
                        result.onStreamFinished(th);
                    }
                    if (res != null) {
                        validator.addStatusIssues(res.getStatus());
                        future.complete(res.isSuccess() ? Result.success(result) : Result.fail(res.getStatus()));
                        result.onStreamFinished(res.getStatus());
                    }
                });

                return future;
            });

            return updateCurrentResult(lazy);
        }

        try {
            QueryReader result = validator.call(QueryType.DATA_QUERY + " >>\n" + yql,
                    () -> QueryReader.readFrom(localTx.createQuery(yql, isAutoCommit, params, settings))
            );
            validator.addStatusIssues(result.getIssueList());

            List<YdbResultSet> readers = new ArrayList<>();
            for (ResultSetReader rst: result) {
                readers.add(new YdbStaticResultSet(statement, rst));
            }
            return updateCurrentResult(new StaticQueryResult(query, readers));
        } finally {
            if (!localTx.isActive()) {
                if (tx.compareAndSet(localTx, null)) {
                    localTx.getSession().close();
                }
            }
        }
    }

    @Override
    public YdbQueryResult executeSchemeQuery(YdbStatement statement, YdbQuery query) throws SQLException {
        ensureOpened();

        String yql = query.getPreparedYql();
        YdbContext ctx = statement.getConnection().getCtx();
        YdbValidator validator = statement.getValidator();

        // Scheme query does not affect transactions or result sets
        ExecuteQuerySettings settings = ctx.withRequestTimeout(ExecuteQuerySettings.newBuilder()).build();
        try (QuerySession session = createNewQuerySession(validator)) {
            validator.call(QueryType.SCHEME_QUERY + " >>\n" + yql, () -> session
                    .createQuery(yql, TxMode.NONE, Params.empty(), settings)
                    .execute(new IssueHandler(validator))
            );
        }

        return updateCurrentResult(new StaticQueryResult(query, Collections.emptyList()));
    }

    @Override
    public YdbQueryResult executeExplainQuery(YdbStatement statement, YdbQuery query) throws SQLException {
        ensureOpened();

        String yql = query.getPreparedYql();
        YdbContext ctx = statement.getConnection().getCtx();
        YdbValidator validator = statement.getValidator();

        // Scheme query does not affect transactions or result sets
        ExecuteQuerySettings settings = ctx.withRequestTimeout(ExecuteQuerySettings.newBuilder())
                .withExecMode(QueryExecMode.EXPLAIN)
                .build();

        try (QuerySession session = createNewQuerySession(validator)) {
            QueryInfo res = validator.call(QueryType.EXPLAIN_QUERY + " >>\n" + yql, () -> session
                    .createQuery(yql, TxMode.NONE, Params.empty(), settings)
                    .execute(new IssueHandler(validator))
            );

            if (!res.hasStats()) {
                throw new SQLException("No explain data");
            }

            return updateCurrentResult(
                    new StaticQueryResult(statement, res.getStats().getQueryAst(), res.getStats().getQueryPlan())
            );
        }
    }

    private class IssueHandler implements QueryStream.PartsHandler {
        private final YdbValidator validator;

        IssueHandler(YdbValidator validator) {
            this.validator = validator;
        }

        @Override
        public void onIssues(Issue[] issues) {
            validator.addStatusIssues(Arrays.asList(issues));
        }

        @Override
        public void onNextPart(QueryResultPart part) {
            // nothing
        }
    }

    @Override
    public boolean isValid(YdbValidator validator, int timeout) throws SQLException {
        ensureOpened();
        return true;
    }

    private static TxMode txMode(int level, boolean isReadOnly) throws SQLException {
        if (!isReadOnly) {
            // YDB support only one RW mode
            if (level != Connection.TRANSACTION_SERIALIZABLE) {
                throw new SQLException(YdbConst.UNSUPPORTED_TRANSACTION_LEVEL + level);
            }

            return TxMode.SERIALIZABLE_RW;
        }

        switch (level) {
            case Connection.TRANSACTION_SERIALIZABLE:
                return TxMode.SNAPSHOT_RO;
            case YdbConst.ONLINE_CONSISTENT_READ_ONLY:
                return TxMode.ONLINE_RO;
            case YdbConst.ONLINE_INCONSISTENT_READ_ONLY:
                return TxMode.ONLINE_INCONSISTENT_RO;
            case YdbConst.STALE_CONSISTENT_READ_ONLY:
                return TxMode.STALE_RO;
            default:
                throw new SQLException(YdbConst.UNSUPPORTED_TRANSACTION_LEVEL + level);
        }
    }
}
