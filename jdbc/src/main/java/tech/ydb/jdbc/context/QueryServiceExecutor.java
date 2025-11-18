package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Issue;
import tech.ydb.core.Status;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbQueryResult;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.YdbTracer;
import tech.ydb.jdbc.impl.YdbQueryResultExplain;
import tech.ydb.jdbc.impl.YdbQueryResultReader;
import tech.ydb.jdbc.impl.YdbQueryResultStatic;
import tech.ydb.jdbc.impl.YdbResultSetMemory;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.result.QueryInfo;
import tech.ydb.query.result.QueryResultPart;
import tech.ydb.query.settings.CommitTransactionSettings;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.settings.QueryExecMode;
import tech.ydb.query.settings.QueryStatsMode;
import tech.ydb.query.settings.RollbackTransactionSettings;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.query.Params;

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

    public QueryServiceExecutor(YdbContext ctx) throws SQLException {
        super(ctx);
        YdbOperationProperties options = ctx.getOperationProperties();
        this.sessionTimeout = options.getSessionTimeout();
        this.queryClient = ctx.getQueryClient();
        this.useStreamResultSet = options.getUseStreamResultSets();

        this.transactionLevel = options.getTransactionLevel();
        this.isAutoCommit = options.isAutoCommit();
        this.isReadOnly = transactionLevel != Connection.TRANSACTION_SERIALIZABLE;
        this.txMode = txMode(transactionLevel, isReadOnly);
        this.isClosed = false;
    }

    protected QuerySession createNewQuerySession(YdbValidator validator) throws SQLException {
        return validator.call("Get query session", null, () -> queryClient.createSession(sessionTimeout));
    }

    @Override
    public void close() throws SQLException {
        clearState();
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
        return isClosed;
    }

    @Override
    public String txID() throws SQLException {
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

        try {
            commitImpl(ctx, validator, localTx);
        } finally {
            if (tx.compareAndSet(localTx, null)) {
                localTx.getSession().close();
            }
            ctx.getTracer().close();
        }
    }

    protected void commitImpl(YdbContext ctx, YdbValidator validator, QueryTransaction tx) throws SQLException {
        YdbTracer tracer = ctx.getTracer();
        tracer.trace("--> commit");
        tracer.query(null);

        CommitTransactionSettings settings = ctx.withRequestTimeout(CommitTransactionSettings.newBuilder()).build();
        validator.clearWarnings();
        validator.call("Commit TxId: " + tx.getId(), tracer, () -> tx.commit(settings));
    }

    @Override
    public void rollback(YdbContext ctx, YdbValidator validator) throws SQLException {
        ensureOpened();

        QueryTransaction localTx = tx.get();
        if (localTx == null || !localTx.isActive()) {
            return;
        }

        YdbTracer tracer = ctx.getTracer();
        tracer.trace("--> rollback");
        tracer.query(null);

        RollbackTransactionSettings settings = ctx.withRequestTimeout(RollbackTransactionSettings.newBuilder())
            .build();

        try {
            validator.clearWarnings();
            validator.execute("Rollback TxId: " + localTx.getId(), tracer, () -> localTx.rollback(settings));
        } finally {
            if (tx.compareAndSet(localTx, null)) {
                localTx.getSession().close();
            }
            tracer.close();
        }
    }

    @Override
    protected YdbQueryResult executeQueryImpl(YdbStatement statement, YdbQuery query, String preparedYql, Params params)
            throws SQLException {
        ensureOpened();

        YdbValidator validator = statement.getValidator();

        int timeout = statement.getQueryTimeout();
        ExecuteQuerySettings.Builder settings = ExecuteQuerySettings.newBuilder();
        settings = settings.withStatsMode(QueryStatsMode.valueOf(statement.getStatsCollectionMode().name()));
        if (timeout > 0) {
            settings = settings.withRequestTimeout(timeout, TimeUnit.SECONDS);
        }

        QueryTransaction nextTx = tx.get();
        while (nextTx == null) {
            nextTx = createNewQuerySession(validator).createNewTransaction(txMode);
            if (!tx.compareAndSet(null, nextTx)) {
                nextTx.getSession().close();
                nextTx = tx.get();
            }
        }

        final QueryTransaction localTx = nextTx;
        YdbTracer tracer = statement.getConnection().getCtx().getTracer();

        String yql = prefixPragma + preparedYql;

        if (useStreamResultSet) {
            tracer.trace("--> stream query");
            tracer.query(yql);
            String msg = "STREAM_QUERY >>\n" + yql;

            final YdbQueryResultReader reader = new YdbQueryResultReader(types, statement, query) {
                @Override
                public void onClose(Status status, Throwable th) {
                    if (th != null) {
                        tracer.trace("<-- " + th.getMessage());
                    }
                    if (status != null) {
                        validator.addStatusIssues(status);
                        tracer.trace("<-- " + status.toString());
                    }

                    if (localTx.isActive()) {
                        tracer.setId(localTx.getId());
                    } else {
                        if (tx.compareAndSet(localTx, null)) {
                            localTx.getSession().close();
                        }
                        tracer.close();
                    }

                    super.onClose(status, th);
                }
            };

            settings = settings.withGrpcFlowControl(reader);
            QueryStream stream = localTx.createQuery(yql, isAutoCommit, params, settings.build());
            validator.execute(msg, tracer, () -> reader.load(validator, stream));
            return updateCurrentResult(reader);
        }

        try {
            tracer.trace("--> data query");
            tracer.query(yql);
            ExecuteQuerySettings requestSettings = settings.build();

            QueryReader result = validator.call(QueryType.DATA_QUERY + " >>\n" + yql, tracer,
                    () -> QueryReader.readFrom(localTx.createQuery(yql, isAutoCommit, params, requestSettings))
            );
            validator.addStatusIssues(result.getIssueList());

            YdbResultSet[] readers = new YdbResultSet[result.getResultSetCount()];
            for (int idx = 0; idx < readers.length; idx++) {
                readers[idx] = new YdbResultSetMemory(types, statement, result.getResultSet(idx));
            }

            YdbQueryResultStatic queryResult = new YdbQueryResultStatic(query, readers);
            queryResult.setQueryStats(result.getQueryInfo().getStats());
            return updateCurrentResult(queryResult);
        } finally {
            if (!localTx.isActive()) {
                if (tx.compareAndSet(localTx, null)) {
                    localTx.getSession().close();
                }
            }

            if (localTx.isActive()) {
                tracer.setId(localTx.getId());
            } else {
                tracer.close();
            }
        }
    }

    @Override
    public YdbQueryResult executeSchemeQuery(YdbStatement statement, YdbQuery query) throws SQLException {
        ensureOpened();

        String yql = prefixPragma + query.getPreparedYql();
        YdbContext ctx = statement.getConnection().getCtx();
        YdbValidator validator = statement.getValidator();

        // Scheme query does not affect transactions or result sets
        YdbTracer tracer = ctx.getTracer();
        tracer.trace("--> scheme query");
        tracer.query(yql);

        ExecuteQuerySettings settings = ctx.withRequestTimeout(ExecuteQuerySettings.newBuilder()).build();
        try (QuerySession session = createNewQuerySession(validator)) {
            validator.call(QueryType.SCHEME_QUERY + " >>\n" + yql, tracer, () -> session
                    .createQuery(yql, TxMode.NONE, Params.empty(), settings)
                    .execute(new IssueHandler(validator))
            );
        } finally {
            if (tx.get() == null) {
                tracer.close();
            }
        }

        return updateCurrentResult(new YdbQueryResultStatic(query));
    }

    @Override
    public YdbQueryResult executeExplainQuery(YdbStatement statement, YdbQuery query) throws SQLException {
        ensureOpened();

        String yql = prefixPragma + query.getPreparedYql();
        YdbContext ctx = statement.getConnection().getCtx();
        YdbValidator validator = statement.getValidator();

        // Scheme query does not affect transactions or result sets
        ExecuteQuerySettings settings = ctx.withRequestTimeout(ExecuteQuerySettings.newBuilder())
                .withExecMode(QueryExecMode.EXPLAIN)
                .build();
        YdbTracer tracer = ctx.getTracer();
        tracer.trace("--> explain query");
        tracer.query(yql);

        try (QuerySession session = createNewQuerySession(validator)) {
            QueryInfo res = validator.call(QueryType.EXPLAIN_QUERY + " >>\n" + yql, tracer, () -> session
                    .createQuery(yql, TxMode.NONE, Params.empty(), settings)
                    .execute(new IssueHandler(validator))
            );

            if (!res.hasStats()) {
                throw new SQLException("No explain data");
            }

            String ast = res.getStats().getQueryAst();
            String plan = res.getStats().getQueryPlan();
            return updateCurrentResult(new YdbQueryResultExplain(types, statement, ast, plan));
        } finally {
            if (tx.get() == null) {
                tracer.close();
            }
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
