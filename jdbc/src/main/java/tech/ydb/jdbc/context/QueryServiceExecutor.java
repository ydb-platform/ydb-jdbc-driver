package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.query.ExplainedQuery;
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

    private int transactionLevel;
    private boolean isReadOnly;
    private boolean isAutoCommit;
    private TxMode txMode;

    private QueryTransaction tx;
    private boolean isClosed;

    public QueryServiceExecutor(YdbContext ctx, int transactionLevel, boolean autoCommit) throws SQLException {
        super(ctx);
        this.sessionTimeout = ctx.getOperationProperties().getSessionTimeout();
        this.queryClient = ctx.getQueryClient();
        this.transactionLevel = transactionLevel;
        this.isReadOnly = transactionLevel != Connection.TRANSACTION_SERIALIZABLE;
        this.isAutoCommit = autoCommit;
        this.txMode = txMode(transactionLevel, isReadOnly);
        this.tx = null;
        this.isClosed = false;
    }

    protected QuerySession createNewQuerySession(YdbValidator validator) throws SQLException {
        try {
            Result<QuerySession> session = queryClient.createSession(sessionTimeout).join();
            validator.addStatusIssues(session.getStatus());
            return session.getValue();
        } catch (UnexpectedResultException ex) {
            throw ExceptionFactory.createException("Cannot create session with " + ex.getStatus(), ex);
        }
    }

    @Override
    public void close() {
        cleanTx();
        isClosed = true;
    }

    private void cleanTx() {
        if (tx != null) {
            tx.getSession().close();
            tx = null;
        }
    }

    @Override
    public void setTransactionLevel(int level) throws SQLException {
        if (level == transactionLevel) {
            return;
        }

        if (tx != null && tx.isActive()) {
            throw new SQLFeatureNotSupportedException(YdbConst.CHANGE_ISOLATION_INSIDE_TX);
        }

        isReadOnly = isReadOnly || level != Connection.TRANSACTION_SERIALIZABLE;
        transactionLevel = level;
        txMode = txMode(transactionLevel, isReadOnly);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly == isReadOnly) {
            return;
        }

        if (tx != null && tx.isActive()) {
            throw new SQLFeatureNotSupportedException(YdbConst.READONLY_INSIDE_TRANSACTION);
        }

        isReadOnly = readOnly;
        txMode = txMode(transactionLevel, isReadOnly);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit == isAutoCommit) {
            return;
        }

        if (tx != null && tx.isActive()) {
            throw new SQLFeatureNotSupportedException(YdbConst.CHANGE_ISOLATION_INSIDE_TX);
        }

        isAutoCommit = autoCommit;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public String txID() {
        return tx != null ? tx.getId() : null;
    }

    @Override
    public boolean isInsideTransaction() throws SQLException {
        ensureOpened();
        return tx != null && tx.isActive();
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

        if (tx == null || !tx.isActive()) {
            return;
        }

        CommitTransactionSettings settings = ctx.withRequestTimeout(CommitTransactionSettings.newBuilder()).build();
        try {
            validator.clearWarnings();
            validator.call("Commit TxId: " + tx.getId(), () -> tx.commit(settings));
        } finally {
            cleanTx();
        }
    }

    @Override
    public void rollback(YdbContext ctx, YdbValidator validator) throws SQLException {
        ensureOpened();

        if (tx == null || !tx.isActive()) {
            return;
        }

        RollbackTransactionSettings settings = ctx.withRequestTimeout(RollbackTransactionSettings.newBuilder())
                .build();

        try {
            validator.clearWarnings();
            validator.execute("Rollback TxId: " + tx.getId(), () -> tx.rollback(settings));
        } finally {
            cleanTx();
        }
    }

    @Override
    public List<ResultSetReader> executeDataQuery(
            YdbContext ctx, YdbValidator validator, YdbQuery query, long timeout, boolean keepInCache, Params params
    ) throws SQLException {
        ensureOpened();

        final String yql = query.withParams(params);
        ExecuteQuerySettings.Builder builder = ExecuteQuerySettings.newBuilder();
        if (timeout > 0) {
            builder = builder.withRequestTimeout(timeout, TimeUnit.SECONDS);
        }
        final ExecuteQuerySettings settings = builder.build();

        if (tx == null) {
            tx = createNewQuerySession(validator).createNewTransaction(txMode);
        }

        try {
            QueryReader result = validator.call(QueryType.DATA_QUERY + " >>\n" + yql,
                    () -> QueryReader.readFrom(tx.createQuery(yql, isAutoCommit, params, settings))
            );
            validator.addStatusIssues(result.getIssueList());

            List<ResultSetReader> readers = new ArrayList<>();
            result.forEach(readers::add);
            return readers;
        } finally {
            if (!tx.isActive()) {
                cleanTx();
            }
        }
    }

    @Override
    public void executeSchemeQuery(YdbContext ctx, YdbValidator validator, YdbQuery query) throws SQLException {
        ensureOpened();

        // Scheme query does not affect transactions or result sets
        ExecuteQuerySettings settings = ctx.withRequestTimeout(ExecuteQuerySettings.newBuilder()).build();
        final String yql = query.withParams(null);

        try (QuerySession session = createNewQuerySession(validator)) {
            validator.call(QueryType.SCHEME_QUERY + " >>\n" + yql, () -> session
                    .createQuery(yql, TxMode.NONE, Params.empty(), settings)
                    .execute(new IssueHandler(validator))
            );
        }
    }

    @Override
    public ExplainedQuery executeExplainQuery(YdbContext ctx, YdbValidator validator, YdbQuery query)
            throws SQLException {
        ensureOpened();

        // Scheme query does not affect transactions or result sets
        ExecuteQuerySettings settings = ctx.withRequestTimeout(ExecuteQuerySettings.newBuilder())
                .withExecMode(QueryExecMode.EXPLAIN)
                .build();
        final String yql = query.withParams(null);

        try (QuerySession session = createNewQuerySession(validator)) {
            QueryInfo res = validator.call(QueryType.EXPLAIN_QUERY + " >>\n" + yql, () -> session
                    .createQuery(yql, TxMode.NONE, Params.empty(), settings)
                    .execute(new IssueHandler(validator))
            );

            if (!res.hasStats()) {
                throw new SQLException("No explain data");
            }
            return new ExplainedQuery(res.getStats().getQueryAst(), res.getStats().getQueryPlan());
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
