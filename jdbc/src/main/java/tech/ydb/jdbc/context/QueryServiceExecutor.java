package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import tech.ydb.core.Result;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryTx;
import tech.ydb.query.impl.TxImpl;
import tech.ydb.query.settings.CommitTransactionSettings;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.settings.RollbackTransactionSettings;
import tech.ydb.query.tools.QueryDataReader;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class QueryServiceExecutor extends BaseYdbExecutor {
    private final Duration sessionTimeout;
    private final QueryClient queryClient;
    private volatile TxState tx;

    public QueryServiceExecutor(YdbContext ctx, int transactionLevel, boolean autoCommit) throws SQLException {
        super(ctx);
        this.sessionTimeout = ctx.getOperationProperties().getSessionTimeout();
        this.queryClient = ctx.getQueryClient();
        this.tx = createTx(transactionLevel, autoCommit);
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
        tx = null;
    }

    private void updateState(TxState newTx) {
        if (this.tx == newTx || this.tx == null) {
            return;
        }
        this.tx = newTx;
    }

    @Override
    public void setTransactionLevel(int level) throws SQLException {
        updateState(tx.withTransactionLevel(level));
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        updateState(tx.withReadOnly(readOnly));
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        updateState(tx.withAutoCommit(autoCommit));
    }

    @Override
    public boolean isClosed() {
        return tx == null;
    }

    @Override
    public String txID() {
        return tx != null ? tx.txID() : null;
    }

    @Override
    public boolean isInsideTransaction() throws SQLException {
        ensureOpened();
        return tx.isInsideTransaction();
    }

    @Override
    public boolean isAutoCommit() throws SQLException {
        ensureOpened();
        return tx.isAutoCommit();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        ensureOpened();
        return tx.isReadOnly();
    }

    @Override
    public int transactionLevel() throws SQLException {
        ensureOpened();
        return tx.transactionLevel();
    }

    @Override
    public void commit(YdbContext ctx, YdbValidator validator) throws SQLException {
        ensureOpened();

        if (!isInsideTransaction()) {
            return;
        }

        QuerySession session = tx.getSession(validator);
        CommitTransactionSettings settings = ctx.withOperationTimeout(CommitTransactionSettings.newBuilder())
                .build();

        try {
            validator.clearWarnings();
            validator.execute(
                    "Commit TxId: " + tx.txID(),
                    () -> session.commitTransaction(tx.txCtrlID(), settings)
            );
        } finally {
            updateState(tx.withCommit(session));
        }
    }

    @Override
    public void rollback(YdbContext ctx, YdbValidator validator) throws SQLException {
        ensureOpened();

        if (!isInsideTransaction()) {
            return;
        }

        QuerySession session = tx.getSession(validator);
        RollbackTransactionSettings settings = ctx.withOperationTimeout(RollbackTransactionSettings.newBuilder())
                .build();

        try {
            validator.clearWarnings();
            validator.execute(
                    "Rollback TxId: " + tx.txID(),
                    () -> session.rollbackTransaction(tx.txCtrlID(), settings)
            );
        } finally {
            updateState(tx.withRollback(session));
        }
    }

    @Override
    public List<ResultSetReader> executeDataQuery(
            YdbContext ctx, YdbValidator validator, YdbQuery query, int timeout, boolean keepInCache, Params params
    ) throws SQLException {
        ensureOpened();

        final String yql = query.getYqlQuery(params);
        final QuerySession session = tx.getSession(validator);
        ExecuteQuerySettings.Builder builder = ExecuteQuerySettings.newBuilder();
        if (timeout > 0) {
            builder = builder.withRequestTimeout(timeout, TimeUnit.SECONDS);
        }
        final ExecuteQuerySettings settings = builder.build();

        try {
            QueryDataReader result = validator.call(
                    QueryType.DATA_QUERY + " >>\n" + yql,
                    () -> QueryDataReader.readFrom(session.executeQuery(yql, tx.txCtrl(), params, settings))
            );
            updateState(tx.withTxID(session, result.txId()));

            List<ResultSetReader> readers = new ArrayList<>();
            for (int idx = 0; idx < result.getResultSetCount(); idx += 1) {
                readers.add(result.getResultSet(idx));
            }

            return readers;
        } catch (SQLException | RuntimeException ex) {
            updateState(tx.withRollback(session));
            throw ex;
        }
    }

    @Override
    public boolean isValid(YdbValidator validator, int timeout) throws SQLException {
        ensureOpened();
        return true;
    }

    private class TxState {
        private final int transactionLevel;
        private final boolean isReadOnly;
        private final boolean isAutoCommit;

        private final QueryTx txCtrl;

        protected TxState(QueryTx txCtrl, int level, boolean isReadOnly, boolean isAutoCommit) {
            this.transactionLevel = level;
            this.isReadOnly = isReadOnly;
            this.isAutoCommit = isAutoCommit;
            this.txCtrl = txCtrl;
        }

        protected TxState(QueryTx txCtrl, TxState other) {
            this.transactionLevel = other.transactionLevel;
            this.isReadOnly = other.isReadOnly;
            this.isAutoCommit = other.isAutoCommit;
            this.txCtrl = txCtrl;
        }

        @Override
        public String toString() {
            return "NoTx";
        }

        public String txID() {
            return null;
        }

        public QueryTx txCtrl() {
            return txCtrl;
        }

        public QueryTx.Id txCtrlID() {
            return null;
        }

        public boolean isInsideTransaction() {
            return false;
        }

        public boolean isAutoCommit() {
            return isAutoCommit;
        }

        public boolean isReadOnly() {
            return isReadOnly;
        }

        public int transactionLevel() {
            return transactionLevel;
        }

        public TxState withAutoCommit(boolean newAutoCommit) throws SQLException {
            if (newAutoCommit == isAutoCommit) {
                return this;
            }

            if (isInsideTransaction()) {
                throw new SQLFeatureNotSupportedException(YdbConst.CHANGE_ISOLATION_INSIDE_TX);
            }

            return emptyTx(transactionLevel, isReadOnly, newAutoCommit);
        }

        public TxState withReadOnly(boolean newReadOnly) throws SQLException {
            if (newReadOnly == isReadOnly()) {
                return this;
            }

            if (isInsideTransaction()) {
                throw new SQLFeatureNotSupportedException(YdbConst.READONLY_INSIDE_TRANSACTION);
            }

            return emptyTx(transactionLevel, newReadOnly, isAutoCommit);
        }

        public TxState withTransactionLevel(int newTransactionLevel) throws SQLException {
            if (newTransactionLevel == transactionLevel) {
                return this;
            }

            if (isInsideTransaction()) {
                throw new SQLFeatureNotSupportedException(YdbConst.CHANGE_ISOLATION_INSIDE_TX);
            }

            boolean newReadOnly = isReadOnly || newTransactionLevel != Connection.TRANSACTION_SERIALIZABLE;
            return emptyTx(newTransactionLevel, newReadOnly, isAutoCommit);
        }

        public TxState withCommit(QuerySession session) {
            session.close();
            return this;
        }

        public TxState withRollback(QuerySession session) {
            session.close();
            return this;
        }

        public TxState withKeepAlive(QuerySession session) {
            session.close();
            return this;
        }

        public TxState withTxID(QuerySession session, QueryTx.Id txID) {
            if (txID != null) {
                return new TransactionInProgress(txID, session, this);
            }

            session.close();
            return this;
        }

        public QuerySession getSession(YdbValidator validator) throws SQLException {
            return createNewQuerySession(validator);
        }
    }

    private class TransactionInProgress extends TxState {
        private final QueryTx.Id tx;
        private final QuerySession session;
        private final TxState previos;

        TransactionInProgress(QueryTx.Id tx, QuerySession session, TxState previosState) {
            super(tx, previosState);
            this.tx = tx;
            this.session = session;
            this.previos = previosState;
        }

        @Override
        public String toString() {
            return "InTx" + transactionLevel() + "[" + tx.txId() + "]";
        }

        @Override
        public String txID() {
            return tx.txId();
        }

        @Override
        public QueryTx.Id txCtrlID() {
            return tx;
        }

        @Override
        public boolean isInsideTransaction() {
            return true;
        }

        @Override
        public QuerySession getSession(YdbValidator validator) throws SQLException {
            return session;
        }

        @Override
        public TxState withCommit(QuerySession session) {
            session.close();
            return previos;
        }

        @Override
        public TxState withRollback(QuerySession session) {
            session.close();
            return previos;
        }

        @Override
        public TxState withKeepAlive(QuerySession session) {
            return this;
        }

        @Override
        public TxState withTxID(QuerySession session, QueryTx.Id txID) {
            if (txID == null) {
                if (this.session != session) {
                    session.close();
                }
                this.session.close();
                return previos;
            }

            if (txID.txId().equals(tx.txId())) {
                if (this.session == session) {
                    return this;
                }
                this.session.close();
                return new TransactionInProgress(txID, session, previos);
            }

            session.close();
            return this;
        }
    }

    private TxState emptyTx(int level, boolean isReadOnly, boolean isAutoCommit) throws SQLException {
        QueryTx.Mode txCtrl = txMode(level, isReadOnly, isAutoCommit);
        return new TxState(txCtrl, level, isReadOnly, isAutoCommit);
    }

    private TxState createTx(int level, boolean isAutoCommit) throws SQLException {
        return emptyTx(level, level != Connection.TRANSACTION_SERIALIZABLE, isAutoCommit);
    }

    private static QueryTx.Mode txMode(int level, boolean isReadOnly, boolean isAutoCommit) throws SQLException {
        if (!isReadOnly) {
            // YDB support only one RW mode
            if (level != Connection.TRANSACTION_SERIALIZABLE) {
                throw new SQLException(YdbConst.UNSUPPORTED_TRANSACTION_LEVEL + level);
            }

            return QueryTx.serializableRw().setCommitTx(isAutoCommit);
        }

        switch (level) {
            case Connection.TRANSACTION_SERIALIZABLE:
                return QueryTx.snapshotRo().setCommitTx(isAutoCommit);
            case YdbConst.ONLINE_CONSISTENT_READ_ONLY:
                return TxImpl.onlineRo().setAllowInconsistentReads(false).setCommitTx(isAutoCommit);
            case YdbConst.ONLINE_INCONSISTENT_READ_ONLY:
                return TxImpl.onlineRo().setAllowInconsistentReads(true).setCommitTx(isAutoCommit);
            case YdbConst.STALE_CONSISTENT_READ_ONLY:
                return QueryTx.staleRo().setCommitTx(isAutoCommit);
            default:
                throw new SQLException(YdbConst.UNSUPPORTED_TRANSACTION_LEVEL + level);
        }
    }

}
