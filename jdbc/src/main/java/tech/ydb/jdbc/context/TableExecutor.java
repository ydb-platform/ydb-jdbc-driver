package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.Session;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.CommitTxSettings;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.settings.KeepAliveSessionSettings;
import tech.ydb.table.settings.RollbackTxSettings;
import tech.ydb.table.transaction.TxControl;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class TableExecutor extends BaseYdbExecutor {
    private volatile TxState tx;

    public TableExecutor(YdbContext ctx, int transactionLevel, boolean autoCommit) throws SQLException {
        super(ctx);
        this.tx = createTx(transactionLevel, autoCommit);
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

        Session session = tx.getSession(validator);
        CommitTxSettings settings = ctx.withDefaultTimeout(new CommitTxSettings());

        try {
            validator.clearWarnings();
            validator.execute(
                    "Commit TxId: " + tx.txID(),
                    () -> session.commitTransaction(tx.txID(), settings)
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

        Session session = tx.getSession(validator);
        RollbackTxSettings settings = ctx.withDefaultTimeout(new RollbackTxSettings());

        try {
            validator.clearWarnings();
            validator.execute(
                    "Rollback TxId: " + tx.txID(),
                    () -> session.rollbackTransaction(tx.txID(), settings)
            );
        } finally {
            updateState(tx.withRollback(session));
        }
    }

    private ExecuteDataQuerySettings dataQuerySettings(int timeout, boolean keepInCache) {
        ExecuteDataQuerySettings settings = new ExecuteDataQuerySettings();
        if (timeout > 0) {
            settings = settings
                    .setOperationTimeout(Duration.ofSeconds(timeout))
                    .setTimeout(Duration.ofSeconds(timeout + 1));
        }
        if (!keepInCache) {
            settings = settings.disableQueryCache();
        }

        return settings;
    }

    @Override
    public List<ResultSetReader> executeDataQuery(
            YdbContext ctx, YdbValidator validator, YdbQuery query, int timeout, boolean keepInCache, Params params
    ) throws SQLException {
        ensureOpened();

        final String yql = query.getYqlQuery(params);
        final Session session = tx.getSession(validator);
        try {
            DataQueryResult result = validator.call(
                    QueryType.DATA_QUERY + " >>\n" + yql,
                    () -> session.executeDataQuery(yql, tx.txControl(), params, dataQuerySettings(timeout, keepInCache))
            );
            updateState(tx.withDataQuery(session, result.getTxId()));

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

        Session session = tx.getSession(validator);
        try {
            KeepAliveSessionSettings settings = new KeepAliveSessionSettings().setTimeout(Duration.ofSeconds(timeout));
            Session.State keepAlive = validator.call(
                    "Keep alive: " + tx.txID(),
                    () -> session.keepAlive(settings)
            );
            return keepAlive == Session.State.READY;
        } finally {
            updateState(tx.withKeepAlive(session));
        }
    }

    private class TxState {
        private final int transactionLevel;
        private final boolean isReadOnly;
        private final boolean isAutoCommit;

        private final TxControl<?> txControl;

        protected TxState(TxControl<?> txControl, int level, boolean isReadOnly, boolean isAutoCommit) {
            this.transactionLevel = level;
            this.isReadOnly = isReadOnly;
            this.isAutoCommit = isAutoCommit;
            this.txControl = txControl;
        }

        protected TxState(TxControl<?> txControl, TxState other) {
            this.transactionLevel = other.transactionLevel;
            this.isReadOnly = other.isReadOnly;
            this.isAutoCommit = other.isAutoCommit;
            this.txControl = txControl;
        }

        @Override
        public String toString() {
            return "NoTx";
        }

        public String txID() {
            return null;
        }

        public boolean isInsideTransaction() {
            return false;
        }

        public TxControl<?> txControl() {
            return txControl;
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

        public TxState withCommit(Session session) {
            session.close();
            return this;
        }

        public TxState withRollback(Session session) {
            session.close();
            return this;
        }

        public TxState withKeepAlive(Session session) {
            session.close();
            return this;
        }

        public TxState withDataQuery(Session session, String txID) {
            if (txID != null && !txID.isEmpty()) {
                return new TransactionInProgress(txID, session, this);
            }

            session.close();
            return this;
        }

        public Session getSession(YdbValidator validator) throws SQLException {
            return createNewTableSession(validator);
        }
    }

    private class TransactionInProgress extends TxState {
        private final String txID;
        private final Session session;
        private final TxState previos;

        TransactionInProgress(String id, Session session, TxState previosState) {
            super(TxControl.id(id).setCommitTx(previosState.isAutoCommit), previosState);
            this.txID = id;
            this.session = session;
            this.previos = previosState;
        }

        @Override
        public String toString() {
            return "InTx" + transactionLevel() + "[" + txID + "]";
        }

        @Override
        public String txID() {
            return txID;
        }

        @Override
        public boolean isInsideTransaction() {
            return true;
        }

        @Override
        public Session getSession(YdbValidator validator) throws SQLException {
            return session;
        }

        @Override
        public TxState withCommit(Session session) {
            session.close();
            return previos;
        }

        @Override
        public TxState withRollback(Session session) {
            session.close();
            return previos;
        }

        @Override
        public TxState withKeepAlive(Session session) {
            return this;
        }

        @Override
        public TxState withDataQuery(Session session, String txID) {
            if (txID == null || txID.isEmpty()) {
                if (this.session != session) {
                    session.close();
                }
                this.session.close();
                return previos;
            }

            if (txID.equals(txID())) {
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
        TxControl<?> txCtrl = txControl(level, isReadOnly, isAutoCommit);
        return new TxState(txCtrl, level, isReadOnly, isAutoCommit);
    }

    private TxState createTx(int level, boolean isAutoCommit) throws SQLException {
        return emptyTx(level, level != Connection.TRANSACTION_SERIALIZABLE, isAutoCommit);
    }

    private static TxControl<?> txControl(int level, boolean isReadOnly, boolean isAutoCommit) throws SQLException {
        if (!isReadOnly) {
            // YDB support only one RW mode
            if (level != Connection.TRANSACTION_SERIALIZABLE) {
                throw new SQLException(YdbConst.UNSUPPORTED_TRANSACTION_LEVEL + level);
            }

            return TxControl.serializableRw().setCommitTx(isAutoCommit);
        }

        switch (level) {
            case Connection.TRANSACTION_SERIALIZABLE:
                return TxControl.snapshotRo().setCommitTx(isAutoCommit);
            case YdbConst.ONLINE_CONSISTENT_READ_ONLY:
                return TxControl.onlineRo().setAllowInconsistentReads(false).setCommitTx(isAutoCommit);
            case YdbConst.ONLINE_INCONSISTENT_READ_ONLY:
                return TxControl.onlineRo().setAllowInconsistentReads(true).setCommitTx(isAutoCommit);
            case YdbConst.STALE_CONSISTENT_READ_ONLY:
                return TxControl.staleRo().setCommitTx(isAutoCommit);
            default:
                throw new SQLException(YdbConst.UNSUPPORTED_TRANSACTION_LEVEL + level);
        }
    }
}
