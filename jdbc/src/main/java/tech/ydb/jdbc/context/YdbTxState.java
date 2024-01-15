package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.table.Session;
import tech.ydb.table.transaction.TxControl;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbTxState {
    private final int transactionLevel;
    private final boolean isReadOnly;
    private final boolean isAutoCommit;

    private final TxControl<?> txControl;

    protected YdbTxState(TxControl<?> txControl, int level, boolean isReadOnly, boolean isAutoCommit) {
        this.transactionLevel = level;
        this.isReadOnly = isReadOnly;
        this.isAutoCommit = isAutoCommit;
        this.txControl = txControl;
    }

    protected YdbTxState(TxControl<?> txControl, YdbTxState other) {
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

    public YdbTxState withAutoCommit(boolean newAutoCommit) throws SQLException {
        if (newAutoCommit == isAutoCommit) {
            return this;
        }

        if (isInsideTransaction()) {
            throw new SQLFeatureNotSupportedException(YdbConst.CHANGE_ISOLATION_INSIDE_TX);
        }

        return emptyTx(transactionLevel, isReadOnly, newAutoCommit);
    }

    public YdbTxState withReadOnly(boolean newReadOnly) throws SQLException {
        if (newReadOnly == isReadOnly()) {
            return this;
        }

        if (isInsideTransaction()) {
            throw new SQLFeatureNotSupportedException(YdbConst.READONLY_INSIDE_TRANSACTION);
        }

        return emptyTx(transactionLevel, newReadOnly, isAutoCommit);
    }

    public YdbTxState withTransactionLevel(int newTransactionLevel) throws SQLException {
        if (newTransactionLevel == transactionLevel) {
            return this;
        }

        if (isInsideTransaction()) {
            throw new SQLFeatureNotSupportedException(YdbConst.CHANGE_ISOLATION_INSIDE_TX);
        }

        boolean newReadOnly = isReadOnly || newTransactionLevel != Connection.TRANSACTION_SERIALIZABLE;
        return emptyTx(newTransactionLevel, newReadOnly, isAutoCommit);
    }

    public YdbTxState withCommit(Session session) {
        session.close();
        return this;
    }

    public YdbTxState withRollback(Session session) {
        session.close();
        return this;
    }

    public YdbTxState withKeepAlive(Session session) {
        session.close();
        return this;
    }

    public YdbTxState withDataQuery(Session session, String txID) {
        if (txID != null && !txID.isEmpty()) {
            return new TransactionInProgress(txID, session, this);
        }

        session.close();
        return this;
    }

    public Session getSession(YdbContext ctx, YdbExecutor executor) throws SQLException {
        return executor.createSession(ctx);
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

    private static YdbTxState emptyTx(int level, boolean isReadOnly, boolean isAutoCommit) throws SQLException {
        TxControl<?> tx = txControl(level, isReadOnly, isAutoCommit);
        return new YdbTxState(tx, level, isReadOnly, isAutoCommit);
    }

    public static YdbTxState create(int level, boolean isAutoCommit) throws SQLException {
        return emptyTx(level, level != Connection.TRANSACTION_SERIALIZABLE, isAutoCommit);
    }

    private static class TransactionInProgress extends YdbTxState {
        private final String txID;
        private final Session session;
        private final YdbTxState previos;

        TransactionInProgress(String id, Session session, YdbTxState previosState) {
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
        public Session getSession(YdbContext ctx, YdbExecutor executor) throws SQLException {
            return session;
        }

        @Override
        public YdbTxState withCommit(Session session) {
            session.close();
            return previos;
        }

        @Override
        public YdbTxState withRollback(Session session) {
            session.close();
            return previos;
        }

        @Override
        public YdbTxState withKeepAlive(Session session) {
            return this;
        }

        @Override
        public YdbTxState withDataQuery(Session session, String txID) {
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
}
