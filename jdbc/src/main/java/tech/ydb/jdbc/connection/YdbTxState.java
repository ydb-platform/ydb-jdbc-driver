package tech.ydb.jdbc.connection;

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
    private final TxControl<?> txControl;
    protected final int transactionLevel;

    protected YdbTxState(TxControl<?> tx, int level) {
        this.txControl = tx;
        this.transactionLevel = level;
    }

    @Override
    public String toString() {
        return "NoTx" + transactionLevel;
    }

    public String txID() {
        return null;
    }

    public TxControl<?> txControl() {
        return txControl;
    }

    public boolean isAutoCommit() {
        return txControl.isCommitTx();
    }

    public boolean isReadOnly() {
        return transactionLevel != YdbConst.TRANSACTION_SERIALIZABLE_READ_WRITE;
    }

    public boolean isInsideTransaction() {
        return false;
    }

    public int transactionLevel() throws SQLException {
        return transactionLevel;
    }

    public YdbTxState withAutoCommit(boolean newAutoCommit) throws SQLException {
        if (newAutoCommit == isAutoCommit()) {
            return this;
        }
        return create(transactionLevel(), newAutoCommit);
    }

    public YdbTxState withReadOnly(boolean readOnly) throws SQLException {
        if (readOnly == isReadOnly()) {
            return this;
        }

        if (readOnly) {
            return create(YdbConst.ONLINE_CONSISTENT_READ_ONLY, isAutoCommit());
        } else {
            return create(YdbConst.TRANSACTION_SERIALIZABLE_READ_WRITE, isAutoCommit());
        }
    }

    public YdbTxState withTransactionLevel(int newTransactionLevel) throws SQLException {
        if (newTransactionLevel == transactionLevel()) {
            return this;
        }

        return create(newTransactionLevel, isAutoCommit());
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
            return new TransactionInProgress(txID, session, isAutoCommit());
        }

        session.close();
        return this;
    }

    public Session getSession(YdbContext ctx, YdbExecutor executor) throws SQLException {
        return executor.createSession(ctx);
    }

    public static YdbTxState create(int level, boolean autoCommit) throws SQLException {
        return create(null, null, level, autoCommit);
    }

    private static YdbTxState create(Session session, String txId, int level, boolean autoCommit)
            throws SQLException {
        switch (level) {
            case YdbConst.TRANSACTION_SERIALIZABLE_READ_WRITE:
                if (txId != null) {
                    return new TransactionInProgress(txId, session, autoCommit);
                } else {
                    if (autoCommit) {
                        return new YdbTxState(TxControl.serializableRw(), level);
                    } else {
                        return new EmptyTransaction();
                    }
                }
            case YdbConst.ONLINE_CONSISTENT_READ_ONLY:
                return new YdbTxState(TxControl.onlineRo(), level);
            case YdbConst.STALE_CONSISTENT_READ_ONLY:
                return new YdbTxState(TxControl.staleRo(), level);
            case YdbConst.ONLINE_INCONSISTENT_READ_ONLY:
                return new YdbTxState(TxControl.onlineRo().setAllowInconsistentReads(true), level);
            default:
                throw new SQLException(YdbConst.UNSUPPORTED_TRANSACTION_LEVEL + level);
        }
    }

    private static class EmptyTransaction extends YdbTxState {
        EmptyTransaction() {
            super(TxControl.serializableRw().setCommitTx(false), YdbConst.TRANSACTION_SERIALIZABLE_READ_WRITE);
        }

        @Override
        public String toString() {
            return "EmptyTx" + transactionLevel;
        }

        @Override
        public YdbTxState withDataQuery(Session session, String txID) {
            if (txID != null && !txID.isEmpty()) {
                return new TransactionInProgress(txID, session, isAutoCommit());
            }

            session.close();
            return this;
        }
    }

    private static class TransactionInProgress extends YdbTxState {
        private final String id;
        private final Session session;

        TransactionInProgress(String id, Session session, boolean autoCommit) {
            super(TxControl.id(id).setCommitTx(autoCommit), YdbConst.TRANSACTION_SERIALIZABLE_READ_WRITE);
            this.id = id;
            this.session = session;
        }

        @Override
        public String toString() {
            return "InTx" + transactionLevel + "[" + id + "]";
        }

        @Override
        public String txID() {
            return id;
        }

        @Override
        public YdbTxState withAutoCommit(boolean newAutoCommit) throws SQLException {
            throw new SQLFeatureNotSupportedException(YdbConst.CHANGE_ISOLATION_INSIDE_TX);
        }

        @Override
        public YdbTxState withTransactionLevel(int newTransactionLevel) throws SQLException {
            throw new SQLFeatureNotSupportedException(YdbConst.CHANGE_ISOLATION_INSIDE_TX);
        }

        @Override
        public YdbTxState withReadOnly(boolean readOnly) throws SQLException {
            throw new SQLFeatureNotSupportedException(YdbConst.READONLY_INSIDE_TRANSACTION);
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
            return new EmptyTransaction();
        }

        @Override
        public YdbTxState withRollback(Session session) {
            session.close();
            return new EmptyTransaction();
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
                return new EmptyTransaction();
            }

            if (this.id.equals(txID)) {
                if (this.session == session) {
                    return this;
                }
                this.session.close();
                return new TransactionInProgress(txID, session, isAutoCommit());
            }

            session.close();
            return this;
        }
    }
}
