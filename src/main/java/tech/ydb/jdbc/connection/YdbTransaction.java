package tech.ydb.jdbc.connection;

import java.sql.SQLException;

import tech.ydb.table.Session;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbTransaction extends AutoCloseable {
    interface Action<T> {
        T apply(Session session) throws SQLException;
    }

    String getId();
    boolean isAutoCommit();
    int transactionLevel();

    YdbTransaction commit() throws SQLException;
    YdbTransaction rollback() throws SQLException;

    YdbTransaction withAutoCommit(boolean value) throws SQLException;
    YdbTransaction withTransactionLevel(int level) throws SQLException;
    YdbTransaction withReadOnly(boolean value) throws SQLException;

    boolean keepAlive(long timeoutMS) throws SQLException;

    <T> T executeInTx(Action<T> action);

    @Override
    void close() throws SQLException;

////    private static final Logger LOGGER = Logger.getLogger(YdbConnectionState.class.getName());
////
////    private final int transactionLevel;
////    private final boolean autoCommit;
////
////    public YdbConnectionState(int transactionLevel, boolean autoCommit) throws SQLException {
////        this.transactionLevel = validateTransactionLevel(transactionLevel);
////        this.autoCommit = autoCommit;
////        LOGGER.log(Level.FINE,
////                "New connection state, isolation level: {0}, autoCommit = {1}",
////                new Object[] { transactionLevel, autoCommit }
////        );
////    }
////
////    static private int validateTransactionLevel(int level) throws SQLException {
////        switch (level) {
////            case YdbConst.TRANSACTION_SERIALIZABLE_READ_WRITE:
////            case YdbConst.ONLINE_CONSISTENT_READ_ONLY:
////            case YdbConst.STALE_CONSISTENT_READ_ONLY:
////            case YdbConst.ONLINE_INCONSISTENT_READ_ONLY:
////                return level;
////            default:
////                throw new SQLException(UNSUPPORTED_TRANSACTION_LEVEL + level);
////        }
////    }
////
////    public YdbConnectionState changeTransactionLevel(int level) throws SQLException {
////        if (transactionLevel == level) {
////            return this;
////        }
////        return new YdbConnectionState(level, autoCommit);
////    }
////
////    public YdbConnectionState setAutoCommit(boolean value) throws SQLException {
////        if (value == this.autoCommit) {
////            return this;
////        }
////
////        return new YdbConnectionState(transactionLevel, value);
////    }
////
////    public YdbConnectionState commit() {
//////        String txId = state.txId;
//////        if (txId != null) {
//////            this.clearWarnings();
//////            try {
//////                this.joinStatusImpl(
//////                        () -> "Commit TxId: " + txId,
//////                        () -> this.session.commitTransaction(txId, validator.init(new CommitTxSettings())));
//////                this.clearTx();
//////            } catch (YdbRetryableException e) {
//////                if (e.getStatusCode() == StatusCode.NOT_FOUND) {
//////                    this.clearTx();
//////                }
//////                throw e; // Should be thrown anyway
//////            }
//////        }
////    }
//
//    public YdbConnectionState rollback() {
//
//    }
}
