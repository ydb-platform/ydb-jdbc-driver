package tech.ydb.jdbc.connection;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;

import com.google.common.base.Suppliers;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbDatabaseMetaData;
import tech.ydb.jdbc.YdbPreparedStatement;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.YdbTypes;
import tech.ydb.jdbc.common.YdbQuery;
import tech.ydb.jdbc.common.YdbWarnings;
import tech.ydb.jdbc.impl.YdbTypesImpl;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.jdbc.statement.YdbPreparedStatementImpl;
import tech.ydb.jdbc.statement.YdbPreparedStatementWithDataQueryImpl;
import tech.ydb.jdbc.statement.YdbStatementImpl;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.TableClient;

import static tech.ydb.jdbc.YdbConst.ABORT_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.ARRAYS_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.AUTO_GENERATED_KEYS_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.BLOB_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.CANNOT_UNWRAP_TO;
import static tech.ydb.jdbc.YdbConst.CLOB_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.CLOSED_CONNECTION;
import static tech.ydb.jdbc.YdbConst.NCLOB_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.PREPARED_CALLS_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.RESULT_SET_CONCURRENCY_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.RESULT_SET_HOLDABILITY_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.RESULT_SET_TYPE_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.SAVEPOINTS_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.SET_NETWORK_TIMEOUT_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.SQLXML_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.STRUCTS_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.TRANSACTION_SERIALIZABLE_READ_WRITE;

public class YdbConnectionImpl implements YdbConnection {
    private static final Logger LOGGER = Logger.getLogger(YdbConnectionImpl.class.getName());

//    private final TableClient tableClient;
    private final SchemeClient schemeClient;
    private final YdbOperationProperties properties;
    private final YdbWarnings warnings = new YdbWarnings();
//    private final Validator validator;

    private final Supplier<YdbDatabaseMetaData> metaDataSupplier;

    private final String url;
    private final String database;

    private YdbTransaction transaction;

    public YdbConnectionImpl(
            TableClient tableClient,
            SchemeClient schemeClient,
            YdbOperationProperties properties,
            String url, String database) throws SQLException {
//        this.tableClient = Objects.requireNonNull(tableClient);
        this.schemeClient = Objects.requireNonNull(schemeClient);
        this.properties = Objects.requireNonNull(properties);
        this.url = Objects.requireNonNull(url);
        this.database = database;

        this.metaDataSupplier = Suppliers.memoize(() -> new YdbDatabaseMetaDataImpl(this))::get;


//        this.transaction = new AtomicReference<>(new YdbConnectionState(properties.getTransactionLevel(), properties.isAutoCommit()));
        this.transaction = null;
    }

    @Override
    public YdbStatement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public String nativeSQL(String sql) {
        return new YdbQuery(properties, sql).nativeSql();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        ensureOpened();
        transaction = transaction.withAutoCommit(autoCommit);

//        boolean changed = state.autoCommit != autoCommit;
//        state.autoCommit = autoCommit;
//        if (changed) {
//            LOGGER.log(Level.FINE, "Set auto-commit: {0}", autoCommit);
//            if (autoCommit) {
//                this.commit();
//            }
//        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        ensureOpened();
        return transaction.isAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        ensureOpened();
        transaction = transaction.commit();
//
//
//        String txId = state.txId;
//        if (txId != null) {
//            this.clearWarnings();
//            try {
//                this.joinStatusImpl(
//                        () -> "Commit TxId: " + txId,
//                        () -> this.session.commitTransaction(txId, validator.init(new CommitTxSettings())));
//                this.clearTx();
//            } catch (YdbRetryableException e) {
//                if (e.getStatusCode() == StatusCode.NOT_FOUND) {
//                    this.clearTx();
//                }
//                throw e; // Should be thrown anyway
//            }
//        }
    }

    @Override
    public void rollback() throws SQLException {
        ensureOpened();
        transaction = transaction.rollback();

//        String txId = state.txId;
//        if (txId != null) {
//            this.clearWarnings();
//            try {
//                this.joinStatusImpl(
//                        () -> "Rollback TxId: " + txId,
//                        () -> this.session.rollbackTransaction(txId, validator.init(new RollbackTxSettings())));
//                this.clearTx();
//            } catch (YdbRetryableException e) {
//                if (e.getStatusCode() == StatusCode.NOT_FOUND) {
//                    this.clearTx();
//                    LOGGER.log(Level.SEVERE,
//                            "Unable to rollback transaction " + txId + ", it seems the transaction is expired", e);
//                } else {
//                    throw e;
//                }
//            }
//        }
    }

    @Override
    public void close() throws SQLException {
        if (transaction == null) {
            return;
        }
        transaction.close();
        transaction = null;
//        if (!state.closed) {
//            this.clearWarnings();
//            try {
//                session.close();
//                LOGGER.log(Level.FINE, "Releasing session: {0}", session.getId());
//            } finally {
//                state.closed = true;
//                this.clearTx();
//            }
//        }
    }

    @Override
    public boolean isClosed() {
        return transaction == null;
    }

    @Override
    public YdbDatabaseMetaData getMetaData() {
        return metaDataSupplier.get();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        ensureOpened();
        transaction = transaction.withReadOnly(readOnly);
//        if (state.txId != null) {
//            throw new SQLFeatureNotSupportedException(READONLY_INSIDE_TRANSACTION);
//        }
//        if (readOnly) {
//            if (this.getTransactionIsolation() == TRANSACTION_SERIALIZABLE_READ_WRITE) {
//                this.setTransactionIsolation(ONLINE_CONSISTENT_READ_ONLY);
//            }
//        } else {
//            if (this.getTransactionIsolation() != TRANSACTION_SERIALIZABLE_READ_WRITE) {
//                this.setTransactionIsolation(TRANSACTION_SERIALIZABLE_READ_WRITE);
//            }
//        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return getTransactionIsolation() != TRANSACTION_SERIALIZABLE_READ_WRITE;
    }

    @Override
    public void setCatalog(String catalog) {
        // do nothing
    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        ensureOpened();
        transaction = transaction.withTransactionLevel(level);

//        if (state.txId != null) {
//            throw new SQLFeatureNotSupportedException(CHANGE_ISOLATION_INSIDE_TX);
//        }
//
//        if (state.transactionLevel != level) {
//            switch (level) {
//                case TRANSACTION_SERIALIZABLE_READ_WRITE:
//                case ONLINE_CONSISTENT_READ_ONLY:
//                case STALE_CONSISTENT_READ_ONLY:
//                case ONLINE_INCONSISTENT_READ_ONLY:
//                    LOGGER.log(Level.FINE, "Set transaction isolation level: {0}", level);
//                    state.transactionLevel = level;
//                    break;
//                default:
//                    throw new SQLException(UNSUPPORTED_TRANSACTION_LEVEL + level);
//            }
//        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        ensureOpened();
        return transaction.transactionLevel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        ensureOpened();
        return warnings.toSQLWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        ensureOpened();
        warnings.clearWarnings();
    }

    @Override
    public YdbStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }


    @Override
    public Map<String, Class<?>> getTypeMap() {
        return new HashMap<>(); // TODO: handle this out
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) {
        // not supported
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        ensureOpened();

        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException(RESULT_SET_HOLDABILITY_UNSUPPORTED);
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        ensureOpened();

        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public YdbStatement createStatement(int resultSetType, int resultSetConcurrency,
                                        int resultSetHoldability) throws SQLException {
        ensureOpened();
        checkStatementParams(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new YdbStatementImpl(this, resultSetType);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String origSql, int resultSetType, int resultSetConcurrency,
                                                 int resultSetHoldability) throws SQLException {
        ensureOpened();
        checkStatementParams(resultSetType, resultSetConcurrency, resultSetHoldability);
        this.clearWarnings();

        return prepareStatementImpl(origSql, resultSetType, PreparedStatementMode.DEFAULT);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String sql, PreparedStatementMode mode) throws SQLException {
        return prepareStatementImpl(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, mode);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_UNSUPPORTED);
        }
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    private YdbPreparedStatement prepareStatementImpl(String origSql,
                                                      int resultSetType,
                                                      PreparedStatementMode mode) throws SQLException {
        ensureOpened();
        warnings.clearWarnings();

        YdbQuery query = new YdbQuery(properties, origSql);

        if (mode == PreparedStatementMode.IN_MEMORY ||
                (mode == PreparedStatementMode.DEFAULT && !properties.isAlwaysPrepareDataQuery())) {
            return new YdbPreparedStatementImpl(this, query, resultSetType);
        }

//        PrepareDataQuerySettings cfg = new PrepareDataQuerySettings();
//        cfg.keepInQueryCache();
//
//        Result<DataQuery> dataQuery = joinResultImpl(
//                () -> "Preparing Query >>\n" + sql,
//                () -> session.prepareDataQuery(sql, validator.init(cfg)));
//        DataQuery prepared = dataQuery.getValue();
//
//        boolean requireBatch = mode == PreparedStatementMode.DATA_QUERY_BATCH;
//        if (properties.isAutoPreparedBatches() || requireBatch) {
//            Optional<StructBatchConfiguration> batchCfgOpt =
//                    YdbPreparedStatementWithDataQueryBatchedImpl.asColumns(prepared.types());
//            if (batchCfgOpt.isPresent()) {
//                return new YdbPreparedStatementWithDataQueryBatchedImpl(this, resultSetType, sql, prepared,
//                        batchCfgOpt.get());
//            } else if (requireBatch) {
//                throw new YdbExecutionException(STATEMENT_IS_NOT_A_BATCH + origSql);
//            }
//        }

        return new YdbPreparedStatementWithDataQueryImpl(this, resultSetType, query, null /*prepared*/);

    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        ensureOpened();
        return transaction.keepAlive(TimeUnit.SECONDS.toMillis(timeout));

//        KeepAliveSessionSettings settings = new KeepAliveSessionSettings();
//        settings.setTimeout(timeout, TimeUnit.SECONDS);
//        try {
//            joinResultImpl(
//                    () -> "Keep alive",
//                    () -> session.keepAlive(settings));
//        } catch (SQLException sql) {
//            return false;
//        }
//        return true;
    }

    @Override
    public void setClientInfo(String name, String value) {
        // not supported
    }

    @Override
    public void setClientInfo(Properties properties) {
        // not supported
    }

    @Override
    public String getClientInfo(String name) {
        return null; // not supported
    }

    @Override
    public Properties getClientInfo() {
        return new Properties(); // not supported
    }

    @Override
    public void setSchema(String schema) {
        // not supported
    }

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        ensureOpened();
        return (int) properties.getDeadlineTimeout().toMillis();
    }

    @Override
    public String getDatabase() {
        return database;
    }


    protected String getUrl() {
        return url;
    }

    @Override
    public YdbTypes getYdbTypes() {
        return YdbTypesImpl.getInstance();
    }

    @Override
    public SchemeClient getYdbScheme() {
        return schemeClient;
    }

    @Override
    public String getYdbTxId() {
        return transaction.getId();
    }

    @Override
    public YdbOperationProperties getYdbProperties() {
        return properties;
    }

    //


//    private void joinStatusImpl(Supplier<String> operation,
//                                Supplier<CompletableFuture<Status>> action) throws SQLException {
//        Status status = validator.joinStatus(LOGGER, operation, action);
//        state.lastIssues = status.getIssues();
//    }
//
//    private <T, R extends Result<T>> R joinResultImpl(Supplier<String> message,
//                                                      Supplier<CompletableFuture<R>> action) throws SQLException {
//        R result = validator.joinResult(LOGGER, message, action);
//        state.lastIssues = result.getStatus().getIssues();
//        return result;
//    }

    private void ensureOpened() throws SQLException {
        if (transaction == null) {
            throw new SQLException(CLOSED_CONNECTION);
        }
    }

//    protected void clearTx() {
//        if (state.txId != null) {
//            LOGGER.log(Level.FINE, "Clear TxID: {0}", state.txId);
//            state.txId = null;
//        }
//    }
//
//    protected void setTx(String txId) {
//        if (txId.isEmpty()) {
//            this.clearTx();
//        } else {
//            if (state.txId != null) {
//                if (!state.txId.equals(txId)) {
//                    throw new IllegalStateException("Internal error, previous transaction " + state.txId +
//                            " not closed, but opened another one: " + txId);
//                }
//            } else {
//                LOGGER.log(Level.FINE, "New TxID: {0}", txId);
//                state.txId = txId;
//            }
//        }
//    }

//    protected TxControl<?> getTxControl() throws SQLException {
//        switch (state.transactionLevel) {
//            case TRANSACTION_SERIALIZABLE_READ_WRITE: {
//                TxControl<?> tx = state.txId != null ?
//                        TxControl.id(state.txId) :
//                        TxControl.serializableRw();
//                return tx.setCommitTx(state.autoCommit);
//            }
//            case ONLINE_CONSISTENT_READ_ONLY:
//                return TxControl.onlineRo();
//            case STALE_CONSISTENT_READ_ONLY:
//                return TxControl.staleRo();
//            case ONLINE_INCONSISTENT_READ_ONLY:
//                return TxControl.onlineRo().setAllowInconsistentReads(true);
//            default:
//                throw new SQLException(UNSUPPORTED_TRANSACTION_LEVEL + state.transactionLevel);
//        }
//    }


    private void checkStatementParams(int resultSetType, int resultSetConcurrency,
                                      int resultSetHoldability) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY && resultSetType != ResultSet.TYPE_SCROLL_INSENSITIVE) {
            throw new SQLFeatureNotSupportedException(RESULT_SET_TYPE_UNSUPPORTED);
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException(RESULT_SET_CONCURRENCY_UNSUPPORTED);
        }
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException(RESULT_SET_HOLDABILITY_UNSUPPORTED);
        }
    }

    // UNSUPPORTED

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException(PREPARED_CALLS_UNSUPPORTED);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException(PREPARED_CALLS_UNSUPPORTED);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException(PREPARED_CALLS_UNSUPPORTED);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_UNSUPPORTED);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_UNSUPPORTED);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException(CLOB_UNSUPPORTED);
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException(BLOB_UNSUPPORTED);
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException(NCLOB_UNSUPPORTED);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException(SQLXML_UNSUPPORTED);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException(ARRAYS_UNSUPPORTED);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException(STRUCTS_UNSUPPORTED);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException(SAVEPOINTS_UNSUPPORTED);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException(SAVEPOINTS_UNSUPPORTED);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException(SAVEPOINTS_UNSUPPORTED);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException(SAVEPOINTS_UNSUPPORTED);
    }


    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException(ABORT_UNSUPPORTED);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException(SET_NETWORK_TIMEOUT_UNSUPPORTED);
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
