package tech.ydb.jdbc.impl;


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
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbDatabaseMetaData;
import tech.ydb.jdbc.YdbPrepareMode;
import tech.ydb.jdbc.YdbPreparedStatement;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.YdbTypes;
import tech.ydb.jdbc.context.TableExecutor;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.jdbc.context.YdbExecutor;
import tech.ydb.jdbc.context.YdbValidator;
import tech.ydb.jdbc.query.JdbcParams;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.settings.FakeTxMode;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.table.query.ExplainDataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;

public class YdbConnectionImpl implements YdbConnection {
    private static final Logger LOGGER = Logger.getLogger(YdbConnectionImpl.class.getName());

    private final YdbContext ctx;
    private final YdbValidator validator;
    private final YdbExecutor executor;
    private final FakeTxMode scanQueryTxMode;
    private final FakeTxMode schemeQueryTxMode;
    private final YdbDatabaseMetaData metaData = new YdbDatabaseMetaDataImpl(this);

    public YdbConnectionImpl(YdbContext context) throws SQLException {
        this.ctx = context;

        YdbOperationProperties props = ctx.getOperationProperties();
        this.scanQueryTxMode = props.getScanQueryTxMode();
        this.schemeQueryTxMode = props.getSchemeQueryTxMode();

        this.validator = new YdbValidator(LOGGER);
        this.executor = new TableExecutor(ctx, props.getTransactionLevel(), props.isAutoCommit());

        this.ctx.register();
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
        try {
            return ctx.parseYdbQuery(sql).getYqlQuery(null);
        } catch (SQLException ex) {
            return ex.getMessage();
        }
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        executor.ensureOpened();
        if (autoCommit == executor.isAutoCommit()) {
            return;
        }

        LOGGER.log(Level.FINE, "Set auto-commit: {0}", autoCommit);
        if (autoCommit) {
            commit();
        }

        executor.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return executor.isAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        executor.commit(ctx, validator);
    }

    @Override
    public void rollback() throws SQLException {
        executor.rollback(ctx, validator);
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }

        commit(); // like Oracle
        validator.clearWarnings();
        executor.close();
        ctx.deregister();
    }

    @Override
    public boolean isClosed() {
        return executor.isClosed();
    }

    @Override
    public YdbDatabaseMetaData getMetaData() {
        return metaData;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (executor.isReadOnly() == readOnly) {
            return;
        }
        executor.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return executor.isReadOnly();
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
        if (executor.transactionLevel() == level) {
            return;
        }

        LOGGER.log(Level.FINE, "Set transaction isolation level: {0}", level);
        executor.setTransactionLevel(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return executor.transactionLevel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        executor.ensureOpened();
        return validator.toSQLWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        executor.ensureOpened();
        validator.clearWarnings();
    }

    @Override
    public void executeSchemeQuery(YdbQuery query, YdbValidator validator) throws SQLException {
        executor.ensureOpened();

        if (executor.isInsideTransaction()) {
            switch (schemeQueryTxMode) {
                case FAKE_TX:
                    break;
                case SHADOW_COMMIT:
                    commit();
                    break;
                case ERROR:
                default:
                    throw new SQLException(YdbConst.SCHEME_QUERY_INSIDE_TRANSACTION);

            }
        }

        executor.executeSchemeQuery(ctx, validator, query);
    }

    @Override
    public List<ResultSetReader> executeDataQuery(YdbQuery query, YdbValidator validator,
            int timeout, boolean poolable, Params params) throws SQLException {
        return executor.executeDataQuery(ctx, validator, query, timeout, poolable, params);
    }

    @Override
    public ResultSetReader executeScanQuery(YdbQuery query, YdbValidator validator, Params params) throws SQLException {
        executor.ensureOpened();

        if (executor.isInsideTransaction()) {
            switch (scanQueryTxMode) {
                case FAKE_TX:
                    break;
                case SHADOW_COMMIT:
                    commit();
                    break;
                case ERROR:
                default:
                    throw new SQLException(YdbConst.SCAN_QUERY_INSIDE_TRANSACTION);
            }
        }

        Duration scanQueryTimeout = ctx.getOperationProperties().getScanQueryTimeout();
        return executor.executeScanQuery(ctx, validator, query, scanQueryTimeout, params);
    }

    @Override
    public ExplainDataQueryResult executeExplainQuery(YdbQuery query, YdbValidator validator) throws SQLException {
        return executor.executeExplainQuery(ctx, validator, query);
    }

    @Override
    public YdbStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
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
        executor.ensureOpened();
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException(YdbConst.RESULT_SET_HOLDABILITY_UNSUPPORTED);
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        executor.ensureOpened();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public YdbStatement createStatement(int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        executor.ensureOpened();
        checkStatementParams(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new YdbStatementImpl(this, resultSetType);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String origSql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        checkStatementParams(resultSetType, resultSetConcurrency, resultSetHoldability);
        return prepareStatement(origSql, resultSetType, YdbPrepareMode.AUTO);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String sql, YdbPrepareMode mode) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, mode);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException(YdbConst.AUTO_GENERATED_KEYS_UNSUPPORTED);
        }
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    private YdbPreparedStatement prepareStatement(String sql, int resultSetType, YdbPrepareMode mode)
            throws SQLException {
        executor.ensureOpened();
        validator.clearWarnings();

        YdbQuery query = ctx.findOrParseYdbQuery(sql);

        if (query.type() != QueryType.DATA_QUERY && query.type() != QueryType.SCAN_QUERY) {
            throw new SQLException(YdbConst.UNSUPPORTED_QUERY_TYPE_IN_PS + query.type());
        }

        JdbcParams params = ctx.findOrCreateJdbcParams(query, mode);
        return new YdbPreparedStatementImpl(this, query, params, resultSetType);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return executor.isValid(validator, timeout);
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
        executor.ensureOpened();
        return (int) ctx.getOperationProperties().getDeadlineTimeout().toMillis();
    }

    @Override
    public YdbTypes getYdbTypes() {
        return YdbTypesImpl.getInstance();
    }

    @Override
    public String getYdbTxId() {
        return executor.txID();
    }

    @Override
    public YdbContext getCtx() {
        return ctx;
    }

    private void checkStatementParams(int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY && resultSetType != ResultSet.TYPE_SCROLL_INSENSITIVE) {
            throw new SQLFeatureNotSupportedException(YdbConst.RESULT_SET_TYPE_UNSUPPORTED);
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException(YdbConst.RESULT_SET_CONCURRENCY_UNSUPPORTED);
        }
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException(YdbConst.RESULT_SET_HOLDABILITY_UNSUPPORTED);
        }
    }

    // UNSUPPORTED
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.PREPARED_CALLS_UNSUPPORTED);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.PREPARED_CALLS_UNSUPPORTED);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.PREPARED_CALLS_UNSUPPORTED);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.AUTO_GENERATED_KEYS_UNSUPPORTED);
    }

    @Override
    public YdbPreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.AUTO_GENERATED_KEYS_UNSUPPORTED);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CLOB_UNSUPPORTED);
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.BLOB_UNSUPPORTED);
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.NCLOB_UNSUPPORTED);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.SQLXML_UNSUPPORTED);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ARRAYS_UNSUPPORTED);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.STRUCTS_UNSUPPORTED);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.SAVEPOINTS_UNSUPPORTED);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.SAVEPOINTS_UNSUPPORTED);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.SAVEPOINTS_UNSUPPORTED);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.SAVEPOINTS_UNSUPPORTED);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ABORT_UNSUPPORTED);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.SET_NETWORK_TIMEOUT_UNSUPPORTED);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException(YdbConst.CANNOT_UNWRAP_TO + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }
}
