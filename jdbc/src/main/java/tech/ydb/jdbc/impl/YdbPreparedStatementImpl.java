package tech.ydb.jdbc.impl;


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Objects;
import java.util.logging.Logger;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbParameterMetaData;
import tech.ydb.jdbc.YdbPreparedStatement;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.common.MappingSetters;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbPreparedQuery;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.query.params.BulkUpsertQuery;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;

public class YdbPreparedStatementImpl extends BaseYdbStatement implements YdbPreparedStatement {
    private static final Logger LOGGER = Logger.getLogger(YdbPreparedStatementImpl.class.getName());
    private final YdbQuery query;
    private final YdbPreparedQuery prepared;

    public YdbPreparedStatementImpl(YdbConnection connection, YdbQuery query, YdbPreparedQuery prepared, int rsType) {
        super(LOGGER, connection, rsType, true); // is poolable by default

        this.query = Objects.requireNonNull(query);
        this.prepared = Objects.requireNonNull(prepared);
    }

    @Override
    public String getQuery() {
        return query.getOriginQuery();
    }

    @Override
    public void addBatch() throws SQLException {
        prepared.addBatch();
    }

    @Override
    public void clearBatch() throws SQLException {
        prepared.clearBatch();
    }

    @Override
    public void clearParameters() {
        prepared.clearParameters();
    }

    @Override
    public YdbParameterMetaData getParameterMetaData() {
        return new YdbParameterMetaDataImpl(prepared);
    }

    @Override
    public void close() throws SQLException {
        clearParameters();
        super.close();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        cleanState();

        int[] results = new int[prepared.batchSize()];
        if (results.length == 0) {
            return results;
        }

        try {
            if (query.getType() == QueryType.BULK_QUERY && (prepared instanceof BulkUpsertQuery)) {
                BulkUpsertQuery bulk = (BulkUpsertQuery) prepared;
                YdbQueryResult newState = executeBulkUpsert(query, bulk.getTablePath(), bulk.getBatchedBulk());
                updateState(newState);
            } else {
                for (Params prm: prepared.getBatchParams()) {
                    YdbQueryResult newState = executeDataQuery(query, prepared.getBatchText(prm), prm);
                    updateState(newState);
                }
            }
        } finally {
            clearBatch();
        }

        Arrays.fill(results, SUCCESS_NO_INFO);
        return results;
    }

    @Override
    public YdbResultSet executeQuery() throws SQLException {
        if (!execute()) {
            throw new SQLException(YdbConst.QUERY_EXPECT_RESULT_SET);
        }
        return getResultSet();
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (execute()) {
            throw new SQLException(YdbConst.QUERY_EXPECT_UPDATE);
        }
        return getUpdateCount();
    }

    @Override
    public boolean execute() throws SQLException {
        cleanState();
        clearBatch();

        YdbQueryResult newState = null;
        Params prms = prepared.getCurrentParams();
        switch (query.getType()) {
            case DATA_QUERY:
                newState = executeDataQuery(query, prepared.getQueryText(prms), prms);
                break;
            case SCAN_QUERY:
                newState = executeScanQuery(query, prepared.getQueryText(prms), prms);
                break;
            case SCHEME_QUERY:
                newState = executeSchemeQuery(query);
                break;
            case EXPLAIN_QUERY:
                newState = executeExplainQuery(query);
                break;
            case BULK_QUERY:
                if (prepared instanceof BulkUpsertQuery) {
                    BulkUpsertQuery bulk = (BulkUpsertQuery) prepared;
                    newState = executeBulkUpsert(query, bulk.getTablePath(), bulk.getCurrentBulk());
                } else {
                    throw new IllegalStateException(
                            "Internal error. Incorrect class of bulk prepared query " + prepared.getClass()
                    );
                }
                break;
            default:
                throw new IllegalStateException("Internal error. Unsupported query type " + query.getType());
        }
        prepared.clearParameters();

        return updateState(newState);
    }

    @Override
    public YdbResultSet executeScanQuery() throws SQLException {
        cleanState();
        Params prms = prepared.getCurrentParams();
        YdbQueryResult result = executeScanQuery(query, prepared.getQueryText(prms), prms);
        prepared.clearParameters();
        updateState(result);
        return result.getCurrentResultSet();
    }

    @Override
    public YdbResultSet executeExplainQuery() throws SQLException {
        cleanState();
        YdbQueryResult state = executeExplainQuery(query);
        updateState(state);
        return getResultSet();
    }

    private void setImplReader(String name, Reader reader, long length) throws SQLException {
        prepared.setParam(name, MappingSetters.CharStream.fromReader(reader, length), Types.VARCHAR);
    }

    private void setImplStream(String name, InputStream stream, long length) throws SQLException {
        prepared.setParam(name, MappingSetters.ByteStream.fromInputStream(stream, length), Types.BINARY);
    }

    private void setImplReader(int index, Reader reader, long length) throws SQLException {
        prepared.setParam(index, MappingSetters.CharStream.fromReader(reader, length), Types.VARCHAR);
    }

    private void setImplStream(int index, InputStream stream, long length) throws SQLException {
        prepared.setParam(index, MappingSetters.ByteStream.fromInputStream(stream, length), Types.BINARY);
    }

    @Override
    public void setObject(String parameterName, Object value, Type type) throws SQLException {
        prepared.setParam(parameterName, value, Types.JAVA_OBJECT);
    }

    @Override
    public void setObject(int parameterIndex, Object value, Type type) throws SQLException {
        prepared.setParam(parameterIndex, value, Types.JAVA_OBJECT);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        if (x == null) {
            throw new SQLDataException(YdbConst.UNABLE_TO_SET_NULL_OBJECT);
        }
        prepared.setParam(parameterName, x, Types.JAVA_OBJECT);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            throw new SQLDataException(YdbConst.UNABLE_TO_SET_NULL_OBJECT);
        }
        prepared.setParam(parameterIndex, x, Types.JAVA_OBJECT);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        prepared.setParam(parameterName, null, sqlType);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        prepared.setParam(parameterName, x, Types.BOOLEAN);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        prepared.setParam(parameterName, x, Types.TINYINT);
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        prepared.setParam(parameterName, x, Types.SMALLINT);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        prepared.setParam(parameterName, x, Types.INTEGER);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        prepared.setParam(parameterName, x, Types.BIGINT);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        prepared.setParam(parameterName, x, Types.FLOAT);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        prepared.setParam(parameterName, x, Types.DOUBLE);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        prepared.setParam(parameterName, x, Types.DECIMAL);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        prepared.setParam(parameterName, x, Types.VARCHAR);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        prepared.setParam(parameterName, x, Types.BINARY);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        prepared.setParam(parameterName, x, Types.DATE);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        prepared.setParam(parameterName, x, Types.TIME);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        prepared.setParam(parameterName, x, Types.TIMESTAMP);
    }

    @Override
    public void setUnicodeStream(String parameterName, InputStream x, int length) throws SQLException {
        setImplStream(parameterName, x, length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        setImplStream(parameterName, x, length);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        prepared.setParam(parameterName, x, targetSqlType);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        setImplReader(parameterName, reader, length);
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        // TODO: check cal
        prepared.setParam(parameterName, x, Types.DATE);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        // TODO: check cal
        prepared.setParam(parameterName, x, Types.TIME);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        // TODO: check cal
        prepared.setParam(parameterName, x, Types.TIMESTAMP);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        prepared.setParam(parameterName, null, sqlType);
    }

    @Override
    public void setURL(String parameterName, URL x) throws SQLException {
        prepared.setParam(parameterName, x, Types.VARCHAR);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        prepared.setParam(parameterName, value, Types.VARCHAR);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        setImplReader(parameterName, value, length);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        setImplReader(parameterName, reader, length);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        setImplStream(parameterName, inputStream, length);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        setImplReader(parameterName, reader, length);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        // TODO: check scaleOrLength
        prepared.setParam(parameterName, x, targetSqlType);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        setImplStream(parameterName, x, length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        setImplReader(parameterName, reader, length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        setImplStream(parameterName, x, -1);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        setImplReader(parameterName, reader, -1);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        setImplReader(parameterName, value, -1);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        setImplReader(parameterName, reader, -1);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        setImplStream(parameterName, inputStream, -1);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        setImplReader(parameterName, reader, -1);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        prepared.setParam(parameterIndex, null, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.TINYINT);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.SMALLINT);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.INTEGER);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.BIGINT);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.FLOAT);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.DOUBLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.DECIMAL);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.VARCHAR);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.BINARY);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.DATE);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.TIME);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.TIMESTAMP);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setImplStream(parameterIndex, x, length);
    }

    @Deprecated
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setImplStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setImplStream(parameterIndex, x, length);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        prepared.setParam(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        setImplReader(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.REF_UNSUPPORTED);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.BLOB_UNSUPPORTED);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CLOB_UNSUPPORTED);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ARRAYS_UNSUPPORTED);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        // TODO: check cal
        prepared.setParam(parameterIndex, x, Types.DATE);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        // TODO: check cal
        prepared.setParam(parameterIndex, x, Types.TIME);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        // TODO: check cal
        prepared.setParam(parameterIndex, x, Types.TIMESTAMP);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        prepared.setParam(parameterIndex, null, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        prepared.setParam(parameterIndex, x, Types.VARCHAR);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        prepared.setParam(parameterIndex, value, Types.VARCHAR);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setImplReader(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.NCLOB_UNSUPPORTED);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setImplReader(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setImplStream(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setImplReader(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.SQLXML_UNSUPPORTED);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        // TODO: handle scaleOrLength
        prepared.setParam(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setImplStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setImplStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setImplReader(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setImplStream(parameterIndex, x, -1);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setImplStream(parameterIndex, x, -1);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setImplReader(parameterIndex, reader, -1);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setImplReader(parameterIndex, value, -1);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setImplReader(parameterIndex, reader, -1);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setImplStream(parameterIndex, inputStream, -1);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setImplReader(parameterIndex, reader, -1);
    }

    // UNSUPPORTED
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.METADATA_RS_UNSUPPORTED_IN_PS);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ROWID_UNSUPPORTED);
    }

    @Override
    public void executeSchemeQuery(String sql) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public YdbResultSet executeScanQuery(String sql) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public YdbResultSet executeExplainQuery(String sql) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public YdbResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLException(YdbConst.CUSTOM_SQL_UNSUPPORTED);
    }

    @Override
    public void setArray(String parameterName, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ARRAYS_UNSUPPORTED);
    }


    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ASCII_STREAM_UNSUPPORTED);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ASCII_STREAM_UNSUPPORTED);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ASCII_STREAM_UNSUPPORTED);
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.BLOB_UNSUPPORTED);
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CLOB_UNSUPPORTED);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.NCLOB_UNSUPPORTED);
    }

    @Override
    public void setRef(String parameterName, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.REF_UNSUPPORTED);
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.SQLXML_UNSUPPORTED);
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
