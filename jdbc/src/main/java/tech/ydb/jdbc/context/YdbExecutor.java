package tech.ydb.jdbc.context;

import java.sql.SQLException;

import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.YdbTracer;
import tech.ydb.jdbc.impl.YdbQueryResult;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.ListValue;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbExecutor {
    void close() throws SQLException;
    boolean isClosed() throws SQLException;
    void ensureOpened() throws SQLException;

    String txID() throws SQLException;
    int transactionLevel() throws SQLException;

    boolean isInsideTransaction() throws SQLException;
    boolean isAutoCommit() throws SQLException;
    boolean isReadOnly() throws SQLException;

    void setTransactionLevel(int level) throws SQLException;
    void setReadOnly(boolean readOnly) throws SQLException;
    void setAutoCommit(boolean autoCommit) throws SQLException;

    YdbQueryResult executeSchemeQuery(YdbStatement statement, YdbQuery query) throws SQLException;
    YdbQueryResult executeBulkUpsert(YdbStatement statement, YdbQuery query, String tablePath, ListValue rows)
            throws SQLException;
    YdbQueryResult executeExplainQuery(YdbStatement statement, YdbQuery query) throws SQLException;
    YdbQueryResult executeScanQuery(YdbStatement statement, YdbQuery query, String yql, Params params)
            throws SQLException;
    YdbQueryResult executeDataQuery(YdbStatement statement, YdbQuery query, String yql, Params params,
            long timeout, boolean poolable) throws SQLException;

    void commit(YdbContext ctx, YdbValidator validator) throws SQLException;
    void rollback(YdbContext ctx, YdbValidator validator) throws SQLException;

    boolean isValid(YdbValidator validator, int timeout) throws SQLException;

    YdbTracer trace(String message);
}
