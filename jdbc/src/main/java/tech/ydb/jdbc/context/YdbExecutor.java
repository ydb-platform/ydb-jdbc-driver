package tech.ydb.jdbc.context;

import java.sql.SQLException;

import tech.ydb.jdbc.YdbQueryResult;
import tech.ydb.jdbc.YdbStatement;
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

    YdbQueryResult executeSchemeQuery(YdbStatement st, YdbQuery query) throws SQLException;
    YdbQueryResult executeBulkUpsert(YdbStatement st, YdbQuery query, String path, ListValue rows) throws SQLException;
    YdbQueryResult executeExplainQuery(YdbStatement st, YdbQuery query) throws SQLException;
    YdbQueryResult executeScanQuery(YdbStatement st, YdbQuery query, String yql, Params prms) throws SQLException;
    YdbQueryResult executeDataQuery(YdbStatement st, YdbQuery query, String yql, Params prms) throws SQLException;

    void commit(YdbContext ctx, YdbValidator validator) throws SQLException;
    void rollback(YdbContext ctx, YdbValidator validator) throws SQLException;

    boolean isValid(YdbValidator validator, int timeout) throws SQLException;
}
