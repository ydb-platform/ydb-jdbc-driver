package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.util.List;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.query.ExplainDataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbExecutor {
    default void ensureOpened() throws SQLException {
        if (isClosed()) {
            throw new SQLException(YdbConst.CLOSED_CONNECTION);
        }
    }

    boolean isClosed();

    String txID();
    int transactionLevel() throws SQLException;

    boolean isInsideTransaction() throws SQLException;
    boolean isAutoCommit() throws SQLException;
    boolean isReadOnly() throws SQLException;

    void setTransactionLevel(int level) throws SQLException;
    void setReadOnly(boolean readOnly) throws SQLException;
    void setAutoCommit(boolean autoCommit) throws SQLException;

    void executeSchemeQuery(YdbContext ctx, YdbValidator validator, YdbQuery query) throws SQLException;

    List<ResultSetReader> executeDataQuery(YdbContext ctx, YdbValidator validator, YdbQuery query,
            long timeout, boolean poolable, Params params) throws SQLException;

    ResultSetReader executeScanQuery(YdbContext ctx, YdbValidator validator, YdbQuery query, Params params) throws SQLException;

    ExplainDataQueryResult executeExplainQuery(YdbContext ctx, YdbValidator validator, YdbQuery query) throws SQLException;

    void commit(YdbContext ctx, YdbValidator validator) throws SQLException;
    void rollback(YdbContext ctx, YdbValidator validator) throws SQLException;

    boolean isValid(YdbValidator validator, int timeout) throws SQLException;

    void close();
}
