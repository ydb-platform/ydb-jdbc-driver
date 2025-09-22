package tech.ydb.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

import tech.ydb.jdbc.context.YdbValidator;

public interface YdbStatement extends Statement {
    /**
     * Explicitly execute query as a schema query
     *
     * @param sql query (DDL) to execute
     * @throws SQLException if query cannot be executed
     */
    void executeSchemeQuery(String sql) throws SQLException;

    /**
     * Explicitly execute query as a scan query
     *
     * @param sql query to execute
     * @return result set
     * @throws SQLException if query cannot be executed
     */
    YdbResultSet executeScanQuery(String sql) throws SQLException;

    /**
     * Explicitly explain this query
     *
     * @param sql query to explain
     * @return result set of two string columns: {@link YdbConst#EXPLAIN_COLUMN_AST}
     * and {@link YdbConst#EXPLAIN_COLUMN_PLAN}
     * @throws SQLException if query cannot be explained
     */
    YdbResultSet executeExplainQuery(String sql) throws SQLException;

    YdbValidator getValidator();

    @Override
    YdbResultSet executeQuery(String sql) throws SQLException;

    @Override
    YdbResultSet getResultSet() throws SQLException;

    @Override
    YdbConnection getConnection() throws SQLException;

    @Override
    int getQueryTimeout();

    @Override
    boolean isPoolable();
}
