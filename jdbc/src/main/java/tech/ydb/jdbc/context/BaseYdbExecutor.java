package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.core.Result;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.impl.YdbQueryResult;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ExecuteScanQuerySettings;
import tech.ydb.table.settings.ExecuteSchemeQuerySettings;
import tech.ydb.table.values.ListValue;

/**
 *
 * @author Aleksandr Gorshenin
 */
public abstract class BaseYdbExecutor implements YdbExecutor {
    private final SessionRetryContext retryCtx;
    private final Duration sessionTimeout;
    private final TableClient tableClient;
    private final AtomicReference<YdbQueryResult> currResult;

    public BaseYdbExecutor(YdbContext ctx) {
        this.retryCtx = ctx.getRetryCtx();
        this.sessionTimeout = ctx.getOperationProperties().getSessionTimeout();
        this.tableClient = ctx.getTableClient();
        this.currResult = new AtomicReference<>();
    }

    protected Session createNewTableSession(YdbValidator validator) throws SQLException {
        try {
            Result<Session> session = tableClient.createSession(sessionTimeout).join();
            validator.addStatusIssues(session.getStatus());
            return session.getValue();
        } catch (UnexpectedResultException ex) {
            throw ExceptionFactory.createException("Cannot create session with " + ex.getStatus(), ex);
        }
    }

    protected void closeCurrentResult() throws SQLException {
        YdbQueryResult rs = currResult.get();
        if (rs != null) {
            rs.close();
        }
    }

    protected YdbQueryResult updateCurrentResult(YdbQueryResult result) throws SQLException {
        YdbQueryResult old = currResult.getAndSet(result);
        if (old != null) {
            old.close();
        }
        return result;
    }

    @Override
    public void ensureOpened() throws SQLException {
        closeCurrentResult();
        if (isClosed()) {
            throw new SQLException(YdbConst.CLOSED_CONNECTION);
        }
    }

    @Override
    public YdbQueryResult executeSchemeQuery(YdbStatement statement, YdbQuery query) throws SQLException {
        ensureOpened();

        String yql = query.getPreparedYql();
        YdbContext ctx = statement.getConnection().getCtx();
        YdbValidator validator = statement.getValidator();

        // Scheme query does not affect transactions or result sets
        ExecuteSchemeQuerySettings settings = ctx.withDefaultTimeout(new ExecuteSchemeQuerySettings());
        validator.execute(QueryType.SCHEME_QUERY + " >>\n" + yql,
                () -> retryCtx.supplyStatus(session -> session.executeSchemeQuery(yql, settings))
        );

        return updateCurrentResult(new StaticQueryResult(query, Collections.emptyList()));
    }

    @Override
    public YdbQueryResult executeBulkUpsert(YdbStatement statement, YdbQuery query, String tablePath, ListValue rows)
            throws SQLException {
        ensureOpened();

        String yql = query.getPreparedYql();
        YdbValidator validator = statement.getValidator();
        validator.execute(QueryType.BULK_QUERY + " >>\n" + yql,
                () -> retryCtx.supplyStatus(session -> session.executeBulkUpsert(tablePath, rows))
        );

        return updateCurrentResult(new StaticQueryResult(query, Collections.emptyList()));
    }

    @Override
    public YdbQueryResult executeScanQuery(YdbStatement statement, YdbQuery query, String yql, Params params)
            throws SQLException {
        ensureOpened();

        YdbContext ctx = statement.getConnection().getCtx();
        YdbValidator validator = statement.getValidator();
        Duration scanQueryTimeout = ctx.getOperationProperties().getScanQueryTimeout();
        ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder()
                .withRequestTimeout(scanQueryTimeout)
                .build();

        final Session session = createNewTableSession(validator);

        String msg = QueryType.SCAN_QUERY + " >>\n" + yql;
        StreamQueryResult lazy = validator.call(msg, () -> {
            GrpcReadStream<ResultSetReader> stream = session.executeScanQuery(yql, params, settings);
            StreamQueryResult result = new StreamQueryResult(msg, statement, query, stream::cancel);
            return result.execute(stream, session::close);
        });

        return updateCurrentResult(lazy);
    }

}
