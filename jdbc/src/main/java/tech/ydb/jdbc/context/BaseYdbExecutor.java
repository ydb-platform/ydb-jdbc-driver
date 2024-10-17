package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.core.Result;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.YdbTracer;
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
    protected final boolean traceEnabled;
    protected final String prefixPragma;

    public BaseYdbExecutor(YdbContext ctx) {
        this.retryCtx = ctx.getRetryCtx();
        this.traceEnabled = ctx.isTxTracerEnabled();
        this.sessionTimeout = ctx.getOperationProperties().getSessionTimeout();
        this.tableClient = ctx.getTableClient();
        this.prefixPragma = ctx.getPrefixPragma();
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
    public YdbTracer trace(String message) {
        if (!traceEnabled) {
            return null;
        }
        YdbTracer tracer = YdbTracer.current();
        tracer.trace(message);
        return tracer;
    }

    protected YdbTracer traceRequest(String type, String message) {
        if (!traceEnabled) {
            return null;
        }
        YdbTracer tracer = YdbTracer.current();
        tracer.trace("--> " + type);
        tracer.traceRequest(message);
        return tracer;
    }

    @Override
    public YdbQueryResult executeSchemeQuery(YdbStatement statement, YdbQuery query) throws SQLException {
        ensureOpened();

        String yql = prefixPragma + query.getPreparedYql();
        YdbContext ctx = statement.getConnection().getCtx();
        YdbValidator validator = statement.getValidator();

        // Scheme query does not affect transactions or result sets
        YdbTracer tracer = traceRequest("scheme query", yql);

        ExecuteSchemeQuerySettings settings = ctx.withDefaultTimeout(new ExecuteSchemeQuerySettings());
        validator.execute(QueryType.SCHEME_QUERY + " >>\n" + yql, tracer,
                () -> retryCtx.supplyStatus(session -> session.executeSchemeQuery(yql, settings))
        );

        if (tracer != null && !isInsideTransaction()) {
            tracer.close();
        }

        return updateCurrentResult(new StaticQueryResult(query, Collections.emptyList()));
    }

    @Override
    public YdbQueryResult executeBulkUpsert(YdbStatement statement, YdbQuery query, String tablePath, ListValue rows)
            throws SQLException {
        ensureOpened();

        String yql = prefixPragma + query.getPreparedYql();
        YdbValidator validator = statement.getValidator();
        YdbTracer tracer = traceRequest("bulk upsert", yql);
        validator.execute(QueryType.BULK_QUERY + " >>\n" + yql, tracer,
                () -> retryCtx.supplyStatus(session -> session.executeBulkUpsert(tablePath, rows))
        );

        if (tracer != null && !isInsideTransaction()) {
            tracer.close();
        }

        return updateCurrentResult(new StaticQueryResult(query, Collections.emptyList()));
    }

    @Override
    public YdbQueryResult executeScanQuery(YdbStatement statement, YdbQuery query, String preparedYql, Params params)
            throws SQLException {
        ensureOpened();

        String yql = prefixPragma + preparedYql;
        YdbContext ctx = statement.getConnection().getCtx();
        YdbValidator validator = statement.getValidator();
        Duration scanQueryTimeout = ctx.getOperationProperties().getScanQueryTimeout();
        ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder()
                .withRequestTimeout(scanQueryTimeout)
                .build();
        String msg = QueryType.SCAN_QUERY + " >>\n" + yql;

        final YdbTracer tracer = traceRequest("scan query", yql);
        final Session session = createNewTableSession(validator);

        StreamQueryResult lazy = validator.call(msg, null, () -> {
            final CompletableFuture<Result<StreamQueryResult>> future = new CompletableFuture<>();
            final GrpcReadStream<ResultSetReader> stream = session.executeScanQuery(yql, params, settings);
            final StreamQueryResult result = new StreamQueryResult(msg, statement, query, stream::cancel);

            stream.start((rsr) -> {
                future.complete(Result.success(result));
                result.onStreamResultSet(0, rsr);
            }).whenComplete((st, th) -> {
                session.close();

                if (th != null) {
                    result.onStreamFinished(th);
                    future.completeExceptionally(th);

                    if (tracer != null) {
                        tracer.trace("<-- " + th.getMessage());
                        tracer.close();
                    }
                }
                if (st != null) {
                    validator.addStatusIssues(st);
                    result.onStreamFinished(st);
                    future.complete(st.isSuccess() ? Result.success(result) : Result.fail(st));

                    if (tracer != null) {
                        tracer.trace("<-- " + st.toString());
                        tracer.close();
                    }
                }
            });

            return future;
        });

        return updateCurrentResult(lazy);
    }

}
