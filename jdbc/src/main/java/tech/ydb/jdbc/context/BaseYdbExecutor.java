package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.YdbTracer;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.jdbc.impl.YdbQueryResult;
import tech.ydb.jdbc.impl.YdbStaticResultSet;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.impl.ProtoValueReaders;
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
    private final boolean useStreamResultSet;

    private final AtomicReference<YdbQueryResult> currResult;
    protected final boolean traceEnabled;
    protected final String prefixPragma;
    protected final YdbTypes types;

    public BaseYdbExecutor(YdbContext ctx) {
        this.retryCtx = ctx.getRetryCtx();
        this.traceEnabled = ctx.isTxTracerEnabled();
        this.sessionTimeout = ctx.getOperationProperties().getSessionTimeout();
        this.useStreamResultSet = ctx.getOperationProperties().getUseStreamResultSets();
        this.tableClient = ctx.getTableClient();
        this.prefixPragma = ctx.getPrefixPragma();
        this.types = ctx.getTypes();
        this.currResult = new AtomicReference<>();
    }

    protected Session createNewTableSession(YdbValidator validator) throws SQLException {
        return validator.call("Get session", null, () -> tableClient.createSession(sessionTimeout));
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

        String yql = prefixPragma + query.getPreparedYql();
        YdbContext ctx = statement.getConnection().getCtx();
        YdbValidator validator = statement.getValidator();

        // Scheme query does not affect transactions or result sets
        YdbTracer tracer = ctx.getTracer();
        tracer.trace("--> scheme query");
        tracer.query(yql);

        ExecuteSchemeQuerySettings settings = ctx.withDefaultTimeout(new ExecuteSchemeQuerySettings());
        validator.execute(QueryType.SCHEME_QUERY + " >>\n" + yql, tracer,
                () -> retryCtx.supplyStatus(session -> session.executeSchemeQuery(yql, settings))
        );

        if (!isInsideTransaction()) {
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
        YdbTracer tracer = statement.getConnection().getCtx().getTracer();
        tracer.trace("--> bulk upsert");
        tracer.query(yql);

        validator.execute(QueryType.BULK_QUERY + " >>\n" + yql, tracer,
                () -> retryCtx.supplyStatus(session -> session.executeBulkUpsert(tablePath, rows))
        );

        if (!isInsideTransaction()) {
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

        YdbTracer tracer = ctx.getTracer();
        tracer.trace("--> scan query");
        tracer.query(yql);

        final Session session = createNewTableSession(validator);

        if (!useStreamResultSet) {
            try {
                Collection<ResultSetReader> resultSets = new LinkedBlockingQueue<>();

                ctx.traceQuery(query, yql);
                validator.execute(QueryType.SCAN_QUERY + " >>\n" + yql, tracer,
                        () -> session.executeScanQuery(yql, params, settings).start(resultSets::add)
                );

                YdbResultSet rs = new YdbStaticResultSet(types, statement, ProtoValueReaders.forResultSets(resultSets));
                return updateCurrentResult(new StaticQueryResult(query, Collections.singletonList(rs)));
            } finally {
                session.close();
                tracer.close();
            }
        }

        StreamQueryResult lazy = validator.call(msg, null, () -> {
            final CompletableFuture<Result<StreamQueryResult>> future = new CompletableFuture<>();
            final GrpcReadStream<ResultSetReader> stream = session.executeScanQuery(yql, params, settings);
            final StreamQueryResult result = new StreamQueryResult(msg, types, statement, query, stream::cancel);

            stream.start((rsr) -> {
                future.complete(Result.success(result));
                result.onStreamResultSet(0, rsr);
            }).whenComplete((st, th) -> {
                session.close();

                if (th != null) {
                    result.onStreamFinished(th);
                    future.completeExceptionally(th);
                    tracer.trace("<-- " + th.getMessage());
                }
                if (st != null) {
                    validator.addStatusIssues(st);
                    result.onStreamFinished(st);
                    future.complete(st.isSuccess() ? Result.success(result) : Result.fail(st));
                    tracer.trace("<-- " + st.toString());
                }
                tracer.close();
            });

            return future;
        });

        return updateCurrentResult(lazy);
    }

}
