package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbQueryResult;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.YdbTracer;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.jdbc.impl.YdbQueryResultReader;
import tech.ydb.jdbc.impl.YdbQueryResultStatic;
import tech.ydb.jdbc.impl.YdbResultSetMemory;
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
    private final Duration sessionTimeout;
    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;
    private final SessionRetryContext idempotentRetryCtx;
    private final boolean useStreamResultSet;

    private final AtomicReference<YdbQueryResult> currResult;
    protected final String prefixPragma;
    protected final YdbTypes types;

    public BaseYdbExecutor(YdbContext ctx) {
        this.sessionTimeout = ctx.getOperationProperties().getSessionTimeout();
        this.useStreamResultSet = ctx.getOperationProperties().getUseStreamResultSets();
        this.tableClient = ctx.getTableClient();
        this.retryCtx = SessionRetryContext.create(tableClient)
                .sessionCreationTimeout(ctx.getOperationProperties().getSessionTimeout())
                .build();
        this.idempotentRetryCtx = SessionRetryContext.create(tableClient)
                .sessionCreationTimeout(ctx.getOperationProperties().getSessionTimeout())
                .idempotent(true)
                .build();
        this.prefixPragma = ctx.getPrefixPragma();
        this.types = ctx.getTypes();
        this.currResult = new AtomicReference<>();
    }

    protected Session createNewTableSession(YdbValidator validator) throws SQLException {
        return validator.call("Get session", null, () -> tableClient.createSession(sessionTimeout));
    }

    @Override
    public void clearState() throws SQLException {
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
        if (isClosed()) {
            throw new SQLException(YdbConst.CLOSED_CONNECTION);
        }
    }

    @Override
    public YdbQueryResult executeSchemeQuery(YdbStatement statement, YdbQuery query, String preparedYql, Params params)
            throws SQLException {
        ensureOpened();

        if (!params.isEmpty()) {
            throw new SQLFeatureNotSupportedException(YdbConst.PARAMETERIZED_SCHEME_QUERIES_UNSUPPORTED);
        }

        String yql = prefixPragma + preparedYql;
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

        return updateCurrentResult(new YdbQueryResultStatic(query));
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
                () -> idempotentRetryCtx.supplyStatus(session -> session.executeBulkUpsert(tablePath, rows))
        );

        if (!isInsideTransaction()) {
            tracer.close();
        }

        return updateCurrentResult(new YdbQueryResultStatic(query));
    }

    @Override
    public YdbQueryResult executeScanQuery(YdbStatement statement, YdbQuery query, String preparedYql, Params params)
            throws SQLException {
        ensureOpened();

        String yql = prefixPragma + preparedYql;
        YdbContext ctx = statement.getConnection().getCtx();
        YdbValidator validator = statement.getValidator();
        Duration scanQueryTimeout = ctx.getOperationProperties().getScanQueryTimeout();
        String msg = QueryType.SCAN_QUERY + " >>\n" + yql;

        YdbTracer tracer = ctx.getTracer();
        tracer.trace("--> scan query");
        tracer.query(yql);

        final Session session = createNewTableSession(validator);

        if (!useStreamResultSet) {
            try {
                ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder()
                        .withRequestTimeout(scanQueryTimeout)
                        .build();

                List<ResultSetReader> parts = new ArrayList<>();

                ctx.traceQueryByFullScanDetector(query, yql);
                validator.execute(QueryType.SCAN_QUERY + " >>\n" + yql, tracer,
                        () -> session.executeScanQuery(yql, params, settings).start(parts::add)
                );

                YdbResultSet rs = new YdbResultSetMemory(types, statement, parts.toArray(new ResultSetReader[0]));
                return updateCurrentResult(new YdbQueryResultStatic(query, rs));
            } finally {
                session.close();
                tracer.close();
            }
        }

        final YdbQueryResultReader reader = new YdbQueryResultReader(types, statement, query) {
            @Override
            public void onClose(Status status, Throwable th) {
                session.close();
                if (th != null) {
                    tracer.trace("<-- " + th.getMessage());
                }
                if (status != null) {
                    validator.addStatusIssues(status);
                    tracer.trace("<-- " + status.toString());
                }
                tracer.close();

                super.onClose(status, th);
            }
        };

        ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder()
                .withRequestTimeout(scanQueryTimeout)
                .setGrpcFlowControl(reader)
                .build();

        GrpcReadStream<ResultSetReader> stream = session.executeScanQuery(yql, params, settings);
        validator.execute(msg, tracer, () -> reader.load(stream));
        return updateCurrentResult(reader);
    }
}
