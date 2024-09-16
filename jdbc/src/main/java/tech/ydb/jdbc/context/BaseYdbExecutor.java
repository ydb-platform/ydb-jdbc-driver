package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.SessionRetryContext;
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

    public BaseYdbExecutor(YdbContext ctx) {
        this.retryCtx = ctx.getRetryCtx();
    }

    @Override
    public void executeSchemeQuery(YdbContext ctx, YdbValidator validator, String yql) throws SQLException {
        ensureOpened();

        // Scheme query does not affect transactions or result sets
        ExecuteSchemeQuerySettings settings = ctx.withDefaultTimeout(new ExecuteSchemeQuerySettings());
        validator.execute(QueryType.SCHEME_QUERY + " >>\n" + yql,
                () -> retryCtx.supplyStatus(session -> session.executeSchemeQuery(yql, settings))
        );
    }

    @Override
    public void executeBulkUpsert(YdbContext ctx, YdbValidator validator, String yql, String tablePath, ListValue rows)
            throws SQLException {
        ensureOpened();
        validator.execute(QueryType.BULK_QUERY + " >>\n" + yql,
                () -> retryCtx.supplyStatus(session -> session.executeBulkUpsert(tablePath, rows))
        );
    }

    @Override
    public ResultSetReader executeScanQuery(
            YdbContext ctx, YdbValidator validator, YdbQuery query, String yql, Params params
    ) throws SQLException {
        ensureOpened();

        Collection<ResultSetReader> resultSets = new LinkedBlockingQueue<>();
        Duration scanQueryTimeout = ctx.getOperationProperties().getScanQueryTimeout();
        ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder()
                .withRequestTimeout(scanQueryTimeout)
                .build();

        ctx.traceQuery(query, yql);
        validator.execute(QueryType.SCAN_QUERY + " >>\n" + yql,
                () -> retryCtx.supplyStatus(session -> {
                    resultSets.clear();
                    return session.executeScanQuery(yql, params, settings).start(resultSets::add);
                })
        );

        return ProtoValueReaders.forResultSets(resultSets);
    }


}
