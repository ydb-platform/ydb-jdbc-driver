package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;

import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.impl.YdbQueryResult;
import tech.ydb.jdbc.impl.YdbStaticResultSet;
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

        return new StaticQueryResult(query, Collections.emptyList());
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

        return new StaticQueryResult(query, Collections.emptyList());
    }

    @Override
    public YdbQueryResult executeScanQuery(YdbStatement statement, YdbQuery query, String yql, Params params)
            throws SQLException {
        ensureOpened();

        YdbContext ctx = statement.getConnection().getCtx();
        YdbValidator validator = statement.getValidator();

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

        YdbResultSet rs = new YdbStaticResultSet(statement, ProtoValueReaders.forResultSets(resultSets));
        return new StaticQueryResult(query, Collections.singletonList(rs));
    }
}
