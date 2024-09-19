package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.util.Collections;

import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.impl.YdbQueryResult;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.SessionRetryContext;
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
}
