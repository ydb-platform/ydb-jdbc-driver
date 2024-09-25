package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.jdbc.YdbConst;
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
    private final AtomicReference<Barrier> barrier;

    public BaseYdbExecutor(YdbContext ctx) {
        this.retryCtx = ctx.getRetryCtx();
        this.barrier = new AtomicReference<>(new Barrier());
        this.barrier.get().open();
    }

    protected void checkBarrier() {
        barrier.get().waitOpening();
    }

    protected Barrier createBarrier() {
        Barrier newBarrier = new Barrier();
        Barrier prev = barrier.getAndSet(newBarrier);
        prev.waitOpening();
        return newBarrier;
    }

    @Override
    public void ensureOpened() throws SQLException {
        checkBarrier();
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

    public class Barrier {
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        public void open() {
            future.complete(null);
        }

        public void waitOpening() {
            future.join();
        }
    }
}
