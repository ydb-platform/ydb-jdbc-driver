package tech.ydb.jdbc.context;

import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import tech.ydb.core.Result;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbPrepareMode;
import tech.ydb.jdbc.YdbTracer;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.query.QueryKey;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbPreparedQuery;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.query.YqlBatcher;
import tech.ydb.jdbc.query.params.BatchedQuery;
import tech.ydb.jdbc.query.params.BulkUpsertQuery;
import tech.ydb.jdbc.query.params.InMemoryQuery;
import tech.ydb.jdbc.query.params.PreparedQuery;
import tech.ydb.jdbc.settings.YdbQueryProperties;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.ExplainDataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.settings.ExplainDataQuerySettings;
import tech.ydb.table.settings.PrepareDataQuerySettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbCache {
    private final YdbContext ctx;
    private final SessionRetryContext retryCtx;
    private final YdbQueryProperties queryOptions;

    private final Cache<QueryKey, YdbQuery> queriesCache;
    private final Cache<String, QueryStat> statsCache;
    private final Cache<String, Map<String, Type>> queryParamsCache;
    private final Cache<String, TableDescription> tableDescribeCache;

    private final Supplier<String> version = Suppliers.memoizeWithExpiration(this::readVersion, 1, TimeUnit.HOURS);

    public YdbCache(YdbContext ctx, YdbQueryProperties queryOptions, int cacheSize, boolean fullScanDetector) {
        this.ctx = ctx;
        this.retryCtx = SessionRetryContext.create(ctx.getTableClient()).build();
        this.queryOptions = queryOptions;

        if (cacheSize > 0) {
            queriesCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
            queryParamsCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
            tableDescribeCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
            if (fullScanDetector) {
                statsCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
            } else {
                statsCache = null;
            }
        } else {
            queriesCache = null;
            statsCache = null;
            queryParamsCache = null;
            tableDescribeCache = null;
        }
    }

    String getDatabaseVersion() {
        return version.get();
    }

    Cache<String, TableDescription> getTableDescriptionCache() {
        return tableDescribeCache;
    }

    YdbQueryProperties getQueryOptions() {
        return this.queryOptions;
    }

    public boolean queryStatsEnabled() {
        return statsCache != null;
    }

    public void resetQueryStats() {
        if (statsCache != null) {
            statsCache.invalidateAll();
        }
    }

    public Collection<QueryStat> getQueryStats() {
        if (statsCache == null) {
            return Collections.emptyList();
        }
        List<QueryStat> sorted = new ArrayList<>(statsCache.asMap().values());
        Collections.sort(sorted,
                Comparator
                        .comparingLong(QueryStat::getUsageCounter).reversed()
                        .thenComparing(QueryStat::getPreparedYQL)
        );
        return sorted;
    }

    private String readVersion() {
        Result<DataQueryResult> res = retryCtx.supplyResult(
                s -> s.executeDataQuery("SELECT version();", TxControl.snapshotRo())
        ).join();

        if (res.isSuccess()) {
            ResultSetReader rs = res.getValue().getResultSet(0);
            if (rs.next()) {
                return rs.getColumn(0).getBytesAsString(StandardCharsets.UTF_8);
            }
        }
        return "unknown";
    }

    public void traceQuery(YdbQuery query, String yql) {
        if (statsCache == null) {
            return;
        }

        QueryStat stat = statsCache.getIfPresent(yql);
        if (stat == null) {
            final ExplainDataQuerySettings settings = ctx.withDefaultTimeout(new ExplainDataQuerySettings());
            Result<ExplainDataQueryResult> res = retryCtx.supplyResult(
                    session -> session.explainDataQuery(yql, settings)
            ).join();

            if (res.isSuccess()) {
                ExplainDataQueryResult exp = res.getValue();
                stat = new QueryStat(query.getOriginQuery(), yql, exp.getQueryAst(), exp.getQueryPlan());
            } else {
                stat = new QueryStat(query.getOriginQuery(), yql, res.getStatus());
            }

            statsCache.put(yql, stat);
        }

        stat.incrementUsage();
    }

    public YdbQuery parseYdbQuery(QueryKey key) throws SQLException {
        if (queriesCache == null) {
            return YdbQuery.parseQuery(key, queryOptions, ctx.getTypes());
        }

        YdbQuery cached = queriesCache.getIfPresent(key);
        if (cached == null) {
            cached = YdbQuery.parseQuery(key, queryOptions, ctx.getTypes());
            queriesCache.put(key, cached);
        }

        return cached;
    }

    public YdbPreparedQuery prepareYdbQuery(YdbQuery query, YdbPrepareMode mode) throws SQLException {
        if (statsCache != null) {
            if (QueryStat.isPrint(query.getOriginQuery()) || QueryStat.isReset(query.getOriginQuery())) {
                return new InMemoryQuery(query, queryOptions.isDeclareJdbcParameters());
            }
        }

        QueryType type = query.getType();
        YqlBatcher batcher = query.getYqlBatcher();

        if (type == QueryType.BULK_QUERY) {
            if (batcher == null || batcher.getCommand() != YqlBatcher.Cmd.UPSERT) {
                throw new SQLException(YdbConst.BULKS_UNSUPPORTED);
            }
        }

        if (type == QueryType.EXPLAIN_QUERY || type == QueryType.SCHEME_QUERY ||
                !queryOptions.isPrepareDataQueries() || mode == YdbPrepareMode.IN_MEMORY) {
            return new InMemoryQuery(query, queryOptions.isDeclareJdbcParameters());
        }

        if (batcher != null && (mode == YdbPrepareMode.AUTO || type == QueryType.BULK_QUERY)) {
            YdbPreparedQuery batched = createBatchQuery(query, batcher);
            if (batched != null) {
                return batched;
            }
        }

        if (!query.isPlainYQL()) {
            return new InMemoryQuery(query, queryOptions.isDeclareJdbcParameters());
        }

        // try to prepare data query
        Map<String, Type> queryTypes = queryParamsCache.getIfPresent(query.getOriginQuery());
        if (queryTypes == null) {
            String yql = ctx.getPrefixPragma() + query.getPreparedYql();
            YdbTracer tracer = ctx.getTracer();
            tracer.trace("--> prepare data query");
            tracer.trace(yql);

            PrepareDataQuerySettings settings = ctx.withDefaultTimeout(new PrepareDataQuerySettings());
            Result<DataQuery> result = retryCtx.supplyResult(
                    session -> session.prepareDataQuery(yql, settings)
            ).join();

            tracer.trace("<-- " + result.getStatus());
            if (!result.isSuccess()) {
                tracer.close();
                throw ExceptionFactory.createException("Cannot prepare data query: " + result.getStatus(),
                        new UnexpectedResultException("Unexpected status", result.getStatus()));
            }

            queryTypes = result.getValue().types();
            queryParamsCache.put(query.getOriginQuery(), queryTypes);
        }

        boolean requireBatch = mode == YdbPrepareMode.DATA_QUERY_BATCH;
        if (requireBatch || (mode == YdbPrepareMode.AUTO && queryOptions.isDetectBatchQueries())) {
            BatchedQuery params = BatchedQuery.tryCreateBatched(ctx.getTypes(), query, queryTypes);
            if (params != null) {
                return params;
            }

            if (requireBatch) {
                throw new SQLDataException(YdbConst.STATEMENT_IS_NOT_A_BATCH + query.getOriginQuery());
            }
        }
        return new PreparedQuery(ctx.getTypes(), query, queryTypes);
    }

    private YdbPreparedQuery createBatchQuery(YdbQuery query, YqlBatcher batcher) throws SQLException {
        String tablePath = YdbContext.joined(ctx.getPrefixPath(), batcher.getTableName());
        Result<TableDescription> description = describeTable(tablePath);

        if (query.getType() == QueryType.BULK_QUERY) {
            if (query.getReturning() != null) {
                throw new SQLException(YdbConst.BULK_NOT_SUPPORT_RETURNING);
            }
            if (!description.isSuccess()) {
                throw new SQLException(YdbConst.BULK_DESCRIBE_ERROR + description.getStatus());
            }
            return BulkUpsertQuery.build(ctx.getTypes(), tablePath, batcher.getColumns(), description.getValue());
        }

        if (description.isSuccess()) {
            BatchedQuery params = BatchedQuery.createAutoBatched(ctx.getTypes(), query, description.getValue());
            if (params != null) {
                return params;
            }
        }

        return null;
    }

    private Result<TableDescription> describeTable(String tablePath) {
        TableDescription cached = tableDescribeCache.getIfPresent(tablePath);
        if (cached != null) {
            return Result.success(cached);
        }

        YdbTracer tracer = ctx.getTracer();
        tracer.trace("--> describe table");
        tracer.trace(tablePath);

        DescribeTableSettings settings = ctx.withDefaultTimeout(new DescribeTableSettings());
        Result<TableDescription> result = retryCtx.supplyResult(session -> session.describeTable(tablePath, settings))
                .join();

        tracer.trace("<-- " + result.getStatus());

        if (result.isSuccess()) {
            tableDescribeCache.put(tablePath, result.getValue());
        }

        return result;
    }
}
