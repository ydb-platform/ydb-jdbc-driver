package tech.ydb.jdbc.context;

import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import tech.ydb.core.Result;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.core.settings.BaseRequestSettings;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbPrepareMode;
import tech.ydb.jdbc.YdbTracer;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.impl.YdbTracerNone;
import tech.ydb.jdbc.query.QueryKey;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.jdbc.query.YdbPreparedQuery;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.query.YqlBatcher;
import tech.ydb.jdbc.query.params.BatchedQuery;
import tech.ydb.jdbc.query.params.BulkUpsertQuery;
import tech.ydb.jdbc.query.params.InMemoryQuery;
import tech.ydb.jdbc.query.params.PreparedQuery;
import tech.ydb.jdbc.settings.YdbClientProperties;
import tech.ydb.jdbc.settings.YdbConfig;
import tech.ydb.jdbc.settings.YdbConnectionProperties;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.jdbc.settings.YdbQueryProperties;
import tech.ydb.query.QueryClient;
import tech.ydb.query.impl.QueryClientImpl;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.impl.PooledTableClient;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.ExplainDataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.settings.ExplainDataQuerySettings;
import tech.ydb.table.settings.PrepareDataQuerySettings;
import tech.ydb.table.settings.RequestSettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */

public class YdbContext implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(YdbContext.class.getName());

    private static final int SESSION_POOL_RESIZE_STEP = 50;
    private static final int SESSION_POOL_RESIZE_THRESHOLD = 10;

    private final YdbConfig config;

    private final YdbOperationProperties operationOptions;
    private final YdbQueryProperties queryOptions;
    private final YdbTypes types;

    private final GrpcTransport grpcTransport;
    private final PooledTableClient tableClient;
    private final QueryClientImpl queryClient;
    private final SchemeClient schemeClient;
    private final SessionRetryContext retryCtx;
    private final String prefixPath;
    private final String prefixPragma;

    private final Cache<QueryKey, YdbQuery> queriesCache;
    private final Cache<String, QueryStat> statsCache;
    private final Cache<String, Map<String, Type>> queryParamsCache;
    private final Cache<String, TableDescription> tableDescribeCache;
    private final Supplier<String> version = Suppliers.memoizeWithExpiration(this::readVersion, 1, TimeUnit.HOURS);

    private final boolean autoResizeSessionPool;
    private final AtomicInteger connectionsCount = new AtomicInteger();

    private YdbContext(
            YdbConfig config,
            YdbOperationProperties operationProperties,
            YdbQueryProperties queryProperties,
            GrpcTransport transport,
            PooledTableClient tableClient,
            QueryClientImpl queryClient,
            boolean autoResize
    ) {
        this.config = config;

        this.operationOptions = operationProperties;
        this.queryOptions = queryProperties;
        this.autoResizeSessionPool = autoResize;

        this.grpcTransport = transport;
        this.tableClient = tableClient;
        this.queryClient = queryClient;
        this.schemeClient = SchemeClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(tableClient).build();

        this.types = new YdbTypes(operationProperties.getForceNewDatetypes());

        int cacheSize = config.getPreparedStatementsCachecSize();
        if (cacheSize > 0) {
            queriesCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
            queryParamsCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
            tableDescribeCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
            if (config.isFullScanDetectorEnabled()) {
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

        if (config.hasPrefixPath()) {
            prefixPath = joined(transport.getDatabase(), config.getPrefixPath());
            prefixPragma = "PRAGMA TablePathPrefix = \"" + prefixPath + "\";\n";
        } else {
            prefixPath = transport.getDatabase();
            prefixPragma = "";
        }
    }

    public YdbTypes getTypes() {
        return types;
    }

    public GrpcTransport getGrpcTransport() {
        return grpcTransport;
    }

    public String getDatabaseVersion() {
        return version.get();
    }

    public YdbTracer getTracer() {
        return config.isTxTracedEnabled() ? YdbTracer.current() : YdbTracerNone.DISABLED;
    }

    private String joined(String path1, String path2) {
        return path1.endsWith("/") || path2.startsWith("/") ? path1 + path2 : path1 + "/" + path2;
    }

    public String getPrefixPath() {
        return prefixPath;
    }

    String getPrefixPragma() {
        return prefixPragma;
    }

    public SchemeClient getSchemeClient() {
        return schemeClient;
    }

    public TableClient getTableClient() {
        return tableClient;
    }

    public QueryClient getQueryClient() {
        return queryClient;
    }

    public String getUrl() {
        return config.getUrl();
    }

    public String getUsername() {
        return config.getUsername();
    }

    public YdbExecutor createExecutor() throws SQLException {
        if (config.isUseQueryService()) {
            String txValidationTable = operationOptions.getTxValidationTable();
            if (txValidationTable != null && !txValidationTable.isEmpty()) {
                String tablePath = joined(prefixPath, txValidationTable);
                TableTxExecutor.validate(this, tablePath, tableDescribeCache);
                return new TableTxExecutor(this, tablePath);
            }
            return new QueryServiceExecutor(this);
        } else {
            return new TableServiceExecutor(this);
        }
    }

    public int getConnectionsCount() {
        return connectionsCount.get();
    }

    public YdbOperationProperties getOperationProperties() {
        return operationOptions;
    }

    @Override
    public void close() {
        try {
            schemeClient.close();
            queryClient.close();
            tableClient.close();
            grpcTransport.close();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unable to close client: " + e.getMessage(), e);
        }
    }

    public boolean hasConnections() {
        return connectionsCount.get() > 0;
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

    public void register() {
        int actual = connectionsCount.incrementAndGet();
        int maxSize = tableClient.sessionPoolStats().getMaxSize();
        if (autoResizeSessionPool && actual > maxSize - SESSION_POOL_RESIZE_THRESHOLD) {
            int newSize = maxSize + SESSION_POOL_RESIZE_STEP;
            if (maxSize == tableClient.sessionPoolStats().getMaxSize()) {
                tableClient.updatePoolMaxSize(newSize);
                queryClient.updatePoolMaxSize(newSize);
            }
        }
    }

    public void deregister() {
        YdbTracer.clear();

        int actual = connectionsCount.decrementAndGet();
        int maxSize = tableClient.sessionPoolStats().getMaxSize();
        if (autoResizeSessionPool && maxSize > SESSION_POOL_RESIZE_STEP) {
            if (actual < maxSize - SESSION_POOL_RESIZE_STEP - 2 * SESSION_POOL_RESIZE_THRESHOLD) {
                int newSize = maxSize - SESSION_POOL_RESIZE_STEP;
                if (maxSize == tableClient.sessionPoolStats().getMaxSize()) {
                    tableClient.updatePoolMaxSize(newSize);
                    queryClient.updatePoolMaxSize(newSize);
                }
            }
        }
    }

    public static YdbContext createContext(YdbConfig config) throws SQLException {
        GrpcTransport grpcTransport = null;
        try {
            LOGGER.log(Level.FINE, "Creating new YDB context to {0}", config.getConnectionString());

            YdbConnectionProperties connProps = new YdbConnectionProperties(config);
            YdbClientProperties clientProps = new YdbClientProperties(config);
            YdbOperationProperties operationProps = new YdbOperationProperties(config);
            YdbQueryProperties queryProps = new YdbQueryProperties(config);

            GrpcTransportBuilder builder = GrpcTransport.forConnectionString(config.getConnectionString());
            connProps.applyToGrpcTransport(builder);

            // Use custom single thread scheduler
            // because JDBC driver doesn't need to execute retries except for DISCOVERY
            builder.withSchedulerFactory(() -> {
                final String namePrefix = "ydb-jdbc-scheduler[" + config.hashCode() + "]-thread-";
                final AtomicInteger threadNumber = new AtomicInteger(1);
                return Executors.newScheduledThreadPool(2, (Runnable r) -> {
                    Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                });
            });

            grpcTransport = builder.build();

            PooledTableClient.Builder tableClient = PooledTableClient.newClient(
                    GrpcTableRpc.useTransport(grpcTransport)
            );
            QueryClientImpl.Builder queryClient = QueryClientImpl.newClient(grpcTransport);

            boolean autoResize = clientProps.applyToTableClient(tableClient, queryClient);

            return new YdbContext(config, operationProps, queryProps, grpcTransport, tableClient.build(),
                    queryClient.build(), autoResize);
        } catch (RuntimeException ex) {
            if (grpcTransport != null) {
                try {
                    grpcTransport.close();
                } catch (Exception exClose) {
                    LOGGER.log(Level.FINE, "Issue when closing gRPC transport", exClose);
                }
            }
            StringBuilder sb = new StringBuilder("Cannot connect to YDB: ").append(ex.getMessage());
            Throwable cause = ex.getCause();
            while (cause != null) {
                sb.append(", ").append(cause.getMessage());
                cause = cause.getCause();
            }
            throw new SQLException(sb.toString(), ex);
        }
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


    public <T extends RequestSettings<?>> T withDefaultTimeout(T settings) {
        Duration operation = operationOptions.getDeadlineTimeout();
        if (!operation.isZero() && !operation.isNegative()) {
            settings.setOperationTimeout(operation);
            settings.setTimeout(operation.plusSeconds(1));
        }
        return settings;
    }

    public <T extends BaseRequestSettings.BaseBuilder<T>> T withRequestTimeout(T builder) {
        Duration operation = operationOptions.getDeadlineTimeout();
        if (operation.isNegative() || operation.isZero()) {
            return builder;
        }

        return builder.withRequestTimeout(operation);
    }

    public YdbQuery parseYdbQuery(String query) throws SQLException {
        return YdbQuery.parseQuery(new QueryKey(query), queryOptions, types);
    }

    public YdbQuery findOrParseYdbQuery(QueryKey key) throws SQLException {
        if (queriesCache == null) {
            return YdbQuery.parseQuery(key, queryOptions, types);
        }

        YdbQuery cached = queriesCache.getIfPresent(key);
        if (cached == null) {
            cached = YdbQuery.parseQuery(key, queryOptions, types);
            queriesCache.put(key, cached);
        }

        return cached;
    }

    public void traceQuery(YdbQuery query, String yql) {
        if (statsCache == null) {
            return;
        }

        QueryStat stat = statsCache.getIfPresent(yql);
        if (stat == null) {
            final ExplainDataQuerySettings settings = withDefaultTimeout(new ExplainDataQuerySettings());
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

    public YdbPreparedQuery findOrPrepareParams(YdbQuery query, YdbPrepareMode mode) throws SQLException {
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
            String tablePath = joined(getPrefixPath(), batcher.getTableName());
            TableDescription description = tableDescribeCache.getIfPresent(tablePath);
            if (description == null) {
                YdbTracer tracer = getTracer();
                tracer.trace("--> describe table");
                tracer.trace(tablePath);

                DescribeTableSettings settings = withDefaultTimeout(new DescribeTableSettings());
                Result<TableDescription> result = retryCtx.supplyResult(
                        session -> session.describeTable(tablePath, settings)
                ).join();

                tracer.trace("<-- " + result.getStatus());

                if (result.isSuccess()) {
                    description = result.getValue();
                    tableDescribeCache.put(tablePath, description);
                } else {
                    if (type == QueryType.BULK_QUERY) {
                        throw new SQLException(YdbConst.BULK_DESCRIBE_ERROR + result.getStatus());
                    }
                }
            }
            if (type == QueryType.BULK_QUERY) {
                if (query.getReturning() != null) {
                    throw new SQLException(YdbConst.BULK_NOT_SUPPORT_RETURNING);
                }
                return BulkUpsertQuery.build(types, tablePath, batcher.getColumns(), description);
            }

            if (description != null) {
                BatchedQuery params = BatchedQuery.createAutoBatched(types, query, description);
                if (params != null) {
                    return params;
                }
            }
        }

        if (!query.isPlainYQL()) {
            return new InMemoryQuery(query, queryOptions.isDeclareJdbcParameters());
        }

        // try to prepare data query
        Map<String, Type> queryTypes = queryParamsCache.getIfPresent(query.getOriginQuery());
        if (queryTypes == null) {
            String yql = prefixPragma + query.getPreparedYql();
            YdbTracer tracer = getTracer();
            tracer.trace("--> prepare data query");
            tracer.trace(yql);

            PrepareDataQuerySettings settings = withDefaultTimeout(new PrepareDataQuerySettings());
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
            BatchedQuery params = BatchedQuery.tryCreateBatched(types, query, queryTypes);
            if (params != null) {
                return params;
            }

            if (requireBatch) {
                throw new SQLDataException(YdbConst.STATEMENT_IS_NOT_A_BATCH + query.getOriginQuery());
            }
        }
        return new PreparedQuery(types, query, queryTypes);
    }
}
