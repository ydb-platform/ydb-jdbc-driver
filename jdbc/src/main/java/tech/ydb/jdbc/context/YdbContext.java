package tech.ydb.jdbc.context;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.core.settings.BaseRequestSettings;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbPrepareMode;
import tech.ydb.jdbc.YdbTracer;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.impl.YdbTracerNone;
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
import tech.ydb.table.query.ExplainDataQueryResult;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.settings.ExplainDataQuerySettings;
import tech.ydb.table.settings.PrepareDataQuerySettings;
import tech.ydb.table.settings.RequestSettings;
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

    private final YdbOperationProperties operationProps;
    private final YdbQueryProperties queryOptions;

    private final GrpcTransport grpcTransport;
    private final PooledTableClient tableClient;
    private final QueryClientImpl queryClient;
    private final SchemeClient schemeClient;
    private final SessionRetryContext retryCtx;
    private final String prefixPath;
    private final String prefixPragma;

    private final Cache<String, YdbQuery> queriesCache;
    private final Cache<String, QueryStat> statsCache;
    private final Cache<String, Map<String, Type>> queryParamsCache;
    private final Cache<String, TableDescription> tableDescribeCache;

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

        this.operationProps = operationProperties;
        this.queryOptions = queryProperties;
        this.autoResizeSessionPool = autoResize;

        this.grpcTransport = transport;
        this.tableClient = tableClient;
        this.queryClient = queryClient;
        this.schemeClient = SchemeClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(tableClient).build();

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

    /**
     * Grpc Transport for other API YDB server clients
     *
     * @return grpcTransport for YDB
     */
    public GrpcTransport getGrpcTransport() {
        return grpcTransport;
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

    public SessionRetryContext getRetryCtx() {
        return retryCtx;
    }

    public String getUrl() {
        return config.getUrl();
    }

    public String getUsername() {
        return config.getUsername();
    }

    public YdbTracer getTracer() {
        return config.isTxTracedEnabled() ? YdbTracer.current() : YdbTracerNone.current();
    }

    public boolean isTxTracerEnabled() {
        return config.isTxTracedEnabled();
    }

    public YdbExecutor createExecutor() throws SQLException {
        if (config.isUseQueryService()) {
            if (operationProps.getProcessUndetermined()) {
                return new QueryServiceExecutorExt(
                        this, operationProps.getTransactionLevel(), operationProps.isAutoCommit());
            } else {
                return new QueryServiceExecutor(
                        this, operationProps.getTransactionLevel(), operationProps.isAutoCommit());
            }
        } else {
            return new TableServiceExecutor(
                    this, operationProps.getTransactionLevel(), operationProps.isAutoCommit());
        }
    }

    public int getConnectionsCount() {
        return connectionsCount.get();
    }

    public YdbOperationProperties getOperationProperties() {
        return operationProps;
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

            YdbContext yc = new YdbContext(config, operationProps, queryProps, grpcTransport,
                    tableClient.build(), queryClient.build(), autoResize);
            if (operationProps.getProcessUndetermined()) {
                if (config.isUseQueryService()) {
                    yc.ensureTransactionTableExists();
                } else {
                    LOGGER.log(Level.WARNING, "UNDETERMINED processing is disabled, "
                            + "because it is only supported for QueryService execution mode.");
                }
            }
            return yc;
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

    public void ensureTransactionTableExists() throws SQLException {
        String tableName = operationProps.getProcessUndeterminedTable();
        if (tableName.isEmpty()) {
            return;
        }
        LOGGER.log(Level.FINE, "Using table {} for UNDETERMINED processing", tableName);
        String sqlCreate = "CREATE TABLE IF NOT EXISTS `" + tableName
                + "` (trans_id Text NOT NULL, trans_tv Timestamp,"
                + "   PRIMARY KEY (trans_id)) WITH ("
                + "TTL=Interval('PT60M') ON trans_tv,"
                + "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=100,"
                + "AUTO_PARTITIONING_MAX_PARTITIONS_COUNT=150,"
                + "AUTO_PARTITIONING_BY_LOAD=ENABLED,"
                + "AUTO_PARTITIONING_BY_SIZE=ENABLED,"
                + "AUTO_PARTITIONING_PARTITION_SIZE_MB=100"
                + ");";
        Status status = retryCtx.supplyStatus(
                session -> session.executeSchemeQuery(sqlCreate))
                .join();
        new YdbValidator().validate(
                "Create table " + tableName,
                getTracer(), status);
    }

    public <T extends RequestSettings<?>> T withDefaultTimeout(T settings) {
        Duration operation = operationProps.getDeadlineTimeout();
        if (!operation.isZero() && !operation.isNegative()) {
            settings.setOperationTimeout(operation);
            settings.setTimeout(operation.plusSeconds(1));
        }
        return settings;
    }

    public <T extends BaseRequestSettings.BaseBuilder<T>> T withRequestTimeout(T builder) {
        Duration operation = operationProps.getDeadlineTimeout();
        if (operation.isNegative() || operation.isZero()) {
            return builder;
        }

        return builder.withRequestTimeout(operation);
    }

    public YdbQuery parseYdbQuery(String sql) throws SQLException {
        return YdbQuery.parseQuery(sql, queryOptions);
    }

    public YdbQuery findOrParseYdbQuery(String sql) throws SQLException {
        if (queriesCache == null) {
            return parseYdbQuery(sql);
        }

        YdbQuery cached = queriesCache.getIfPresent(sql);
        if (cached == null) {
            cached = parseYdbQuery(sql);
            queriesCache.put(sql, cached);
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

        if (type == QueryType.BULK_QUERY) {
            if (query.getYqlBatcher() == null || query.getYqlBatcher().getCommand() != YqlBatcher.Cmd.UPSERT) {
                throw new SQLException(YdbConst.BULKS_UNSUPPORTED);
            }
        }

        if (type == QueryType.EXPLAIN_QUERY || type == QueryType.SCHEME_QUERY ||
                !queryOptions.isPrepareDataQueries() || mode == YdbPrepareMode.IN_MEMORY) {
            return new InMemoryQuery(query, queryOptions.isDeclareJdbcParameters());
        }

        if (query.getYqlBatcher() != null && (mode == YdbPrepareMode.AUTO || type == QueryType.BULK_QUERY)) {
            String tablePath = joined(getPrefixPath(), query.getYqlBatcher().getTableName());
            TableDescription description = tableDescribeCache.getIfPresent(tablePath);
            if (description == null) {
                YdbTracer tracer = getTracer();
                tracer.trace("--> describe table");
                tracer.query(tablePath);

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
                        throw new SQLException(YdbConst.BULKS_DESCRIBE_ERROR + result.getStatus());
                    }
                }
            }
            if (type == QueryType.BULK_QUERY) {
                return BulkUpsertQuery.build(tablePath, query.getYqlBatcher().getColumns(), description);
            }

            if (description != null) {
                BatchedQuery params = BatchedQuery.createAutoBatched(query.getYqlBatcher(), description);
                if (params != null) {
                    return params;
                }
            }
        }

        if (!query.isPlainYQL()) {
            return new InMemoryQuery(query, queryOptions.isDeclareJdbcParameters());
        }

        // try to prepare data query
        Map<String, Type> types = queryParamsCache.getIfPresent(query.getOriginQuery());
        if (types == null) {
            String yql = prefixPragma + query.getPreparedYql();
            YdbTracer tracer = getTracer();
            tracer.trace("--> prepare data query");
            tracer.query(yql);

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

            types = result.getValue().types();
            queryParamsCache.put(query.getOriginQuery(), types);
        }

        boolean requireBatch = mode == YdbPrepareMode.DATA_QUERY_BATCH;
        if (requireBatch || (mode == YdbPrepareMode.AUTO && queryOptions.isDetectBatchQueries())) {
            BatchedQuery params = BatchedQuery.tryCreateBatched(query, types);
            if (params != null) {
                return params;
            }

            if (requireBatch) {
                throw new SQLDataException(YdbConst.STATEMENT_IS_NOT_A_BATCH + query.getOriginQuery());
            }
        }
        return new PreparedQuery(query, types);
    }
}
