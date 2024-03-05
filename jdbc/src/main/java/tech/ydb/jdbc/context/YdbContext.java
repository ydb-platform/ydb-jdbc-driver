package tech.ydb.jdbc.context;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import tech.ydb.core.UnexpectedResultException;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbPrepareMode;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.query.JdbcParams;
import tech.ydb.jdbc.query.JdbcQueryLexer;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.query.YdbQueryBuilder;
import tech.ydb.jdbc.query.params.BatchedParams;
import tech.ydb.jdbc.query.params.InMemoryParams;
import tech.ydb.jdbc.query.params.PreparedParams;
import tech.ydb.jdbc.settings.YdbClientProperties;
import tech.ydb.jdbc.settings.YdbConfig;
import tech.ydb.jdbc.settings.YdbConnectionProperties;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.jdbc.settings.YdbQueryProperties;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.impl.PooledTableClient;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
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

    private final YdbOperationProperties operationProperties;
    private final YdbQueryProperties queryOptions;

    private final GrpcTransport grpcTransport;
    private final PooledTableClient tableClient;
    private final SchemeClient schemeClient;
    private final SessionRetryContext retryCtx;

    private final Cache<String, YdbQuery> queriesCache;
    private final Cache<String, Map<String, Type>> queryParamsCache;

    private final boolean autoResizeSessionPool;
    private final AtomicInteger connectionsCount = new AtomicInteger();

    private YdbContext(
            YdbConfig config,
            YdbOperationProperties operationProperties,
            YdbQueryProperties queryProperties,
            GrpcTransport transport,
            PooledTableClient tableClient,
            boolean autoResize
    ) {
        this.config = config;

        this.operationProperties = operationProperties;
        this.queryOptions = queryProperties;
        this.autoResizeSessionPool = autoResize;

        this.grpcTransport = transport;
        this.tableClient = tableClient;
        this.schemeClient = SchemeClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(tableClient).build();

        int cacheSize = config.getPreparedStatementsCachecSize();
        if (cacheSize > 0) {
            queriesCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
            queryParamsCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
        } else {
            queriesCache = null;
            queryParamsCache = null;
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

    public String getDatabase() {
        return grpcTransport.getDatabase();
    }

    public SchemeClient getSchemeClient() {
        return schemeClient;
    }

    public TableClient getTableClient() {
        return tableClient;
    }

    public String getUrl() {
        return config.getUrl();
    }

    public int getConnectionsCount() {
        return connectionsCount.get();
    }

    public YdbOperationProperties getOperationProperties() {
        return operationProperties;
    }

    @Override
    public void close() {
        try {
            schemeClient.close();
            tableClient.close();
            grpcTransport.close();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unable to close client: " + e.getMessage(), e);
        }
    }

    public void register() {
        int actual = connectionsCount.incrementAndGet();
        int maxSize = tableClient.sessionPoolStats().getMaxSize();
        if (autoResizeSessionPool && actual > maxSize - SESSION_POOL_RESIZE_THRESHOLD) {
            int newSize = maxSize + SESSION_POOL_RESIZE_STEP;
            if (maxSize == tableClient.sessionPoolStats().getMaxSize()) {
                tableClient.updatePoolMaxSize(newSize);
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
                }
            }
        }
    }

    public static YdbContext createContext(YdbConfig config) throws SQLException {
        try {
            LOGGER.log(Level.INFO, "Creating new YDB connection to {0}", config.getConnectionString());

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
                return Executors.newScheduledThreadPool(1, (Runnable r) -> {
                    Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                });
            });

            GrpcTransport grpcTransport = builder.build();

            PooledTableClient.Builder tableClient = PooledTableClient.newClient(
                    GrpcTableRpc.useTransport(grpcTransport)
            );
            boolean autoResize = clientProps.applyToTableClient(tableClient);

            return new YdbContext(config, operationProps, queryProps, grpcTransport, tableClient.build(), autoResize);
        } catch (RuntimeException ex) {
            throw new SQLException("Cannot connect to YDB: " + ex.getMessage(), ex);
        }
    }

    public <T extends RequestSettings<?>> T withDefaultTimeout(T settings) {
        Duration operation = operationProperties.getDeadlineTimeout();
        if (!operation.isZero() && !operation.isNegative()) {
            settings.setOperationTimeout(operation);
            settings.setTimeout(operation.plusSeconds(1));
        }
        return settings;
    }

    public YdbQuery parseYdbQuery(String sql) throws SQLException {
        YdbQueryBuilder builder = new YdbQueryBuilder(sql, queryOptions.getForcedQueryType());
        JdbcQueryLexer.buildQuery(builder, queryOptions);
        return builder.build(queryOptions);
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

    public JdbcParams findOrCreateJdbcParams(YdbQuery query, YdbPrepareMode mode) throws SQLException {
        if (query.hasIndexesParameters()
                || mode == YdbPrepareMode.IN_MEMORY
                || !queryOptions.iPrepareDataQueries()) {
            return new InMemoryParams(query.getIndexesParameters());
        }

        String yql = query.getYqlQuery(null);
        PrepareDataQuerySettings settings = withDefaultTimeout(new PrepareDataQuerySettings());
        try {
            Map<String, Type> types = queryParamsCache.getIfPresent(query.originSQL());
            if (types == null) {
                types = retryCtx.supplyResult(session -> session.prepareDataQuery(yql, settings))
                        .join()
                        .getValue()
                        .types();
                queryParamsCache.put(query.originSQL(), types);
            }

            boolean requireBatch = mode == YdbPrepareMode.DATA_QUERY_BATCH;
            if (requireBatch || (mode == YdbPrepareMode.AUTO && queryOptions.isDetectBatchQueries())) {
                BatchedParams params = BatchedParams.tryCreateBatched(types);
                if (params != null) {
                    return params;
                }

                if (requireBatch) {
                    throw new SQLDataException(YdbConst.STATEMENT_IS_NOT_A_BATCH + query.originSQL());
                }
            }
            return new PreparedParams(types);
        } catch (UnexpectedResultException ex) {
            throw ExceptionFactory.createException("Cannot prepare data query: " + ex.getMessage(), ex);
        }
    }
}
