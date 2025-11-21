package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.core.impl.SingleChannelTransport;
import tech.ydb.core.settings.BaseRequestSettings;
import tech.ydb.jdbc.YdbPrepareMode;
import tech.ydb.jdbc.YdbTracer;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.jdbc.impl.YdbTracerNone;
import tech.ydb.jdbc.query.QueryKey;
import tech.ydb.jdbc.query.YdbPreparedQuery;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.settings.YdbClientProperties;
import tech.ydb.jdbc.settings.YdbConfig;
import tech.ydb.jdbc.settings.YdbConnectionProperties;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.jdbc.settings.YdbQueryProperties;
import tech.ydb.jdbc.spi.YdbQueryExtentionService;
import tech.ydb.query.QueryClient;
import tech.ydb.query.impl.QueryClientImpl;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.TableClient;
import tech.ydb.table.impl.PooledTableClient;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.settings.RequestSettings;

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
    private final YdbTypes types;
    private final YdbCache cache;

    private final GrpcTransport grpcTransport;
    private final PooledTableClient tableClient;
    private final QueryClientImpl queryClient;
    private final SchemeClient schemeClient;
    private final String prefixPath;
    private final String prefixPragma;

    private final boolean autoResizeSessionPool;
    private final AtomicInteger connectionsCount = new AtomicInteger();

    private final YdbQueryExtentionService querySpi;

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
        this.autoResizeSessionPool = autoResize;

        this.grpcTransport = transport;
        this.tableClient = tableClient;
        this.queryClient = queryClient;
        this.schemeClient = SchemeClient.newClient(transport).build();

        if (config.hasPrefixPath()) {
            prefixPath = joined(transport.getDatabase(), config.getPrefixPath());
            prefixPragma = "PRAGMA TablePathPrefix = \"" + prefixPath + "\";\n";
        } else {
            prefixPath = transport.getDatabase();
            prefixPragma = "";
        }

        this.types = new YdbTypes(operationProperties.getForceNewDatetypes());

        String queryRewriteTable = operationOptions.getQueryRewriteTable();
        if (queryRewriteTable != null && !queryRewriteTable.isEmpty()) {
            String tablePath = joined(prefixPath, queryRewriteTable);
            this.cache = new YdbQueryRewriteCache(this, tablePath, operationOptions.getQueryRewriteTtl(),
                    queryProperties, config.getPreparedStatementsCachecSize(), config.isFullScanDetectorEnabled());
        } else {
            this.cache = new YdbCache(this,
                    queryProperties, config.getPreparedStatementsCachecSize(), config.isFullScanDetectorEnabled());
        }

        this.querySpi = YdbServiceLoader.loadQuerySpi();
    }

    public YdbTypes getTypes() {
        return types;
    }

    public GrpcTransport getGrpcTransport() {
        return grpcTransport;
    }

    public String getDatabaseVersion() {
        return cache.getDatabaseVersion();
    }

    public YdbTracer getTracer() {
        return config.isTxTracedEnabled() ? YdbTracer.current() : YdbTracerNone.DISABLED;
    }

    public YdbQueryExtentionService getQuerySpi() {
        return querySpi;
    }

    static String joined(String path1, String path2) {
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
        cache.validate();
        if (config.isUseQueryService()) {
            String txValidationTable = operationOptions.getTxValidationTable();
            if (txValidationTable != null && !txValidationTable.isEmpty()) {
                String tablePath = joined(prefixPath, txValidationTable);
                TableTxExecutor.validate(this, tablePath, cache.getTableDescriptionCache());
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

            grpcTransport = config.isUseDiscovery() ? builder.build() : new SingleChannelTransport(builder);

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

    public boolean isFullScanDetectorEnabled() {
        return cache.queryStatsEnabled();
    }

    public void traceQueryByFullScanDetector(YdbQuery query, String yql) {
        cache.traceQuery(query, yql);
    }

    public void resetFullScanDetector() {
        cache.resetQueryStats();
    }

    public Collection<QueryStat> getFullScanDetectorStats() {
        return cache.getQueryStats();
    }

    public YdbQuery createYdbQuery(String query) throws SQLException {
        return YdbQuery.parseQuery(new QueryKey(query), cache.getQueryOptions(), types);
    }

    public YdbQuery parseYdbQuery(QueryKey key) throws SQLException {
        return cache.parseYdbQuery(key);
    }

    public YdbPreparedQuery prepareYdbQuery(YdbQuery query, YdbPrepareMode mode) throws SQLException {
        return cache.prepareYdbQuery(query, mode);
    }
}
