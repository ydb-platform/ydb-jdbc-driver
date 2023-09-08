package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.jdbc.exception.YdbConfigurationException;
import tech.ydb.jdbc.settings.ParsedProperty;
import tech.ydb.jdbc.settings.YdbClientProperties;
import tech.ydb.jdbc.settings.YdbClientProperty;
import tech.ydb.jdbc.settings.YdbConnectionProperties;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.TableClient;
import tech.ydb.table.impl.PooledTableClient;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;

/**
 *
 * @author Aleksandr Gorshenin
 */

public class YdbContext implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(YdbContext.class.getName());
    private static final int SESSION_POOL_STEP = 50;
    private static final int SESSION_POOL_THRESHOLD = 10;

    private final YdbConfig config;

    private final GrpcTransport grpcTransport;
    private final PooledTableClient tableClient;
    private final SchemeClient schemeClient;
    private final boolean autoResizeSessionPool;

    private final AtomicInteger connectionsCount = new AtomicInteger();

    private YdbContext(YdbConfig config, GrpcTransport transport, PooledTableClient tableClient, boolean autoResize) {
        this.config = config;
        this.grpcTransport = Objects.requireNonNull(transport);
        this.tableClient = Objects.requireNonNull(tableClient);
        this.schemeClient = SchemeClient.newClient(transport).build();
        this.autoResizeSessionPool = autoResize;
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

    public YdbOperationProperties getOperationProperties() {
        return config.getOperationProperties();
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
        if (autoResizeSessionPool && actual > maxSize - SESSION_POOL_THRESHOLD) {
            int newSize = maxSize + SESSION_POOL_STEP;
            if (maxSize == tableClient.sessionPoolStats().getMaxSize()) {
                tableClient.updatePoolMaxSize(newSize);
            }
        }
    }

    public void deregister() {
        int actual = connectionsCount.decrementAndGet();
        int maxSize = tableClient.sessionPoolStats().getMaxSize();
        if (autoResizeSessionPool && maxSize > SESSION_POOL_STEP) {
            if (actual < maxSize - SESSION_POOL_STEP + SESSION_POOL_THRESHOLD) {
                int newSize = maxSize - SESSION_POOL_STEP;
                if (maxSize == tableClient.sessionPoolStats().getMaxSize()) {
                    tableClient.updatePoolMaxSize(newSize);
                }
            }
        }
    }

    public static YdbContext createContext(YdbConfig config) throws SQLException {
        try {
            YdbConnectionProperties connProps = config.getConnectionProperties();
            YdbClientProperties clientProps = config.getClientProperties();

            LOGGER.log(Level.INFO, "Creating new YDB connection to {0}", connProps.getConnectionString());

            GrpcTransport grpcTransport = connProps.toGrpcTransport();
            PooledTableClient.Builder tableClient = PooledTableClient.newClient(
                    GrpcTableRpc.useTransport(grpcTransport)
            );
            boolean autoResize = buildTableClient(tableClient, clientProps);

            return new YdbContext(config, grpcTransport, tableClient.build(), autoResize);
        } catch (Exception ex) {
            throw new YdbConfigurationException("Cannot connect to YDB", ex);
        }
    }

    private static boolean buildTableClient(TableClient.Builder builder, YdbClientProperties props) {
        for (Map.Entry<YdbClientProperty<?>, ParsedProperty> entry : props.getParams().entrySet()) {
            if (entry.getValue() != null) {
                entry.getKey().getSetter().accept(builder, entry.getValue().getParsedValue());
            }
        }

        ParsedProperty minSizeConfig = props.getProperty(YdbClientProperty.SESSION_POOL_SIZE_MIN);
        ParsedProperty maxSizeConfig = props.getProperty(YdbClientProperty.SESSION_POOL_SIZE_MAX);

        if (minSizeConfig == null && maxSizeConfig == null) {
            return true;
        }

        int minSize = 0;
        int maxSize = 50;

        if (minSizeConfig != null) {
            minSize = Math.max(0, minSizeConfig.getParsedValue());
            maxSize = Math.min(maxSize, minSize);
        }
        if (maxSizeConfig != null) {
            maxSize = Math.max(minSize, maxSizeConfig.getParsedValue());
        }

        builder.sessionPoolSize(minSize, maxSize);
        return false;
    }
}
