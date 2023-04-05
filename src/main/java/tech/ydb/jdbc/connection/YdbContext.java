package tech.ydb.jdbc.connection;

import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.jdbc.settings.YdbClientProperties;
import tech.ydb.jdbc.settings.YdbConnectionProperties;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.TableClient;

/**
 *
 * @author Aleksandr Gorshenin
 */

public class YdbContext implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(YdbContext.class.getName());

    private final YdbConfig config;

    private final GrpcTransport grpcTransport;
    private final TableClient tableClient;
    private final SchemeClient schemeClient;

    private YdbContext(YdbConfig config, GrpcTransport grpcTransport, TableClient tableClient) {
        this.config = config;
        this.grpcTransport = Objects.requireNonNull(grpcTransport);
        this.tableClient = Objects.requireNonNull(tableClient);
        this.schemeClient = SchemeClient.newClient(grpcTransport).build();
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

    public static YdbContext createContext(YdbConfig config) {
        YdbConnectionProperties connProps = config.getConnectionProperties();
        YdbClientProperties clientProps = config.getClientProperties();

        LOGGER.log(Level.INFO, "Creating new YDB connection to {0}", connProps.getConnectionString());

        GrpcTransport grpcTransport = connProps.toGrpcTransport();

        TableClient tableClient = clientProps.toTableClient(grpcTransport);

        return new YdbContext(config, grpcTransport, tableClient);
    }
}
