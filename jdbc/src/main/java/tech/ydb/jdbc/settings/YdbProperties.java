package tech.ydb.jdbc.settings;


import java.sql.DriverPropertyInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class YdbProperties {
    private final YdbConnectionProperties connectionProperties;
    private final YdbClientProperties clientProperties;
    private final YdbOperationProperties operationProperties;

    public YdbProperties(YdbConnectionProperties connectionProperties, YdbClientProperties clientProperties,
                         YdbOperationProperties operationProperties) {
        this.connectionProperties = Objects.requireNonNull(connectionProperties);
        this.clientProperties = Objects.requireNonNull(clientProperties);
        this.operationProperties = Objects.requireNonNull(operationProperties);
    }

    public YdbConnectionProperties getConnectionProperties() {
        return connectionProperties;
    }

    public YdbClientProperties getClientProperties() {
        return clientProperties;
    }

    public YdbOperationProperties getOperationProperties() {
        return operationProperties;
    }

    public DriverPropertyInfo[] toDriverProperties() {
        List<DriverPropertyInfo> properties = new ArrayList<>();
        connectionProperties.getParams().forEach((property, value) ->
                properties.add(property.toDriverPropertyInfoFrom(value)));
        clientProperties.getParams().forEach((property, value) ->
                properties.add(property.toDriverPropertyInfoFrom(value)));
        operationProperties.getParams().forEach((property, value) ->
                properties.add(property.toDriverPropertyInfoFrom(value)));
        return properties.toArray(new DriverPropertyInfo[0]);
    }
}
