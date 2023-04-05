package tech.ydb.jdbc.settings;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;

public class YdbConnectionProperties {
    private final String safeURL;
    private final String connectionString;
    private final Map<YdbConnectionProperty<?>, ParsedProperty> params;

    public YdbConnectionProperties(String safeURL, String connectionString,
                                   Map<YdbConnectionProperty<?>, ParsedProperty> params) {
        this.safeURL = safeURL;
        this.connectionString = Objects.requireNonNull(connectionString);
        this.params = Objects.requireNonNull(params);
    }

    public String getSafeUrl() {
        return safeURL;
    }

    public String getConnectionString() {
        return connectionString;
    }

    @Nullable
    public ParsedProperty getProperty(YdbConnectionProperty<?> property) {
        return params.get(property);
    }

    public Map<YdbConnectionProperty<?>, ParsedProperty> getParams() {
        return params;
    }

    public GrpcTransport toGrpcTransport() {
        GrpcTransportBuilder builder = GrpcTransport.forConnectionString(connectionString);
        for (Map.Entry<YdbConnectionProperty<?>, ParsedProperty> entry : params.entrySet()) {
            if (entry.getValue() != null) {
                entry.getKey().getSetter().accept(builder, entry.getValue().getParsedValue());
            }
        }
        return builder.build();
    }
}
