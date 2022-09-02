package tech.ydb.jdbc.settings;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.net.HostAndPort;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;

@SuppressWarnings("UnstableApiUsage")
public class YdbConnectionProperties {
    private final HostAndPort address;
    private final Map<YdbConnectionProperty<?>, ParsedProperty> params;

    public YdbConnectionProperties(HostAndPort address,
                                   Map<YdbConnectionProperty<?>, ParsedProperty> params) {
        this.address = Objects.requireNonNull(address);
        this.params = Objects.requireNonNull(params);
    }

    public HostAndPort getAddress() {
        return address;
    }

    @Nullable
    public String getDatabase() {
        ParsedProperty databaseProperty = getProperty(YdbConnectionProperty.DATABASE);
        return databaseProperty != null ? databaseProperty.getParsedValue() : null;
    }

    @Nullable
    public ParsedProperty getProperty(YdbConnectionProperty<?> property) {
        return params.get(property);
    }

    public Map<YdbConnectionProperty<?>, ParsedProperty> getParams() {
        return params;
    }

    public GrpcTransport toGrpcTransport() {
        GrpcTransportBuilder builder = GrpcTransport.forHost(address, getDatabase());
        for (Map.Entry<YdbConnectionProperty<?>, ParsedProperty> entry : params.entrySet()) {
            if (entry.getValue() != null) {
                entry.getKey().getSetter().accept(builder, entry.getValue().getParsedValue());
            }
        }
        return builder.build();
    }

}
