package tech.ydb.jdbc.settings;

import java.util.Map;

import javax.annotation.Nullable;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.TableClient;

public class YdbClientProperties {
    private final Map<YdbClientProperty<?>, ParsedProperty> params;

    public YdbClientProperties(Map<YdbClientProperty<?>, ParsedProperty> params) {
        this.params = params;
    }

    @Nullable
    public ParsedProperty getProperty(YdbClientProperty<?> property) {
        return params.get(property);
    }

    public Map<YdbClientProperty<?>, ParsedProperty> getParams() {
        return params;
    }

    public TableClient toTableClient(GrpcTransport grpc) {
        TableClient.Builder builder = TableClient.newClient(grpc);
        for (Map.Entry<YdbClientProperty<?>, ParsedProperty> entry : params.entrySet()) {
            if (entry.getValue() != null) {
                entry.getKey().getSetter().accept(builder, entry.getValue().getParsedValue());
            }
        }

        int minSize = 0;
        int maxSize = 50;

        ParsedProperty minSizeConfig = params.get(YdbClientProperty.SESSION_POOL_SIZE_MIN);
        ParsedProperty maxSizeConfig = params.get(YdbClientProperty.SESSION_POOL_SIZE_MAX);

        if (minSizeConfig != null) {
            minSize = Math.max(0, minSizeConfig.getParsedValue());
            maxSize = Math.min(maxSize, minSize);
        }
        if (maxSizeConfig != null) {
            maxSize = Math.max(minSize, maxSizeConfig.getParsedValue());
        }

        return builder.sessionPoolSize(minSize, maxSize).build();
    }
}
