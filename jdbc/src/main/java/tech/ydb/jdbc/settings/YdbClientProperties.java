package tech.ydb.jdbc.settings;

import java.util.Map;

import javax.annotation.Nullable;

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
}
