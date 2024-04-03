package tech.ydb.jdbc.settings;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

import tech.ydb.table.TableClient;


public class YdbClientProperties {
    private static final int SESSION_POOL_DEFAULT_MIN_SIZE = 0;
    private static final int SESSION_POOL_DEFAULT_MAX_SIZE = 50;

    static final YdbProperty<Boolean> KEEP_QUERY_TEXT = YdbProperty.bool(
            "keepQueryText", "Keep Query text"
    );

    static final YdbProperty<Duration> SESSION_KEEP_ALIVE_TIME = YdbProperty.duration(
            "sessionKeepAliveTime", "Session keep-alive timeout"
    );

    static final YdbProperty<Duration> SESSION_MAX_IDLE_TIME = YdbProperty.duration(
            "sessionMaxIdleTime", "Session max idle time"
    );

    static final YdbProperty<Integer> SESSION_POOL_SIZE_MIN = YdbProperty.integer(
            "sessionPoolSizeMin", "Session pool min size (with with sessionPoolSizeMax)"
    );

    static final YdbProperty<Integer> SESSION_POOL_SIZE_MAX = YdbProperty.integer(
            "sessionPoolSizeMax", "Session pool max size (with with sessionPoolSizeMin)"
    );

    private final YdbPropertyValue<Boolean> keepQueryText;
    private final YdbPropertyValue<Duration> sessionKeepAliveTime;
    private final YdbPropertyValue<Duration> sessionMaxIdleTime;
    private final YdbPropertyValue<Integer> sessionPoolMinSize;
    private final YdbPropertyValue<Integer> sessionPoolMaxSize;

    public YdbClientProperties(YdbConfig config) throws SQLException {
        Properties props = config.getProperties();

        this.keepQueryText = KEEP_QUERY_TEXT.readValue(props);
        this.sessionKeepAliveTime = SESSION_KEEP_ALIVE_TIME.readValue(props);
        this.sessionMaxIdleTime = SESSION_MAX_IDLE_TIME.readValue(props);
        this.sessionPoolMinSize = SESSION_POOL_SIZE_MIN.readValue(props);
        this.sessionPoolMaxSize = SESSION_POOL_SIZE_MAX.readValue(props);
    }

    public boolean applyToTableClient(TableClient.Builder builder) {
        if (keepQueryText.hasValue()) {
            builder.keepQueryText(keepQueryText.getValue());
        }

        if (sessionKeepAliveTime.hasValue()) {
            builder.sessionKeepAliveTime(sessionKeepAliveTime.getValue());
        }

        if (sessionMaxIdleTime.hasValue()) {
            builder.sessionMaxIdleTime(sessionMaxIdleTime.getValue());
        }

        if (!sessionPoolMinSize.hasValue() && !sessionPoolMaxSize.hasValue()) {
            return true;
        }

        int minSize = SESSION_POOL_DEFAULT_MIN_SIZE;
        int maxSize = SESSION_POOL_DEFAULT_MAX_SIZE;

        if (sessionPoolMinSize.hasValue()) {
            minSize = Math.max(0, sessionPoolMinSize.getValue());
            maxSize = Math.max(maxSize, minSize);
        }
        if (sessionPoolMaxSize.hasValue()) {
            maxSize = Math.max(minSize + 1, sessionPoolMaxSize.getValue());
        }

        builder.sessionPoolSize(minSize, maxSize);
        return false;
    }

}
