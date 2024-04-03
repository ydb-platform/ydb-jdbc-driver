package tech.ydb.jdbc.settings;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;



public class YdbProperty<T> {
    private interface Parser<T> {
        T parse(String value) throws SQLException;
    }

    private final String name;
    private final String description;
    private final String defaultValue;
    private final Class<T> clazz;
    private final Parser<T> parser;

    private YdbProperty(String name, String description, String defaultValue, Class<T> clazz, Parser<T> parser) {
        this.name = Objects.requireNonNull(name);
        this.description = Objects.requireNonNull(description);
        this.defaultValue = defaultValue;
        this.clazz = clazz;
        this.parser = Objects.requireNonNull(parser);
    }

    public String getName() {
        return name;
    }

    public YdbPropertyValue<T> readValue(Properties props) throws SQLException {
        Object value = props.get(name);
        if (value == null) {
            if (defaultValue == null || defaultValue.isEmpty()) {
                return new YdbPropertyValue<>(false, "", null);
            } else {
                return new YdbPropertyValue<>(false, defaultValue, parser.parse(defaultValue));
            }
        }

        if (value instanceof String) {
            try {
                String stringValue = (String) value;
                return new YdbPropertyValue<>(true, stringValue, parser.parse(stringValue));
            } catch (RuntimeException e) {
                throw new SQLException("Unable to convert property " + name + ": " + e.getMessage(), e);
            }
        } else {
            if (clazz.isAssignableFrom(value.getClass())) {
                T typed = clazz.cast(value);
                return new YdbPropertyValue<>(true, typed.toString(), typed);
            } else {
                throw new SQLException("Invalid object property " + name +", must be " + clazz +
                        ", got " + value.getClass());
            }
        }
    }

    public static YdbProperty<String> string(String name, String description) {
        return string(name, description, null);
    }

    public static YdbProperty<String> string(String name, String description, String defaultValue) {
        return new YdbProperty<>(name, description, defaultValue, String.class, v -> v);
    }

    public static YdbProperty<Boolean> bool(String name, String description, boolean defaultValue) {
        return bool(name, description, String.valueOf(defaultValue));
    }

    public static YdbProperty<Boolean> bool(String name, String description) {
        return bool(name, description, null);
    }

    private static YdbProperty<Boolean> bool(String name, String description, String defaultValue) {
        return new YdbProperty<>(name, description, defaultValue, Boolean.class, Boolean::valueOf);
    }

    public static YdbProperty<Integer> integer(String name, String description) {
        return integer(name, description, null);
    }

    public static YdbProperty<Integer> integer(String name, String description, int defaultValue) {
        return integer(name, description, String.valueOf(defaultValue));
    }

    private static YdbProperty<Integer> integer(String name, String description, String defaultValue) {
        return new YdbProperty<>(name, description, defaultValue, Integer.class, value -> {
            try {
                return Integer.valueOf(value);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Unable to parse value [" + value + "] as Integer: " +
                        e.getMessage(), e);
            }
        });
    }

    public static <E extends Enum<E>> YdbProperty<E> enums(String name, Class<E> clazz, String description) {
        return enums(name, description, clazz, null);
    }

    public static <E extends Enum<E>> YdbProperty<E> enums(String name, Class<E> clazz, String description, E def) {
        return enums(name, description, clazz, def.toString());
    }

    private static <E extends Enum<E>> YdbProperty<E> enums(String name, String desc, Class<E> clazz, String def) {
        return new YdbProperty<>(name, desc, def, clazz, value -> {
            for (E v: clazz.getEnumConstants()) {
                if (v.name().equalsIgnoreCase(value)) {
                    return v;
                }
            }
            return null;
        });
    }

    public static YdbProperty<Duration> duration(String name, String description) {
        return duration(name, description, null);
    }

    public static YdbProperty<Duration> duration(String name, String description, String defaultValue) {
        return new YdbProperty<>(name, description, defaultValue, Duration.class, value -> {
            String targetValue = "PT" + value.replace(" ", "").toUpperCase(Locale.ROOT);
            try {
                return Duration.parse(targetValue);
            } catch (DateTimeParseException e) {
                throw new RuntimeException("Unable to parse value [" + value + "] -> [" +
                        targetValue + "] as Duration: " + e.getMessage(), e);
            }
        });
    }

    public static YdbProperty<byte[]> bytes(String name, String description) {
        return new YdbProperty<>(name, description, null, byte[].class, YdbLookup::byteFileReference);
    }

    public static YdbProperty<String> content(String name, String description) {
        return new YdbProperty<>(name, description, null, String.class, YdbLookup::stringFileReference);
    }

    public static DriverPropertyInfo[] getPropertyInfo(YdbConfig config) throws SQLException {
        Properties values = config.getProperties();
        return new DriverPropertyInfo[] {
            info(values, YdbConfig.CACHE_CONNECTIONS_IN_DRIVER),
            info(values, YdbConfig.PREPARED_STATEMENT_CACHE_SIZE),

            info(values, YdbConnectionProperties.LOCAL_DATACENTER),
            info(values, YdbConnectionProperties.USE_SECURE_CONNECTION),
            info(values, YdbConnectionProperties.SECURE_CONNECTION_CERTIFICATE),
            info(values, YdbConnectionProperties.TOKEN),
            info(values, YdbConnectionProperties.SERVICE_ACCOUNT_FILE),
            info(values, YdbConnectionProperties.USE_METADATA),

            info(values, YdbClientProperties.KEEP_QUERY_TEXT),
            info(values, YdbClientProperties.SESSION_KEEP_ALIVE_TIME),
            info(values, YdbClientProperties.SESSION_MAX_IDLE_TIME),
            info(values, YdbClientProperties.SESSION_POOL_SIZE_MIN),
            info(values, YdbClientProperties.SESSION_POOL_SIZE_MAX),

            info(values, YdbOperationProperties.JOIN_DURATION),
            info(values, YdbOperationProperties.QUERY_TIMEOUT),
            info(values, YdbOperationProperties.SCAN_QUERY_TIMEOUT),
            info(values, YdbOperationProperties.FAIL_ON_TRUNCATED_RESULT),
            info(values, YdbOperationProperties.SESSION_TIMEOUT),
            info(values, YdbOperationProperties.DEADLINE_TIMEOUT),
            info(values, YdbOperationProperties.AUTOCOMMIT),
            info(values, YdbOperationProperties.TRANSACTION_LEVEL),
            info(values, YdbOperationProperties.SCHEME_QUERY_TX_MODE),
            info(values, YdbOperationProperties.SCAN_QUERY_TX_MODE),

            info(values, YdbQueryProperties.DISABLE_PREPARE_DATAQUERY),
            info(values, YdbQueryProperties.DISABLE_AUTO_PREPARED_BATCHES),
            info(values, YdbQueryProperties.DISABLE_DETECT_SQL_OPERATIONS),
            info(values, YdbQueryProperties.DISABLE_JDBC_PARAMETERS),
            info(values, YdbQueryProperties.DISABLE_JDBC_PARAMETERS_DECLARE),
            info(values, YdbQueryProperties.FORCE_QUERY_MODE),
        };
    }

    private static DriverPropertyInfo info(Properties values, YdbProperty<?> property) throws SQLException {
        YdbPropertyValue<?> value = property.readValue(values);
        DriverPropertyInfo info = new DriverPropertyInfo(property.name, value.rawValue());
        info.description = property.description;
        info.required = false;
        return info;
    }
}
