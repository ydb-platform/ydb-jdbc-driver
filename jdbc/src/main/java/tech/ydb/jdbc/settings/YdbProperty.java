package tech.ydb.jdbc.settings;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;



class YdbProperty<T> {
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

    public YdbValue<T> readValue(Properties props) throws SQLException {
        Object value = props.get(name);
        if (value == null) {
            if (defaultValue == null || defaultValue.isEmpty()) {
                return new YdbValue<>(false, "", null);
            } else {
                return new YdbValue<>(false, defaultValue, parser.parse(defaultValue));
            }
        }

        if (value instanceof String) {
            try {
                String stringValue = (String) value;
                return new YdbValue<>(true, stringValue, parser.parse(stringValue));
            } catch (RuntimeException e) {
                throw new SQLException("Unable to convert property " + name + ": " + e.getMessage(), e);
            }
        } else {
            if (clazz.isAssignableFrom(value.getClass())) {
                T typed = clazz.cast(value);
                return new YdbValue<>(true, typed.toString(), typed);
            } else {
                throw new SQLException("Invalid object property " + name +", must be " + clazz +
                        ", got " + value.getClass());
            }
        }
    }

    DriverPropertyInfo toInfo(Properties values) throws SQLException {
        YdbValue<?> value = readValue(values);
        DriverPropertyInfo info = new DriverPropertyInfo(name, value.rawValue());
        info.description = description;
        info.required = false;
        return info;
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
}
