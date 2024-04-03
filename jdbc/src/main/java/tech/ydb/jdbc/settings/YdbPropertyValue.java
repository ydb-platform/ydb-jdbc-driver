package tech.ydb.jdbc.settings;

import java.util.Objects;

public class YdbPropertyValue<T> {
    private final boolean isPresent;
    private final String rawValue;
    private final T value;

    YdbPropertyValue(boolean isPresent, String rawValue, T value) {
        this.isPresent = isPresent;
        this.rawValue = Objects.requireNonNull(rawValue);
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    public boolean hasValue() {
        return isPresent;
    }

    String rawValue() {
        return rawValue;
    }
}
