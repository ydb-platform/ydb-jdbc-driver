package tech.ydb.jdbc.settings;

import java.util.Objects;

class YdbValue<T> {
    private final boolean isPresent;
    private final String rawValue;
    private final T value;

    YdbValue(boolean isPresent, String rawValue, T value) {
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
