package tech.ydb.jdbc.settings;

import java.util.function.Supplier;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class BadTokenProvider implements Supplier<String> {
    private final String token;

    public BadTokenProvider(String token) {
        this.token = token;
    }

    @Override
    public String get() {
        return token;
    }
}
