package tech.ydb.jdbc.settings;

import java.util.function.Supplier;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class StaticTokenProvider implements Supplier<String>{
    @Override
    public String get() {
        return "STATIC";
    }
}
