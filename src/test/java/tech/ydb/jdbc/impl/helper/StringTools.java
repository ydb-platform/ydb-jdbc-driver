package tech.ydb.jdbc.impl.helper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;

import javax.annotation.Nullable;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class StringTools {
    private StringTools() { }

    @Nullable
    public static InputStream stream(@Nullable String value) {
        return value == null ? null : new ByteArrayInputStream(value.getBytes()) {
            @Override
            public void close() {
                this.reset();
            }
        };
    }

    @Nullable
    public static Reader reader(@Nullable String value) {
        return value == null ? null : new StringReader(value) {
            @Override
            public void close() {
                try {
                    this.reset();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
