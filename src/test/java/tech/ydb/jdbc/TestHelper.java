package tech.ydb.jdbc;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import tech.ydb.jdbc.exception.YdbConfigurationException;
import tech.ydb.jdbc.settings.YdbProperties;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    // Significantly reduce total time spending in tests
    private static final boolean IGNORE_EXISTING_TABLE =
            Boolean.parseBoolean(System.getProperty("IGNORE_EXISTING_TABLE", "false"));

    private static final Set<Class<?>> CONFIGURED = new HashSet<>();

    private TestHelper() {
        //
    }

    public static void assertThrowsMsg(Class<? extends Throwable> type, Executable exec, String expectMessage) {
        assertThrowsMsg(type, exec, e -> assertEquals(expectMessage, e.getMessage(), "Error message"), null);
    }

    public static void assertThrowsMsgLike(Class<? extends Throwable> type, Executable exec, String expectLike) {
        assertThrowsMsgLike(type, exec, expectLike, null);
    }

    public static void assertThrowsMsgLike(Class<? extends Throwable> type, Executable exec, String expectLike,
                                           String description) {
        assertThrowsMsg(type, exec, e -> {
            String errorMessage = e.getMessage();
            assertTrue(errorMessage.contains(expectLike),
                    String.format("Error message [%s] must contains [%s]", errorMessage, expectLike));
        }, description);
    }


    public static <T extends Throwable> void assertThrowsMsg(Class<T> type,
                                                             Executable exec,
                                                             Consumer<T> check,
                                                             @Nullable String description) {
        Throwable throwable = assertThrows(Throwable.class, exec, description);
        assertTrue(type.isAssignableFrom(throwable.getClass()),
                () -> "Unexpected exception type thrown, expected " + type + ", got " + throwable.getClass());
        LOGGER.trace("Catch exception", throwable);
        check.accept(type.cast(throwable));
    }

    //

    public static String stringFileReference(String reference) {
        try {
            return YdbProperties.stringFileReference(reference);
        } catch (YdbConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    public static void initTable(YdbConnection connection, String tableName, String expression) throws SQLException {
        try (YdbStatement statement = connection.createStatement()) {
            if (IGNORE_EXISTING_TABLE) {
                try {
                    statement.execute(String.format("select * from %s limit 1", tableName));
                } catch (Exception e) {
                    statement.executeSchemeQuery(withTableName(tableName, expression));
                }
            } else {
                try {
                    statement.executeSchemeQuery(String.format("drop table %s", tableName));
                } catch (Exception e) {
                    // do nothing
                }
                statement.executeSchemeQuery(withTableName(tableName, expression));
            }
        }
    }

    public static String withTableName(String tableName, String sql) {
        return sql.replace("${tableName}", tableName);
    }

    public static void configureOnce(Class<?> type, SQLSimpleRun run) throws SQLException {
        if (CONFIGURED.contains(type)) {
            return; // --- already configured
        }
        run.run();
        CONFIGURED.add(type);
    }

    public interface SQLRun {
        void run(YdbConnection connection) throws SQLException;
    }

    public interface SQLSimpleRun {
        void run() throws SQLException;
    }
}
