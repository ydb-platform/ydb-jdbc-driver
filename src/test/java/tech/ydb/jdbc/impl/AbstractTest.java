package tech.ydb.jdbc.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbDriver;
import tech.ydb.jdbc.impl.helper.TestHelper;
import tech.ydb.jdbc.settings.YdbLookup;
import tech.ydb.test.junit5.YdbHelperExtention;


public abstract class AbstractTest {
    protected static final String CREATE_TABLE = YdbLookup.stringFileReference("classpath:sql/create_table.sql");
    private static final String SIMPLE_TABLE = "unit_1";
    private static final String PREPARED_TABLE = "unit_2";

    @RegisterExtension
    private static final YdbHelperExtention ydb = new YdbHelperExtention();

    @AfterAll
    public static void cleanUp() {
        YdbDriver.getConnectionsCache().close();
    }

    protected static String jdbcURl() {
        return String.format("jdbc:ydb:%s%s", ydb.endpoint(), ydb.database());
    }

    protected static YdbConnection createTestConnection() throws SQLException {
        return (YdbConnection) DriverManager.getConnection(jdbcURl());
    }

    protected void cleanupSimpleTestTable() throws SQLException {
        cleanupTable(SIMPLE_TABLE);
    }

    protected void cleanupTable(String table) throws SQLException {
        try (YdbConnection connection = createTestConnection()) {
            connection.createStatement().executeUpdate("delete from " + table);
            connection.commit();
        }
    }

    protected static void recreatePreparedTestTable() throws SQLException {
        createTestTable(PREPARED_TABLE, CREATE_TABLE);
    }

    protected static void recreateSimpleTestTable() throws SQLException {
        createTestTable(SIMPLE_TABLE, CREATE_TABLE);
    }

    protected static void createTestTable(String tableName, String expression) throws SQLException {
        try (YdbConnection connection = createTestConnection()) {
            TestHelper.initTable(connection, tableName, expression);
        }
    }

    protected static String subst(String tableName, String sql) {
        return TestHelper.withTableName(tableName, sql);
    }

    protected static void closeIfPossible(Object value) throws IOException {
        if (value instanceof Reader) {
            ((Reader) value).close();
        } else if (value instanceof InputStream) {
            ((InputStream) value).close();
        }
    }

    protected static <T> Object castCompatible(T value) throws SQLException {
        if (value instanceof Date) {
            return ((Date) value).getTime();
        } else if (value instanceof Time) {
            return ((Time) value).getTime();
        } else if (value instanceof byte[]) {
            return new String((byte[]) value);
        } else if (value instanceof Reader) {
            try {
                try (Reader reader = (Reader) value) {
                    return CharStreams.toString(reader);
                }
            } catch (IOException e) {
                throw new SQLException("Unable to read from reader", e);
            }
        } else if (value instanceof InputStream) {
            try {
                try (InputStream stream = (InputStream) value) {
                    return new String(ByteStreams.toByteArray(stream));
                }
            } catch (IOException e) {
                throw new SQLException("Unable to read from inputStream", e);
            }
        } else {
            return value;
        }
    }

    @SafeVarargs
    protected static <T> List<T> merge(List<T>... lists) {
        return Stream.of(lists)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @SafeVarargs
    protected static <T> Set<T> set(T... values) {
        return new HashSet<>(Arrays.asList(values));
    }
}
