package tech.ydb.jdbc.spi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.query.result.QueryStats;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.settings.QueryStatsMode;
import tech.ydb.table.query.stats.QueryStatsCollectionMode;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbDriverQuerySpiTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb)
            .withArg("cacheConnectionsInDriver", "false");

    @BeforeEach
    public void clean() {
        EmptiSpi.COUNT.set(0);
        EmptiSpi.TX.set(0);
        FullStatsSpi.QUEUE.clear();
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    public void defaultUsageTest(String useQS) throws SQLException {
        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(new QuerySpiTestLoader(prev, EmptiSpi.class));

        try (Connection conn = DriverManager.getConnection(jdbcURL.withArg("useQueryService", useQS).build())) {
            Assertions.assertEquals(0, EmptiSpi.COUNT.get());
            Assertions.assertEquals(0, EmptiSpi.TX.get());

            try (ResultSet rs = conn.createStatement().executeQuery("SELECT 1 + 2")) {
                Assertions.assertTrue(rs.next());
                Assertions.assertFalse(rs.next());
            }

            Assertions.assertEquals(1, EmptiSpi.COUNT.get());
            Assertions.assertEquals(1, EmptiSpi.TX.get());

            try (PreparedStatement ps = conn.prepareStatement("SELECT ? + ?")) {
                ps.setInt(1, 1);
                ps.setInt(2, 2);
                Assertions.assertTrue(ps.execute());

                Assertions.assertEquals(2, EmptiSpi.COUNT.get());

                ps.setInt(1, 2);
                ps.setInt(2, 3);
                ps.addBatch();
                ps.setLong(1, 2);
                ps.setLong(2, 3);
                ps.addBatch();

                Assertions.assertEquals(2, ps.executeBatch().length);
                Assertions.assertEquals(4, EmptiSpi.COUNT.get());
                Assertions.assertEquals(3, EmptiSpi.TX.get()); // batched queries are always in one tx
            }

            try (Statement st = conn.createStatement()) {
                ExceptionAssert.ydbException("code = GENERIC_ERROR", () -> st.executeQuery("SELECT 1 + 'test'u"));
                Assertions.assertEquals(5, EmptiSpi.COUNT.get());
                Assertions.assertEquals(4, EmptiSpi.TX.get());
            }
        } finally {
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    public void enableStatsModeTest(String useQS) throws SQLException {
        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(new QuerySpiTestLoader(prev, FullStatsSpi.class));

        try (Connection conn = DriverManager.getConnection(jdbcURL.withArg("useQueryService", useQS).build())) {
            Assertions.assertTrue(FullStatsSpi.QUEUE.isEmpty());

            try (ResultSet rs = conn.createStatement().executeQuery("SELECT 1 + 2")) {
                Assertions.assertTrue(rs.next());
                Assertions.assertFalse(rs.next());
            }

            Assertions.assertFalse(FullStatsSpi.QUEUE.isEmpty());
            Assertions.assertNotNull(FullStatsSpi.QUEUE.poll().stats);
            Assertions.assertTrue(FullStatsSpi.QUEUE.isEmpty());

            try (PreparedStatement ps = conn.prepareStatement("SELECT ? + ?")) {
                ps.setInt(1, 1);
                ps.setInt(2, 2);
                Assertions.assertTrue(ps.execute());

                Assertions.assertEquals(1, FullStatsSpi.QUEUE.size());
                FullStatsSpi.Record record = FullStatsSpi.QUEUE.poll();
                Assertions.assertNotNull(record.stats);
                Assertions.assertEquals(Status.SUCCESS, record.status);
                Assertions.assertNull(record.th);

                ps.setInt(1, 2);
                ps.setInt(2, 3);
                ps.addBatch();
                ps.setLong(1, 2);
                ps.setLong(2, 3);
                ps.addBatch();

                Assertions.assertEquals(2, ps.executeBatch().length);

                Assertions.assertEquals(2, FullStatsSpi.QUEUE.size());

                record = FullStatsSpi.QUEUE.poll();
                Assertions.assertNotNull(record.stats);
                Assertions.assertEquals(Status.SUCCESS, record.status);
                Assertions.assertNull(record.th);

                record = FullStatsSpi.QUEUE.poll();
                Assertions.assertNotNull(record.stats);
                Assertions.assertEquals(Status.SUCCESS, record.status);
                Assertions.assertNull(record.th);
            }

            try (Statement st = conn.createStatement()) {
                ExceptionAssert.ydbException("code = GENERIC_ERROR", () -> st.executeQuery("SELECT 1 + 'test'u"));

                Assertions.assertEquals(1, FullStatsSpi.QUEUE.size());
                FullStatsSpi.Record record = FullStatsSpi.QUEUE.poll();
                Assertions.assertNull(record.stats);
                Assertions.assertEquals(StatusCode.GENERIC_ERROR, record.status.getCode());
                Assertions.assertNull(record.th);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    public void validateQueryTest(String useQS) throws SQLException {
        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(new QuerySpiTestLoader(prev, FullStatsSpi.class, ValidateSpi.class,
                EmptiSpi.class));

        try (Connection conn = DriverManager.getConnection(jdbcURL.withArg("useQueryService", useQS).build())) {
            Assertions.assertTrue(FullStatsSpi.QUEUE.isEmpty());
            Assertions.assertEquals(0, EmptiSpi.COUNT.get());

            try (ResultSet rs = conn.createStatement().executeQuery("SELECT 1 + 2")) {
                Assertions.assertTrue(rs.next());
                Assertions.assertFalse(rs.next());
            }

            Assertions.assertEquals(1, FullStatsSpi.QUEUE.size());
            FullStatsSpi.Record record = FullStatsSpi.QUEUE.poll();
            Assertions.assertNotNull(record.stats);
            Assertions.assertEquals(Status.SUCCESS, record.status);
            Assertions.assertNull(record.th);
            Assertions.assertEquals(1, EmptiSpi.COUNT.get());

            try (Statement st = conn.createStatement()) {
                ExceptionAssert.sqlException("INVALID QUERY", () -> st.executeQuery("SELECT 2 + 3"));
            }

            Assertions.assertEquals(1, FullStatsSpi.QUEUE.size());
            record = FullStatsSpi.QUEUE.poll();
            Assertions.assertNull(record.stats);
            Assertions.assertNull(record.status);
            Assertions.assertNotNull(record.th);
            Assertions.assertEquals("INVALID QUERY", record.th.getMessage());

            Assertions.assertEquals(1, EmptiSpi.COUNT.get());
        } finally {
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    public static class EmptiSpi implements YdbQueryExtentionService {
        private static final AtomicLong TX = new AtomicLong(0);
        private static final AtomicLong COUNT = new AtomicLong(0);

        @Override
        public void onNewTransaction() {
            TX.incrementAndGet();
        }

        @Override
        public QueryCall newDataQuery(YdbStatement statement, YdbQuery query, String yql) {
            COUNT.incrementAndGet();
            return new YdbQueryExtentionService.QueryCall() {
            };
        }
    }

    public static class ValidateSpi implements YdbQueryExtentionService {

        @Override
        public QueryCall newDataQuery(YdbStatement statement, YdbQuery query, String yql) throws SQLException {
            if ("SELECT 2 + 3".equals(query.getOriginQuery())) {
                throw new SQLException("INVALID QUERY");
            }

            return new YdbQueryExtentionService.QueryCall() {
            };
        }
    }

    public static class FullStatsSpi implements YdbQueryExtentionService {
        private static final ConcurrentLinkedQueue<Record> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public QueryCall newDataQuery(YdbStatement statement, YdbQuery query, String yql) {
            Record r = new Record();
            QUEUE.add(r);
            return r;
        }

        static class Record implements YdbQueryExtentionService.QueryCall {

            private QueryStats stats = null;
            private Status status = null;
            private Throwable th = null;

            @Override
            public ExecuteQuerySettings.Builder prepareQuerySettings(ExecuteQuerySettings.Builder builder) {
                return builder.withStatsMode(QueryStatsMode.FULL);
            }

            @Override
            public ExecuteDataQuerySettings prepareDataQuerySettings(ExecuteDataQuerySettings settings) {
                return settings.setCollectStats(QueryStatsCollectionMode.FULL);
            }

            @Override
            public void onQueryStats(QueryStats stats) {
                this.stats = stats;
            }

            @Override
            public void onQueryResult(Status status, Throwable th) {
                this.status = status;
                this.th = th;
            }
        }
    }
}
