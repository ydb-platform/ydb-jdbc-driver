package tech.ydb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.jdbc.impl.helper.TableAssert;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbDriverQueryRewriteTest {
    private static final String READ_REWRITES = "SELECT * FROM query_rewrite ORDER BY hash";
    private static final String UPDATE_REWRITES = "UPDATE query_rewrite SET rewritten = ? WHERE query = ?";

    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private static final JdbcConnectionExtention jdbc = new JdbcConnectionExtention(ydb)
            .withArg("usePrefixPath", "query_rewrite");

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb)
            .withArg("enableTxTracer", "true")
            .withArg("usePrefixPath", "query_rewrite");

    @Test
    public void basicUsageTest() throws SQLException {
        String url = jdbcURL.withArg("withQueryRewriteTable", "query_rewrite").build();
        QueryRewrite checker = new QueryRewrite();

        try (Connection conn = DriverManager.getConnection(url)) {
            // table was created automatically
            try (ResultSet rs = jdbc.connection().createStatement().executeQuery(READ_REWRITES)) {
                checker.check(rs).assertNoRows();
            }

            // only prepared statements can be overridden
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE test (id Int32, value Text, PRIMARY KEY (id))");
            }

            try (ResultSet rs = jdbc.connection().createStatement().executeQuery(READ_REWRITES)) {
                checker.check(rs).assertNoRows();
            }

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO test (id, value) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "v1");
                ps.execute();

                ps.setInt(1, 2);
                ps.setString(2, "v2");
                ps.addBatch();

                ps.setInt(1, 3);
                ps.setString(2, "v3");
                ps.addBatch();

                ps.executeBatch();
            }

            try (ResultSet rs = jdbc.connection().createStatement().executeQuery(READ_REWRITES)) {
                checker.check(rs).nextRow(
                        checker.hash("97d172fccb92b8a1938746597c6c5dbd7f817950af51600f51b4560b7f61e32b"),
                        checker.query("INSERT INTO test (id, value) VALUES (?, ?)"),
                        checker.hasNotRewritten(),
                        checker.hasUsedAt()
                ).assertAll().assertNoRows();
            }

            try (PreparedStatement update = jdbc.connection().prepareStatement(UPDATE_REWRITES)) {
                update.setString(1, "UPSERT INTO test (id, value) VALUES (?, ?)");
                update.setString(2, "INSERT INTO test (id, value) VALUES (?, ?)");

                update.execute();
            }

            try (ResultSet rs = jdbc.connection().createStatement().executeQuery(READ_REWRITES)) {
                checker.check(rs).nextRow(
                        checker.hash("97d172fccb92b8a1938746597c6c5dbd7f817950af51600f51b4560b7f61e32b"),
                        checker.query("INSERT INTO test (id, value) VALUES (?, ?)"),
                        checker.rewritten("UPSERT INTO test (id, value) VALUES (?, ?)"),
                        checker.hasUsedAt()
                ).assertAll().assertNoRows();
            }

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO test (id, value) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "v11");
                ExceptionAssert.ydbException("PRECONDITION_FAILED", ps::execute);
            }
        }

        try (Connection conn = DriverManager.getConnection(url)) {
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO test (id, value) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "v11");
                ps.execute();
            }
        }

        try (Connection conn = DriverManager.getConnection(url)) {
            conn.createStatement().execute("DROP TABLE test");
            // reuse current table
            try (ResultSet rs = jdbc.connection().createStatement().executeQuery(READ_REWRITES)) {
                checker.check(rs)
                        .nextRow(checker.hash("97d172fccb92b8a1938746597c6c5dbd7f817950af51600f51b4560b7f61e32b"))
                        .assertValues()
                        .assertNoRows();
            }
        } finally {
            jdbc.connection().createStatement().execute("DROP TABLE query_rewrite");
        }
    }

    @Test
    public void testContextCacheConncurrent() throws SQLException {
        String url = jdbcURL.withArg("withQueryRewriteTable", "query_rewrite2").build();
        List<CompletableFuture<YdbConnection>> list = new ArrayList<>();

        for (int idx = 0; idx < 20; idx++) {
            list.add(CompletableFuture.supplyAsync(() -> {
                try {
                    Connection connection = DriverManager.getConnection(url);
                    return connection.unwrap(YdbConnection.class);
                } catch (SQLException ex) {
                    throw new RuntimeException("Cannot connect", ex);
                }
            }));
        }

        YdbContext first = list.get(0).join().getCtx();

        for (CompletableFuture<YdbConnection> future: list) {
            Assertions.assertEquals(first, future.join().getCtx());
        }

        for (CompletableFuture<YdbConnection> future: list) {
            future.join().close();
        }
    }

    @Test
    public void invalididQueryRewriteTest() throws SQLException {
        String url = jdbcURL.withArg("withQueryRewriteTable", "query rewrite").build();
        ExceptionAssert.ydbException(
                "Cannot initialize executor with rewrite table " + ydb.database() + "/query_rewrite/query rewrite",
                () -> DriverManager.getConnection(url)
        );
    }

    public final class QueryRewrite extends TableAssert {
        private final TextColumn hash = addTextColumn("hash", "Text");;
        private final TextColumn query = addTextColumn("query", "Text");;
        private final TextColumn rewritten = addTextColumn("rewritten", "Text");
        private final TextColumn usedAt = addTextColumn("used_at", "Timestamp");

        public ValueAssert query(String q) {
            return query.eq(q);
        }

        public ValueAssert hash(String h) {
            return hash.eq(h);
        }

        public ValueAssert rewritten(String q) {
            return rewritten.eq(q);
        }

        public ValueAssert hasNotRewritten() {
            return rewritten.isNull();
        }

        public ValueAssert hasUsedAt() {
            return usedAt.isNotEmpty();
        }
    }
}
