package tech.ydb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.connection.YdbConnectionImpl;
import tech.ydb.jdbc.connection.YdbContext;
import tech.ydb.test.junit5.YdbHelperExtention;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbDriverIntegrationTest {
    @RegisterExtension
    private static final YdbHelperExtention ydb = new YdbHelperExtention();

    private static String jdbcURL() {
        return String.format("jdbc:ydb:%s%s", ydb.endpoint(), ydb.database());
    }

    @AfterAll
    public void afterAll() throws SQLException {
        if (YdbDriver.isRegistered()) {
            YdbDriver.deregister();
        }
    }

    @Test
    public void connect() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL())) {
            Assertions.assertTrue(connection instanceof YdbConnectionImpl);

            YdbConnection unwrapped = connection.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());
            Assertions.assertNotNull(unwrapped.getCtx().getSchemeClient());
            Assertions.assertNotNull(unwrapped.getCtx().getTableClient());
        }
    }

    @Test
    public void testContextCache() throws SQLException {
        YdbContext ctx;
        try (Connection conn1 = DriverManager.getConnection(jdbcURL())) {
            Assertions.assertTrue(conn1.isValid(5000));

            YdbConnection unwrapped = conn1.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());

            ctx = unwrapped.getCtx();
        }

        try (Connection conn2 = DriverManager.getConnection(jdbcURL())) {
            Assertions.assertTrue(conn2.isValid(5000));

            YdbConnection unwrapped = conn2.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());
            Assertions.assertSame(ctx, unwrapped.getCtx());
        }

        try (Connection conn3 = DriverManager.getConnection(jdbcURL(), new Properties())) {
            Assertions.assertTrue(conn3.isValid(5000));

            YdbConnection unwrapped = conn3.unwrap(YdbConnection.class);
            Assertions.assertNotNull(unwrapped.getCtx());
            Assertions.assertSame(ctx, unwrapped.getCtx());
        }
    }

//    @Test
//    public void connectMultipleTimes() throws SQLException {
//        YdbDriver.getConnectionsCache().close();
//        String url = jdbcURl();
//        try ( YdbConnection connection1 = driver.connect(url, new Properties())) {
//            logger.info("Session 1 opened: {}", connection1.getYdbSession());
//            try ( YdbConnection connection2 = driver.connect(url, new Properties())) {
//                logger.info("Session 2 opened: {}", connection2.getYdbSession());
//
//                // Expect only single connection
//                Assertions.assertEquals(1, YdbDriver.getConnectionsCache().getConnectionCount());
//            }
//        }
//    }

//    @Test
//    public void testYdb() throws SQLException {
//        try (Connection connection = DriverManager.getConnection(jdbcURL())) {
//            try {
//                connection.createStatement().execute(
//                        "--jdbc:SCHEME\n"
//                        + "drop table jdbc_table_sample");
//            } catch (SQLException e) {
//                //
//            }
//            connection.createStatement()
//                    .execute("--jdbc:SCHEME\n"
//                            + "create table table_sample(id Int32, value Text, primary key (id))");
//
//            PreparedStatement ps = connection
//                    .prepareStatement("" +
//                            "declare $p1 as Int32;\n" +
//                            "declare $p2 as Text;\n" +
//                            "upsert into table_sample (id, value) values ($p1, $p2)");
//            ps.setInt(1, 1);
//            ps.setString(2, "value-1");
//            ps.executeUpdate();
//
//            ps.setInt("p1", 2);
//            ps.setString("p2", "value-2");
//            ps.executeUpdate();
//
//            connection.commit();
//
//
//            PreparedStatement select = connection
//                    .prepareStatement("select count(1) as cnt from table_sample");
//            ResultSet rs = select.executeQuery();
//            rs.next();
//            Assertions.assertEquals(2, rs.getLong("cnt"));
//
//            PreparedStatement psBatch = connection
//                    .prepareStatement("" +
//                            "declare $values as List<Struct<id:Int32,value:Text>>;\n" +
//                            "upsert into table_sample select * from as_table($values)");
//            psBatch.setInt("id", 3);
//            psBatch.setString("value", "value-3");
//            psBatch.addBatch();
//
//            psBatch.setInt("id", 4);
//            psBatch.setString("value", "value-4");
//            psBatch.addBatch();
//
//            psBatch.executeBatch();
//
//            connection.commit();
//
//            rs = select.executeQuery();
//            rs.next();
//            Assertions.assertEquals(4, rs.getLong("cnt"));
//        }
//    }
}
