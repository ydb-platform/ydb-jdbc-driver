package tech.ydb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.test.junit5.YdbHelperExtention;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbDriverExampleTest {
    @RegisterExtension
    private static final YdbHelperExtention ydb = new YdbHelperExtention();

    private static String jdbcURL() {
        StringBuilder jdbc = new StringBuilder("jdbc:ydb:")
                .append(ydb.useTls() ? "grpcs://" : "grpc://")
                .append(ydb.endpoint())
                .append(ydb.database())
                .append("?");

        if (ydb.authToken() != null) {
            jdbc.append("token=").append(ydb.authToken()).append("&");
        }

        return jdbc.toString();
    }

    @Test
    public void testYdb() throws SQLException {
        String url = jdbcURL(); // "jdbc:ydb:localhost:2135/local"
        try (Connection connection = DriverManager.getConnection(url)) {
            try {
                connection.createStatement()
                        .execute("--jdbc:SCHEME\n" +
                                "drop table table_sample");
            } catch (SQLException e) {
                //
            }
            connection.createStatement()
                    .execute("--jdbc:SCHEME\n" +
                            "create table table_sample(id Int32, value Text, primary key (id))");

            try (PreparedStatement ps = connection.prepareStatement("" +
                            "upsert into table_sample (id, value) values (?, ?)")) {

                ps.setInt(1, 1);
                ps.setString(2, "value-1");
                ps.executeUpdate();

                ps.setInt(1, 2);
                ps.setString(2, "value-2");
                ps.executeUpdate();
            }

            try (PreparedStatement ps = connection.prepareStatement("" +
                            "declare $p1 as Int32;\n" +
                            "declare $p2 as Text;\n" +
                            "upsert into table_sample (id, value) values ($p1, $p2)")) {

                ps.setInt(1, 3);
                ps.setString(2, "value-3");
                ps.executeUpdate();

                ps.setInt(1, 4);
                ps.setString(2, "value-4");
                ps.executeUpdate();
            }

            try (PreparedStatement select = connection
                    .prepareStatement("select count(1) as cnt from table_sample")) {
                ResultSet rs = select.executeQuery();
                rs.next();
                Assertions.assertEquals(4, rs.getLong("cnt"));
            }

            try (YdbPreparedStatement psBatch = connection
                    .prepareStatement("" +
                            "declare $values as List<Struct<id:Int32,value:Text>>;\n" +
                            "upsert into table_sample select * from as_table($values)")
                    .unwrap(YdbPreparedStatement.class)) {

                psBatch.setInt("id", 5);
                psBatch.setString("value", "value-5");
                psBatch.addBatch();

                psBatch.setInt("id", 6);
                psBatch.setString("value", "value-6");
                psBatch.addBatch();

                psBatch.executeBatch();
            }

            try (PreparedStatement select = connection
                    .prepareStatement("select count(1) as cnt from table_sample")) {
                ResultSet rs = select.executeQuery();
                rs.next();
                Assertions.assertEquals(6, rs.getLong("cnt"));
            }
        }
    }
}
