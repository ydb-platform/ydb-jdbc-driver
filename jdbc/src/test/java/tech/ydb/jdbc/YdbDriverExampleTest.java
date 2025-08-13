package tech.ydb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbDriverExampleTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static String jdbcURL() {
        StringBuilder jdbc = new StringBuilder("jdbc:ydb:")
                .append(ydb.useTls() ? "grpcs://" : "grpc://")
                .append(ydb.endpoint())
                .append("/?database=")
                .append(ydb.database());

        if (ydb.authToken() != null) {
            jdbc.append("&").append("token=").append(ydb.authToken());
        }

        return jdbc.toString();
    }

    @Test
    public void testYdb() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL())) {
            try {
                connection.createStatement()
                        .execute("drop table table_sample");
            } catch (SQLException e) {
                //
            }
            connection.createStatement()
                    .execute("create table table_sample(id Int32, value Text, primary key (id))");

            try (PreparedStatement ps = connection.prepareStatement("" +
                            "upsert into table_sample (id, value) values (?, ?)")) {

                ps.setInt(1, 1);
                ps.setString(2, "value-1");
                ps.executeUpdate();

                ps.setInt(1, 2);
                ps.setString(2, "value-2");
                ps.executeUpdate();

                ps.setInt(1, 3);
                ps.setNull(2, Types.VARCHAR);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = connection.prepareStatement("" +
                            "declare $p1 as Int32;\n" +
                            "declare $p2 as Optional<Text>;\n" +
                            "upsert into table_sample (id, value) values ($p1, $p2)")) {

                ps.setInt(1, 4);
                ps.setString(2, "value-3");
                ps.executeUpdate();

                ps.setInt(1, 5);
                ps.setString(2, "value-4");
                ps.executeUpdate();

                ps.setInt(1, 6);
                ps.setNull(2, Types.VARCHAR);
                ps.executeUpdate();
            }

            try (PreparedStatement select = connection
                    .prepareStatement("select count(1) as cnt from table_sample")) {
                ResultSet rs = select.executeQuery();
                rs.next();
                Assertions.assertEquals(6, rs.getLong("cnt"));
            }

            // Named variables can be accessed by alphabetic order
            try (PreparedStatement psBatch = connection
                    .prepareStatement("" +
                            "declare $values as List<Struct<id:Int32, value:Optional<Text>>>;\n" +
                            "upsert into table_sample select * from as_table($values)")) {

                psBatch.setInt(1, 7); // id
                psBatch.setString(2, "value-7"); // value
                psBatch.addBatch();

                psBatch.setInt(1, 8); // id
                psBatch.setString(2, "value-8"); // value
                psBatch.addBatch();

                psBatch.setInt(1, 9);
                psBatch.setNull(2, 0);
                psBatch.addBatch();

                psBatch.executeBatch();
            }

            try (PreparedStatement psBatch = connection
                    .prepareStatement("" +
                            "declare $values as List<Struct<value:Text, id:Int32>>;\n" +
                            "upsert into table_sample select * from as_table($values)")) {

                psBatch.setInt(1, 10);
                psBatch.setString(2, "value-10");
                psBatch.addBatch();

                psBatch.setInt(1, 11);
                psBatch.setString(2, "value-11");
                psBatch.addBatch();

                psBatch.executeBatch();
            }

            try (PreparedStatement bulkPs = connection.prepareStatement("" +
                            "BULK UPSERT INTO table_sample (id, value) VALUES (?, ?)")) {

                bulkPs.setInt(1, 12);
                bulkPs.setString(2, "value-12");
                bulkPs.addBatch();

                bulkPs.setInt(1, 13);
                bulkPs.setString(2, "value-13");
                bulkPs.addBatch();

                bulkPs.executeBatch();
            }

            try (PreparedStatement select = connection
                    .prepareStatement("select count(1) as cnt from table_sample")) {
                ResultSet rs = select.executeQuery();
                rs.next();
                Assertions.assertEquals(13, rs.getLong("cnt"));
            }
        }
    }

    @Test
    public void testYdbNotNull() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL())) {
            try {
                connection.createStatement().execute("drop table table_sample");
            } catch (SQLException e) {
                //
            }
            connection.createStatement()
                    .execute("create table table_sample(id Int32 not null, value Text, primary key (id))");

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

            try (PreparedStatement psBatch = connection
                    .prepareStatement("" +
                            "declare $values as List<Struct<p1:Int32,p2:Text>>;\n" +
                            "$mapper = ($row) -> (AsStruct( $row.p1 as id, $row.p2 as value));\n" +
                            "upsert into table_sample select * from as_table(ListMap($values, $mapper));")
                    ) {

                psBatch.setInt(1, 7);
                psBatch.setString(2, "value-7");
                psBatch.addBatch();

                psBatch.setInt(1, 8);
                psBatch.setString(2, "value-8");
                psBatch.addBatch();

                psBatch.executeBatch();
            }

            try (PreparedStatement bulkPs = connection.prepareStatement("" +
                            "BULK UPSERT INTO table_sample (id, value) VALUES (?, ?)")) {

                bulkPs.setInt(1, 9);
                bulkPs.setString(2, "value-9");
                bulkPs.addBatch();

                bulkPs.setInt(1, 10);
                bulkPs.setString(2, "value-10");
                bulkPs.addBatch();

                bulkPs.executeBatch();
            }

            try (PreparedStatement select = connection
                    .prepareStatement("select count(1) as cnt from table_sample")) {
                ResultSet rs = select.executeQuery();
                rs.next();
                Assertions.assertEquals(10, rs.getLong("cnt"));
            }
        }
    }
}
