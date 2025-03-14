package tech.ydb.jdbc.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbPreparedStatement;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.SqlQueries;
import tech.ydb.jdbc.impl.helper.TextSelectAssert;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.test.junit5.YdbHelperExtension;

public class YdbPreparedStatementWithDataQueryImplTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private static final JdbcConnectionExtention jdbc = new JdbcConnectionExtention(ydb);

    private static final String TEST_TABLE_NAME = "ydb_prepared_statement_with_data_query_test";
    private static final SqlQueries TEST_TABLE = new SqlQueries(TEST_TABLE_NAME);

    private static final String UPSERT_SQL = ""
            + "declare $key as Optional<Int32>;\n"
            + "declare $#column as #type;\n"
            + "upsert into #tableName (key, #column) values ($key, $#column)";

    private static final String SCAN_UPSERT_SQL = ""
            + "declare $key as Optional<Int32>;\n"
            + "declare $#column as #type;\n"
            + "scan upsert into #tableName (key, #column) values ($key, $#column)";

    private static final String SIMPLE_SELECT_SQL = "select key, #column from #tableName";
    private static final String SELECT_BY_KEY_SQL = ""
            + "declare $key as Optional<Int32>;\n"
            + "select key, #column from #tableName where key=$key";

    private static final String SCAN_SELECT_BY_KEY_SQL = ""
            + "declare $key as Optional<Int32>;\n"
            + "scan select key, #column from #tableName where key=$key";
    @BeforeAll
    public static void initTable() throws SQLException {
        try (Statement statement = jdbc.connection().createStatement();) {
            // create test table
            statement.execute(TEST_TABLE.createTableSQL());
        }
        jdbc.connection().commit();
    }

    @AfterAll
    public static void dropTable() throws SQLException {
        try (Statement statement = jdbc.connection().createStatement();) {
            statement.execute(TEST_TABLE.dropTableSQL());
        }
        jdbc.connection().commit();
    }

    @AfterEach
    public void afterEach() throws SQLException {
        if (jdbc.connection().isClosed()) {
            return;
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            statement.execute(TEST_TABLE.deleteAllSQL());
        }

        jdbc.connection().commit(); // MUST be auto rollbacked
        jdbc.connection().close();
    }

    private String upsertSql(String column, String type) {
        return UPSERT_SQL
                .replaceAll("#column", column)
                .replaceAll("#type", type)
                .replaceAll("#tableName", TEST_TABLE_NAME);
    }

    private String scanSelectSql(String column) {
        return SCAN_SELECT_BY_KEY_SQL
                .replaceAll("#column", column)
                .replaceAll("#tableName", TEST_TABLE_NAME);
    }

    private String scanUpsertSql(String column, String type) {
        return SCAN_UPSERT_SQL
                .replaceAll("#column", column)
                .replaceAll("#type", type)
                .replaceAll("#tableName", TEST_TABLE_NAME);
    }

    private PreparedStatement prepareSimpleSelect(String column) throws SQLException {
        String sql = SIMPLE_SELECT_SQL
                .replaceAll("#column", column)
                .replaceAll("#tableName", TEST_TABLE_NAME);
        return jdbc.connection().prepareStatement(sql);
    }

    private YdbPreparedStatement prepareSelectByKey(String column) throws SQLException {
        String sql = SELECT_BY_KEY_SQL
                .replaceAll("#column", column)
                .replaceAll("#tableName", TEST_TABLE_NAME);
        return jdbc.connection().prepareStatement(sql).unwrap(YdbPreparedStatement.class);
    }

    @Test
    public void executeWrongParameters() throws SQLException {
        String sql = upsertSql("c_Text", "Text");  // Must be Text?
        try (YdbPreparedStatement statement = jdbc.connection().unwrap(YdbConnection.class).prepareStatement(sql)) {
            statement.setInt("key", 1);
            ExceptionAssert.sqlDataException("Missing value for parameter: $c_Text", statement::execute);
        }

        try (YdbPreparedStatement statement = jdbc.connection().unwrap(YdbConnection.class).prepareStatement(sql)) {
            statement.setInt("key", 1);
            ExceptionAssert.sqlException("Missing required value for parameter: $c_Text", () -> {
                statement.setObject("c_Text", PrimitiveType.Text.makeOptional().emptyValue());
            });
        }
    }

    @Test
    public void executeDataQuery() throws SQLException {
        String sql = upsertSql("c_Text", "Text?");
        try (YdbPreparedStatement statement = jdbc.connection().unwrap(YdbConnection.class).prepareStatement(sql)) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");
            statement.execute();
        }

        jdbc.connection().commit();

        try (YdbPreparedStatement statement = prepareSelectByKey("c_Text")) {
            statement.setInt("key", 2);

            TextSelectAssert.of(statement.executeQuery(), "c_Text", "Text")
                    .noNextRows();

            statement.setInt("key", 1);

            TextSelectAssert.of(statement.executeQuery(), "c_Text", "Text")
                    .nextRow(1, "value-1")
                    .noNextRows();
        }

        try (YdbPreparedStatement statement = jdbc.connection().unwrap(YdbConnection.class).prepareStatement(sql)) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");
            statement.execute();

            statement.setInt("key", 2);
            statement.setString("c_Text", "value-2");
            statement.execute();
        }

        jdbc.connection().commit();

        try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
            TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                    .nextRow(1, "value-1")
                    .nextRow(2, "value-2")
                    .noNextRows();
        }
    }

    @Test
    public void executeQueryInTx() throws SQLException {
        jdbc.connection().setAutoCommit(false);
        try {
            String sql = upsertSql("c_Text", "Optional<Text>");
            try (YdbPreparedStatement statement = jdbc.connection().unwrap(YdbConnection.class).prepareStatement(sql)) {
                statement.setInt("key", 1);
                statement.setString("c_Text", "value-1");
                statement.execute();
            }

            try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
                TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                        .nextRow(1, "value-1")
                        .noNextRows();
            }
        } finally {
            jdbc.connection().setAutoCommit(true);
        }

        try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
            TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                    .nextRow(1, "value-1")
                    .noNextRows();
        }
    }

    @Test
    public void executeScanQueryInTx() throws SQLException {
        jdbc.connection().setAutoCommit(false);
        try {
            String sql = upsertSql("c_Text", "Optional<Text>");
            try (YdbPreparedStatement statement = jdbc.connection().unwrap(YdbConnection.class).prepareStatement(sql)) {
                statement.setInt("key", 1);
                statement.setString("c_Text", "value-1");
                statement.execute();
            }

            String scan = scanSelectSql("c_Text");
            try (YdbPreparedStatement select = jdbc.connection().unwrap(YdbConnection.class).prepareStatement(scan)) {
                select.setInt("key", 1);

                ExceptionAssert.sqlException(YdbConst.SCAN_QUERY_INSIDE_TRANSACTION, () -> select.executeQuery());

                jdbc.connection().commit();

                select.setInt("key", 1);
                TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                        .nextRow(1, "value-1")
                        .noNextRows();
            }
        } finally {
            jdbc.connection().setAutoCommit(true);
        }
    }

    @Test
    public void executeScanQueryInFakeTx() throws SQLException {
        try (Connection connection = jdbc.createCustomConnection("scanQueryTxMode", "FAKE_TX")) {
            connection.setAutoCommit(false);

            String sql = upsertSql("c_Text", "Text?");
            try (YdbPreparedStatement statement = connection.unwrap(YdbConnection.class).prepareStatement(sql)) {
                statement.setInt("key", 1);
                statement.setString("c_Text", "value-1");
                statement.execute();
            }

            String scan = scanSelectSql("c_Text");
            try (YdbPreparedStatement select = connection.unwrap(YdbConnection.class).prepareStatement(scan)) {
                select.setInt("key", 1);
                TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                        .noNextRows();

                connection.commit();

                select.setInt("key", 1);
                TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                        .nextRow(1, "value-1")
                        .noNextRows();
            }
        }
    }

    @Test
    public void executeScanQueryInTxWithShadowCommit() throws SQLException {
        try (Connection connection = jdbc.createCustomConnection("scanQueryTxMode", "SHADOW_COMMIT")) {
            connection.setAutoCommit(false);

            String sql = upsertSql("c_Text", "Text?");
            try (YdbPreparedStatement statement = connection.unwrap(YdbConnection.class).prepareStatement(sql)) {
                statement.setInt("key", 1);
                statement.setString("c_Text", "value-1");
                statement.execute();
            }

            String scan = scanSelectSql("c_Text");
            try (YdbPreparedStatement select = connection.unwrap(YdbConnection.class).prepareStatement(scan)) {
                select.setInt("key", 1);
                TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                        .nextRow(1, "value-1")
                        .noNextRows();
            }
        }
    }

    @Test
    public void executeScanQueryAsUpdate() throws SQLException {
        String sql = scanUpsertSql("c_Text", "Optional<Text>");

        try (YdbPreparedStatement statement = jdbc.connection().unwrap(YdbConnection.class)
                .prepareStatement(sql)
                .unwrap(YdbPreparedStatement.class)) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");

            ExceptionAssert.ydbException("Scan query should have a single result set", statement::execute);
        }
    }
}
