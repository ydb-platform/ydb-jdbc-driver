package tech.ydb.jdbc.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbPrepareMode;
import tech.ydb.jdbc.YdbPreparedStatement;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.SqlQueries;
import tech.ydb.jdbc.impl.helper.TextSelectAssert;
import tech.ydb.test.junit5.YdbHelperExtension;

public class YdbPreparedStatementWithDataQueryBatchedImplTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private static final JdbcConnectionExtention jdbc = new JdbcConnectionExtention(ydb)
            .withArg("useQueryService", "false");

    private static final String TEST_TABLE_NAME = "ydb_prepared_statement_with_batch_test";
    private static final SqlQueries TEST_TABLE = new SqlQueries(TEST_TABLE_NAME);

    private static final String BATCH_UPSERT_SQL = ""
            + "declare $values as List<Struct<key:Int32, #column:#type>>; \n"
            + "upsert into #tableName select * from as_table($values)";

    private static final String INVALID_BATCH_UPSERT_SQL = ""
            + "declare $values as List<Struct<key:Int32, #column:#type, key:Int32>>; \n"
            + "upsert into #tableName select * from as_table($values)";

    private static final String SIMPLE_SELECT_SQL = "select key, #column from #tableName";

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
            // create test table
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

    private String batchUpsertSql(String column, String type) {
        return BATCH_UPSERT_SQL
                .replaceAll("#column", column)
                .replaceAll("#type", type)
                .replaceAll("#tableName", TEST_TABLE_NAME);
    }

    private String invalidBatchUpsertSql(String column, String type) {
        return INVALID_BATCH_UPSERT_SQL
                .replaceAll("#column", column)
                .replaceAll("#type", type)
                .replaceAll("#tableName", TEST_TABLE_NAME);
    }

    private YdbPreparedStatement prepareBatchUpsert(String column, String type) throws SQLException {
        return jdbc.connection().unwrap(YdbConnection.class)
                .prepareStatement(batchUpsertSql(column, type), YdbPrepareMode.DATA_QUERY_BATCH);
    }

    private PreparedStatement prepareSimpleSelect(String column) throws SQLException {
        String sql = SIMPLE_SELECT_SQL
                .replaceAll("#column", column)
                .replaceAll("#tableName", TEST_TABLE_NAME);
        return jdbc.connection().prepareStatement(sql);
    }

    private PreparedStatement prepareScanSelect(String column) throws SQLException {
        String sql = SIMPLE_SELECT_SQL
                .replaceAll("#column", column)
                .replaceAll("#tableName", TEST_TABLE_NAME);
        return jdbc.connection().prepareStatement("SCAN\n" + sql);
    }

    @Test
    public void testInvalidStruct() throws SQLException {
        ExceptionAssert.ydbException("Duplicated member: key", () -> {
            jdbc.connection().unwrap(YdbConnection.class).prepareStatement(
                    invalidBatchUpsertSql("c_Text", "Text"),
                    YdbPrepareMode.DATA_QUERY_BATCH);
        });
    }

    @Test
    public void executeEmpty() throws SQLException {
        try (YdbPreparedStatement statement = prepareBatchUpsert("c_Text", "Optional<Text>")) {
            statement.executeBatch();
            jdbc.connection().commit();

            try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
                TextSelectAssert.of(select.executeQuery(), "c_Text", "Text").noNextRows();
            }
        }
    }

    @Test
    public void executeEmptyNoResultSet() throws SQLException {
        try (YdbPreparedStatement statement = prepareBatchUpsert("c_Text", "Optional<Text>")) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");
            ExceptionAssert.sqlException("Query must return ResultSet", statement::executeQuery);
        }
        jdbc.connection().commit();
    }

    @Test
    public void executeWithoutBatch() throws SQLException {
        try (YdbPreparedStatement statement = prepareBatchUpsert("c_Text", "Optional<Text>")) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");
            statement.addBatch();

            statement.setInt("key", 2);
            statement.setString("c_Text", "value-2");

            statement.execute(); // Just added only last params
        }

        jdbc.connection().commit();

        try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
            TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                    .nextRow(2, "value-2")
                    .noNextRows();
        }
    }

    @Test
    public void addBatchClearParameters() throws SQLException {
        try (YdbPreparedStatement statement = prepareBatchUpsert("c_Text", "Optional<Text>")) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");
            statement.addBatch();

            statement.setInt("key", 10);
            statement.setString("c_Text", "value-11");
            statement.clearParameters();

            statement.setInt("key", 2);
            statement.setString("c_Text", "value-2");
            statement.addBatch();

            statement.executeBatch();
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
    public void addBatch() throws SQLException {
        try (YdbPreparedStatement statement = prepareBatchUpsert("c_Text", "Optional<Text>")) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");
            statement.addBatch();

            statement.setInt("key", 2);
            statement.setString("c_Text", "value-2");
            statement.addBatch();

            // No add batch, must be skipped
            statement.setInt("key", 3);
            statement.setString("c_Text", "value-3");

            Assertions.assertArrayEquals(new int[]{ Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO },
                    statement.executeBatch());

            // does nothing
            Assertions.assertArrayEquals(new int[0], statement.executeBatch());
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
    public void addAndClearBatch() throws SQLException {
        try (YdbPreparedStatement statement = prepareBatchUpsert("c_Text", "Optional<Text>")) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");
            statement.addBatch();
            statement.executeBatch();

            statement.setInt("key", 11);
            statement.setString("c_Text", "value-11");
            statement.addBatch();
            statement.clearBatch();

            statement.setInt("key", 2);
            statement.setString("c_Text", "value-2");
            statement.addBatch();
            statement.executeBatch();
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
    public void executeQueryBatchWithScanRead() throws SQLException {
        int valuesCount = 5000;
        String[] values = new String[valuesCount];
        for (int idx = 1; idx <= valuesCount; idx += 1) {
            values[idx - 1] = "Row#" + idx;
        }

        try (YdbPreparedStatement statement = prepareBatchUpsert("c_Text", "Optional<Text>")) {
            for (int idx = 1; idx <= valuesCount; idx += 1) {
                statement.setInt("key", idx);
                statement.setString("c_Text", values[idx - 1]);
                statement.addBatch();
            }

            int[] results = statement.executeBatch();
            Assertions.assertEquals(values.length, results.length);

            for (int idx = 0; idx < results.length; idx += 1) {
                Assertions.assertEquals(Statement.SUCCESS_NO_INFO, results[idx], "Wrong batch " + idx);
            }
        }

        jdbc.connection().commit();

        ExceptionAssert.sqlException("Result #0 was truncated to 1000 rows", () -> {
            // Result is truncated (and we catch that)
            try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
                select.executeQuery();
            }
        });

        try (PreparedStatement select = prepareScanSelect("c_Text")) {
            TextSelectAssert check = TextSelectAssert.of(select.executeQuery(), "c_Text", "Text");

            for (int idx = 1; idx <= valuesCount; idx += 1) {
                check.nextRow(idx, values[idx - 1]);
            }

            check.noNextRows();
        }
    }
}
