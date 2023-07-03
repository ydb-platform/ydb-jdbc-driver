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
import tech.ydb.jdbc.YdbPreparedStatement;
import tech.ydb.jdbc.common.QueryType;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.SqlQueries;
import tech.ydb.jdbc.impl.helper.TextSelectAssert;
import tech.ydb.jdbc.statement.YdbPreparedStatementImpl;
import tech.ydb.jdbc.statement.YdbPreparedStatementWithDataQueryBatchedImpl;
import tech.ydb.jdbc.statement.YdbPreparedStatementWithDataQueryImpl;
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

    private static final String SIMPLE_SELECT_SQL = "select key, #column from #tableName";
    private static final String SELECT_BY_KEY_SQL = ""
            + "declare $key as Optional<Int32>;\n"
            + "select key, #column from #tableName where key=$key";

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

    private YdbPreparedStatement prepareUpsert(String column, String type) throws SQLException {
        return jdbc.connection().unwrap(YdbConnection.class)
                .prepareStatement(upsertSql(column, type));
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

    private YdbPreparedStatement prepareScanSelectByKey(String column) throws SQLException {
        String sql = SELECT_BY_KEY_SQL
                .replaceAll("#column", column)
                .replaceAll("#tableName", TEST_TABLE_NAME);
        return jdbc.connection().prepareStatement(QueryType.SCAN_QUERY.getPrefix() + "\n" + sql)
                .unwrap(YdbPreparedStatement.class);
    }

    @Test
    public void prepareStatement() throws SQLException {
        try (YdbPreparedStatement statement = prepareUpsert("c_Text", "Optional<Text>")) {
            Assertions.assertFalse(statement.isWrapperFor(YdbPreparedStatementImpl.class));
            Assertions.assertTrue(statement.isWrapperFor(YdbPreparedStatementWithDataQueryImpl.class));
            Assertions.assertFalse(statement.isWrapperFor(YdbPreparedStatementWithDataQueryBatchedImpl.class));

            Assertions.assertNotNull(statement.unwrap(YdbPreparedStatementWithDataQueryImpl.class));
        }
    }

    @Test
    public void executeWrongParameters() throws SQLException {
        try (YdbPreparedStatement statement = prepareUpsert("c_Text", "Text")) { // Must be Text?
            statement.setInt("key", 1);
            ExceptionAssert.ydbNonRetryable("Missing value for parameter", statement::execute);
        }

        try (YdbPreparedStatement statement = prepareUpsert("c_Text", "Text")) {
            statement.setInt("key", 1);
            ExceptionAssert.sqlException("Missing required value for parameter: $c_Text", () -> {
                statement.setObject("c_Text", PrimitiveType.Text.makeOptional().emptyValue());
            });
        }
    }

    @Test
    public void addBatch() throws SQLException {
        try (YdbPreparedStatement statement = prepareUpsert("c_Text", "Optional<Text>")) {
            ExceptionAssert.sqlFeatureNotSupported(
                    "Batches are not supported in simple prepared statements", statement::addBatch);
            ExceptionAssert.sqlFeatureNotSupported(
                    "Batches are not supported in simple prepared statements", statement::clearBatch);
            ExceptionAssert.sqlFeatureNotSupported(
                    "Batches are not supported in simple prepared statements", statement::executeBatch);
        }
    }

    @Test
    public void executeDataQuery() throws SQLException {
        try (YdbPreparedStatement statement = prepareUpsert("c_Text", "Optional<Text>")) {
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

        try (YdbPreparedStatement statement = prepareUpsert("c_Text", "Optional<Text>")) {
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
            try (YdbPreparedStatement statement = prepareUpsert("c_Text", "Optional<Text>")) {
                statement.setInt("key", 1);
                statement.setString("c_Text", "value-1");
                statement.execute();
            }

            // without jdbc.connection().commit() driver continues to use single transaction;

            ExceptionAssert.ydbNonRetryable("Data modifications previously made to table", () -> {
                try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
                    select.executeQuery();
                }
            });
        } finally {
            jdbc.connection().setAutoCommit(true);
        }
    }

    @Test
    public void executeScanQueryInTx() throws SQLException {
        jdbc.connection().setAutoCommit(false);
        try {
            try (YdbPreparedStatement statement = prepareUpsert("c_Text", "Optional<Text>")) {
                statement.setInt("key", 1);
                statement.setString("c_Text", "value-1");
                statement.execute();
            }

            try (YdbPreparedStatement select = prepareScanSelectByKey("c_Text")) {
                select.setInt("key", 1);
                TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                        .noNextRows();

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
    public void executeScanQueryAsUpdate() throws SQLException {
        String sql = QueryType.SCAN_QUERY.getPrefix() + "\n" + upsertSql("c_Text", "Optional<Text>");

        try (YdbPreparedStatement statement = jdbc.connection().unwrap(YdbConnection.class)
                .prepareStatement(sql, YdbConnection.PreparedStatementMode.IN_MEMORY)
                .unwrap(YdbPreparedStatement.class)) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");

            ExceptionAssert.ydbConditionallyRetryable("Scan query should have a single result set", statement::execute);
        }
    }

    @Test
    public void executeUnsupportedModes() throws SQLException {
        for (QueryType type: QueryType.values()) {
            if (type == QueryType.DATA_QUERY || type == QueryType.SCAN_QUERY) { // --- supported
                continue;
            }

            ExceptionAssert.sqlException("Query type in prepared statement not supported: " + type, () -> {
                String sql = type.getPrefix() + "\n" + upsertSql("c_Text", "Optional<Text>");
                jdbc.connection().prepareStatement(sql);
            });
        }
    }
}
