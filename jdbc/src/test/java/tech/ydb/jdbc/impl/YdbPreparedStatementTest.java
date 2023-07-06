package tech.ydb.jdbc.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import tech.ydb.jdbc.YdbPreparedStatement;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.SqlQueries;
import tech.ydb.jdbc.statement.YdbPreparedStatementImpl;
import tech.ydb.jdbc.statement.YdbPreparedStatementWithDataQueryBatchedImpl;
import tech.ydb.jdbc.statement.YdbPreparedStatementWithDataQueryImpl;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.test.junit5.YdbHelperExtension;

import static tech.ydb.jdbc.impl.helper.SqlQueries.JdbcQuery.BATCHED;
import static tech.ydb.jdbc.impl.helper.SqlQueries.JdbcQuery.STANDART;
import static tech.ydb.jdbc.impl.helper.SqlQueries.JdbcQuery.TYPED;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbPreparedStatementTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private static final JdbcConnectionExtention jdbc = new JdbcConnectionExtention(ydb);

    private static final String TEST_TABLE_NAME = "ydb_prepared_test";
    private static final SqlQueries TEST_TABLE = new SqlQueries(TEST_TABLE_NAME);

    @BeforeAll
    public static void createTable() throws SQLException {
        try (Statement statement = jdbc.connection().createStatement();) {
            // create test table
            statement.execute(TEST_TABLE.createTableSQL());
        }
    }

    @AfterAll
    public static void dropTable() throws SQLException {
        try (Statement statement = jdbc.connection().createStatement();) {
            statement.execute(TEST_TABLE.dropTableSQL());
        }
    }

    @AfterEach
    public void afterEach() throws SQLException {
        if (jdbc.connection().isClosed()) {
            return;
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            statement.execute(TEST_TABLE.deleteAllSQL());
        }

        jdbc.connection().close();
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void prepareStatementTest(SqlQueries.JdbcQuery query) throws SQLException {
        String sql = TEST_TABLE.upsertOne(query, "c_Text", "Text");
        try (PreparedStatement statement = jdbc.connection().prepareStatement(sql)) {
            Assertions.assertTrue(statement.isWrapperFor(YdbPreparedStatement.class));

            Assertions.assertEquals(query == STANDART,
                    statement.isWrapperFor(YdbPreparedStatementImpl.class));
            Assertions.assertEquals(query == TYPED,
                    statement.isWrapperFor(YdbPreparedStatementWithDataQueryImpl.class));
            Assertions.assertEquals(query == BATCHED,
                    statement.isWrapperFor(YdbPreparedStatementWithDataQueryBatchedImpl.class));
        }
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void executeUpdateTest(SqlQueries.JdbcQuery query) throws SQLException {
        String sql = TEST_TABLE.upsertOne(query, "c_Text", "Text");
        try (PreparedStatement statement = jdbc.connection().prepareStatement(sql)) {
            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.executeQuery("select 1 + 2"));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.execute("select 1 + 2"));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.execute("select 1 + 2", Statement.NO_GENERATED_KEYS));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.execute("select 1 + 2", new int[0]));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.execute("select 1 + 2", new String[0]));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.executeUpdate("select 1 + 2"));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.executeUpdate("select 1 + 2", Statement.NO_GENERATED_KEYS));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.executeUpdate("select 1 + 2", new int[0]));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.executeUpdate("select 1 + 2", new String[0]));

            ExceptionAssert.sqlException("PreparedStatement cannot execute custom SQL",
                    () -> statement.addBatch("select 1 + 2"));
        }
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(SqlQueries.JdbcQuery.class)
    public void executeWithMissingParameter(SqlQueries.JdbcQuery query) throws SQLException {
        String sql = TEST_TABLE.upsertOne(query, "c_Text", "Text");

        try (PreparedStatement statement = jdbc.connection().prepareStatement(sql)) {
            statement.setInt(1, 1);
            ExceptionAssert.ydbNonRetryable("Missing value for parameter", statement::execute);
        }
    }

    // Standart mode doesn't use types
    @ParameterizedTest(name = "with {0}")
    @EnumSource(value=SqlQueries.JdbcQuery.class, mode=EnumSource.Mode.EXCLUDE, names = { "STANDART" })
    public void executeWithWrongType(SqlQueries.JdbcQuery query) throws SQLException {
        String sql = TEST_TABLE.upsertOne(query, "c_Text", "Text"); // Must be Optional<Text>

        try (PreparedStatement statement = jdbc.connection().prepareStatement(sql)) {
            statement.setInt(1, 1);
            ExceptionAssert.sqlException("Missing required value for parameter: $p2",
                    () -> statement.setObject(2, PrimitiveType.Text.makeOptional().emptyValue())
            );
        }
    }
}
