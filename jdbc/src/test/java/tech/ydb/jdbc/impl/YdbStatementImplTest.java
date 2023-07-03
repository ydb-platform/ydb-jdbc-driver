package tech.ydb.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.jdbc.YdbPreparedStatement;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.SqlQueries;
import tech.ydb.jdbc.impl.helper.TableAssert;
import tech.ydb.test.junit5.YdbHelperExtension;

public class YdbStatementImplTest {
    private static final Logger logger = LoggerFactory.getLogger(YdbStatementImplTest.class);

    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private static final JdbcConnectionExtention jdbc = new JdbcConnectionExtention(ydb, false);

    private static final SqlQueries TEST_TABLE = new SqlQueries("ydb_statement_test");

    private static final String TEST_UPSERT1_SQL = TEST_TABLE
            .withTableName("upsert into ${tableName} (key, c_Text) values (1, '2')");
    private static final String TEST_UPSERT2_SQL = TEST_TABLE
            .withTableName("upsert into ${tableName} (key, c_Text) values (2, '3')");
    private static final String TEST_UPSERT3_SQL = TEST_TABLE
            .withTableName("upsert into ${tableName} (key, c_Text) values (3, '4')");

    private Statement statement;

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

    @BeforeEach
    public void beforeEach() throws SQLException {
        statement = jdbc.connection().createStatement();
    }

    @AfterEach
    public void afterEach() throws SQLException {
        if (jdbc.connection().isClosed()) {
            return;
        }

        statement.close();

        try (Statement st = jdbc.connection().createStatement()) {
            st.execute(TEST_TABLE.deleteAllSQL());
        }
        jdbc.connection().commit(); // MUST be auto rollbacked
        jdbc.connection().close();
    }

    @Test
    public void unwrap() throws SQLException {
        Assertions.assertTrue(statement.isWrapperFor(YdbStatement.class));
        Assertions.assertSame(statement, statement.unwrap(YdbStatement.class));

        Assertions.assertFalse(statement.isWrapperFor(YdbPreparedStatement.class));
        ExceptionAssert.sqlException("Cannot unwrap to " + YdbPreparedStatement.class,
                () -> statement.unwrap(YdbPreparedStatement.class));
    }

    @Test
    public void close() throws SQLException {
        Assertions.assertFalse(statement.isClosed());
        statement.close();
        Assertions.assertTrue(statement.isClosed());
    }

    @Test
    public void getConnection() throws SQLException {
        Assertions.assertSame(jdbc.connection(), statement.getConnection());
    }


    @Test
    public void getGeneratedKeys() throws SQLException {
        Assertions.assertNull(statement.getGeneratedKeys());
    }

    @Test
    public void getResultSetHoldability() throws SQLException {
        Assertions.assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());
    }

    @Test
    public void poolable() throws SQLException {
        Assertions.assertFalse(statement.isPoolable());
        statement.execute("select 1 + 1");

        statement.setPoolable(true);
        Assertions.assertTrue(statement.isPoolable());
        statement.execute("select 1 + 1");

        statement.setPoolable(false);
        Assertions.assertFalse(statement.isPoolable());
        statement.execute("select 1 + 1");

        // basically no visible effect
    }

    @Test
    public void closeOnCompletion() throws SQLException {
        Assertions.assertFalse(statement.isCloseOnCompletion());
        statement.closeOnCompletion();

        Assertions.assertFalse(statement.isCloseOnCompletion());
    }

    @Test
    public void maxFieldSize() throws SQLException {
        Assertions.assertEquals(0, statement.getMaxFieldSize());
        statement.setMaxFieldSize(99); // do nothing
        Assertions.assertEquals(0, statement.getMaxFieldSize());
    }

    @Test
    public void maxRows() throws SQLException {
        Assertions.assertEquals(1000, statement.getMaxRows());
        statement.setMaxRows(99); // do nothing
        Assertions.assertEquals(1000, statement.getMaxRows());
    }

    @Test
    public void setEscapeProcessing() throws SQLException {
        statement.setEscapeProcessing(true); // do nothing
        statement.setEscapeProcessing(false);
        statement.setEscapeProcessing(true);
    }

    @Test
    public void queryTimeout() throws SQLException {
        Assertions.assertEquals(0, statement.getQueryTimeout());
        statement.setQueryTimeout(3);
        Assertions.assertEquals(3, statement.getQueryTimeout());
    }

    @Test
    public void cancel() throws SQLException {
        statement.cancel(); // do nothing
        statement.cancel();
    }

    @Test
    public void warnings() throws SQLException {
        // TODO: check warnings
        Assertions.assertNull(statement.getWarnings());
        statement.clearWarnings();
        Assertions.assertNull(statement.getWarnings());
    }

    @Test
    public void setCursorName() {
        ExceptionAssert.sqlFeatureNotSupported("Named cursors are not supported", () -> statement.setCursorName("cur"));
    }

    @Test
    public void fetchDirection() throws SQLException {
        Assertions.assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());

        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        Assertions.assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());

        statement.setFetchDirection(ResultSet.FETCH_UNKNOWN);
        Assertions.assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());

        ExceptionAssert.sqlException("Direction is not supported: " + ResultSet.FETCH_REVERSE,
                () -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
    }

    @Test
    public void fetchSize() throws SQLException {
        Assertions.assertEquals(1000, statement.getFetchSize());
        statement.setFetchSize(100); // do nothing
        Assertions.assertEquals(1000, statement.getFetchSize());
    }

    @Test
    public void getResultSetConcurrency() throws SQLException {
        Assertions.assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
    }

    @Test
    public void getResultSetType() throws SQLException {
        Assertions.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, statement.getResultSetType());
    }

    @Test
    public void execute() throws SQLException {
        Assertions.assertTrue(statement.execute("select 2 + 2"));
        Assertions.assertFalse(statement.execute(TEST_UPSERT1_SQL));

        Assertions.assertTrue(statement.execute("select 2 + 2", Statement.NO_GENERATED_KEYS));

        jdbc.connection().commit();
    }

    @Test
    public void executeInvalid() {
        ExceptionAssert.sqlException("Auto-generated keys are not supported",
                () -> statement.execute("select 2 + 2", new int[0]));
        ExceptionAssert.sqlException("Auto-generated keys are not supported",
                () -> statement.execute("select 2 + 2", new String[0]));
        ExceptionAssert.sqlException("Auto-generated keys are not supported",
                () -> statement.execute("select 2 + 2", Statement.RETURN_GENERATED_KEYS));
    }

    @Test
    public void executeQuery() throws SQLException {
        try (ResultSet rs = statement.executeQuery("select 1 + 2")) {
            Assertions.assertFalse(rs.isClosed());
            Assertions.assertSame(statement, rs.getStatement());
        }
    }

    @Test
    public void executeUpdate() throws SQLException {
        Assertions.assertEquals(1, statement.executeUpdate(TEST_UPSERT1_SQL));
        Assertions.assertEquals(1, statement.executeUpdate(TEST_UPSERT2_SQL, Statement.NO_GENERATED_KEYS));
        jdbc.connection().commit();
    }

    @Test
    public void executeUpdateInvalid() {
        // Cannot be select operation
        ExceptionAssert.sqlException("Query must not return ResultSet", () -> statement.executeUpdate("select 2 + 2"));

        ExceptionAssert.sqlException("Auto-generated keys are not supported",
                () -> statement.executeUpdate("select 2 + 2", new int[0]));
        ExceptionAssert.sqlException("Auto-generated keys are not supported",
                () -> statement.executeUpdate("select 2 + 2", new String[0]));
        ExceptionAssert.sqlException("Auto-generated keys are not supported",
                () -> statement.executeUpdate("select 2 + 2", Statement.RETURN_GENERATED_KEYS));
    }

    @Test
    public void executeSchemeQueryExplicitly() throws SQLException {
        // create test table
        statement.execute("--jdbc:SCHEME\n create table scheme_query_test(id Int32, primary key(id))");

        String dropSql = "drop table scheme_query_test";
        ExceptionAssert.ydbNonRetryable("'DROP TABLE' not supported in query prepare mode",
                () -> statement.execute(dropSql));
        statement.unwrap(YdbStatement.class).executeSchemeQuery(dropSql);
    }

    @Test
    public void executeDataQuery() throws SQLException {
        try (ResultSet rs = statement.executeQuery("--jdbc:DATA\nselect 2 + 2")) {
            Assertions.assertFalse(rs.isClosed());
            TableAssert.assertSelectInt(4, rs);
        }
    }

    @Test
    public void executeDataQueryInTx() throws SQLException {
        // Cannot select from table already updated in transaction
        statement.executeUpdate(TEST_UPSERT1_SQL);
        ExceptionAssert.ydbNonRetryable("Data modifications previously made to table",
                () -> statement.executeQuery(TEST_TABLE.selectAllSQL()));
    }

    @Test
    public void executeScanQuery() throws SQLException {
        try (ResultSet rs = statement.executeQuery("--jdbc:SCAN\n" + "select 2 + 2")) {
            Assertions.assertFalse(rs.isClosed());
            TableAssert.assertSelectInt(4, rs);
        }
    }

    @Test
    public void executeScanQueryOnSystemTable() throws SQLException {
        try (ResultSet rs = statement.executeQuery("--jdbc:SCAN\n" + "select * from `.sys/partition_stats`")) {
            Assertions.assertFalse(rs.isClosed());
        }
    }

    @Test
    public void executeScanQueryMultiResult() {
        ExceptionAssert.ydbConditionallyRetryable("Scan query should have a single result set",
                () -> statement.executeUpdate("--jdbc:SCAN\n" + "select 2 + 2;select 2 + 3")
        );
    }

    @Test
    public void executeScanQueryInTx() throws SQLException {
        statement.executeUpdate(TEST_UPSERT1_SQL);

        try (ResultSet rs = statement.executeQuery("--jdbc:SCAN\n" + TEST_TABLE.selectSQL())) {
            Assertions.assertFalse(rs.next());
        }

        jdbc.connection().commit();
        try (ResultSet rs = statement.executeQuery("--jdbc:SCAN\n" + TEST_TABLE.selectSQL())) {
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1, rs.getInt("key"));
        }
    }

    @Test
    public void executeScanQueryExplicitlyInTx() throws SQLException {
        statement.executeUpdate(TEST_UPSERT1_SQL);
        try (ResultSet rs = statement.executeQuery("--jdbc:SCAN\n" + TEST_TABLE.selectSQL())) {
            Assertions.assertFalse(rs.next());
        }

        jdbc.connection().commit();
        try (ResultSet rs = statement.executeQuery("--jdbc:SCAN\n" + TEST_TABLE.selectSQL())) {
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1, rs.getInt("key"));
        }

        try (ResultSet rs = statement.unwrap(YdbStatement.class).executeScanQuery(TEST_TABLE.selectSQL())) {
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1, rs.getInt("key"));
        }
    }

    @Test
    public void executeScanQueryAsUpdate() {
        // Looks weird
        ExceptionAssert.ydbConditionallyRetryable("Scan query should have a single result set",
                () -> statement.executeUpdate("--jdbc:SCAN\n" + TEST_UPSERT1_SQL)
        );
    }

    @Test
    public void executeQueryExplainAndExplicitly() throws SQLException {
        String ast = "AST";
        String plan = "PLAN";

        try (ResultSet rs = statement.executeQuery("--jdbc:EXPLAIN\n" + "select 2 + 2")) {
            Assertions.assertFalse(rs.isClosed());

            Assertions.assertTrue(rs.next());
            Assertions.assertNotNull(rs.getString(ast));
            Assertions.assertNotNull(rs.getString(plan));
            logger.info("AST: {}", rs.getString(ast));
            logger.info("PLAN: {}", rs.getString(plan));
            Assertions.assertFalse(rs.next());
        }

        try (ResultSet rs = statement.executeQuery("--jdbc:EXPLAIN\n" + TEST_UPSERT1_SQL)) {
            Assertions.assertTrue(rs.next());
            logger.info("AST: {}", rs.getString(ast));
            logger.info("PLAN: {}", rs.getString(plan));
            Assertions.assertFalse(rs.next());
        }

        try (ResultSet rs = statement.unwrap(YdbStatement.class).executeExplainQuery(TEST_UPSERT1_SQL)) {
            Assertions.assertTrue(rs.next());
            logger.info("AST: {}", rs.getString(ast));
            logger.info("PLAN: {}", rs.getString(plan));
            Assertions.assertFalse(rs.next());
        }
    }

    @Test
    public void getResultSet() throws SQLException {
        Assertions.assertNull(statement.getResultSet());

        statement.execute("select 2 + 2");
        Assertions.assertNotNull(statement.getResultSet());

        statement.execute(TEST_UPSERT1_SQL);
        Assertions.assertNull(statement.getResultSet());

        jdbc.connection().commit();
    }

    @Test
    public void getUpdateCount() throws SQLException {
        Assertions.assertEquals(-1, statement.getUpdateCount());

        statement.execute("select 2 + 2");
        Assertions.assertEquals(-1, statement.getUpdateCount());

        statement.execute(TEST_UPSERT1_SQL);
        Assertions.assertEquals(1, statement.getUpdateCount());

        statement.execute(TEST_UPSERT1_SQL + ";\n" + TEST_UPSERT2_SQL + ";");
        Assertions.assertEquals(1, statement.getUpdateCount()); // just a single statement

        statement.execute("select 2 + 2");
        Assertions.assertEquals(-1, statement.getUpdateCount());

        jdbc.connection().commit();
    }

    @Test
    public void getMoreResults() throws SQLException {
        Assertions.assertFalse(statement.getMoreResults());
        Assertions.assertFalse(statement.getMoreResults());

        statement.execute("select 2 + 2");
        Assertions.assertNotNull(statement.getResultSet());
        Assertions.assertFalse(statement.getMoreResults());


        statement.execute("select 1 + 2; select 2 + 3; select 3 + 4");
        ResultSet rs0 = statement.getResultSet();
        Assertions.assertTrue(statement.getMoreResults());
        ResultSet rs1 = statement.getResultSet();
        Assertions.assertTrue(statement.getMoreResults());
        ResultSet rs2 = statement.getResultSet();
        Assertions.assertFalse(statement.getMoreResults());

        Assertions.assertNotSame(rs0, rs1);
        Assertions.assertNotSame(rs0, rs2);


        Assertions.assertSame(rs0, statement.unwrap(YdbStatement.class).getResultSetAt(0));
        Assertions.assertSame(rs1, statement.unwrap(YdbStatement.class).getResultSetAt(1));
        Assertions.assertSame(rs2, statement.unwrap(YdbStatement.class).getResultSetAt(2));
    }

    @Test
    public void getMoreResultsDifferentMode() throws SQLException {
        statement.execute("select 1 + 2; select 2 + 3; select 3 + 4");

        ResultSet rs0 = statement.getResultSet();
        Assertions.assertTrue(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        Assertions.assertTrue(statement.unwrap(YdbStatement.class).getResultSetAt(0).isClosed());

        ResultSet rs1 = statement.getResultSet();
        Assertions.assertTrue(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        Assertions.assertSame(rs1, statement.unwrap(YdbStatement.class).getResultSetAt(1));

        ResultSet rs2 = statement.getResultSet();
        Assertions.assertFalse(statement.getMoreResults(Statement.CLOSE_ALL_RESULTS));

        Assertions.assertTrue(statement.unwrap(YdbStatement.class).getResultSetAt(1).isClosed());
        Assertions.assertSame(rs2, statement.unwrap(YdbStatement.class).getResultSetAt(2));

        Assertions.assertNotSame(rs0, rs1);
        Assertions.assertNotSame(rs0, rs2);
    }

    @Test
    public void executeBatch() throws SQLException {
        statement.addBatch(TEST_UPSERT1_SQL);
        statement.addBatch(TEST_UPSERT2_SQL);
        statement.addBatch(TEST_UPSERT3_SQL);

        int NI = Statement.SUCCESS_NO_INFO;
        Assertions.assertArrayEquals(new int[]{NI, NI, NI}, statement.executeBatch());
        Assertions.assertNull(statement.getResultSet());

        // Second run does nothing - batch is cleared
        Assertions.assertArrayEquals(new int[0], statement.executeBatch());

        jdbc.connection().commit();

        try (YdbResultSet result = statement.executeQuery(TEST_TABLE.selectSQL()).unwrap(YdbResultSet.class)) {
            Assertions.assertEquals(3, result.getYdbResultSetReader().getRowCount());
        }
    }

    @Test
    public void clearBatch() throws SQLException {
        statement.addBatch(TEST_UPSERT1_SQL);
        statement.addBatch(TEST_UPSERT2_SQL);
        statement.addBatch(TEST_UPSERT3_SQL);
        statement.clearBatch();

        Assertions.assertArrayEquals(new int[0], statement.executeBatch()); // no actions were executed
    }
}
