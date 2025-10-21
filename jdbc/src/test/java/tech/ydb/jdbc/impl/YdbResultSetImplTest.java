package tech.ydb.jdbc.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.UUID;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbResultSetMetaData;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.SqlQueries;
import tech.ydb.jdbc.impl.types.ArrayImpl;
import tech.ydb.jdbc.impl.types.NClobImpl;
import tech.ydb.jdbc.impl.types.RefImpl;
import tech.ydb.jdbc.impl.types.RowIdImpl;
import tech.ydb.jdbc.impl.types.SQLXMLImpl;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;
import tech.ydb.test.junit5.YdbHelperExtension;

public class YdbResultSetImplTest {

    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private static final JdbcConnectionExtention jdbc = new JdbcConnectionExtention(ydb)
            .withArg("usePrefixPath", "rs")
            .withArg("useStreamResultSets", "true");

    private static final SqlQueries SMALL = new SqlQueries("small_table");
    private static final SqlQueries BIG = new SqlQueries("big_table");

    @BeforeAll
    public static void initTables() throws SQLException {
        try (Statement statement = jdbc.connection().createStatement();) {
            // create test tables
            statement.execute(SMALL.createTableSQL());
            statement.execute(SMALL.initTableSQL());
            statement.execute(BIG.createTableSQL());
        }

        // BULK UPSERT
        String bulkUpsertQuery = BIG.upsertOne(SqlQueries.JdbcQuery.BULK, "c_Text", null);
        try (PreparedStatement ps = jdbc.connection().prepareStatement(bulkUpsertQuery)) {
            for (int idx = 1; idx <= 10000; idx++) {
                ps.setInt(1, idx);
                ps.setString(2, "value-" + idx);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    @AfterAll
    public static void dropTable() throws SQLException {
        try (Statement statement = jdbc.connection().createStatement();) {
            statement.execute(SMALL.dropTableSQL());
            statement.execute(BIG.dropTableSQL());
        }
    }

    private static ResultSet selectSmall() throws SQLException {
        return jdbc.connection().createStatement().executeQuery(SMALL.selectSQL());
    }

    private static void assertForwardOnly(Executable exec) {
        ExceptionAssert.sqlFeatureNotSupported("ResultSet in TYPE_FORWARD_ONLY mode", exec);
    }

    private static void assertCursorUpdates(Executable exec) {
        ExceptionAssert.sqlFeatureNotSupported("Cursor updates are not supported", exec);
    }

    private static void assertUpdateObject(Executable exec) {
        ExceptionAssert.sqlFeatureNotSupported("updateObject not implemented", exec);
    }

    private void assertIsEmpty(ResultSet rs) throws SQLException {
        Assertions.assertFalse(rs.isBeforeFirst());
        Assertions.assertFalse(rs.isAfterLast());
        Assertions.assertFalse(rs.isFirst());
        Assertions.assertFalse(rs.isLast());
        Assertions.assertEquals(0, rs.getRow());
    }

    @Test
    public void resultSetType() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            st.setFetchSize(0);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
                Assertions.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
                Assertions.assertEquals(0, rs.getFetchSize());
                Assertions.assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());

                Assertions.assertNotNull(rs.getStatement());
                Assertions.assertSame(st, rs.getStatement());

                Assertions.assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, rs.getHoldability());
            }

            st.setFetchSize(1000);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
                Assertions.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
                Assertions.assertEquals(1000, rs.getFetchSize());
                Assertions.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());

                Assertions.assertNotNull(rs.getStatement());
                Assertions.assertSame(st, rs.getStatement());

                Assertions.assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, rs.getHoldability());
            }
        }
    }

    @Test
    public void close() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            ResultSet rs = st.executeQuery(SMALL.selectSQL());

            Assertions.assertFalse(rs.isClosed());
            rs.close();
            Assertions.assertTrue(rs.isClosed());
            rs.close(); // double closing is allowed
            Assertions.assertTrue(rs.isClosed());
        }

        Statement st = jdbc.connection().createStatement();
        ResultSet rs = st.executeQuery(SMALL.selectSQL());
        Assertions.assertFalse(rs.isClosed());
        st.close(); // statement closes current result set
        Assertions.assertTrue(rs.isClosed());
        Assertions.assertTrue(st.isClosed());
    }

    @Test
    public void unwrap() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertTrue(rs.isWrapperFor(YdbResultSet.class));
                Assertions.assertSame(rs, rs.unwrap(YdbResultSet.class));

                Assertions.assertFalse(rs.isWrapperFor(YdbStatement.class));
                ExceptionAssert.sqlException("Cannot unwrap to " + YdbStatement.class, () -> rs.unwrap(YdbStatement.class));
            }

            st.setFetchSize(0);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertTrue(rs.isWrapperFor(YdbResultSetMemory.class));
                Assertions.assertSame(rs, rs.unwrap(YdbResultSetMemory.class));
                Assertions.assertEquals(0, rs.getFetchSize());
                Assertions.assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());
            }

            st.setFetchSize(1000);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertTrue(rs.isWrapperFor(YdbResultSetForwardOnly.class));
                Assertions.assertSame(rs, rs.unwrap(YdbResultSetForwardOnly.class));
                Assertions.assertEquals(1000, rs.getFetchSize());
                Assertions.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
            }
        }
    }

    @Test
    public void invalidColumnsTest() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                // invalid location
                ExceptionAssert.sqlException("Current row index is out of bounds: 0", () -> rs.getString(2));
                ExceptionAssert.sqlException("Current row index is out of bounds: 0", () -> rs.getString("c_Text"));

                // invalid case
                ExceptionAssert.sqlException("Column not found: c_text", () -> rs.getString("c_text"));
                ExceptionAssert.sqlException("Column not found: C_TEXT", () -> rs.getString("C_TEXT"));
                ExceptionAssert.sqlException("Current row index is out of bounds: 0", () -> rs.getString("c_Text"));

                // invalid name
                ExceptionAssert.sqlException("Column not found: value0", () -> rs.getString("value0"));
            }
        }
    }

    @Test
    public void findColumn() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertEquals(1, rs.findColumn("key"));
                Assertions.assertEquals(14, rs.findColumn("c_Text"));
                ExceptionAssert.sqlException("Column not found: value0", () -> rs.findColumn("value0"));
            }
        }
    }

    @Test
    public void warnings() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            try (ResultSet rs = st.executeQuery("$ignored = 1; " + SMALL.selectSQL())) {
                Assertions.assertNull(rs.getWarnings());
                rs.clearWarnings();
                Assertions.assertNull(rs.getWarnings());
            }
        }
    }

    @Test
    public void getCursorName() {
        ExceptionAssert.sqlFeatureNotSupported("Named cursors are not supported", () -> selectSmall().getCursorName());
    }

    @Test
    public void getMetaData() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                ResultSetMetaData metadata = rs.getMetaData();
                Assertions.assertSame(metadata, rs.getMetaData(), "Metadata is cached");

                Assertions.assertTrue(metadata.isWrapperFor(YdbResultSetMetaData.class));
                YdbResultSetMetaData ydbMetadata = metadata.unwrap(YdbResultSetMetaData.class);
                Assertions.assertSame(metadata, ydbMetadata);

                ExceptionAssert.sqlException("Column is out of range: 995", () -> metadata.getColumnName(995));

                Assertions.assertEquals(29, metadata.getColumnCount());

                for (int index = 0; index < metadata.getColumnCount(); index++) {
                    int column = index + 1;
                    String name = metadata.getColumnName(column);
                    Assertions.assertNotNull(name);
                    Assertions.assertEquals(name, metadata.getColumnLabel(column));

                    Assertions.assertFalse(metadata.isAutoIncrement(column), "All columns are not isAutoIncrement");
                    Assertions.assertTrue(metadata.isCaseSensitive(column), "All columns are isCaseSensitive");
                    Assertions.assertFalse(metadata.isSearchable(column), "All columns are not isSearchable");
                    Assertions.assertFalse(metadata.isCurrency(column), "All columns are not isCurrency");
                    Assertions.assertEquals(ResultSetMetaData.columnNullable, metadata.isNullable(column),
                            "All columns in table are nullable, but pseudo-columns are not");
                    Assertions.assertFalse(metadata.isSigned(column), "All columns are not isSigned");
                    Assertions.assertEquals(0, metadata.getColumnDisplaySize(column), "No display size available");
                    Assertions.assertEquals("", metadata.getSchemaName(column), "No schema available");
                    Assertions.assertEquals(0, metadata.getPrecision(column), "No precision available");
                    Assertions.assertEquals(0, metadata.getScale(column), "No scale available");
                    Assertions.assertEquals("", metadata.getTableName(column), "No table name available");
                    Assertions.assertEquals("", metadata.getCatalogName(column), "No catalog name available");
                    Assertions.assertTrue(metadata.isReadOnly(column), "All columns are isReadOnly");
                    Assertions.assertFalse(metadata.isWritable(column), "All columns are not isWritable");
                    Assertions.assertFalse(metadata.isDefinitelyWritable(column),
                            "All columns are not isDefinitelyWritable");

                    if (name.startsWith("c_")) {
                        String expectType = name.substring("c_".length()).toLowerCase();
                        if (expectType.equals("decimal")) {
                            expectType = "decimal(22, 9)";
                        }
                        if (expectType.equals("bigdecimal")) {
                            expectType = "decimal(35, 0)";
                        }
                        if (expectType.equals("bankdecimal")) {
                            expectType = "decimal(31, 9)";
                        }

                        String actualType = metadata.getColumnTypeName(column);
                        Assertions.assertNotNull(actualType, "All columns have database types");
                        Assertions.assertEquals(expectType, actualType.toLowerCase(),
                                "All column names are similar to types");
                    }

                    Assertions.assertTrue(metadata.getColumnType(column) != 0,
                            "All columns have sql type, including " + name);
                    // getColumnClassName is checkering already
                }
            }
        }
    }

    @Test
    public void fetchDirection() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            st.setFetchSize(0);
            st.setFetchDirection(ResultSet.FETCH_UNKNOWN);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());
                rs.setFetchDirection(ResultSet.FETCH_REVERSE);
                Assertions.assertEquals(ResultSet.FETCH_REVERSE, rs.getFetchDirection());
                rs.setFetchDirection(ResultSet.FETCH_FORWARD);
                Assertions.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
            }

            st.setFetchSize(0);
            st.setFetchDirection(ResultSet.FETCH_REVERSE);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertEquals(ResultSet.FETCH_REVERSE, rs.getFetchDirection());
                rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
                Assertions.assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());
                rs.setFetchDirection(ResultSet.FETCH_FORWARD);
                Assertions.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
            }

            st.setFetchSize(1000);
            st.setFetchDirection(ResultSet.FETCH_UNKNOWN);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
                rs.setFetchDirection(ResultSet.FETCH_FORWARD);
                assertForwardOnly(() -> rs.setFetchDirection(ResultSet.FETCH_REVERSE));
                assertForwardOnly(() -> rs.setFetchDirection(ResultSet.FETCH_UNKNOWN));
                Assertions.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
            }

            st.setFetchSize(1000);
            st.setFetchDirection(ResultSet.FETCH_FORWARD);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
                rs.setFetchDirection(ResultSet.FETCH_FORWARD);
                assertForwardOnly(() -> rs.setFetchDirection(ResultSet.FETCH_REVERSE));
                assertForwardOnly(() -> rs.setFetchDirection(ResultSet.FETCH_UNKNOWN));
                Assertions.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
            }

            st.setFetchSize(1000);
            st.setFetchDirection(ResultSet.FETCH_REVERSE);
            ExceptionAssert.sqlException("Requested scrollable ResutlSet, but this ResultSet is FORWARD_ONLY.",
                    () -> st.executeQuery(SMALL.selectSQL())
            );
        }
    }

    @Test
    public void moveOnEmptyResultSet() throws SQLException {
        String select = SMALL.withTableName("scan select * from #tableName where 1 = 0");
        try (Statement st = jdbc.connection().createStatement()) {
            try (ResultSet rs = st.executeQuery(select)) {
                assertIsEmpty(rs);
                Assertions.assertFalse(rs.next());
                assertIsEmpty(rs);
            }
        }
    }

    @Test
    public void closeResultSetOnExecuteNext() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            ResultSet rs1 = st.executeQuery(SMALL.selectSQL());
            Assertions.assertFalse(rs1.isClosed());
            ResultSet rs2 = st.executeQuery(SMALL.selectSQL());
            Assertions.assertTrue(rs1.isClosed());
            Assertions.assertFalse(rs2.isClosed());

            rs2.close();
        }
    }

    @Test
    public void closeResultSetOnCreateStatement() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            ResultSet rs1 = st.executeQuery(SMALL.selectSQL());
            Assertions.assertFalse(rs1.isClosed());

            Statement other = jdbc.connection().createStatement();

            Assertions.assertFalse(rs1.isClosed()); // new statement doesn't close current result set

            ResultSet rs2 = other.executeQuery(SMALL.selectSQL());
            Assertions.assertTrue(rs1.isClosed());
            Assertions.assertFalse(rs2.isClosed());

            other.close();
            Assertions.assertTrue(rs2.isClosed());

            rs2.close();
        }
    }

    @Test
    public void closeResultSetOnPrepareStatement() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            ResultSet rs1 = st.executeQuery(SMALL.selectSQL());
            Assertions.assertFalse(rs1.isClosed());

            PreparedStatement ps = jdbc.connection().prepareStatement(SMALL.selectAllByKey("?"));

            Assertions.assertFalse(rs1.isClosed()); // prepare statement doesn't close current result set
            ps.setInt(1, 1);

            Assertions.assertFalse(rs1.isClosed()); // prepare statement doesn't close current result set

            ResultSet rs2 = ps.executeQuery();
            Assertions.assertTrue(rs1.isClosed());
            Assertions.assertFalse(rs2.isClosed());

            ps.close();
            Assertions.assertTrue(rs2.isClosed());

            rs2.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 0, 1000 })
    public void next(int fetchSize) throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            st.setFetchSize(fetchSize);
            ResultSet rs = st.executeQuery(SMALL.selectSQL());

            Assertions.assertEquals(0, rs.getRow());
            Assertions.assertTrue(rs.isBeforeFirst());
            Assertions.assertFalse(rs.isFirst());
            Assertions.assertFalse(rs.isLast());
            Assertions.assertFalse(rs.isAfterLast());

            Assertions.assertTrue(rs.next());

            Assertions.assertEquals(1, rs.getRow());
            Assertions.assertFalse(rs.isBeforeFirst());
            Assertions.assertTrue(rs.isFirst());
            Assertions.assertFalse(rs.isLast());
            Assertions.assertFalse(rs.isAfterLast());

            Assertions.assertTrue(rs.next());

            Assertions.assertEquals(2, rs.getRow());
            Assertions.assertFalse(rs.isBeforeFirst());
            Assertions.assertFalse(rs.isFirst());
            Assertions.assertFalse(rs.isLast());
            Assertions.assertFalse(rs.isAfterLast());

            Assertions.assertTrue(rs.next());

            Assertions.assertEquals(3, rs.getRow());
            Assertions.assertFalse(rs.isBeforeFirst());
            Assertions.assertFalse(rs.isFirst());
            Assertions.assertFalse(rs.isLast());
            Assertions.assertFalse(rs.isAfterLast());

            Assertions.assertTrue(rs.next());

            Assertions.assertEquals(4, rs.getRow());
            Assertions.assertFalse(rs.isBeforeFirst());
            Assertions.assertFalse(rs.isFirst());
            Assertions.assertFalse(rs.isLast());
            Assertions.assertFalse(rs.isAfterLast());

            Assertions.assertTrue(rs.next());

            Assertions.assertEquals(5, rs.getRow());
            Assertions.assertFalse(rs.isBeforeFirst());
            Assertions.assertFalse(rs.isFirst());
            Assertions.assertTrue(rs.isLast());
            Assertions.assertFalse(rs.isAfterLast());

            Assertions.assertFalse(rs.next());

            Assertions.assertEquals(0, rs.getRow()); // like Postgres
            Assertions.assertFalse(rs.isBeforeFirst());
            Assertions.assertFalse(rs.isFirst());
            Assertions.assertFalse(rs.isLast());
            Assertions.assertTrue(rs.isAfterLast());

            Assertions.assertFalse(rs.next());
            Assertions.assertFalse(rs.next());
        }
    }

    @Test
    public void forwarnOnlyUnsupportedMethods() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            st.setFetchSize(100);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertEquals(0, rs.getRow());
                assertForwardOnly(() -> { rs.first(); });
                assertForwardOnly(() -> { rs.last(); });
                assertForwardOnly(() -> { rs.previous(); });
                assertForwardOnly(() -> { rs.beforeFirst(); });
                assertForwardOnly(() -> { rs.afterLast(); });
                assertForwardOnly(() -> { rs.absolute(0); });
                assertForwardOnly(() -> { rs.relative(0); });
            }
        }
    }

    @Test
    public void first() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            st.setFetchSize(0);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertFalse(rs.isFirst());
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertTrue(rs.next());
                Assertions.assertTrue(rs.isFirst());
                Assertions.assertEquals(1, rs.getRow());

                Assertions.assertTrue(rs.next());
                Assertions.assertFalse(rs.isFirst());
                Assertions.assertEquals(2, rs.getRow());
            }
        }
    }

    @Test
    public void last() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            st.setFetchSize(0);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertFalse(rs.isLast());
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertTrue(rs.last());
                Assertions.assertTrue(rs.isLast());
                Assertions.assertEquals(5, rs.getRow());

                Assertions.assertTrue(rs.previous());
                Assertions.assertFalse(rs.isLast());
                Assertions.assertEquals(4, rs.getRow());

                Assertions.assertTrue(rs.next());
                Assertions.assertTrue(rs.isLast());
                Assertions.assertEquals(5, rs.getRow());

                Assertions.assertFalse(rs.next());
                Assertions.assertFalse(rs.isLast());
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertFalse(rs.next());
                Assertions.assertEquals(0, rs.getRow());
            }
        }
    }

    @Test
    public void beforeFirst() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            st.setFetchSize(0);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertTrue(rs.isBeforeFirst());
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertTrue(rs.next());
                Assertions.assertFalse(rs.isBeforeFirst());
                Assertions.assertEquals(1, rs.getRow());

                rs.beforeFirst();
                Assertions.assertTrue(rs.isBeforeFirst());
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertTrue(rs.next());
                Assertions.assertFalse(rs.isBeforeFirst());
                Assertions.assertEquals(1, rs.getRow());
            }
        }
    }

    @Test
    public void afterLast() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            st.setFetchSize(0);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertFalse(rs.isAfterLast());
                Assertions.assertEquals(0, rs.getRow());

                rs.afterLast();
                Assertions.assertTrue(rs.isAfterLast());
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertFalse(rs.next());
                Assertions.assertTrue(rs.isAfterLast());
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertTrue(rs.previous());
                Assertions.assertFalse(rs.isAfterLast());
                Assertions.assertEquals(5, rs.getRow());

                Assertions.assertFalse(rs.next());
                Assertions.assertTrue(rs.isAfterLast());
                Assertions.assertEquals(0, rs.getRow());
            }
        }
    }

    @Test
    public void absolute() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            st.setFetchSize(0);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertFalse(rs.absolute(0));
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertTrue(rs.absolute(1));
                Assertions.assertEquals(1, rs.getRow());

                Assertions.assertTrue(rs.absolute(-1));
                Assertions.assertEquals(5, rs.getRow());

                Assertions.assertTrue(rs.absolute(-2));
                Assertions.assertEquals(4, rs.getRow());

                Assertions.assertTrue(rs.absolute(4));
                Assertions.assertEquals(4, rs.getRow());

                Assertions.assertTrue(rs.absolute(5));
                Assertions.assertEquals(5, rs.getRow());

                Assertions.assertFalse(rs.absolute(6));
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertFalse(rs.absolute(7));
                Assertions.assertEquals(0, rs.getRow());
            }
        }
    }

    @Test
    public void relative() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            st.setFetchSize(0);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertTrue(rs.relative(1));
                Assertions.assertEquals(1, rs.getRow());

                Assertions.assertTrue(rs.relative(2));
                Assertions.assertEquals(3, rs.getRow());

                Assertions.assertTrue(rs.relative(0));
                Assertions.assertEquals(3, rs.getRow());

                Assertions.assertFalse(rs.relative(3));
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertFalse(rs.relative(2));
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertTrue(rs.relative(-1));
                Assertions.assertEquals(5, rs.getRow());

                Assertions.assertTrue(rs.relative(-1));
                Assertions.assertEquals(4, rs.getRow());

                Assertions.assertFalse(rs.relative(-10));
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertFalse(rs.relative(-1));
                Assertions.assertEquals(0, rs.getRow());
            }
        }
    }

    @Test
    public void previous() throws SQLException {
        try (Statement st = jdbc.connection().createStatement()) {
            st.setFetchSize(0);
            try (ResultSet rs = st.executeQuery(SMALL.selectSQL())) {
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertTrue(rs.last());
                Assertions.assertEquals(5, rs.getRow());

                Assertions.assertTrue(rs.previous());
                Assertions.assertEquals(4, rs.getRow());

                Assertions.assertTrue(rs.next());
                Assertions.assertEquals(5, rs.getRow());

                Assertions.assertFalse(rs.next());
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertFalse(rs.next());
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertTrue(rs.previous());
                Assertions.assertEquals(5, rs.getRow());

                Assertions.assertTrue(rs.previous());
                Assertions.assertEquals(4, rs.getRow());

                Assertions.assertTrue(rs.previous());
                Assertions.assertEquals(3, rs.getRow());

                Assertions.assertTrue(rs.previous());
                Assertions.assertEquals(2, rs.getRow());

                Assertions.assertTrue(rs.previous());
                Assertions.assertEquals(1, rs.getRow());

                Assertions.assertFalse(rs.previous());
                Assertions.assertEquals(0, rs.getRow());

                Assertions.assertFalse(rs.previous());
                Assertions.assertEquals(0, rs.getRow());
            }
        }
    }

    @Test
    public void cursorUpdatesIsNotSupportedTest() throws SQLException {
        ResultSet resultSet = selectSmall();
        // row actions
        assertCursorUpdates(() -> resultSet.rowUpdated());
        assertCursorUpdates(() -> resultSet.rowInserted());
        assertCursorUpdates(() -> resultSet.rowDeleted());

        assertCursorUpdates(() -> resultSet.insertRow());
        assertCursorUpdates(() -> resultSet.updateRow());
        assertCursorUpdates(() -> resultSet.deleteRow());
        assertCursorUpdates(() -> resultSet.refreshRow());

        assertCursorUpdates(() -> resultSet.cancelRowUpdates());
        assertCursorUpdates(() -> resultSet.moveToInsertRow());
        assertCursorUpdates(() -> resultSet.moveToCurrentRow());

        // updateNull
        assertCursorUpdates(() -> resultSet.updateNull(1));
        assertCursorUpdates(() -> resultSet.updateNull("value"));

        // updateBoolean
        assertCursorUpdates(() -> resultSet.updateBoolean(1, true));
        assertCursorUpdates(() -> resultSet.updateBoolean("value", true));

        // updateByte
        assertCursorUpdates(() -> resultSet.updateByte(1, (byte) 1));
        assertCursorUpdates(() -> resultSet.updateByte("value", (byte) 1));

        // updateShort
        assertCursorUpdates(() -> resultSet.updateShort(1, (short) 1));
        assertCursorUpdates(() -> resultSet.updateShort("value", (short) 1));

        // updateInt
        assertCursorUpdates(() -> resultSet.updateInt(1, 1));
        assertCursorUpdates(() -> resultSet.updateInt("value", 1));

        // updateLong
        assertCursorUpdates(() -> resultSet.updateLong(1, 1l));
        assertCursorUpdates(() -> resultSet.updateLong("value", 1l));

        // updateFloat
        assertCursorUpdates(() -> resultSet.updateFloat(1, 1f));
        assertCursorUpdates(() -> resultSet.updateFloat("value", 1f));

        // updateDouble
        assertCursorUpdates(() -> resultSet.updateDouble(1, 1d));
        assertCursorUpdates(() -> resultSet.updateDouble("value", 1d));

        // updateBigDecimal
        assertCursorUpdates(() -> resultSet.updateBigDecimal(1, BigDecimal.ONE));
        assertCursorUpdates(() -> resultSet.updateBigDecimal("value", BigDecimal.ONE));

        // updateString
        assertCursorUpdates(() -> resultSet.updateString(1, "value"));
        assertCursorUpdates(() -> resultSet.updateString("value", "value"));

        // updateBytes
        assertCursorUpdates(() -> resultSet.updateBytes(1, new byte[]{}));
        assertCursorUpdates(() -> resultSet.updateBytes("value", new byte[]{}));

        // updateDate
        assertCursorUpdates(() -> resultSet.updateDate(1, new Date(0)));
        assertCursorUpdates(() -> resultSet.updateDate("value", new Date(0)));

        // updateTime
        assertCursorUpdates(() -> resultSet.updateTime(1, new Time(0)));
        assertCursorUpdates(() -> resultSet.updateTime("value", new Time(0)));

        // updateTimestamp
        assertCursorUpdates(() -> resultSet.updateTimestamp(1, new Timestamp(0)));
        assertCursorUpdates(() -> resultSet.updateTimestamp("value", new Timestamp(0)));

        // updateAsciiStream
        assertCursorUpdates(() -> resultSet.updateAsciiStream(1, new ByteArrayInputStream(new byte[0])));
        assertCursorUpdates(() -> resultSet.updateAsciiStream(1, new ByteArrayInputStream(new byte[0]), 1));
        assertCursorUpdates(() -> resultSet.updateAsciiStream(1, new ByteArrayInputStream(new byte[0]), 1L));
        assertCursorUpdates(() -> resultSet.updateAsciiStream("value", new ByteArrayInputStream(new byte[0])));
        assertCursorUpdates(() -> resultSet.updateAsciiStream("value", new ByteArrayInputStream(new byte[0]), 1));
        assertCursorUpdates(() -> resultSet.updateAsciiStream("value", new ByteArrayInputStream(new byte[0]), 1L));

        // updateBinaryStream
        assertCursorUpdates(() -> resultSet.updateBinaryStream(1, new ByteArrayInputStream(new byte[0])));
        assertCursorUpdates(() -> resultSet.updateBinaryStream(1, new ByteArrayInputStream(new byte[0]), 1));
        assertCursorUpdates(() -> resultSet.updateBinaryStream(1, new ByteArrayInputStream(new byte[0]), 1L));
        assertCursorUpdates(() -> resultSet.updateBinaryStream("value", new ByteArrayInputStream(new byte[0])));
        assertCursorUpdates(() -> resultSet.updateBinaryStream("value", new ByteArrayInputStream(new byte[0]), 1));
        assertCursorUpdates(() -> resultSet.updateBinaryStream("value", new ByteArrayInputStream(new byte[0]), 1L));

        // updateCharacterStream
        assertCursorUpdates(() -> resultSet.updateCharacterStream(1, new StringReader("")));
        assertCursorUpdates(() -> resultSet.updateCharacterStream(1, new StringReader(""), 1));
        assertCursorUpdates(() -> resultSet.updateCharacterStream(1, new StringReader(""), 1L));
        assertCursorUpdates(() -> resultSet.updateCharacterStream("value", new StringReader("")));
        assertCursorUpdates(() -> resultSet.updateCharacterStream("value", new StringReader(""), 1));
        assertCursorUpdates(() -> resultSet.updateCharacterStream("value", new StringReader(""), 1L));

        // updateObject
        assertCursorUpdates(() -> resultSet.updateObject(1, true));
        assertCursorUpdates(() -> resultSet.updateObject(1, true, 1));
        assertUpdateObject(() -> resultSet.updateObject(1, true, JDBCType.INTEGER));
        assertUpdateObject(() -> resultSet.updateObject(1, true, JDBCType.INTEGER, 1));
        assertCursorUpdates(() -> resultSet.updateObject("value", true));
        assertCursorUpdates(() -> resultSet.updateObject("value", true, 1));
        assertUpdateObject(() -> resultSet.updateObject("value", true, JDBCType.INTEGER));
        assertUpdateObject(() -> resultSet.updateObject("value", true, JDBCType.INTEGER, 1));

        // updateRef
        assertCursorUpdates(() -> resultSet.updateRef(1, new RefImpl()));
        assertCursorUpdates(() -> resultSet.updateRef("value", new RefImpl()));

        // updateBlob
        assertCursorUpdates(() -> resultSet.updateBlob(1, new SerialBlob(new byte[0])));
        assertCursorUpdates(() -> resultSet.updateBlob(1, new ByteArrayInputStream(new byte[0])));
        assertCursorUpdates(() -> resultSet.updateBlob(1, new ByteArrayInputStream(new byte[0]), 1));
        assertCursorUpdates(() -> resultSet.updateBlob("value", new SerialBlob(new byte[0])));
        assertCursorUpdates(() -> resultSet.updateBlob("value", new ByteArrayInputStream(new byte[0])));
        assertCursorUpdates(() -> resultSet.updateBlob("value", new ByteArrayInputStream(new byte[0]), 1));

        // updateClob
        assertCursorUpdates(() -> resultSet.updateClob(1, new SerialClob(new char[0])));
        assertCursorUpdates(() -> resultSet.updateClob(1, new StringReader("")));
        assertCursorUpdates(() -> resultSet.updateClob(1, new StringReader(""), 1));
        assertCursorUpdates(() -> resultSet.updateClob("value", new SerialClob(new char[0])));
        assertCursorUpdates(() -> resultSet.updateClob("value", new StringReader("")));
        assertCursorUpdates(() -> resultSet.updateClob("value", new StringReader(""), 1));

        // updateArray
        assertCursorUpdates(() -> resultSet.updateArray(1, new ArrayImpl()));
        assertCursorUpdates(() -> resultSet.updateArray("value", new ArrayImpl()));

        // updateRowId
        assertCursorUpdates(() -> resultSet.updateRowId(1, new RowIdImpl()));
        assertCursorUpdates(() -> resultSet.updateRowId(1, new RowIdImpl()));

        // updateNCharacterStream
        assertCursorUpdates(() -> resultSet.updateNCharacterStream(1, new StringReader("")));
        assertCursorUpdates(() -> resultSet.updateNCharacterStream(1, new StringReader(""), 1));
        assertCursorUpdates(() -> resultSet.updateNCharacterStream("value", new StringReader("")));
        assertCursorUpdates(() -> resultSet.updateNCharacterStream("value", new StringReader(""), 1));

        // updateNClob
        assertCursorUpdates(() -> resultSet.updateNClob(1, new NClobImpl(new char[0])));
        assertCursorUpdates(() -> resultSet.updateNClob(1, new StringReader("")));
        assertCursorUpdates(() -> resultSet.updateNClob(1, new StringReader(""), 1));
        assertCursorUpdates(() -> resultSet.updateNClob("value", new NClobImpl(new char[0])));
        assertCursorUpdates(() -> resultSet.updateNClob("value", new StringReader("")));
        assertCursorUpdates(() -> resultSet.updateNClob("value", new StringReader(""), 1));

        // updateNString
        assertCursorUpdates(() -> resultSet.updateNString(1, ""));
        assertCursorUpdates(() -> resultSet.updateNString("value", ""));

        // updateSQLXML
        assertCursorUpdates(() -> resultSet.updateSQLXML(1, new SQLXMLImpl()));
        assertCursorUpdates(() -> resultSet.updateSQLXML("value", new SQLXMLImpl()));
    }

    @Test
    public void getString() throws SQLException {
        ResultSetChecker<String> checker = check(selectSmall(), ResultSet::getString, ResultSet::getString);

        checker.nextRow()
                .value(1, "key", "1")
                .value(2, "c_Bool", "true")
                .value(3, "c_Int8", "101")
                .value(4, "c_Int16", "20001")
                .value(5, "c_Int32", "2000000001")
                .value(6, "c_Int64", "2000000000001")
                .value(7, "c_Uint8", "100")
                .value(8, "c_Uint16", "20002")
                .value(9, "c_Uint32", "2000000002")
                .value(10, "c_Uint64", "2000000000002")
                .value(11, "c_Float", "123456.78")
                .value(12, "c_Double", "1.2345678912345679E8")
                .value(13, "c_Bytes", "bytes array")
                .value(14, "c_Text", "text text text")
                .value(15, "c_Json", "{\"key\": \"value Json\"}")
                .value(16, "c_JsonDocument", "{\"key\":\"value JsonDocument\"}")
                .value(17, "c_Yson", "{key=\"value yson\"}")
                .value(18, "c_Uuid", "6e73b41c-4ede-4d08-9cfb-b7462d9e498b")
                .value(19, "c_Date", "1978-07-09")
                .value(20, "c_Datetime", "1979-11-10T19:45:56")
                .value(21, "c_Timestamp", "1970-01-04T14:25:11.223342Z")
                .value(22, "c_Interval", "PT3.111113S")
                .value(27, "c_Decimal", "3.335000000")
                .value(28, "c_BigDecimal", "12345678901234567890123456789012345")
                .value(29, "c_BankDecimal", "9999999999999999999999.999999999");

        checker.nextRow()
                .value(1, "key", "2")
                .value(2, "c_Bool", "false")
                .value(3, "c_Int8", "-101")
                .value(4, "c_Int16", "-20001")
                .value(5, "c_Int32", "-2000000001")
                .value(6, "c_Int64", "-2000000000001")
                .value(7, "c_Uint8", "200")
                .value(8, "c_Uint16", "40002")
                .value(9, "c_Uint32", "4000000002")
                .value(10, "c_Uint64", "4000000000002")
                .value(11, "c_Float", "-123456.78")
                .value(12, "c_Double", "-1.2345678912345679E8")
                .value(13, "c_Bytes", "")
                .value(14, "c_Text", "")
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", "\"\"")
                .value(18, "c_Uuid", null)
                .value(19, "c_Date", "1978-07-10")
                .value(20, "c_Datetime", "1976-09-10T13:45")
                .value(21, "c_Timestamp", "1970-01-02T06:51:51.223342Z")
                .value(22, "c_Interval", "PT3.112113S")
                .value(27, "c_Decimal", "-3.335000000")
                .value(28, "c_BigDecimal", "-98765432109876543210987654321098765")
                .value(29, "c_BankDecimal", "-9999999999999999999999.999999999");

        checker.nextRow()
                .value(1, "key", "3")
                .value(2, "c_Bool", "false")
                .value(3, "c_Int8", "0")
                .value(4, "c_Int16", "0")
                .value(5, "c_Int32", "0")
                .value(6, "c_Int64", "0")
                .value(7, "c_Uint8", "0")
                .value(8, "c_Uint16", "0")
                .value(9, "c_Uint32", "0")
                .value(10, "c_Uint64", "0")
                .value(11, "c_Float", "0.0")
                .value(12, "c_Double", "0.0")
                .value(13, "c_Bytes", "0")
                .value(14, "c_Text", "0")
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", "0")
                .value(18, "c_Uuid", null)
                .value(19, "c_Date", "1970-01-01")
                .value(20, "c_Datetime", "1970-01-01T00:00")
                .value(21, "c_Timestamp", "1970-01-01T00:00:00Z")
                .value(22, "c_Interval", "PT0S")
                .value(27, "c_Decimal", "0")      // Zero always converts without scale
                .value(28, "c_BigDecimal", "0")   // Zero always converts without scale
                .value(29, "c_BankDecimal", "0"); // Zero always converts without scale

        checker.nextRow()
                .value(1, "key", "4")
                .value(2, "c_Bool", "true")
                .value(3, "c_Int8", "1")
                .value(4, "c_Int16", "1")
                .value(5, "c_Int32", "1")
                .value(6, "c_Int64", "1")
                .value(7, "c_Uint8", "1")
                .value(8, "c_Uint16", "1")
                .value(9, "c_Uint32", "1")
                .value(10, "c_Uint64", "1")
                .value(11, "c_Float", "1.0")
                .value(12, "c_Double", "1.0")
                .value(13, "c_Bytes", "file:///tmp/report.txt")
                .value(14, "c_Text", "https://ydb.tech")
                .value(15, "c_Json", "{}")
                .value(16, "c_JsonDocument", "{}")
                .value(17, "c_Yson", "1")
                .value(18, "c_Uuid", "00000000-0000-0000-0000-000000000000")
                .value(19, "c_Date", "1970-01-02")
                .value(20, "c_Datetime", "1970-01-01T00:00:01")
                .value(21, "c_Timestamp", "1970-01-01T00:00:00.000001Z")
                .value(22, "c_Interval", "PT0.000001S")
                .value(27, "c_Decimal", "1.000000000")
                .value(28, "c_BigDecimal", "1")
                .value(29, "c_BankDecimal", "1.000000000");

        checker.nextRow()
                .value(1, "key", "5")
                .value(2, "c_Bool", null)
                .value(3, "c_Int8", null)
                .value(4, "c_Int16", null)
                .value(5, "c_Int32", null)
                .value(6, "c_Int64", null)
                .value(7, "c_Uint8", null)
                .value(8, "c_Uint16", null)
                .value(9, "c_Uint32", null)
                .value(10, "c_Uint64", null)
                .value(11, "c_Float", null)
                .value(12, "c_Double", null)
                .value(13, "c_Bytes", null)
                .value(14, "c_Text", null)
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", null)
                .value(18, "c_Uuid", null)
                .value(19, "c_Date", null)
                .value(20, "c_Datetime", null)
                .value(21, "c_Timestamp", null)
                .value(22, "c_Interval", null)
                .value(27, "c_Decimal", null)
                .value(28, "c_BigDecimal", null)
                .value(29, "c_BankDecimal", null);

        checker.assertNoRows();
    }

    @Test
    public void getBoolean() throws SQLException {
        ResultSetChecker<Boolean> checker = check(selectSmall(), ResultSet::getBoolean, ResultSet::getBoolean);

        checker.nextRow()
                .value(1, "key", true)
                .value(2, "c_Bool", true)
                .value(3, "c_Int8", true)
                .value(4, "c_Int16", true)
                .value(5, "c_Int32", true)
                .value(6, "c_Int64", true)
                .value(7, "c_Uint8", true)
                .value(8, "c_Uint16", true)
                .value(9, "c_Uint32", true)
                .value(10, "c_Uint64", true);

        checker.nextRow()
                .value(1, "key", true)
                .value(2, "c_Bool", false)
                .value(3, "c_Int8", true)
                .value(4, "c_Int16", true)
                .value(5, "c_Int32", true)
                .value(6, "c_Int64", true)
                .value(7, "c_Uint8", true)
                .value(8, "c_Uint16", true)
                .value(9, "c_Uint32", true)
                .value(10, "c_Uint64", true);

        checker.nextRow()
                .value(1, "key", true)
                .value(2, "c_Bool", false)
                .value(3, "c_Int8", false)
                .value(4, "c_Int16", false)
                .value(5, "c_Int32", false)
                .value(6, "c_Int64", false)
                .value(7, "c_Uint8", false)
                .value(8, "c_Uint16", false)
                .value(9, "c_Uint32", false)
                .value(10, "c_Uint64", false);

        checker.nextRow()
                .value(1, "key", true)
                .value(3, "c_Int8", true)
                .value(4, "c_Int16", true)
                .value(5, "c_Int32", true)
                .value(6, "c_Int64", true)
                .value(7, "c_Uint8", true)
                .value(8, "c_Uint16", true)
                .value(9, "c_Uint32", true)
                .value(10, "c_Uint64", true);

        checker.nextRow()
                .value(1, "key", true)
                .nullValue(2, "c_Bool", false)
                .nullValue(3, "c_Int8", false)
                .nullValue(4, "c_Int16", false)
                .nullValue(5, "c_Int32", false)
                .nullValue(6, "c_Int64", false)
                .nullValue(7, "c_Uint8", false)
                .nullValue(8, "c_Uint16", false)
                .nullValue(9, "c_Uint32", false)
                .nullValue(10, "c_Uint64", false);

        checker.assertNoRows();
    }

    @Test
    public void getByte() throws SQLException {
        ResultSetChecker<Byte> checker = check(selectSmall(), ResultSet::getByte, ResultSet::getByte);

        checker.nextRow()
                .value(1, "key", (byte) 1)
                .value(2, "c_Bool", (byte) 1)
                .value(3, "c_Int8", (byte) 101)
                .exceptionValue(4, "c_Int16", "Cannot cast [Int16] with value [20001] to [byte]")
                .exceptionValue(5, "c_Int32", "Cannot cast [Int32] with value [2000000001] to [byte]")
                .exceptionValue(6, "c_Int64", "Cannot cast [Int64] with value [2000000000001] to [byte]")
                .value(7, "c_Uint8", (byte) 100)
                .exceptionValue(8, "c_Uint16", "Cannot cast [Uint16] with value [20002] to [byte]")
                .exceptionValue(9, "c_Uint32", "Cannot cast [Uint32] with value [2000000002] to [byte]")
                .exceptionValue(10, "c_Uint64", "Cannot cast [Uint64] with value [2000000000002] to [byte]")
                .exceptionValue(19, "c_Date", "Cannot cast [Date] to [byte]")
                .exceptionValue(20, "c_Datetime", "Cannot cast [Datetime] to [byte]")
                .exceptionValue(21, "c_Timestamp", "Cannot cast [Timestamp] to [byte]")
                .exceptionValue(22, "c_Interval", "Cannot cast [Interval] to [byte]")
                .exceptionValue(23, "c_Date32", "Cannot cast [Date32] to [byte]")
                .exceptionValue(24, "c_Datetime64", "Cannot cast [Datetime64] to [byte]")
                .exceptionValue(25, "c_Timestamp64", "Cannot cast [Timestamp64] to [byte]")
                .exceptionValue(26, "c_Interval64", "Cannot cast [Interval64] to [byte]")
                .exceptionValue(27, "c_Decimal", "Cannot cast [DECIMAL] to [byte]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [DECIMAL] to [byte]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [DECIMAL] to [byte]");

        checker.nextRow()
                .value(1, "key", (byte) 2)
                .value(2, "c_Bool", (byte) 0)
                .value(3, "c_Int8", (byte) (-101))
                .exceptionValue(4, "c_Int16", "Cannot cast [Int16] with value [-20001] to [byte]")
                .exceptionValue(5, "c_Int32", "Cannot cast [Int32] with value [-2000000001] to [byte]")
                .exceptionValue(6, "c_Int64", "Cannot cast [Int64] with value [-2000000000001] to [byte]")
                .exceptionValue(7, "c_Uint8", "Cannot cast [Uint8] with value [200] to [byte]")
                .exceptionValue(8, "c_Uint16", "Cannot cast [Uint16] with value [40002] to [byte]")
                .exceptionValue(9, "c_Uint32", "Cannot cast [Uint32] with value [4000000002] to [byte]")
                .exceptionValue(10, "c_Uint64", "Cannot cast [Uint64] with value [4000000000002] to [byte]")
                .exceptionValue(19, "c_Date", "Cannot cast [Date] to [byte]")
                .exceptionValue(20, "c_Datetime", "Cannot cast [Datetime] to [byte]")
                .exceptionValue(21, "c_Timestamp", "Cannot cast [Timestamp] to [byte]")
                .exceptionValue(22, "c_Interval", "Cannot cast [Interval] to [byte]")
                .exceptionValue(23, "c_Date32", "Cannot cast [Date32] to [byte]")
                .exceptionValue(24, "c_Datetime64", "Cannot cast [Datetime64] to [byte]")
                .exceptionValue(25, "c_Timestamp64", "Cannot cast [Timestamp64] to [byte]")
                .exceptionValue(26, "c_Interval64", "Cannot cast [Interval64] to [byte]")
                .exceptionValue(27, "c_Decimal", "Cannot cast [DECIMAL] to [byte]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [DECIMAL] to [byte]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [DECIMAL] to [byte]");

        checker.nextRow()
                .value(1, "key", (byte) 3)
                .value(2, "c_Bool", (byte) 0)
                .value(3, "c_Int8", (byte) 0)
                .value(4, "c_Int16", (byte) 0)
                .value(5, "c_Int32", (byte) 0)
                .value(6, "c_Int64", (byte) 0)
                .value(7, "c_Uint8", (byte) 0)
                .value(8, "c_Uint16", (byte) 0)
                .value(9, "c_Uint32", (byte) 0)
                .value(10, "c_Uint64", (byte) 0)
                .exceptionValue(19, "c_Date", "Cannot cast [Date] to [byte]")
                .exceptionValue(20, "c_Datetime", "Cannot cast [Datetime] to [byte]")
                .exceptionValue(21, "c_Timestamp", "Cannot cast [Timestamp] to [byte]")
                .exceptionValue(22, "c_Interval", "Cannot cast [Interval] to [byte]")
                .exceptionValue(23, "c_Date32", "Cannot cast [Date32] to [byte]")
                .exceptionValue(24, "c_Datetime64", "Cannot cast [Datetime64] to [byte]")
                .exceptionValue(25, "c_Timestamp64", "Cannot cast [Timestamp64] to [byte]")
                .exceptionValue(26, "c_Interval64", "Cannot cast [Interval64] to [byte]")
                .exceptionValue(27, "c_Decimal", "Cannot cast [DECIMAL] to [byte]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [DECIMAL] to [byte]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [DECIMAL] to [byte]");

        checker.nextRow()
                .value(1, "key", (byte) 4)
                .value(2, "c_Bool", (byte) 1)
                .value(3, "c_Int8", (byte) 1)
                .value(4, "c_Int16", (byte) 1)
                .value(5, "c_Int32", (byte) 1)
                .value(6, "c_Int64", (byte) 1)
                .value(7, "c_Uint8", (byte) 1)
                .value(8, "c_Uint16", (byte) 1)
                .value(9, "c_Uint32", (byte) 1)
                .value(10, "c_Uint64", (byte) 1)
                .exceptionValue(19, "c_Date", "Cannot cast [Date] to [byte]")
                .exceptionValue(20, "c_Datetime", "Cannot cast [Datetime] to [byte]")
                .exceptionValue(21, "c_Timestamp", "Cannot cast [Timestamp] to [byte]")
                .exceptionValue(22, "c_Interval", "Cannot cast [Interval] to [byte]")
                .exceptionValue(23, "c_Date32", "Cannot cast [Date32] to [byte]")
                .exceptionValue(24, "c_Datetime64", "Cannot cast [Datetime64] to [byte]")
                .exceptionValue(25, "c_Timestamp64", "Cannot cast [Timestamp64] to [byte]")
                .exceptionValue(26, "c_Interval64", "Cannot cast [Interval64] to [byte]")
                .exceptionValue(27, "c_Decimal", "Cannot cast [DECIMAL] to [byte]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [DECIMAL] to [byte]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [DECIMAL] to [byte]");

        checker.nextRow()
                .value(1, "key", (byte) 5)
                .nullValue(2, "c_Bool", (byte) 0)
                .nullValue(3, "c_Int8", (byte) 0)
                .nullValue(4, "c_Int16", (byte) 0)
                .nullValue(5, "c_Int32", (byte) 0)
                .nullValue(6, "c_Int64", (byte) 0)
                .nullValue(7, "c_Uint8", (byte) 0)
                .nullValue(8, "c_Uint16", (byte) 0)
                .nullValue(9, "c_Uint32", (byte) 0)
                .nullValue(10, "c_Uint64", (byte) 0)
                .nullValue(11, "c_Float", (byte) 0)
                .nullValue(12, "c_Double", (byte) 0)
                .nullValue(13, "c_Bytes", (byte) 0)
                .nullValue(14, "c_Text", (byte) 0)
                .nullValue(15, "c_Json", (byte) 0)
                .nullValue(16, "c_JsonDocument", (byte) 0)
                .nullValue(17, "c_Yson", (byte) 0)
                .nullValue(18, "c_Uuid", (byte) 0)
                .nullValue(19, "c_Date", (byte) 0)
                .nullValue(20, "c_Datetime", (byte) 0)
                .nullValue(21, "c_Timestamp", (byte) 0)
                .nullValue(22, "c_Interval", (byte) 0)
                .nullValue(23, "c_Date32", (byte) 0)
                .nullValue(24, "c_Datetime64", (byte) 0)
                .nullValue(25, "c_Timestamp64", (byte) 0)
                .nullValue(26, "c_Interval64", (byte) 0)
                .nullValue(27, "c_Decimal", (byte) 0)
                .nullValue(28, "c_BigDecimal", (byte) 0)
                .nullValue(29, "c_BankDecimal", (byte) 0);

        checker.assertNoRows();
    }

    @Test
    public void getShort() throws SQLException {
        ResultSetChecker<Short> checker = check(selectSmall(), ResultSet::getShort, ResultSet::getShort);

        checker.nextRow()
                .value(1, "key", (short) 1)
                .value(2, "c_Bool", (short) 1)
                .value(3, "c_Int8", (short) 101)
                .value(4, "c_Int16", (short) 20001)
                .exceptionValue(5, "c_Int32", "Cannot cast [Int32] with value [2000000001] to [short]")
                .exceptionValue(6, "c_Int64", "Cannot cast [Int64] with value [2000000000001] to [short]")
                .value(7, "c_Uint8", (short) 100)
                .value(8, "c_Uint16", (short) 20002)
                .exceptionValue(9, "c_Uint32", "Cannot cast [Uint32] with value [2000000002] to [short]")
                .exceptionValue(10, "c_Uint64", "Cannot cast [Uint64] with value [2000000000002] to [short]")
                .exceptionValue(27, "c_Decimal", "Cannot cast [DECIMAL] to [short]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [DECIMAL] to [short]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [DECIMAL] to [short]");

        checker.nextRow()
                .value(1, "key", (short) 2)
                .value(2, "c_Bool", (short) 0)
                .value(3, "c_Int8", (short) (-101))
                .value(4, "c_Int16", (short) -20001)
                .exceptionValue(5, "c_Int32", "Cannot cast [Int32] with value [-2000000001] to [short]")
                .exceptionValue(6, "c_Int64", "Cannot cast [Int64] with value [-2000000000001] to [short]")
                .value(7, "c_Uint8", (short) 200)
                .exceptionValue(8, "c_Uint16", "Cannot cast [Uint16] with value [40002] to [short]")
                .exceptionValue(9, "c_Uint32", "Cannot cast [Uint32] with value [4000000002] to [short]")
                .exceptionValue(10, "c_Uint64", "Cannot cast [Uint64] with value [4000000000002] to [short]")
                .exceptionValue(27, "c_Decimal", "Cannot cast [DECIMAL] to [short]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [DECIMAL] to [short]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [DECIMAL] to [short]");


        checker.nextRow()
                .value(1, "key", (short) 3)
                .value(2, "c_Bool", (short) 0)
                .value(3, "c_Int8", (short) 0)
                .value(4, "c_Int16", (short) 0)
                .value(5, "c_Int32", (short) 0)
                .value(6, "c_Int64", (short) 0)
                .value(7, "c_Uint8", (short) 0)
                .value(8, "c_Uint16", (short) 0)
                .value(9, "c_Uint32", (short) 0)
                .value(10, "c_Uint64", (short) 0)
                .exceptionValue(27, "c_Decimal", "Cannot cast [DECIMAL] to [short]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [DECIMAL] to [short]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [DECIMAL] to [short]");

        checker.nextRow()
                .value(1, "key", (short) 4)
                .value(2, "c_Bool", (short) 1)
                .value(3, "c_Int8", (short) 1)
                .value(4, "c_Int16", (short) 1)
                .value(5, "c_Int32", (short) 1)
                .value(6, "c_Int64", (short) 1)
                .value(7, "c_Uint8", (short) 1)
                .value(8, "c_Uint16", (short) 1)
                .value(9, "c_Uint32", (short) 1)
                .value(10, "c_Uint64", (short) 1)
                .exceptionValue(27, "c_Decimal", "Cannot cast [DECIMAL] to [short]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [DECIMAL] to [short]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [DECIMAL] to [short]");

        checker.nextRow()
                .value(1, "key", (short) 5)
                .nullValue(2, "c_Bool", (short) 0)
                .nullValue(3, "c_Int8", (short) 0)
                .nullValue(4, "c_Int16", (short) 0)
                .nullValue(5, "c_Int32", (short) 0)
                .nullValue(6, "c_Int64", (short) 0)
                .nullValue(7, "c_Uint8", (short) 0)
                .nullValue(8, "c_Uint16", (short) 0)
                .nullValue(9, "c_Uint32", (short) 0)
                .nullValue(10, "c_Uint64", (short) 0)
                .nullValue(11, "c_Float", (short) 0)
                .nullValue(12, "c_Double", (short) 0)
                .nullValue(13, "c_Bytes", (short) 0)
                .nullValue(14, "c_Text", (short) 0)
                .nullValue(15, "c_Json", (short) 0)
                .nullValue(16, "c_JsonDocument", (short) 0)
                .nullValue(17, "c_Yson", (short) 0)
                .nullValue(18, "c_Uuid", (short) 0)
                .nullValue(19, "c_Date", (short) 0)
                .nullValue(20, "c_Datetime", (short) 0)
                .nullValue(21, "c_Timestamp", (short) 0)
                .nullValue(22, "c_Interval", (short) 0)
                .nullValue(27, "c_Decimal", (short) 0)
                .nullValue(28, "c_BigDecimal", (short) 0)
                .nullValue(29, "c_BankDecimal", (short) 0);

        checker.assertNoRows();
    }

    @Test
    public void getInt() throws SQLException {
        ResultSetChecker<Integer> checker = check(selectSmall(), ResultSet::getInt, ResultSet::getInt);

        checker.nextRow()
                .value(1, "key", 1)
                .value(2, "c_Bool", 1)
                .value(3, "c_Int8", 101)
                .value(4, "c_Int16", 20001)
                .value(5, "c_Int32", 2000000001)
                .exceptionValue(6, "c_Int64", "Cannot cast [Int64] with value [2000000000001] to [int]")
                .value(7, "c_Uint8", 100)
                .value(8, "c_Uint16", 20002)
                .value(9, "c_Uint32", 2000000002)
                .exceptionValue(10, "c_Uint64", "Cannot cast [Uint64] with value [2000000000002] to [int]")
                .exceptionValue(11, "c_Float", "Cannot cast [Float] to [int]")
                .exceptionValue(12, "c_Double", "Cannot cast [Double] to [int]")
                .exceptionValue(13, "c_Bytes", "Cannot cast [Bytes] to [int]")
                .exceptionValue(14, "c_Text", "Cannot cast [Text] to [int]")
                .exceptionValue(15, "c_Json", "Cannot cast [Json] to [int]")
                .exceptionValue(16, "c_JsonDocument", "Cannot cast [JsonDocument] to [int]")
                .exceptionValue(17, "c_Yson", "Cannot cast [Yson] to [int]")
                .exceptionValue(18, "c_Uuid", "Cannot cast [Uuid] to [int]")
                .value(19, "c_Date", 3111)
                .exceptionValue(20, "c_Datetime", "Cannot cast [Datetime] to [int]")
                .exceptionValue(21, "c_Timestamp", "Cannot cast [Timestamp] to [int]")
                .exceptionValue(22, "c_Interval", "Cannot cast [Interval] to [int]")
                .value(23, "c_Date32", -3111)
                .exceptionValue(24, "c_Datetime64", "Cannot cast [Datetime64] to [int]")
                .exceptionValue(25, "c_Timestamp64", "Cannot cast [Timestamp64] to [int]")
                .exceptionValue(26, "c_Interval64", "Cannot cast [Interval64] to [int]")
                .exceptionValue(27, "c_Decimal", "Cannot cast [Decimal(22, 9)] with value [3.335000000] to [int]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [Decimal(35, 0)] with value [12345678901234567890123456789012345] to [int]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [Decimal(31, 9)] with value [9999999999999999999999.999999999] to [int]");

        checker.nextRow()
                .value(1, "key", 2)
                .value(2, "c_Bool", 0)
                .value(3, "c_Int8", -101)
                .value(4, "c_Int16", -20001)
                .value(5, "c_Int32", -2000000001)
                .exceptionValue(6, "c_Int64", "Cannot cast [Int64] with value [-2000000000001] to [int]")
                .value(7, "c_Uint8", 200)
                .value(8, "c_Uint16", 40002)
                .exceptionValue(9, "c_Uint32", "Cannot cast [Uint32] with value [4000000002] to [int]")
                .exceptionValue(10, "c_Uint64", "Cannot cast [Uint64] with value [4000000000002] to [int]")
                .exceptionValue(11, "c_Float", "Cannot cast [Float] to [int]")
                .exceptionValue(12, "c_Double", "Cannot cast [Double] to [int]")
                .exceptionValue(13, "c_Bytes", "Cannot cast [Bytes] to [int]")
                .exceptionValue(14, "c_Text", "Cannot cast [Text] to [int]")
                .nullValue(15, "c_Json", 0)
                .nullValue(16, "c_JsonDocument", 0)
                .exceptionValue(17, "c_Yson", "Cannot cast [Yson] to [int]")
                .nullValue(18, "c_Uuid", 0)
                .value(19, "c_Date", 3112)
                .exceptionValue(20, "c_Datetime", "Cannot cast [Datetime] to [int]")
                .exceptionValue(21, "c_Timestamp", "Cannot cast [Timestamp] to [int]")
                .exceptionValue(22, "c_Interval", "Cannot cast [Interval] to [int]")
                .value(23, "c_Date32", -3112)
                .exceptionValue(24, "c_Datetime64", "Cannot cast [Datetime64] to [int]")
                .exceptionValue(25, "c_Timestamp64", "Cannot cast [Timestamp64] to [int]")
                .exceptionValue(26, "c_Interval64", "Cannot cast [Interval64] to [int]")
                .exceptionValue(27, "c_Decimal", "Cannot cast [Decimal(22, 9)] with value [-3.335000000] to [int]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [Decimal(35, 0)] with value [-98765432109876543210987654321098765] to [int]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [Decimal(31, 9)] with value [-9999999999999999999999.999999999] to [int]");


        checker.nextRow()
                .value(1, "key", 3)
                .value(2, "c_Bool", 0)
                .value(3, "c_Int8", 0)
                .value(4, "c_Int16", 0)
                .value(5, "c_Int32", 0)
                .value(6, "c_Int64", 0)
                .value(7, "c_Uint8", 0)
                .value(8, "c_Uint16", 0)
                .value(9, "c_Uint32", 0)
                .value(10, "c_Uint64", 0)
                .exceptionValue(11, "c_Float", "Cannot cast [Float] to [int]")
                .exceptionValue(12, "c_Double", "Cannot cast [Double] to [int]")
                .exceptionValue(13, "c_Bytes", "Cannot cast [Bytes] to [int]")
                .exceptionValue(14, "c_Text", "Cannot cast [Text] to [int]")
                .nullValue(15, "c_Json", 0)
                .nullValue(16, "c_JsonDocument", 0)
                .exceptionValue(17, "c_Yson", "Cannot cast [Yson] to [int]")
                .nullValue(18, "c_Uuid", 0)
                .value(19, "c_Date", 0)
                .exceptionValue(20, "c_Datetime", "Cannot cast [Datetime] to [int]")
                .exceptionValue(21, "c_Timestamp", "Cannot cast [Timestamp] to [int]")
                .exceptionValue(22, "c_Interval", "Cannot cast [Interval] to [int]")
                .value(23, "c_Date32", 0)
                .exceptionValue(24, "c_Datetime64", "Cannot cast [Datetime64] to [int]")
                .exceptionValue(25, "c_Timestamp64", "Cannot cast [Timestamp64] to [int]")
                .exceptionValue(26, "c_Interval64", "Cannot cast [Interval64] to [int]")
                .value(27, "c_Decimal", 0)
                .value(28, "c_BigDecimal", 0)
                .value(29, "c_BankDecimal", 0);

        checker.nextRow()
                .value(1, "key", 4)
                .value(2, "c_Bool", 1)
                .value(3, "c_Int8", 1)
                .value(4, "c_Int16", 1)
                .value(5, "c_Int32", 1)
                .value(6, "c_Int64", 1)
                .value(7, "c_Uint8", 1)
                .value(8, "c_Uint16", 1)
                .value(9, "c_Uint32", 1)
                .value(10, "c_Uint64", 1)
                .exceptionValue(11, "c_Float", "Cannot cast [Float] to [int]")
                .exceptionValue(12, "c_Double", "Cannot cast [Double] to [int]")
                .exceptionValue(13, "c_Bytes", "Cannot cast [Bytes] to [int]")
                .exceptionValue(14, "c_Text", "Cannot cast [Text] to [int]")
                .exceptionValue(15, "c_Json", "Cannot cast [Json] to [int]")
                .exceptionValue(16, "c_JsonDocument", "Cannot cast [JsonDocument] to [int]")
                .exceptionValue(17, "c_Yson", "Cannot cast [Yson] to [int]")
                .exceptionValue(18, "c_Uuid", "Cannot cast [Uuid] to [int]")
                .value(19, "c_Date", 1)
                .exceptionValue(20, "c_Datetime", "Cannot cast [Datetime] to [int]")
                .exceptionValue(21, "c_Timestamp", "Cannot cast [Timestamp] to [int]")
                .exceptionValue(22, "c_Interval", "Cannot cast [Interval] to [int]")
                .value(23, "c_Date32", -1)
                .exceptionValue(24, "c_Datetime64", "Cannot cast [Datetime64] to [int]")
                .exceptionValue(25, "c_Timestamp64", "Cannot cast [Timestamp64] to [int]")
                .exceptionValue(26, "c_Interval64", "Cannot cast [Interval64] to [int]")
                .value(27, "c_Decimal", 1)
                .value(28, "c_BigDecimal", 1)
                .value(29, "c_BankDecimal", 1);

        checker.nextRow()
                .value(1, "key", 5)
                .nullValue(2, "c_Bool", 0)
                .nullValue(3, "c_Int8", 0)
                .nullValue(4, "c_Int16", 0)
                .nullValue(5, "c_Int32", 0)
                .nullValue(6, "c_Int64", 0)
                .nullValue(7, "c_Uint8", 0)
                .nullValue(8, "c_Uint16", 0)
                .nullValue(9, "c_Uint32", 0)
                .nullValue(10, "c_Uint64", 0)
                .nullValue(11, "c_Float", 0)
                .nullValue(12, "c_Double", 0)
                .nullValue(13, "c_Bytes", 0)
                .nullValue(14, "c_Text", 0)
                .nullValue(15, "c_Json", 0)
                .nullValue(16, "c_JsonDocument", 0)
                .nullValue(17, "c_Yson", 0)
                .nullValue(18, "c_Uuid", 0)
                .nullValue(19, "c_Date", 0)
                .nullValue(20, "c_Datetime", 0)
                .nullValue(21, "c_Timestamp", 0)
                .nullValue(22, "c_Interval", 0)
                .nullValue(23, "c_Date32", 0)
                .nullValue(24, "c_Datetime64", 0)
                .nullValue(25, "c_Timestamp64", 0)
                .nullValue(26, "c_Interval64", 0)
                .nullValue(27, "c_Decimal", 0)
                .nullValue(28, "c_BigDecimal", 0)
                .nullValue(29, "c_BankDecimal", 0);

        checker.assertNoRows();
    }

    @Test
    public void getLong() throws SQLException {
        ResultSetChecker<Long> checker = check(selectSmall(), ResultSet::getLong, ResultSet::getLong);

        checker.nextRow()
                .value(1, "key", 1L)
                .value(2, "c_Bool", 1L)
                .value(3, "c_Int8", 101L)
                .value(4, "c_Int16", 20001L)
                .value(5, "c_Int32", 2000000001L)
                .value(6, "c_Int64", 2000000000001L)
                .value(7, "c_Uint8", 100L)
                .value(8, "c_Uint16", 20002L)
                .value(9, "c_Uint32", 2000000002L)
                .value(10, "c_Uint64", 2000000000002L)
                .exceptionValue(11, "c_Float", "Cannot cast [Float] to [long]")
                .exceptionValue(12, "c_Double", "Cannot cast [Double] to [long]")
                .exceptionValue(13, "c_Bytes", "Cannot cast [Bytes] to [long]")
                .exceptionValue(14, "c_Text", "Cannot cast [Text] to [long]")
                .exceptionValue(15, "c_Json", "Cannot cast [Json] to [long]")
                .exceptionValue(16, "c_JsonDocument", "Cannot cast [JsonDocument] to [long]")
                .exceptionValue(17, "c_Yson", "Cannot cast [Yson] to [long]")
                .exceptionValue(18, "c_Uuid", "Cannot cast [Uuid] to [long]")
                .value(19, "c_Date", 3111l)
                .value(20, "c_Datetime", 311111156L)
                .value(21, "c_Timestamp", 311111223342L / 1000)
                .value(22, "c_Interval", 3111113L)
                .value(23, "c_Date32", -3111l)
                .value(24, "c_Datetime64", -311111156L)
                .value(25, "c_Timestamp64", -311111224L) // -311111223342L / 1000 = -311111223
                .value(26, "c_Interval64", -3111113L)
                .exceptionValue(27, "c_Decimal", "Cannot cast [Decimal(22, 9)] with value [3.335000000] to [long]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [Decimal(35, 0)] with value [12345678901234567890123456789012345] to [long]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [Decimal(31, 9)] with value [9999999999999999999999.999999999] to [long]");

        checker.nextRow()
                .value(1, "key", 2L)
                .value(2, "c_Bool", 0L)
                .value(3, "c_Int8", -101L)
                .value(4, "c_Int16", -20001L)
                .value(5, "c_Int32", -2000000001L)
                .value(6, "c_Int64", -2000000000001L)
                .value(7, "c_Uint8", 200L)
                .value(8, "c_Uint16", 40002L)
                .value(9, "c_Uint32", 4000000002L)
                .value(10, "c_Uint64", 4000000000002L)
                .exceptionValue(11, "c_Float", "Cannot cast [Float] to [long]")
                .exceptionValue(12, "c_Double", "Cannot cast [Double] to [long]")
                .exceptionValue(13, "c_Bytes", "Cannot cast [Bytes] to [long]")
                .exceptionValue(14, "c_Text", "Cannot cast [Text] to [long]")
                .nullValue(15, "c_Json", 0l)
                .nullValue(16, "c_JsonDocument", 0l)
                .exceptionValue(17, "c_Yson", "Cannot cast [Yson] to [long]")
                .nullValue(18, "c_Uuid", 0l)
                .value(19, "c_Date", 3112l)
                .value(20, "c_Datetime", 211211100L)
                .value(21, "c_Timestamp", 111111223342L / 1000)
                .value(22, "c_Interval", 3112113L)
                .value(23, "c_Date32", -3112l)
                .value(24, "c_Datetime64", -211211100L)
                .value(25, "c_Timestamp64", -111111224L)
                .value(26, "c_Interval64", -3112113L)
                .exceptionValue(27, "c_Decimal", "Cannot cast [Decimal(22, 9)] with value [-3.335000000] to [long]")
                .exceptionValue(28, "c_BigDecimal", "Cannot cast [Decimal(35, 0)] with value [-98765432109876543210987654321098765] to [long]")
                .exceptionValue(29, "c_BankDecimal", "Cannot cast [Decimal(31, 9)] with value [-9999999999999999999999.999999999] to [long]");

        checker.nextRow()
                .value(1, "key", 3l)
                .value(2, "c_Bool", 0l)
                .value(3, "c_Int8", 0l)
                .value(4, "c_Int16", 0l)
                .value(5, "c_Int32", 0l)
                .value(6, "c_Int64", 0l)
                .value(7, "c_Uint8", 0l)
                .value(8, "c_Uint16", 0l)
                .value(9, "c_Uint32", 0l)
                .value(10, "c_Uint64", 0l)
                .exceptionValue(11, "c_Float", "Cannot cast [Float] to [long]")
                .exceptionValue(12, "c_Double", "Cannot cast [Double] to [long]")
                .exceptionValue(13, "c_Bytes", "Cannot cast [Bytes] to [long]")
                .exceptionValue(14, "c_Text", "Cannot cast [Text] to [long]")
                .nullValue(15, "c_Json", 0l)
                .nullValue(16, "c_JsonDocument", 0l)
                .exceptionValue(17, "c_Yson", "Cannot cast [Yson] to [long]")
                .nullValue(18, "c_Uuid", 0l)
                .value(19, "c_Date", 0l)
                .value(20, "c_Datetime", 0l)
                .value(21, "c_Timestamp", 0l)
                .value(22, "c_Interval", 0l)
                .value(23, "c_Date32", 0l)
                .value(24, "c_Datetime64", 0l)
                .value(25, "c_Timestamp64", 0l)
                .value(26, "c_Interval64", 0l)
                .value(27, "c_Decimal", 0l)
                .value(28, "c_BigDecimal", 0l)
                .value(29, "c_BankDecimal", 0l);

        checker.nextRow()
                .value(1, "key", 4L)
                .value(2, "c_Bool", 1L)
                .value(3, "c_Int8", 1L)
                .value(4, "c_Int16", 1L)
                .value(5, "c_Int32", 1L)
                .value(6, "c_Int64", 1L)
                .value(7, "c_Uint8", 1L)
                .value(8, "c_Uint16", 1L)
                .value(9, "c_Uint32", 1L)
                .value(10, "c_Uint64", 1L)
                .exceptionValue(11, "c_Float", "Cannot cast [Float] to [long]")
                .exceptionValue(12, "c_Double", "Cannot cast [Double] to [long]")
                .exceptionValue(13, "c_Bytes", "Cannot cast [Bytes] to [long]")
                .exceptionValue(14, "c_Text", "Cannot cast [Text] to [long]")
                .exceptionValue(15, "c_Json", "Cannot cast [Json] to [long]")
                .exceptionValue(16, "c_JsonDocument", "Cannot cast [JsonDocument] to [long]")
                .exceptionValue(17, "c_Yson", "Cannot cast [Yson] to [long]")
                .exceptionValue(18, "c_Uuid", "Cannot cast [Uuid] to [long]")
                .value(19, "c_Date", 1l)
                .value(20, "c_Datetime", 1l)
                .value(21, "c_Timestamp", 1l / 1000)
                .value(22, "c_Interval", 1l)
                .value(23, "c_Date32", -1l)
                .value(24, "c_Datetime64", -1l)
                .value(25, "c_Timestamp64", -1l)
                .value(26, "c_Interval64", -1l)
                .value(27, "c_Decimal", 1l)
                .value(28, "c_BigDecimal", 1l)
                .value(29, "c_BankDecimal", 1l);

        checker.nextRow()
                .value(1, "key", 5L)
                .nullValue(2, "c_Bool", 0l)
                .nullValue(3, "c_Int8", 0l)
                .nullValue(4, "c_Int16", 0l)
                .nullValue(5, "c_Int32", 0l)
                .nullValue(6, "c_Int64", 0l)
                .nullValue(7, "c_Uint8", 0l)
                .nullValue(8, "c_Uint16", 0l)
                .nullValue(9, "c_Uint32", 0l)
                .nullValue(10, "c_Uint64", 0l)
                .nullValue(11, "c_Float", 0l)
                .nullValue(12, "c_Double", 0l)
                .nullValue(13, "c_Bytes", 0l)
                .nullValue(14, "c_Text", 0l)
                .nullValue(15, "c_Json", 0l)
                .nullValue(16, "c_JsonDocument", 0l)
                .nullValue(17, "c_Yson", 0l)
                .nullValue(18, "c_Uuid", 0l)
                .nullValue(19, "c_Date", 0l)
                .nullValue(20, "c_Datetime", 0l)
                .nullValue(21, "c_Timestamp", 0l)
                .nullValue(22, "c_Interval", 0l)
                .nullValue(23, "c_Date32", 0l)
                .nullValue(24, "c_Datetime64", 0l)
                .nullValue(25, "c_Timestamp64", 0l)
                .nullValue(26, "c_Interval64", 0l)
                .nullValue(27, "c_Decimal", 0l)
                .nullValue(28, "c_BigDecimal", 0l)
                .nullValue(29, "c_BankDecimal", 0l);

        checker.assertNoRows();
    }

    @Test
    public void getFloat() throws SQLException {
        ResultSetChecker<Float> checker = check(selectSmall(), ResultSet::getFloat, ResultSet::getFloat);

        checker.nextRow()
                .value(1, "key", 1f)
                .value(2, "c_Bool", 1f)
                .value(3, "c_Int8", 101f)
                .value(4, "c_Int16", 20001f)
                .value(5, "c_Int32", 2000000001f)
                .value(6, "c_Int64", 2000000000001f)
                .value(7, "c_Uint8", 100f)
                .value(8, "c_Uint16", 20002f)
                .value(9, "c_Uint32", 2000000002f)
                .value(10, "c_Uint64", 2000000000002f)
                .value(11, "c_Float", 123456.78f)
                .value(12, "c_Double", 123456789.123456789f)
                .value(27, "c_Decimal", 3.335f)
                .value(28, "c_BigDecimal", 1.2345679e34f)
                .value(29, "c_BankDecimal", 1e22f);

        checker.nextRow()
                .value(1, "key", 2f)
                .value(2, "c_Bool", 0f)
                .value(3, "c_Int8", -101f)
                .value(4, "c_Int16", -20001f)
                .value(5, "c_Int32", -2000000001f)
                .value(6, "c_Int64", -2000000000001f)
                .value(7, "c_Uint8", 200f)
                .value(8, "c_Uint16", 40002f)
                .value(9, "c_Uint32", 4000000002f)
                .value(10, "c_Uint64", 4000000000002f)
                .value(11, "c_Float", -123456.78f)
                .value(12, "c_Double", -123456789.123456789f)
                .value(27, "c_Decimal", -3.335f)
                .value(28, "c_BigDecimal", -9.8765432e34f)
                .value(29, "c_BankDecimal", -1e22f);

        checker.nextRow()
                .value(1, "key", 3f)
                .value(2, "c_Bool", 0f)
                .value(3, "c_Int8", 0f)
                .value(4, "c_Int16", 0f)
                .value(5, "c_Int32", 0f)
                .value(6, "c_Int64", 0f)
                .value(7, "c_Uint8", 0f)
                .value(8, "c_Uint16", 0f)
                .value(9, "c_Uint32", 0f)
                .value(10, "c_Uint64", 0f)
                .value(11, "c_Float", 0f)
                .value(12, "c_Double", 0f)
                .value(27, "c_Decimal", 0f)
                .value(28, "c_BigDecimal", 0f)
                .value(29, "c_BankDecimal", 0f);

        checker.nextRow()
                .value(1, "key", 4f)
                .value(2, "c_Bool", 1f)
                .value(3, "c_Int8", 1f)
                .value(4, "c_Int16", 1f)
                .value(5, "c_Int32", 1f)
                .value(6, "c_Int64", 1f)
                .value(7, "c_Uint8", 1f)
                .value(8, "c_Uint16", 1f)
                .value(9, "c_Uint32", 1f)
                .value(10, "c_Uint64", 1f)
                .value(11, "c_Float", 1f)
                .value(12, "c_Double", 1f)
                .value(27, "c_Decimal", 1f)
                .value(28, "c_BigDecimal", 1f)
                .value(29, "c_BankDecimal", 1f);

        checker.nextRow()
                .value(1, "key", 5f)
                .nullValue(2, "c_Bool", 0f)
                .nullValue(3, "c_Int8", 0f)
                .nullValue(4, "c_Int16", 0f)
                .nullValue(5, "c_Int32", 0f)
                .nullValue(6, "c_Int64", 0f)
                .nullValue(7, "c_Uint8", 0f)
                .nullValue(8, "c_Uint16", 0f)
                .nullValue(9, "c_Uint32", 0f)
                .nullValue(10, "c_Uint64", 0f)
                .nullValue(11, "c_Float", 0f)
                .nullValue(12, "c_Double", 0f)
                .nullValue(27, "c_Decimal", 0f)
                .nullValue(28, "c_BigDecimal", 0f)
                .nullValue(29, "c_BankDecimal", 0f);

        checker.assertNoRows();
    }

    @Test
    public void getDouble() throws SQLException {
        ResultSetChecker<Double> checker = check(selectSmall(), ResultSet::getDouble, ResultSet::getDouble);

        checker.nextRow()
                .value(1, "key", 1d)
                .value(2, "c_Bool", 1d)
                .value(3, "c_Int8", 101d)
                .value(4, "c_Int16", 20001d)
                .value(5, "c_Int32", 2000000001d)
                .value(6, "c_Int64", 2000000000001d)
                .value(7, "c_Uint8", 100d)
                .value(8, "c_Uint16", 20002d)
                .value(9, "c_Uint32", 2000000002d)
                .value(10, "c_Uint64", 2000000000002d)
                .value(11, "c_Float", 123456.78125d) // TODO: cannot be casted from float without loosing precision
                .value(12, "c_Double", 123456789.123456789d)
                .value(27, "c_Decimal", 3.335d)
                .value(28, "c_BigDecimal", 1.234567890123456789e34d)
                .value(29, "c_BankDecimal", 1e22d);

        checker.nextRow()
                .value(1, "key", 2d)
                .value(2, "c_Bool", 0d)
                .value(3, "c_Int8", -101d)
                .value(4, "c_Int16", -20001d)
                .value(5, "c_Int32", -2000000001d)
                .value(6, "c_Int64", -2000000000001d)
                .value(7, "c_Uint8", 200d)
                .value(8, "c_Uint16", 40002d)
                .value(9, "c_Uint32", 4000000002d)
                .value(10, "c_Uint64", 4000000000002d)
                .value(11, "c_Float", -123456.78125d) // TODO: cannot be casted from float without loosing precision
                .value(12, "c_Double", -123456789.123456789d)
                .value(27, "c_Decimal", -3.335d)
                .value(28, "c_BigDecimal", -9.876543210987654e34d)
                .value(29, "c_BankDecimal", -1e22d);

        checker.nextRow()
                .value(1, "key", 3d)
                .value(2, "c_Bool", 0d)
                .value(3, "c_Int8", 0d)
                .value(4, "c_Int16", 0d)
                .value(5, "c_Int32", 0d)
                .value(6, "c_Int64", 0d)
                .value(7, "c_Uint8", 0d)
                .value(8, "c_Uint16", 0d)
                .value(9, "c_Uint32", 0d)
                .value(10, "c_Uint64", 0d)
                .value(11, "c_Float", 0d)
                .value(12, "c_Double", 0d)
                .value(27, "c_Decimal", 0d)
                .value(28, "c_BigDecimal", 0d)
                .value(29, "c_BankDecimal", 0d);

        checker.nextRow()
                .value(1, "key", 4d)
                .value(2, "c_Bool", 1d)
                .value(3, "c_Int8", 1d)
                .value(4, "c_Int16", 1d)
                .value(5, "c_Int32", 1d)
                .value(6, "c_Int64", 1d)
                .value(7, "c_Uint8", 1d)
                .value(8, "c_Uint16", 1d)
                .value(9, "c_Uint32", 1d)
                .value(10, "c_Uint64", 1d)
                .value(11, "c_Float", 1d)
                .value(12, "c_Double", 1d)
                .value(27, "c_Decimal", 1d)
                .value(28, "c_BigDecimal", 1d)
                .value(29, "c_BankDecimal", 1d);

        checker.nextRow()
                .value(1, "key", 5d)
                .nullValue(2, "c_Bool", 0d)
                .nullValue(3, "c_Int8", 0d)
                .nullValue(4, "c_Int16", 0d)
                .nullValue(5, "c_Int32", 0d)
                .nullValue(6, "c_Int64", 0d)
                .nullValue(7, "c_Uint8", 0d)
                .nullValue(8, "c_Uint16", 0d)
                .nullValue(9, "c_Uint32", 0d)
                .nullValue(10, "c_Uint64", 0d)
                .nullValue(11, "c_Float", 0d)
                .nullValue(12, "c_Double", 0d)
                .nullValue(27, "c_Decimal", 0d)
                .nullValue(28, "c_BigDecimal", 0d)
                .nullValue(29, "c_BankDecimal", 0d);

        checker.assertNoRows();
    }

    @Test
    public void getBigDecimal() throws SQLException {
        ResultSetChecker<BigDecimal> checker = check(selectSmall(), ResultSet::getBigDecimal, ResultSet::getBigDecimal);

        checker.nextRow()
                .value(1, "key", BigDecimal.valueOf(1))
                .value(2, "c_Bool", BigDecimal.valueOf(1))
                .value(3, "c_Int8", BigDecimal.valueOf(101))
                .value(4, "c_Int16", BigDecimal.valueOf(20001))
                .value(5, "c_Int32", BigDecimal.valueOf(2000000001))
                .value(6, "c_Int64", BigDecimal.valueOf(2000000000001l))
                .value(7, "c_Uint8", BigDecimal.valueOf(100))
                .value(8, "c_Uint16", BigDecimal.valueOf(20002))
                .value(9, "c_Uint32", BigDecimal.valueOf(2000000002))
                .value(10, "c_Uint64", BigDecimal.valueOf(2000000000002l))
                .value(11, "c_Float", BigDecimal.valueOf(123456.78125f))
                .value(12, "c_Double", BigDecimal.valueOf(123456789.123456789d))
                .value(27, "c_Decimal", new BigDecimal("3.335000000"))
                .value(28, "c_BigDecimal", new BigDecimal("12345678901234567890123456789012345"))
                .value(29, "c_BankDecimal", new BigDecimal("9999999999999999999999.999999999"));

        checker.nextRow()
                .value(1, "key", BigDecimal.valueOf(2))
                .value(2, "c_Bool", BigDecimal.valueOf(0))
                .value(3, "c_Int8", BigDecimal.valueOf(-101))
                .value(4, "c_Int16", BigDecimal.valueOf(-20001))
                .value(5, "c_Int32", BigDecimal.valueOf(-2000000001))
                .value(6, "c_Int64", BigDecimal.valueOf(-2000000000001l))
                .value(7, "c_Uint8", BigDecimal.valueOf(200))
                .value(8, "c_Uint16", BigDecimal.valueOf(40002))
                .value(9, "c_Uint32", BigDecimal.valueOf(4000000002l))
                .value(10, "c_Uint64", BigDecimal.valueOf(4000000000002l))
                .value(11, "c_Float", BigDecimal.valueOf(-123456.78125f))
                .value(12, "c_Double", BigDecimal.valueOf(-123456789.123456789d))
                .value(27, "c_Decimal", new BigDecimal("-3.335000000"))
                .value(28, "c_BigDecimal", new BigDecimal("-98765432109876543210987654321098765"))
                .value(29, "c_BankDecimal", new BigDecimal("-9999999999999999999999.999999999"));

        checker.nextRow()
                .value(1, "key", BigDecimal.valueOf(3))
                .value(2, "c_Bool", BigDecimal.ZERO)
                .value(3, "c_Int8", BigDecimal.ZERO)
                .value(4, "c_Int16", BigDecimal.ZERO)
                .value(5, "c_Int32", BigDecimal.ZERO)
                .value(6, "c_Int64", BigDecimal.ZERO)
                .value(7, "c_Uint8", BigDecimal.ZERO)
                .value(8, "c_Uint16", BigDecimal.ZERO)
                .value(9, "c_Uint32", BigDecimal.ZERO)
                .value(10, "c_Uint64", BigDecimal.ZERO)
                .value(11, "c_Float", new BigDecimal("0.0"))
                .value(12, "c_Double", new BigDecimal("0.0"))
                .value(27, "c_Decimal", new BigDecimal("0.000000000"))
                .value(28, "c_BigDecimal", new BigDecimal("0"))
                .value(29, "c_BankDecimal", new BigDecimal("0.000000000"));

        checker.nextRow()
                .value(1, "key", BigDecimal.valueOf(4))
                .value(2, "c_Bool", BigDecimal.valueOf(1))
                .value(3, "c_Int8", BigDecimal.valueOf(1))
                .value(4, "c_Int16", BigDecimal.valueOf(1))
                .value(5, "c_Int32", BigDecimal.valueOf(1))
                .value(6, "c_Int64", BigDecimal.valueOf(1))
                .value(7, "c_Uint8", BigDecimal.valueOf(1))
                .value(8, "c_Uint16", BigDecimal.valueOf(1))
                .value(9, "c_Uint32", BigDecimal.valueOf(1))
                .value(10, "c_Uint64", BigDecimal.valueOf(1))
                .value(11, "c_Float", new BigDecimal("1.0"))
                .value(12, "c_Double", new BigDecimal("1.0"))
                .value(27, "c_Decimal", new BigDecimal("1.000000000"))
                .value(28, "c_BigDecimal", new BigDecimal("1"))
                .value(29, "c_BankDecimal", new BigDecimal("1.000000000"));

        checker.nextRow()
                .value(1, "key", BigDecimal.valueOf(5))
                .value(2, "c_Bool", null)
                .value(3, "c_Int8", null)
                .value(4, "c_Int16", null)
                .value(5, "c_Int32", null)
                .value(6, "c_Int64", null)
                .value(7, "c_Uint8", null)
                .value(8, "c_Uint16", null)
                .value(9, "c_Uint32", null)
                .value(10, "c_Uint64", null)
                .value(11, "c_Float", null)
                .value(12, "c_Double", null)
                .value(27, "c_Decimal", null)
                .value(28, "c_BigDecimal", null)
                .value(29, "c_BankDecimal", null);

        checker.assertNoRows();
    }

    private byte[] bytes(String string) {
        return string.getBytes();
    }

    @Test
    public void getBytes() throws SQLException {
        ResultSetChecker<byte[]> checker = check(selectSmall(), ResultSet::getBytes, ResultSet::getBytes);

        checker.nextRow()
                .value(13, "c_Bytes", bytes("bytes array"))
                .value(14, "c_Text", bytes("text text text"))
                .value(15, "c_Json", bytes("{\"key\": \"value Json\"}"))
                .value(16, "c_JsonDocument", bytes("{\"key\":\"value JsonDocument\"}"))
                .value(17, "c_Yson", bytes("{key=\"value yson\"}"));

        checker.nextRow()
                .value(13, "c_Bytes", bytes(""))
                .value(14, "c_Text", bytes(""))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", bytes("\"\""));

        checker.nextRow()
                .value(13, "c_Bytes", bytes("0"))
                .value(14, "c_Text", bytes("0"))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", bytes("0"));

        checker.nextRow()
                .value(13, "c_Bytes", bytes("file:///tmp/report.txt"))
                .value(14, "c_Text", bytes("https://ydb.tech"))
                .value(15, "c_Json", bytes("{}"))
                .value(16, "c_JsonDocument", bytes("{}"))
                .value(17, "c_Yson", bytes("1"));

        checker.nextRow()
                .value(13, "c_Bytes", null)
                .value(14, "c_Text", null)
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", null);

        checker.assertNoRows();
    }

    @Test
    public void getDate() throws SQLException {
        ResultSetChecker<Date> checker = check(selectSmall(), ResultSet::getDate, ResultSet::getDate);

        checker.nextRow()
                .value(3, "c_Int8", Date.valueOf(LocalDate.ofEpochDay(101)))
                .value(4, "c_Int16", Date.valueOf(LocalDate.ofEpochDay(20001)))
                .value(5, "c_Int32", Date.valueOf(LocalDate.ofEpochDay(2000000001)))
                .exceptionValue(6, "c_Int64", "Cannot cast [Int64] with value [2000000000001] to [class java.sql.Date]")
                .value(7, "c_Uint8", Date.valueOf(LocalDate.ofEpochDay(100)))
                .value(8, "c_Uint16", Date.valueOf(LocalDate.ofEpochDay(20002)))
                .value(9, "c_Uint32", Date.valueOf(LocalDate.ofEpochDay(2000000002)))
                .exceptionValue(10, "c_Uint64", "Cannot cast [Uint64] with value [2000000000002] to [class java.sql.Date]")
                // 3111 unix days = Sunday, July 9, 1978 0:00:00 UTC
                .value(19, "c_Date", Date.valueOf(LocalDate.of(1978, Month.JULY, 9)))
                // 311111156 unix seconds = Sat Nov 10 1979 19:45:56 UTC
                .value(20, "c_Datetime", Date.valueOf(LocalDate.of(1979, Month.NOVEMBER, 10)))
                // 311111223342 unix microseconds = Sun Jan 04 1970 14:25:11 UTC
                .value(21, "c_Timestamp", new Date(311111223342L / 1000))
                // -3111 unix days
                .value(23, "c_Date32", Date.valueOf(LocalDate.of(1961, Month.JUNE, 26)))
                // -311111156 unix seconds
                .value(24, "c_Datetime64", Date.valueOf(LocalDate.of(1960, Month.FEBRUARY, 22)))
                // -311111223342 unix microseconds
                .value(25, "c_Timestamp64", new Date(-311111224342L / 1000));

        checker.nextRow()
                .value(3, "c_Int8", Date.valueOf(LocalDate.ofEpochDay(-101)))
                .value(4, "c_Int16", Date.valueOf(LocalDate.ofEpochDay(-20001)))
                .value(5, "c_Int32", Date.valueOf(LocalDate.ofEpochDay(-2000000001)))
                .exceptionValue(6, "c_Int64", "Cannot cast [Int64] with value [-2000000000001] to [class java.sql.Date]")
                .value(7, "c_Uint8", Date.valueOf(LocalDate.ofEpochDay(200)))
                .value(8, "c_Uint16", Date.valueOf(LocalDate.ofEpochDay(40002)))
                .value(9, "c_Uint32", Date.valueOf(LocalDate.ofEpochDay(4000000002l)))
                .exceptionValue(10, "c_Uint64", "Cannot cast [Uint64] with value [4000000000002] to [class java.sql.Date]")
                // 3111 unix days = Sunday, July 9, 1978 0:00:00 UTC
                .value(19, "c_Date", Date.valueOf(LocalDate.of(1978, Month.JULY, 10)))
                // 211211100 unix seconds = Fri Sep 10 1976 13:45:00 UTC
                .value(20, "c_Datetime", Date.valueOf(LocalDate.of(1976, Month.SEPTEMBER, 10)))
                // 111111223342 unix microseconds = Fri Jan 02 1970 06:51:51 UTC
                .value(21, "c_Timestamp", new Date(111111223342l / 1000))
                // -3111 unix days = Monday, June 25, 1961
                .value(23, "c_Date32", Date.valueOf(LocalDate.of(1961, Month.JUNE, 25)))
                // -211211100 unix seconds = Tuesday, April 23, 1963 10:15:00 AM
                .value(24, "c_Datetime64", Date.valueOf(LocalDate.of(1963, Month.APRIL, 23)))
                // -111111223342 unix microseconds
                .value(25, "c_Timestamp64", new Date(-111111224L)); // round up

        checker.nextRow()
                .value(3, "c_Int8", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(4, "c_Int16", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(5, "c_Int32", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(6, "c_Int64", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(7, "c_Uint8", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(8, "c_Uint16", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(9, "c_Uint32", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(10, "c_Uint64", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(19, "c_Date", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(20, "c_Datetime", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(21, "c_Timestamp", new Date(1 / 1000))
                .value(23, "c_Date32", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(24, "c_Datetime64", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(25, "c_Timestamp64", new Date(1 / 1000));

        checker.nextRow()
                .value(3, "c_Int8", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 2)))
                .value(4, "c_Int16", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 2)))
                .value(5, "c_Int32", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 2)))
                .value(6, "c_Int64", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 2)))
                .value(7, "c_Uint8", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 2)))
                .value(8, "c_Uint16", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 2)))
                .value(9, "c_Uint32", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 2)))
                .value(10, "c_Uint64", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 2)))
                .value(19, "c_Date", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 2)))
                .value(20, "c_Datetime", Date.valueOf(LocalDate.of(1970, Month.JANUARY, 1)))
                .value(21, "c_Timestamp", new Date(0))
                .value(23, "c_Date32", Date.valueOf(LocalDate.of(1969, Month.DECEMBER, 31)))
                .value(24, "c_Datetime64", Date.valueOf(LocalDate.of(1969, Month.DECEMBER, 31)))
                .value(25, "c_Timestamp64", new Date(-1));

        checker.nextRow()
                .value(3, "c_Int8", null)
                .value(4, "c_Int16", null)
                .value(5, "c_Int32", null)
                .value(6, "c_Int64", null)
                .value(7, "c_Uint8", null)
                .value(8, "c_Uint16", null)
                .value(9, "c_Uint32", null)
                .value(10, "c_Uint64", null)
                .value(19, "c_Date", null)
                .value(20, "c_Datetime", null)
                .value(21, "c_Timestamp", null)
                .value(23, "c_Date32", null)
                .value(24, "c_Datetime64", null)
                .value(25, "c_Timestamp64", null);

        checker.assertNoRows();
    }

    @Test
    public void getTime() throws SQLException {
        ResultSetChecker<Time> checker = check(selectSmall(), ResultSet::getTime, ResultSet::getTime);

        checker.nextRow()
                .value(3, "c_Int8", Time.valueOf(LocalTime.ofSecondOfDay(101)))
                .value(4, "c_Int16", Time.valueOf(LocalTime.ofSecondOfDay(20001)))
                .exceptionValue(5, "c_Int32", "Cannot cast [Int32] with value [2000000001] to [class java.sql.Time]")
                .exceptionValue(6, "c_Int64", "Cannot cast [Int64] with value [2000000000001] to [class java.sql.Time]")
                .value(7, "c_Uint8", Time.valueOf(LocalTime.ofSecondOfDay(100)))
                .value(8, "c_Uint16", Time.valueOf(LocalTime.ofSecondOfDay(20002)))
                .exceptionValue(9, "c_Uint32", "Cannot cast [Uint32] with value [2000000002] to [class java.sql.Time]")
                .exceptionValue(10, "c_Uint64", "Cannot cast [Uint64] with value [2000000000002] to [class java.sql.Time]")
                // Any value of Date doesn't have a time
                .value(19, "c_Date", Time.valueOf(LocalTime.MIN))
                // 311111156 unix seconds = Sat Nov 10 1979 19:45:56 UTC
                .value(20, "c_Datetime", Time.valueOf(LocalTime.of(19, 45, 56)))
                // 311111223342 unix microseconds = Sun Jan 04 1970 10:25:11 in UTC-04
                .value(21, "c_Timestamp", new Time(311111223342l / 1000))
                // Any value of Date doesn't have a time
                .value(23, "c_Date32", Time.valueOf(LocalTime.MIN))
                // 311111156 unix seconds = Sat Nov 10 1979 19:45:56 UTC
                .value(24, "c_Datetime64", Time.valueOf(LocalTime.of(4, 14, 4)))
                // 311111223342 unix microseconds = Sun Jan 04 1970 10:25:11 in UTC-04
                .value(25, "c_Timestamp64", new Time(-311111224342l / 1000));

        checker.nextRow()
                .exceptionValue(3, "c_Int8", "Cannot cast [Int8] with value [-101] to [class java.sql.Time]")
                .exceptionValue(4, "c_Int16", "Cannot cast [Int16] with value [-20001] to [class java.sql.Time]")
                .exceptionValue(5, "c_Int32", "Cannot cast [Int32] with value [-2000000001] to [class java.sql.Time]")
                .exceptionValue(6, "c_Int64", "Cannot cast [Int64] with value [-2000000000001] to [class java.sql.Time]")
                .value(7, "c_Uint8", Time.valueOf(LocalTime.ofSecondOfDay(200)))
                .value(8, "c_Uint16", Time.valueOf(LocalTime.ofSecondOfDay(40002)))
                .exceptionValue(9, "c_Uint32", "Cannot cast [Uint32] with value [4000000002] to [class java.sql.Time]")
                .exceptionValue(10, "c_Uint64", "Cannot cast [Uint64] with value [4000000000002] to [class java.sql.Time]")
                .value(19, "c_Date", Time.valueOf(LocalTime.MIN))
                // 211211100 unix seconds = Fri Sep 10 1976 13:45:00 UTC
                .value(20, "c_Datetime", Time.valueOf(LocalTime.of(13, 45, 00)))
                // 111111223342 unix microseconds = Fri Jan 02 1970 06:51:51 in UTC-04
                .value(21, "c_Timestamp", new Time(111111223342l / 1000))
                // Any value of Date doesn't have a time
                .value(23, "c_Date32", Time.valueOf(LocalTime.MIN))
                // -211211100 unix seconds = Tuesday, April 23, 1963 10:15:00 UTC
                .value(24, "c_Datetime64", Time.valueOf(LocalTime.of(10, 15, 00)))
                // -111111223342 unix microseconds = Friday, June 24, 1966 11:46:16.658
                .value(25, "c_Timestamp64", new Time(-111111224l));

        checker.nextRow()
                .value(3, "c_Int8", Time.valueOf(LocalTime.MIN))
                .value(4, "c_Int16", Time.valueOf(LocalTime.MIN))
                .value(5, "c_Int32", Time.valueOf(LocalTime.MIN))
                .value(6, "c_Int64", Time.valueOf(LocalTime.MIN))
                .value(7, "c_Uint8", Time.valueOf(LocalTime.MIN))
                .value(8, "c_Uint16", Time.valueOf(LocalTime.MIN))
                .value(9, "c_Uint32", Time.valueOf(LocalTime.MIN))
                .value(10, "c_Uint64", Time.valueOf(LocalTime.MIN))
                .value(19, "c_Date", Time.valueOf(LocalTime.MIN))
                .value(20, "c_Datetime", Time.valueOf(LocalTime.MIN))
                .value(21, "c_Timestamp", new Time(0))
                .value(23, "c_Date32", Time.valueOf(LocalTime.MIN))
                .value(24, "c_Datetime64", Time.valueOf(LocalTime.MIN))
                .value(25, "c_Timestamp64", new Time(0));

        checker.nextRow()
                .value(3, "c_Int8", Time.valueOf(LocalTime.ofSecondOfDay(1)))
                .value(4, "c_Int16", Time.valueOf(LocalTime.ofSecondOfDay(1)))
                .value(5, "c_Int32", Time.valueOf(LocalTime.ofSecondOfDay(1)))
                .value(6, "c_Int64", Time.valueOf(LocalTime.ofSecondOfDay(1)))
                .value(7, "c_Uint8", Time.valueOf(LocalTime.ofSecondOfDay(1)))
                .value(8, "c_Uint16", Time.valueOf(LocalTime.ofSecondOfDay(1)))
                .value(9, "c_Uint32", Time.valueOf(LocalTime.ofSecondOfDay(1)))
                .value(10, "c_Uint64", Time.valueOf(LocalTime.ofSecondOfDay(1)))
                .value(19, "c_Date", Time.valueOf(LocalTime.MIN))
                .value(20, "c_Datetime", Time.valueOf(LocalTime.ofSecondOfDay(1)))
                .value(21, "c_Timestamp", new Time(1 / 1000))
                .value(23, "c_Date32", Time.valueOf(LocalTime.MIN))
                .value(24, "c_Datetime64", Time.valueOf(LocalTime.of(23, 59, 59)))
                .value(25, "c_Timestamp64", new Time(-1));

        checker.nextRow()
                .value(3, "c_Int8", null)
                .value(4, "c_Int16", null)
                .value(5, "c_Int32", null)
                .value(6, "c_Int64", null)
                .value(7, "c_Uint8", null)
                .value(8, "c_Uint16", null)
                .value(9, "c_Uint32", null)
                .value(10, "c_Uint64", null)
                .value(19, "c_Date", null)
                .value(20, "c_Datetime", null)
                .value(21, "c_Timestamp", null)
                .value(23, "c_Date32", null)
                .value(24, "c_Datetime64", null)
                .value(25, "c_Timestamp64", null);

        checker.assertNoRows();
    }

    private Timestamp timestampUTC(long seconds, int nanos) {
        return Timestamp.valueOf(LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC));
    }

    private Timestamp timestampDefault(long seconds, int nanos) {
        return Timestamp.from(Instant.ofEpochSecond(seconds, nanos));
    }

    @Test
    public void getTimestamp() throws SQLException {
        ResultSetChecker<Timestamp> checker = check(selectSmall(), ResultSet::getTimestamp, ResultSet::getTimestamp);

        checker.nextRow()
                .value(3, "c_Int8", new Timestamp(101))
                .value(4, "c_Int16", new Timestamp(20001))
                .value(5, "c_Int32", new Timestamp(2000000001))
                .value(6, "c_Int64", new Timestamp(2000000000001l))
                .value(7, "c_Uint8", new Timestamp(100))
                .value(8, "c_Uint16", new Timestamp(20002))
                .value(9, "c_Uint32", new Timestamp(2000000002))
                .value(10, "c_Uint64", new Timestamp(2000000000002l))
                .value(19, "c_Date", timestampUTC(3111 * 24 * 60 * 60, 0))
                .value(20, "c_Datetime", timestampUTC(311111156, 0))
                .value(21, "c_Timestamp", timestampDefault(311111, 223342000));

        checker.nextRow()
                .value(3, "c_Int8", new Timestamp(-101))
                .value(4, "c_Int16", new Timestamp(-20001))
                .value(5, "c_Int32", new Timestamp(-2000000001))
                .value(6, "c_Int64", new Timestamp(-2000000000001l))
                .value(7, "c_Uint8", new Timestamp(200))
                .value(8, "c_Uint16", new Timestamp(40002))
                .value(9, "c_Uint32", new Timestamp(4000000002l))
                .value(10, "c_Uint64", new Timestamp(4000000000002l))
                .value(19, "c_Date", timestampUTC(3112 * 24 * 60 * 60, 0))
                .value(20, "c_Datetime", timestampUTC(211211100, 0))
                .value(21, "c_Timestamp", timestampDefault(111111, 223342000));

        checker.nextRow()
                .value(3, "c_Int8", new Timestamp(0))
                .value(4, "c_Int16", new Timestamp(0))
                .value(5, "c_Int32", new Timestamp(0))
                .value(6, "c_Int64", new Timestamp(0))
                .value(7, "c_Uint8", new Timestamp(0))
                .value(8, "c_Uint16", new Timestamp(0))
                .value(9, "c_Uint32", new Timestamp(0))
                .value(10, "c_Uint64", new Timestamp(0))
                .value(19, "c_Date", timestampUTC(0, 0))
                .value(20, "c_Datetime", timestampUTC(0, 0))
                .value(21, "c_Timestamp", timestampDefault(0, 0));

        checker.nextRow()
                .value(3, "c_Int8", new Timestamp(1))
                .value(4, "c_Int16", new Timestamp(1))
                .value(5, "c_Int32", new Timestamp(1))
                .value(6, "c_Int64", new Timestamp(1))
                .value(7, "c_Uint8", new Timestamp(1))
                .value(8, "c_Uint16", new Timestamp(1))
                .value(9, "c_Uint32", new Timestamp(1))
                .value(10, "c_Uint64", new Timestamp(1))
                .value(19, "c_Date", timestampUTC(1 * 24 * 60 * 60, 0))
                .value(20, "c_Datetime", timestampUTC(1, 0))
                .value(21, "c_Timestamp", timestampDefault(0, 1000));

        checker.nextRow()
                .value(3, "c_Int8", null)
                .value(4, "c_Int16", null)
                .value(5, "c_Int32", null)
                .value(6, "c_Int64", null)
                .value(7, "c_Uint8", null)
                .value(8, "c_Uint16", null)
                .value(9, "c_Uint32", null)
                .value(10, "c_Uint64", null)
                .value(19, "c_Date", null)
                .value(20, "c_Datetime", null)
                .value(21, "c_Timestamp", null);

        checker.assertNoRows();
    }

    private InputStream stream(String string) {
        return new ByteArrayInputStream(string.getBytes());
    }

    @Test
    public void getUnicodeStream() throws SQLException {
        @SuppressWarnings("deprecation")
        ResultSetChecker<InputStream> checker = check(selectSmall(),
                ResultSet::getUnicodeStream, ResultSet::getUnicodeStream);

        checker.nextRow()
                .value(13, "c_Bytes", stream("bytes array"))
                .value(14, "c_Text", stream("text text text"))
                .value(15, "c_Json", stream("{\"key\": \"value Json\"}"))
                .value(16, "c_JsonDocument", stream("{\"key\":\"value JsonDocument\"}"))
                .value(17, "c_Yson", stream("{key=\"value yson\"}"));

        checker.nextRow()
                .value(13, "c_Bytes", stream(""))
                .value(14, "c_Text", stream(""))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", stream("\"\""));

        checker.nextRow()
                .value(13, "c_Bytes", stream("0"))
                .value(14, "c_Text", stream("0"))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", stream("0"));

        checker.nextRow()
                .value(13, "c_Bytes", stream("file:///tmp/report.txt"))
                .value(14, "c_Text", stream("https://ydb.tech"))
                .value(15, "c_Json", stream("{}"))
                .value(16, "c_JsonDocument", stream("{}"))
                .value(17, "c_Yson", stream("1"));

        checker.nextRow()
                .value(13, "c_Bytes", null)
                .value(14, "c_Text", null)
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", null);

        checker.assertNoRows();
    }

    @Test
    public void getBinaryStream() throws SQLException {
        ResultSetChecker<InputStream> checker = check(selectSmall(),
                ResultSet::getBinaryStream, ResultSet::getBinaryStream);

        checker.nextRow()
                .value(13, "c_Bytes", stream("bytes array"))
                .value(14, "c_Text", stream("text text text"))
                .value(15, "c_Json", stream("{\"key\": \"value Json\"}"))
                .value(16, "c_JsonDocument", stream("{\"key\":\"value JsonDocument\"}"))
                .value(17, "c_Yson", stream("{key=\"value yson\"}"));

        checker.nextRow()
                .value(13, "c_Bytes", stream(""))
                .value(14, "c_Text", stream(""))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", stream("\"\""));

        checker.nextRow()
                .value(13, "c_Bytes", stream("0"))
                .value(14, "c_Text", stream("0"))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", stream("0"));

        checker.nextRow()
                .value(13, "c_Bytes", stream("file:///tmp/report.txt"))
                .value(14, "c_Text", stream("https://ydb.tech"))
                .value(15, "c_Json", stream("{}"))
                .value(16, "c_JsonDocument", stream("{}"))
                .value(17, "c_Yson", stream("1"));

        checker.nextRow()
                .value(13, "c_Bytes", null)
                .value(14, "c_Text", null)
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", null);

        checker.assertNoRows();
    }

    @Test
    public void getObject() throws SQLException {
        ResultSetChecker<Object> checker = check(selectSmall(), ResultSet::getObject, ResultSet::getObject);

        checker.nextRow()
                .typedValue(1, "key", 1)
                .typedValue(2, "c_Bool", true)
                .typedValue(3, "c_Int8", (byte) 101)
                .typedValue(4, "c_Int16", (short) 20001)
                .typedValue(5, "c_Int32", 2000000001)
                .typedValue(6, "c_Int64", 2000000000001l)
                .typedValue(7, "c_Uint8", 100)
                .typedValue(8, "c_Uint16", 20002)
                .typedValue(9, "c_Uint32", 2000000002l)
                .typedValue(10, "c_Uint64", 2000000000002l)
                .typedValue(11, "c_Float", 123456.78f)
                .typedValue(12, "c_Double", 1.2345678912345679E8d)
                .typedValue(13, "c_Bytes", bytes("bytes array"))
                .typedValue(14, "c_Text", "text text text")
                .typedValue(15, "c_Json", "{\"key\": \"value Json\"}")
                .typedValue(16, "c_JsonDocument", "{\"key\":\"value JsonDocument\"}")
                .typedValue(17, "c_Yson", bytes("{key=\"value yson\"}"))
                .typedValue(18, "c_Uuid", UUID.fromString("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"))
                .typedValue(19, "c_Date", LocalDate.ofEpochDay(3111))
                .typedValue(20, "c_Datetime", LocalDateTime.ofEpochSecond(311111156, 0, ZoneOffset.UTC))
                .typedValue(21, "c_Timestamp", Instant.ofEpochSecond(311111, 223342000))
                .typedValue(22, "c_Interval", Duration.parse("PT3.111113S"))
                .typedValue(23, "c_Date32", LocalDate.ofEpochDay(-3111))
                .typedValue(24, "c_Datetime64", LocalDateTime.ofEpochSecond(-311111156, 0, ZoneOffset.UTC))
                .typedValue(25, "c_Timestamp64", Instant.ofEpochSecond(-311111, -223342000))
                .typedValue(26, "c_Interval64", Duration.parse("PT-3.111113S"))
                .typedValue(27, "c_Decimal", new BigDecimal("3.335000000"))
                .typedValue(28, "c_BigDecimal", new BigDecimal("12345678901234567890123456789012345"))
                .typedValue(29, "c_BankDecimal", new BigDecimal("9999999999999999999999.999999999"));

        checker.nextRow()
                .typedValue(1, "key", 2)
                .typedValue(2, "c_Bool", false)
                .typedValue(3, "c_Int8", (byte) -101)
                .typedValue(4, "c_Int16", (short) -20001)
                .typedValue(5, "c_Int32", -2000000001)
                .typedValue(6, "c_Int64", -2000000000001l)
                .typedValue(7, "c_Uint8", 200)
                .typedValue(8, "c_Uint16", 40002)
                .typedValue(9, "c_Uint32", 4000000002l)
                .typedValue(10, "c_Uint64", 4000000000002l)
                .typedValue(11, "c_Float", -123456.78f)
                .typedValue(12, "c_Double", -1.2345678912345679E8d)
                .typedValue(13, "c_Bytes", bytes(""))
                .typedValue(14, "c_Text", "")
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .typedValue(17, "c_Yson", bytes("\"\""))
                .value(18, "c_Uuid", null)
                .typedValue(19, "c_Date", LocalDate.ofEpochDay(3112))
                .typedValue(20, "c_Datetime", LocalDateTime.ofEpochSecond(211211100, 0, ZoneOffset.UTC))
                .typedValue(21, "c_Timestamp", Instant.ofEpochSecond(111111, 223342000))
                .typedValue(22, "c_Interval", Duration.parse("PT3.112113S"))
                .typedValue(23, "c_Date32", LocalDate.ofEpochDay(-3112))
                .typedValue(24, "c_Datetime64", LocalDateTime.ofEpochSecond(-211211100, 0, ZoneOffset.UTC))
                .typedValue(25, "c_Timestamp64", Instant.ofEpochSecond(-111111, -223342000))
                .typedValue(26, "c_Interval64", Duration.parse("PT-3.112113S"))
                .typedValue(27, "c_Decimal", new BigDecimal("-3.335000000"))
                .typedValue(28, "c_BigDecimal", new BigDecimal("-98765432109876543210987654321098765"))
                .typedValue(29, "c_BankDecimal", new BigDecimal("-9999999999999999999999.999999999"));

        checker.nextRow()
                .typedValue(1, "key", 3)
                .typedValue(2, "c_Bool", false)
                .typedValue(3, "c_Int8", (byte) 0)
                .typedValue(4, "c_Int16", (short) 0)
                .typedValue(5, "c_Int32", 0)
                .typedValue(6, "c_Int64", 0l)
                .typedValue(7, "c_Uint8", 0)
                .typedValue(8, "c_Uint16", 0)
                .typedValue(9, "c_Uint32", 0l)
                .typedValue(10, "c_Uint64", 0l)
                .typedValue(11, "c_Float", 0.0f)
                .typedValue(12, "c_Double", 0.0d)
                .typedValue(13, "c_Bytes", bytes("0"))
                .typedValue(14, "c_Text", "0")
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .typedValue(17, "c_Yson", bytes("0"))
                .value(18, "c_Uuid", null)
                .typedValue(19, "c_Date", LocalDate.ofEpochDay(0))
                .typedValue(20, "c_Datetime", LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC))
                .typedValue(21, "c_Timestamp", Instant.ofEpochSecond(0, 0))
                .typedValue(22, "c_Interval", Duration.parse("PT0.000000S"))
                .typedValue(23, "c_Date32", LocalDate.ofEpochDay(0))
                .typedValue(24, "c_Datetime64", LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC))
                .typedValue(25, "c_Timestamp64", Instant.ofEpochSecond(0, 0))
                .typedValue(26, "c_Interval64", Duration.parse("PT0.000000S"))
                .typedValue(27, "c_Decimal", new BigDecimal("0.000000000"))
                .typedValue(28, "c_BigDecimal", new BigDecimal("0"))
                .typedValue(29, "c_BankDecimal", new BigDecimal("0.000000000"));

        checker.nextRow()
                .typedValue(1, "key", 4)
                .typedValue(2, "c_Bool", true)
                .typedValue(3, "c_Int8", (byte) 1)
                .typedValue(4, "c_Int16", (short) 1)
                .typedValue(5, "c_Int32", 1)
                .typedValue(6, "c_Int64", 1l)
                .typedValue(7, "c_Uint8", 1)
                .typedValue(8, "c_Uint16", 1)
                .typedValue(9, "c_Uint32", 1l)
                .typedValue(10, "c_Uint64", 1l)
                .typedValue(11, "c_Float", 1.0f)
                .typedValue(12, "c_Double", 1.0d)
                .typedValue(13, "c_Bytes", bytes("file:///tmp/report.txt"))
                .typedValue(14, "c_Text", "https://ydb.tech")
                .typedValue(15, "c_Json", "{}")
                .typedValue(16, "c_JsonDocument", "{}")
                .typedValue(17, "c_Yson", bytes("1"))
                .typedValue(18, "c_Uuid", UUID.fromString("00000000-0000-0000-0000-000000000000"))
                .typedValue(19, "c_Date", LocalDate.ofEpochDay(1))
                .typedValue(20, "c_Datetime", LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC))
                .typedValue(21, "c_Timestamp", Instant.ofEpochSecond(0, 1000))
                .typedValue(22, "c_Interval", Duration.parse("PT0.000001S"))
                .typedValue(23, "c_Date32", LocalDate.ofEpochDay(-1))
                .typedValue(24, "c_Datetime64", LocalDateTime.ofEpochSecond(-1, 0, ZoneOffset.UTC))
                .typedValue(25, "c_Timestamp64", Instant.ofEpochSecond(0, -1000))
                .typedValue(26, "c_Interval64", Duration.parse("PT0.000001S").negated())
                .typedValue(27, "c_Decimal", new BigDecimal("1.000000000"))
                .typedValue(28, "c_BigDecimal", new BigDecimal("1"))
                .typedValue(29, "c_BankDecimal", new BigDecimal("1.000000000"));

        checker.nextRow()
                .value(1, "key", 5)
                .value(2, "c_Bool", null)
                .value(3, "c_Int8", null)
                .value(4, "c_Int16", null)
                .value(5, "c_Int32", null)
                .value(6, "c_Int64", null)
                .value(7, "c_Uint8", null)
                .value(8, "c_Uint16", null)
                .value(9, "c_Uint32", null)
                .value(10, "c_Uint64", null)
                .value(11, "c_Float", null)
                .value(12, "c_Double", null)
                .value(13, "c_Bytes", null)
                .value(14, "c_Text", null)
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", null)
                .value(18, "c_Uuid", null)
                .value(19, "c_Date", null)
                .value(20, "c_Datetime", null)
                .value(21, "c_Timestamp", null)
                .value(22, "c_Interval", null)
                .value(23, "c_Date32", null)
                .value(24, "c_Datetime64", null)
                .value(25, "c_Timestamp64", null)
                .value(26, "c_Interval64", null)
                .value(27, "c_Decimal", null)
                .value(28, "c_BigDecimal", null)
                .value(29, "c_BankDecimal", null);

        checker.assertNoRows();
    }

    private Reader reader(String string) {
        return new StringReader(string);
    }

    @Test
    public void getCharacterStream() throws SQLException {
        ResultSetChecker<Reader> checker = check(selectSmall(), ResultSet::getCharacterStream, ResultSet::getCharacterStream);

        checker.nextRow()
                .value(13, "c_Bytes", reader("bytes array"))
                .value(14, "c_Text", reader("text text text"))
                .value(15, "c_Json", reader("{\"key\": \"value Json\"}"))
                .value(16, "c_JsonDocument", reader("{\"key\":\"value JsonDocument\"}"))
                .value(17, "c_Yson", reader("{key=\"value yson\"}"));

        checker.nextRow()
                .value(13, "c_Bytes", reader(""))
                .value(14, "c_Text", reader(""))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", reader("\"\""));

        checker.nextRow()
                .value(13, "c_Bytes", reader("0"))
                .value(14, "c_Text", reader("0"))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", reader("0"));

        checker.nextRow()
                .value(13, "c_Bytes", reader("file:///tmp/report.txt"))
                .value(14, "c_Text", reader("https://ydb.tech"))
                .value(15, "c_Json", reader("{}"))
                .value(16, "c_JsonDocument", reader("{}"))
                .value(17, "c_Yson", reader("1"));

        checker.nextRow()
                .value(13, "c_Bytes", null)
                .value(14, "c_Text", null)
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", null);

        checker.assertNoRows();
    }

    @Test
    public void getURL() throws MalformedURLException, SQLException {
        ResultSetChecker<URL> checker = check(selectSmall(), ResultSet::getURL, ResultSet::getURL);

        checker.nextRow();
        checker.nextRow();
        checker.nextRow();
        checker.nextRow()
                .value(13, "c_Bytes", new URL("file:///tmp/report.txt"))
                .value(14, "c_Text", new URL("https://ydb.tech"));
        checker.nextRow()
                .value(13, "c_Bytes", null)
                .value(14, "c_Text", null);

        checker.assertNoRows();
    }

    @Test
    public void getNString() throws SQLException {
        ResultSetChecker<String> checker = check(selectSmall(), ResultSet::getNString, ResultSet::getNString);

        checker.nextRow()
                .value(13, "c_Bytes", "bytes array")
                .value(14, "c_Text", "text text text")
                .value(15, "c_Json", "{\"key\": \"value Json\"}")
                .value(16, "c_JsonDocument", "{\"key\":\"value JsonDocument\"}")
                .value(17, "c_Yson", "{key=\"value yson\"}");

        checker.nextRow()
                .value(13, "c_Bytes", "")
                .value(14, "c_Text", "")
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", "\"\"");

        checker.nextRow()
                .value(13, "c_Bytes", "0")
                .value(14, "c_Text", "0")
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", "0");

        checker.nextRow()
                .value(13, "c_Bytes", "file:///tmp/report.txt")
                .value(14, "c_Text", "https://ydb.tech")
                .value(15, "c_Json", "{}")
                .value(16, "c_JsonDocument", "{}")
                .value(17, "c_Yson", "1");

        checker.nextRow()
                .value(13, "c_Bytes", null)
                .value(14, "c_Text", null)
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", null);

        checker.assertNoRows();
    }

    @Test
    public void getNCharacterStream() throws SQLException {
        ResultSetChecker<Reader> checker = check(selectSmall(), ResultSet::getNCharacterStream, ResultSet::getNCharacterStream);

        checker.nextRow()
                .value(13, "c_Bytes", reader("bytes array"))
                .value(14, "c_Text", reader("text text text"))
                .value(15, "c_Json", reader("{\"key\": \"value Json\"}"))
                .value(16, "c_JsonDocument", reader("{\"key\":\"value JsonDocument\"}"))
                .value(17, "c_Yson", reader("{key=\"value yson\"}"));

        checker.nextRow()
                .value(13, "c_Bytes", reader(""))
                .value(14, "c_Text", reader(""))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", reader("\"\""));

        checker.nextRow()
                .value(13, "c_Bytes", reader("0"))
                .value(14, "c_Text", reader("0"))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", reader("0"));

        checker.nextRow()
                .value(13, "c_Bytes", reader("file:///tmp/report.txt"))
                .value(14, "c_Text", reader("https://ydb.tech"))
                .value(15, "c_Json", reader("{}"))
                .value(16, "c_JsonDocument", reader("{}"))
                .value(17, "c_Yson", reader("1"));

        checker.nextRow()
                .value(13, "c_Bytes", null)
                .value(14, "c_Text", null)
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", null);

        checker.assertNoRows();
    }

    @Test
    public void unsupportedGetters() throws SQLException {
        ResultSet resultSet = selectSmall();
        // getObject with type
//        ExceptionAssert.sqlFeatureNotSupported("Object with type conversion is not supported yet",
//                () -> resultSet.getObject(1, Integer.class));
//        ExceptionAssert.sqlFeatureNotSupported("Object with type conversion is not supported yet",
//                () -> resultSet.getObject("Column", Integer.class));

        // getObject with type map
        ExceptionAssert.sqlFeatureNotSupported("Object with type conversion is not supported yet",
                () -> resultSet.getObject(1, Collections.emptyMap()));
        ExceptionAssert.sqlFeatureNotSupported("Object with type conversion is not supported yet",
                () -> resultSet.getObject("Column", Collections.emptyMap()));

        // getAsciiStream
        ExceptionAssert.sqlFeatureNotSupported("AsciiStreams are not supported", () -> resultSet.getAsciiStream(1));
        ExceptionAssert.sqlFeatureNotSupported("AsciiStreams are not supported", () -> resultSet.getAsciiStream("ss"));

        // getRef
        ExceptionAssert.sqlFeatureNotSupported("Refs are not supported", () -> resultSet.getRef(1));
        ExceptionAssert.sqlFeatureNotSupported("Refs are not supported", () -> resultSet.getRef("ref"));

        // getBlob
        ExceptionAssert.sqlFeatureNotSupported("Blobs are not supported", () -> resultSet.getBlob(1));
        ExceptionAssert.sqlFeatureNotSupported("Blobs are not supported", () -> resultSet.getBlob("blob"));

        // getClob
        ExceptionAssert.sqlFeatureNotSupported("Clobs are not supported", () -> resultSet.getClob(1));
        ExceptionAssert.sqlFeatureNotSupported("Clobs are not supported", () -> resultSet.getClob("clob"));

        // getNClob
        ExceptionAssert.sqlFeatureNotSupported("NClobs are not supported", () -> resultSet.getNClob(1));
        ExceptionAssert.sqlFeatureNotSupported("NClobs are not supported", () -> resultSet.getNClob("nclob"));

        // getArray
        ExceptionAssert.sqlFeatureNotSupported("Arrays are not supported", () -> resultSet.getArray(1));
        ExceptionAssert.sqlFeatureNotSupported("Arrays are not supported", () -> resultSet.getArray("array"));

        // getSQLXML
        ExceptionAssert.sqlFeatureNotSupported("SQLXMLs are not supported", () -> resultSet.getSQLXML(1));
        ExceptionAssert.sqlFeatureNotSupported("SQLXMLs are not supported", () -> resultSet.getSQLXML("sqlxml"));
    }


    @Test
    public void getNativeColumn() throws SQLException {
        ResultSetChecker<Value<?>> checker = check(selectSmall(),
                (rs, index) -> rs.unwrap(YdbResultSet.class).getNativeColumn(index),
                (rs, label) -> rs.unwrap(YdbResultSet.class).getNativeColumn(label)
        );

        checker.nextRow()
                .typedValue(1, "key", PrimitiveValue.newInt32(1))
                .typedValue(2, "c_Bool", PrimitiveValue.newBool(true))
                .typedValue(3, "c_Int8", PrimitiveValue.newInt8((byte) 101))
                .typedValue(4, "c_Int16", PrimitiveValue.newInt16((short) 20001))
                .typedValue(5, "c_Int32", PrimitiveValue.newInt32(2000000001))
                .typedValue(6, "c_Int64", PrimitiveValue.newInt64(2000000000001L))
                .typedValue(7, "c_Uint8", PrimitiveValue.newUint8((short) 100))
                .typedValue(8, "c_Uint16", PrimitiveValue.newUint16(20002))
                .typedValue(9, "c_Uint32", PrimitiveValue.newUint32(2000000002))
                .typedValue(10, "c_Uint64", PrimitiveValue.newUint64(2000000000002L))
                .typedValue(11, "c_Float", PrimitiveValue.newFloat(123456.78f))
                .typedValue(12, "c_Double", PrimitiveValue.newDouble(123456789.123456789d))
                .typedValue(13, "c_Bytes", PrimitiveValue.newBytes(bytes("bytes array")))
                .typedValue(14, "c_Text", PrimitiveValue.newText("text text text"))
                .typedValue(15, "c_Json", PrimitiveValue.newJson("{\"key\": \"value Json\"}"))
                .typedValue(16, "c_JsonDocument", PrimitiveValue.newJsonDocument("{\"key\":\"value JsonDocument\"}"))
                .typedValue(17, "c_Yson", PrimitiveValue.newYson(bytes("{key=\"value yson\"}")))
                .typedValue(18, "c_Uuid", PrimitiveValue.newUuid("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"))
                .typedValue(19, "c_Date", PrimitiveValue.newDate(LocalDate.ofEpochDay(3111)))
                .typedValue(20, "c_Datetime", PrimitiveValue.newDatetime(Instant.ofEpochSecond(311111156)))
                .typedValue(21, "c_Timestamp", PrimitiveValue.newTimestamp(Instant.ofEpochSecond(311111, 223342000)))
                .typedValue(22, "c_Interval", PrimitiveValue.newInterval(Duration.parse("PT3.111113S")))
                .typedValue(23, "c_Date32", PrimitiveValue.newDate32(LocalDate.ofEpochDay(-3111)))
                .typedValue(24, "c_Datetime64", PrimitiveValue.newDatetime64(Instant.ofEpochSecond(-311111156)))
                .typedValue(25, "c_Timestamp64", PrimitiveValue.newTimestamp64(Instant.ofEpochSecond(-311111, -223342000)))
                .typedValue(26, "c_Interval64", PrimitiveValue.newInterval(Duration.parse("PT-3.111113S")))
                .typedValue(27, "c_Decimal", DecimalType.getDefault().newValue("3.335000000"))
                .typedValue(28, "c_BigDecimal", DecimalType.of(35, 0).newValue("12345678901234567890123456789012345"))
                .typedValue(29, "c_BankDecimal", DecimalType.of(31, 9).newValue("9999999999999999999999.999999999"));

        checker.nextRow()
                .typedValue(1, "key", PrimitiveValue.newInt32(2))
                .typedValue(2, "c_Bool", PrimitiveValue.newBool(false))
                .typedValue(3, "c_Int8", PrimitiveValue.newInt8((byte) -101))
                .typedValue(4, "c_Int16", PrimitiveValue.newInt16((short) -20001))
                .typedValue(5, "c_Int32", PrimitiveValue.newInt32(-2000000001))
                .typedValue(6, "c_Int64", PrimitiveValue.newInt64(-2000000000001L))
                .typedValue(7, "c_Uint8", PrimitiveValue.newUint8((short) 200))
                .typedValue(8, "c_Uint16", PrimitiveValue.newUint16(40002))
                .typedValue(9, "c_Uint32", PrimitiveValue.newUint32(4000000002l))
                .typedValue(10, "c_Uint64", PrimitiveValue.newUint64(4000000000002L))
                .typedValue(11, "c_Float", PrimitiveValue.newFloat(-123456.78f))
                .typedValue(12, "c_Double", PrimitiveValue.newDouble(-123456789.123456789d))
                .typedValue(13, "c_Bytes", PrimitiveValue.newBytes(bytes("")))
                .typedValue(14, "c_Text", PrimitiveValue.newText(""))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .typedValue(17, "c_Yson", PrimitiveValue.newYson(bytes("\"\"")))
                .value(18, "c_Uuid", null)
                .typedValue(19, "c_Date", PrimitiveValue.newDate(LocalDate.ofEpochDay(3112)))
                .typedValue(20, "c_Datetime", PrimitiveValue.newDatetime(Instant.ofEpochSecond(211211100)))
                .typedValue(21, "c_Timestamp", PrimitiveValue.newTimestamp(Instant.ofEpochSecond(111111, 223342000)))
                .typedValue(22, "c_Interval", PrimitiveValue.newInterval(Duration.parse("PT3.112113S")))
                .typedValue(23, "c_Date32", PrimitiveValue.newDate32(LocalDate.ofEpochDay(-3112)))
                .typedValue(24, "c_Datetime64", PrimitiveValue.newDatetime64(Instant.ofEpochSecond(-211211100)))
                .typedValue(25, "c_Timestamp64", PrimitiveValue.newTimestamp64(Instant.ofEpochSecond(-111111, -223342000)))
                .typedValue(26, "c_Interval64", PrimitiveValue.newInterval64(Duration.parse("PT-3.112113S")))
                .typedValue(27, "c_Decimal", DecimalType.getDefault().newValue("-3.335000000"))
                .typedValue(28, "c_BigDecimal", DecimalType.of(35, 0).newValue("-98765432109876543210987654321098765"))
                .typedValue(29, "c_BankDecimal", DecimalType.of(31, 9).newValue("-9999999999999999999999.999999999"));

        checker.nextRow()
                .typedValue(1, "key", PrimitiveValue.newInt32(3))
                .typedValue(2, "c_Bool", PrimitiveValue.newBool(false))
                .typedValue(3, "c_Int8", PrimitiveValue.newInt8((byte) 0))
                .typedValue(4, "c_Int16", PrimitiveValue.newInt16((short) 0))
                .typedValue(5, "c_Int32", PrimitiveValue.newInt32(0))
                .typedValue(6, "c_Int64", PrimitiveValue.newInt64(0))
                .typedValue(7, "c_Uint8", PrimitiveValue.newUint8((short) 0))
                .typedValue(8, "c_Uint16", PrimitiveValue.newUint16(0))
                .typedValue(9, "c_Uint32", PrimitiveValue.newUint32(0))
                .typedValue(10, "c_Uint64", PrimitiveValue.newUint64(0))
                .typedValue(11, "c_Float", PrimitiveValue.newFloat(0))
                .typedValue(12, "c_Double", PrimitiveValue.newDouble(0))
                .typedValue(13, "c_Bytes", PrimitiveValue.newBytes(bytes("0")))
                .typedValue(14, "c_Text", PrimitiveValue.newText("0"))
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .typedValue(17, "c_Yson", PrimitiveValue.newYson(bytes("0")))
                .value(18, "c_Uuid", null)
                .typedValue(19, "c_Date", PrimitiveValue.newDate(LocalDate.parse("1970-01-01")))
                .typedValue(20, "c_Datetime", PrimitiveValue.newDatetime(Instant.parse("1970-01-01T00:00:00Z")))
                .typedValue(21, "c_Timestamp", PrimitiveValue.newTimestamp(Instant.ofEpochMilli(0)))
                .typedValue(22, "c_Interval", PrimitiveValue.newInterval(Duration.parse("PT0S")))
                .typedValue(23, "c_Date32", PrimitiveValue.newDate32(LocalDate.parse("1970-01-01")))
                .typedValue(24, "c_Datetime64", PrimitiveValue.newDatetime64(Instant.parse("1970-01-01T00:00:00Z")))
                .typedValue(25, "c_Timestamp64", PrimitiveValue.newTimestamp64(Instant.ofEpochMilli(0)))
                .typedValue(26, "c_Interval64", PrimitiveValue.newInterval64(Duration.parse("PT0S")))
                .typedValue(27, "c_Decimal", DecimalType.getDefault().newValue(0))
                .typedValue(28, "c_BigDecimal", DecimalType.of(35, 0).newValue(0))
                .typedValue(29, "c_BankDecimal", DecimalType.of(31, 9).newValue(0));

        checker.nextRow()
                .typedValue(1, "key", PrimitiveValue.newInt32(4))
                .typedValue(2, "c_Bool", PrimitiveValue.newBool(true))
                .typedValue(3, "c_Int8", PrimitiveValue.newInt8((byte) 1))
                .typedValue(4, "c_Int16", PrimitiveValue.newInt16((short) 1))
                .typedValue(5, "c_Int32", PrimitiveValue.newInt32(1))
                .typedValue(6, "c_Int64", PrimitiveValue.newInt64(1))
                .typedValue(7, "c_Uint8", PrimitiveValue.newUint8((short) 1))
                .typedValue(8, "c_Uint16", PrimitiveValue.newUint16(1))
                .typedValue(9, "c_Uint32", PrimitiveValue.newUint32(1))
                .typedValue(10, "c_Uint64", PrimitiveValue.newUint64(1))
                .typedValue(11, "c_Float", PrimitiveValue.newFloat(1))
                .typedValue(12, "c_Double", PrimitiveValue.newDouble(1))
                .typedValue(13, "c_Bytes", PrimitiveValue.newBytes(bytes("file:///tmp/report.txt")))
                .typedValue(14, "c_Text", PrimitiveValue.newText("https://ydb.tech"))
                .typedValue(15, "c_Json", PrimitiveValue.newJson("{}"))
                .typedValue(16, "c_JsonDocument", PrimitiveValue.newJsonDocument("{}"))
                .typedValue(17, "c_Yson", PrimitiveValue.newYson(bytes("1")))
                .typedValue(18, "c_Uuid", PrimitiveValue.newUuid("00000000-0000-0000-0000-000000000000"))
                .typedValue(19, "c_Date", PrimitiveValue.newDate(LocalDate.parse("1970-01-02")))
                .typedValue(20, "c_Datetime", PrimitiveValue.newDatetime(Instant.parse("1970-01-01T00:00:01Z")))
                .typedValue(21, "c_Timestamp", PrimitiveValue.newTimestamp(1))
                .typedValue(22, "c_Interval", PrimitiveValue.newInterval(Duration.parse("PT0.000001S")))
                .typedValue(23, "c_Date32", PrimitiveValue.newDate32(LocalDate.parse("1969-12-31")))
                .typedValue(24, "c_Datetime64", PrimitiveValue.newDatetime64(Instant.parse("1969-12-31T23:59:59Z")))
                .typedValue(25, "c_Timestamp64", PrimitiveValue.newTimestamp64(-1))
                .typedValue(26, "c_Interval64", PrimitiveValue.newInterval64(Duration.parse("PT0.000001S").negated()))
                .typedValue(27, "c_Decimal", DecimalType.getDefault().newValue(1))
                .typedValue(28, "c_BigDecimal", DecimalType.of(35, 0).newValue(1))
                .typedValue(29, "c_BankDecimal", DecimalType.of(31, 9).newValue(1));

        checker.nextRow()
                .value(1, "key", PrimitiveValue.newInt32(5))
                .value(2, "c_Bool", null)
                .value(3, "c_Int8", null)
                .value(4, "c_Int16", null)
                .value(5, "c_Int32", null)
                .value(6, "c_Int64", null)
                .value(7, "c_Uint8", null)
                .value(8, "c_Uint16", null)
                .value(9, "c_Uint32", null)
                .value(10, "c_Uint64", null)
                .value(11, "c_Float", null)
                .value(12, "c_Double", null)
                .value(13, "c_Bytes", null)
                .value(14, "c_Text", null)
                .value(15, "c_Json", null)
                .value(16, "c_JsonDocument", null)
                .value(17, "c_Yson", null)
                .value(18, "c_Uuid", null)
                .value(19, "c_Date", null)
                .value(20, "c_Datetime", null)
                .value(21, "c_Timestamp", null)
                .value(22, "c_Interval", null)
                .value(23, "c_Date32", null)
                .value(24, "c_Datetime64", null)
                .value(25, "c_Timestamp64", null)
                .value(26, "c_Interval64", null)
                .value(27, "c_Decimal", null)
                .value(28, "c_BigDecimal", null)
                .value(29, "c_BankDecimal", null);

        checker.assertNoRows();
    }

    interface IndexFunctor<T> {
        T apply(ResultSet rs, int index) throws SQLException;
    }

    interface StringFunctor<T> {
        T apply(ResultSet rs, String name) throws SQLException;
    }

    private static <T> ResultSetChecker<T> check(
            ResultSet rs,
            IndexFunctor<T> indexFunctor,
            StringFunctor<T> nameFunctor) {
        return new ResultSetChecker<>(rs, indexFunctor, nameFunctor);
    }

    private static class ResultSetChecker<T> {
        private final ResultSet rs;
        private final IndexFunctor<T> indexFunctor;
        private final StringFunctor<T> nameFunctor;

        public ResultSetChecker(ResultSet rs, IndexFunctor<T> indexFunctor, StringFunctor<T> nameFunctor) {
            this.rs = rs;
            this.indexFunctor = indexFunctor;
            this.nameFunctor = nameFunctor;
        }

        public ResultSetChecker<T> value(int index, String column, T expected) throws SQLException {
            Assertions.assertEquals(index, rs.findColumn(column));
            assertValue(expected, nameFunctor.apply(rs, column), expected == null, "for column label " + column);
            assertValue(expected, indexFunctor.apply(rs, index), expected == null, "for column index " + index);
            return this;
        }

        public ResultSetChecker<T> nullValue(int index, String column, T expected) throws SQLException {
            Assertions.assertEquals(index, rs.findColumn(column));
            assertValue(expected, nameFunctor.apply(rs, column), true, "for column label " + column);
            assertValue(expected, indexFunctor.apply(rs, index), true, "for column index " + index);
            return this;
        }

        public ResultSetChecker<T> exceptionValue(int index, String column, String message) throws SQLException {
            Assertions.assertEquals(index, rs.findColumn(column));
            ExceptionAssert.sqlException(message, () -> nameFunctor.apply(rs, column));
            ExceptionAssert.sqlException(message, () -> indexFunctor.apply(rs, index));
            return this;
        }

        public ResultSetChecker<T> typedValue(int index, String column, T expected) throws SQLException {
            Assertions.assertEquals(index, rs.findColumn(column));
            Assertions.assertNotNull(expected, "Expected typed value is null");

            T v1 = assertValue(expected, nameFunctor.apply(rs, column), false, "for column label " + column);
            T v2 = assertValue(expected, indexFunctor.apply(rs, index), false, "for column index " + index);

            Assertions.assertEquals((Class) expected.getClass(), (Class) v1.getClass(),
                    "Wrong Java class for column label " + column);
            Assertions.assertEquals((Class) expected.getClass(), (Class) v2.getClass(),
                    "Wrong Java class for column index " + index);
            return this;
        }

        private T assertValue(T expected, T value, boolean isNull, String message) throws SQLException {
            if (expected == null) {
                Assertions.assertNull(value, "Not empty value " + message);
                Assertions.assertEquals(isNull, rs.wasNull(), "Unexpected wasNull " + message);
                return value;
            }

            if (expected instanceof byte[]) {
                Assertions.assertArrayEquals((byte[]) expected, (byte[]) value, "Wrong array value " + message);
                Assertions.assertEquals(isNull, rs.wasNull(), "Unexpected wasNull " + message);
                return value;
            }

            if (expected instanceof InputStream) {
                try {
                    InputStream expectedStream = (InputStream) expected;
                    InputStream valueStream = (InputStream) value;

                    Assertions.assertArrayEquals(
                            ByteStreams.toByteArray(expectedStream),
                            ByteStreams.toByteArray(valueStream),
                            "Wrong InputStream value " + message);
                    Assertions.assertEquals(isNull, rs.wasNull(), "Unexpected wasNull " + message);

                    expectedStream.reset(); // expected may be used multipli times
                    valueStream.close();
                } catch (IOException e) {
                    throw new AssertionError("Can't read Input Stream", e);
                }
                return value;
            }

            if (expected instanceof Reader) {
                try {
                    Reader expectedReader = (Reader) expected;
                    Reader valueReader = (Reader) value;

                    Assertions.assertEquals(
                            CharStreams.toString(expectedReader),
                            CharStreams.toString(valueReader),
                            "Wrong Reader value " + message);
                    Assertions.assertEquals(isNull, rs.wasNull(), "Unexpected wasNull " + message);

                    expectedReader.reset(); // expected may be used multipli times
                    valueReader.close();
                } catch (IOException e) {
                    throw new AssertionError("Can't read Reader", e);
                }
                return value;
            }

            Assertions.assertEquals(expected, value, "Wrong value " + message);
            Assertions.assertEquals(isNull, rs.wasNull(), "Unexpected wasNull " + message);

            return value;
        }

        public ResultSetChecker<T> nextRow() throws SQLException {
            Assertions.assertTrue(rs.next(), "Unexpected end of result set");
            return this;
        }

        public void assertNoRows() throws SQLException {
            Assertions.assertFalse(rs.next(), "Unexpected non-empty result set");
            rs.close();
            Assertions.assertTrue(rs.isClosed(), "Result set is not closed");
        }
    }
}
