package tech.ydb.jdbc.impl;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbParameterMetaData;
import tech.ydb.jdbc.YdbPrepareMode;
import tech.ydb.jdbc.YdbPreparedStatement;
import tech.ydb.jdbc.YdbTypes;
import tech.ydb.jdbc.common.QueryType;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcConnectionExtention;
import tech.ydb.jdbc.impl.helper.SqlQueries;
import tech.ydb.jdbc.impl.helper.TextSelectAssert;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.test.junit5.YdbHelperExtension;


public class YdbPreparedStatementImplTest {
    private static final Logger LOGGER = Logger.getLogger(YdbPreparedStatementImplTest.class.getName());

    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private static final JdbcConnectionExtention jdbc = new JdbcConnectionExtention(ydb);

    private static final String TEST_TABLE_NAME = "ydb_prepared_statement_test";
    private static final SqlQueries TEST_TABLE = new SqlQueries(TEST_TABLE_NAME);

    private static final String UPSERT_SQL = ""
            + "declare $key as Int32;\n"
            + "declare $#column as #type;\n"
            + "upsert into #tableName (key, #column) values ($key, $#column)";

    private static final String BATCH_UPSERT_SQL = ""
            + "declare $values as List<Struct<key:Int32, #column:#type>>; \n"
            + "upsert into #tableName select * from as_table($values)";

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

    private String upsertSql(String column, String type) {
        return UPSERT_SQL
                .replaceAll("#column", column)
                .replaceAll("#type", type)
                .replaceAll("#tableName", TEST_TABLE_NAME);
    }

    private String batchUpsertSql(String column, String type) {
        return BATCH_UPSERT_SQL
                .replaceAll("#column", column)
                .replaceAll("#type", type)
                .replaceAll("#tableName", TEST_TABLE_NAME);
    }

    private YdbPreparedStatement prepareUpsert(YdbPrepareMode mode,String column, String type)
            throws SQLException {
        return jdbc.connection().unwrap(YdbConnection.class).prepareStatement(upsertSql(column, type), mode);
    }

    private YdbPreparedStatement prepareBatchUpsertInMemory(String column, String type) throws SQLException {
        return jdbc.connection().unwrap(YdbConnection.class)
                .prepareStatement(batchUpsertSql(column, type), YdbPrepareMode.IN_MEMORY);
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

    private PreparedStatement prepareScanSelect(String column) throws SQLException {
        String sql = SIMPLE_SELECT_SQL
                .replaceAll("#column", column)
                .replaceAll("#tableName", TEST_TABLE_NAME);
        return jdbc.connection().prepareStatement(QueryType.SCAN_QUERY.getPrefix() + "\n" + sql);
    }

    private YdbPreparedStatement prepareScanSelectByKey(String column) throws SQLException {
        String sql = SELECT_BY_KEY_SQL
                .replaceAll("#column", column)
                .replaceAll("#tableName", TEST_TABLE_NAME);
        return jdbc.connection().prepareStatement(QueryType.SCAN_QUERY.getPrefix() + "\n" + sql)
                .unwrap(YdbPreparedStatement.class);
    }

    private YdbPreparedStatement prepareUpsertValues() throws SQLException {
        return jdbc.connection().prepareStatement(SqlQueries.namedUpsertSQL(TEST_TABLE_NAME))
                .unwrap(YdbPreparedStatement.class);
    }

    private YdbPreparedStatement prepareSelectAll() throws SQLException {
        return jdbc.connection().prepareStatement(TEST_TABLE.selectSQL())
                .unwrap(YdbPreparedStatement.class);
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(YdbPrepareMode.class)
    public void unknownColumns(YdbPrepareMode mode) throws SQLException {
        try (YdbPreparedStatement statement = prepareUpsert(mode, "c_Text", "Optional<Text>")) {
            statement.setInt("key", 1);
            statement.setObject("column0", "value");
            statement.execute();
        }

        jdbc.connection().commit();

        try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
            TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                    .nextRowIsEmpty()
                    .noNextRows();
        }
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(YdbPrepareMode.class)
    public void executeWithoutBatch(YdbPrepareMode mode) throws SQLException {
        try (YdbPreparedStatement statement = prepareUpsert(mode, "c_Text", "Text")) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");
            statement.addBatch();

            statement.setInt("key", 2);
            statement.setString("c_Text", "value-2");

            ExceptionAssert.ydbExecution("Cannot call #execute method after #addBatch, must use #executeBatch",
                    () -> statement.execute());

            // clear will be called automatically
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");
            statement.execute();

            statement.setInt("key", 2);
            statement.setString("c_Text", "value-2");
            statement.execute();
        }

        try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
            TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                    .nextRow(1, "value-1")
                    .nextRow(2, "value-2")
                    .noNextRows();
        }
    };

    @Test
    public void addBatchClearParameters() throws SQLException {
        try (YdbPreparedStatement statement = prepareBatchUpsertInMemory("c_Text", "Text")) {
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

        try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
            TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                    .nextRow(1, "value-1")
                    .nextRow(2, "value-2")
                    .noNextRows();
        }
    }

    @Test
    public void addBatch() throws SQLException {
        try (YdbPreparedStatement statement = prepareBatchUpsertInMemory("c_Text", "Text")) {
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

        try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
            TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                    .nextRow(1, "value-1")
                    .nextRow(2, "value-2")
                    .noNextRows();
        }
    }

    @Test
    public void addAndClearBatch() throws SQLException {
        try (YdbPreparedStatement statement = prepareBatchUpsertInMemory("c_Text", "Text")) {
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

        try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
            TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                    .nextRow(1, "value-1")
                    .nextRow(2, "value-2")
                    .noNextRows();
        }
    }

    @Test
    public void executeEmptyBatch() throws SQLException {
        try (YdbPreparedStatement statement = prepareBatchUpsertInMemory("c_Text", "Optional<Text>")) {
            ExceptionAssert.ydbNonRetryable("Missing value for parameter", () -> statement.execute());
            ExceptionAssert.ydbNonRetryable("Missing value for parameter", () -> statement.executeUpdate());
            statement.executeBatch();
        }

        try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
            TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
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

        try (YdbPreparedStatement statement = prepareBatchUpsertInMemory("c_Text", "Text")) {
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

        ExceptionAssert.ydbResultTruncated("Result #0 was truncated to 1000 rows", () -> {
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

    @ParameterizedTest(name = "with {0}")
    @EnumSource(YdbPrepareMode.class)
    public void executeDataQuery(YdbPrepareMode mode) throws SQLException {
        try (YdbPreparedStatement statement = prepareUpsert(mode, "c_Text", "Text")) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");
            statement.execute();
        }

        try (YdbPreparedStatement statement = prepareSelectByKey("c_Text")) {
            statement.setInt("key", 2);

            TextSelectAssert.of(statement.executeQuery(), "c_Text", "Text")
                    .noNextRows();

            statement.setInt("key", 1);

            TextSelectAssert.of(statement.executeQuery(), "c_Text", "Text")
                    .nextRow(1, "value-1")
                    .noNextRows();
        }

        try (YdbPreparedStatement statement = prepareUpsert(mode, "c_Text", "Text")) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");
            statement.execute();

            statement.setInt("key", 2);
            statement.setString("c_Text", "value-2");
            statement.execute();
        }

        try (PreparedStatement select = prepareSimpleSelect("c_Text")) {
            TextSelectAssert.of(select.executeQuery(), "c_Text", "Text")
                    .nextRow(1, "value-1")
                    .nextRow(2, "value-2")
                    .noNextRows();
        }
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(YdbPrepareMode.class)
    public void executeQueryInTx(YdbPrepareMode mode) throws SQLException {
        jdbc.connection().setAutoCommit(false);
        try {
            try (YdbPreparedStatement statement = prepareUpsert(mode, "c_Text", "Text")) {
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

    @ParameterizedTest(name = "with {0}")
    @EnumSource(YdbPrepareMode.class)
    public void executeScanQueryInTx(YdbPrepareMode mode) throws SQLException {
        jdbc.connection().setAutoCommit(false);
        try {
            try (YdbPreparedStatement statement = prepareUpsert(mode, "c_Text", "Text")) {
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
                .prepareStatement(sql, YdbPrepareMode.IN_MEMORY)
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

    @ParameterizedTest(name = "with {0}")
    @EnumSource(YdbPrepareMode.class)
    public void executeExplainQueryExplicitly(YdbPrepareMode mode) throws SQLException {
        try (YdbPreparedStatement statement = prepareUpsert(mode, "c_Text", "Optional<Text>")) {
            statement.setInt("key", 1);
            statement.setString("c_Text", "value-1");

            ResultSet rs = statement.executeExplainQuery();

            Assertions.assertTrue(rs.next());
            String ast = rs.getString(YdbConst.EXPLAIN_COLUMN_AST);
            String plan = rs.getString(YdbConst.EXPLAIN_COLUMN_AST);

            Assertions.assertNotNull(ast);
            Assertions.assertNotNull(plan);

            LOGGER.log(Level.INFO, "AST: {0}", ast);
            LOGGER.log(Level.INFO, "PLAN: {0}", plan);

            Assertions.assertFalse(rs.next());
        }

        try (YdbPreparedStatement statement = prepareSelectByKey("c_Text")) {
            ResultSet rs = statement.executeExplainQuery();
            Assertions.assertTrue(rs.next());

            String ast = rs.getString(YdbConst.EXPLAIN_COLUMN_AST);
            String plan = rs.getString(YdbConst.EXPLAIN_COLUMN_AST);

            Assertions.assertNotNull(ast);
            Assertions.assertNotNull(plan);

            LOGGER.log(Level.INFO, "AST: {0}", ast);
            LOGGER.log(Level.INFO, "PLAN: {0}", plan);

            Assertions.assertFalse(rs.next());
        }
    }

    @Test
    public void testSetNull() throws SQLException {
        try (YdbPreparedStatement ps = prepareUpsertValues()) {
            ps.setInt("key", 1);
            YdbTypes types = ps.getConnection().getYdbTypes();
            ps.setNull("c_Bool", types.wrapYdbJdbcType(PrimitiveType.Bool));
            ps.setNull("c_Int8", types.wrapYdbJdbcType(PrimitiveType.Int8));
            ps.setNull("c_Int16", types.wrapYdbJdbcType(PrimitiveType.Int16));
            ps.setNull("c_Int32", types.wrapYdbJdbcType(PrimitiveType.Int32));
            ps.setNull("c_Int64", types.wrapYdbJdbcType(PrimitiveType.Int64));
            ps.setNull("c_Uint8", types.wrapYdbJdbcType(PrimitiveType.Uint8));
            ps.setNull("c_Uint16", types.wrapYdbJdbcType(PrimitiveType.Uint16));
            ps.setNull("c_Uint32", types.wrapYdbJdbcType(PrimitiveType.Uint32));
            ps.setNull("c_Uint64", types.wrapYdbJdbcType(PrimitiveType.Uint64));
            ps.setNull("c_Float", types.wrapYdbJdbcType(PrimitiveType.Float));
            ps.setNull("c_Double", types.wrapYdbJdbcType(PrimitiveType.Double));
            ps.setNull("c_Bytes", types.wrapYdbJdbcType(PrimitiveType.Bytes));
            ps.setNull("c_Text", types.wrapYdbJdbcType(PrimitiveType.Text));
            ps.setNull("c_Json", types.wrapYdbJdbcType(PrimitiveType.Json));
            ps.setNull("c_JsonDocument", types.wrapYdbJdbcType(PrimitiveType.JsonDocument));
            ps.setNull("c_Yson", types.wrapYdbJdbcType(PrimitiveType.Yson));
            ps.setNull("c_Date", types.wrapYdbJdbcType(PrimitiveType.Date));
            ps.setNull("c_Datetime", types.wrapYdbJdbcType(PrimitiveType.Datetime));
            ps.setNull("c_Timestamp", types.wrapYdbJdbcType(PrimitiveType.Timestamp));
            ps.setNull("c_Interval", types.wrapYdbJdbcType(PrimitiveType.Interval));
            ps.setNull("c_Decimal", types.wrapYdbJdbcType(YdbTypes.DEFAULT_DECIMAL_TYPE));

            ps.executeUpdate();
        }

        try (YdbPreparedStatement ps = prepareUpsertValues()) {
            ps.setInt("key", 2);
            ps.setNull("c_Bool", -1, "Bool");
            ps.setNull("c_Int8", -1, "Int8");
            ps.setNull("c_Int16", -1, "Int16");
            ps.setNull("c_Int32", -1, "Int32");
            ps.setNull("c_Int64", -1, "Int64");
            ps.setNull("c_Uint8", -1, "Uint8");
            ps.setNull("c_Uint16", -1, "Uint16");
            ps.setNull("c_Uint32", -1, "Uint32");
            ps.setNull("c_Uint64", -1, "Uint64");
            ps.setNull("c_Float", -1, "Float");
            ps.setNull("c_Double", -1, "Double");
            ps.setNull("c_Bytes", -1, "String");
            ps.setNull("c_Text", -1, "Text");
            ps.setNull("c_Json", -1, "Json");
            ps.setNull("c_JsonDocument", -1, "JsonDocument");
            ps.setNull("c_Yson", -1, "Yson");
            ps.setNull("c_Date", -1, "Date");
            ps.setNull("c_Datetime", -1, "Datetime");
            ps.setNull("c_Timestamp", -1, "Timestamp");
            ps.setNull("c_Interval", -1, "Interval");
            ps.setNull("c_Decimal", -1, "Decimal(22, 9)");

            ps.executeUpdate();
        }

        try (YdbPreparedStatement ps = prepareUpsertValues()) {
            ps.setInt("key", 3);
            ps.setNull("c_Bool", -1);
            ps.setNull("c_Int8", -1);
            ps.setNull("c_Int16", -1);
            ps.setNull("c_Int32", -1);
            ps.setNull("c_Int64", -1);
            ps.setNull("c_Uint8", -1);
            ps.setNull("c_Uint16", -1);
            ps.setNull("c_Uint32", -1);
            ps.setNull("c_Uint64", -1);
            ps.setNull("c_Float", -1);
            ps.setNull("c_Double", -1);
            ps.setNull("c_Bytes", -1);
            ps.setNull("c_Text", -1);
            ps.setNull("c_Json", -1);
            ps.setNull("c_JsonDocument", -1);
            ps.setNull("c_Yson", -1);
            ps.setNull("c_Date", -1);
            ps.setNull("c_Datetime", -1);
            ps.setNull("c_Timestamp", -1);
            ps.setNull("c_Interval", -1);
            ps.setNull("c_Decimal", -1);

            ps.executeUpdate();
        }

        try (YdbPreparedStatement ps = prepareSelectAll()) {
            ResultSet rs = ps.executeQuery();

            for (int key = 1; key <= 3; key += 1) {
                Assertions.assertTrue(rs.next());

                ResultSetMetaData metaData = rs.getMetaData();
                Assertions.assertEquals(22, metaData.getColumnCount());
                Assertions.assertEquals(key, rs.getInt("key")); // key

                for (int i = 2; i <= metaData.getColumnCount(); i++) {
                    Assertions.assertNull(rs.getObject(i)); // everything else
                }
            }

            Assertions.assertFalse(rs.next());
        }
    }

    @Test
    public void testParametersMeta() throws SQLException {
        try (YdbPreparedStatement ps = prepareUpsertValues()) {
            final ParameterMetaData meta = ps.getParameterMetaData();
            final YdbParameterMetaData ydbMeta = meta.unwrap(YdbParameterMetaData.class);

            ExceptionAssert.sqlException("Parameter is out of range: 335",
                    () -> meta.getParameterType(335)
            );

            Assertions.assertEquals(22, meta.getParameterCount());
            for (int param = 1; param <= meta.getParameterCount(); param++) {
                String name = ydbMeta.getParameterName(param);

                Assertions.assertFalse(meta.isSigned(param), "All params are not isSigned");
                Assertions.assertEquals(0, meta.getPrecision(param), "No precision available");
                Assertions.assertEquals(0, meta.getScale(param), "No scale available");
                Assertions.assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(param),
                        "All params are in");

                int type = meta.getParameterType(param);
                Assertions.assertTrue(type != 0, "All params have sql type, including " + name);

                if (name.equals("$key")) {
                    continue;
                }

                Assertions.assertEquals(ParameterMetaData.parameterNullable, meta.isNullable(param),
                        "All parameters expect primary key defined as nullable, including " + name);

                String expectType = name.substring("$c_".length()).toLowerCase();
                if (expectType.equals("decimal")) {
                    expectType += "(22, 9)";
                }

                String actualType = meta.getParameterTypeName(param);
                Assertions.assertNotNull(actualType, "All parameters have database types");
                Assertions.assertEquals(expectType, actualType.toLowerCase(),
                        "All parameter names are similar to types");

                String expectClassName;
                switch (name) {
                    case "$key":
                    case "$c_Bool":
                        expectClassName = Boolean.class.getName();
                        break;
                    case "$c_Int8":
                        expectClassName = Byte.class.getName();
                        break;
                    case "$c_Int16":
                        expectClassName = Short.class.getName();
                        break;
                    case "$c_Int32":
                    case "$c_Uint8":
                    case "$c_Uint16":
                        expectClassName = Integer.class.getName();
                        break;
                    case "$c_Int64":
                    case "$c_Uint64":
                    case "$c_Uint32":
                        expectClassName = Long.class.getName();
                        break;
                    case "$c_Float":
                        expectClassName = Float.class.getName();
                        break;
                    case "$c_Double":
                        expectClassName = Double.class.getName();
                        break;
                    case "$c_Text":
                    case "$c_Json":
                    case "$c_JsonDocument":
                        expectClassName = String.class.getName();
                        break;
                    case "$c_Bytes":
                    case "$c_Yson":
                        expectClassName = byte[].class.getName();
                        break;
                    case "$c_Date":
                        expectClassName = LocalDate.class.getName();
                        break;
                    case "$c_Datetime":
                        expectClassName = LocalDateTime.class.getName();
                        break;
                    case "$c_Timestamp":
                        expectClassName = Instant.class.getName();
                        break;
                    case "$c_Interval":
                        expectClassName = Duration.class.getName();
                        break;
                    case "$c_Decimal":
                        expectClassName = DecimalValue.class.getName();
                        break;
                    default:
                        throw new IllegalStateException("Unknown param: " + name);
                }
                Assertions.assertEquals(expectClassName, meta.getParameterClassName(param),
                        "Check class name for parameter: " + name);
            }
        }
    }

/*
    @Test
    void setBoolean() throws SQLException {
        checkInsert("c_Bool", "Bool",
                YdbPreparedStatement::setBoolean,
                YdbPreparedStatement::setBoolean,
                ResultSet::getBoolean,
                Arrays.asList(
                        pair(true, true),
                        pair(false, false)
                ),
                Arrays.asList(
                        pair(true, true),
                        pair(false, false),
                        pair(1, true),
                        pair(0, false),
                        pair(-1, false),
                        pair(2, true),
                        pair(0.1, false), // round to 0
                        pair(1.1, true),
                        pair(-0.1, false),
                        pair((byte) 1, true),
                        pair((byte) 0, false),
                        pair((short) 1, true),
                        pair((short) 0, false),
                        pair(1, true),
                        pair(0, false),
                        pair(1L, true),
                        pair(0L, false),
                        pair(PrimitiveValue.newBool(true), true),
                        pair(PrimitiveValue.newBool(false), false)
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        PrimitiveValue.newInt32(1),
                        PrimitiveValue.newInt32(1).makeOptional()
                )
        );
    }


    @ParameterizedTest
    @ValueSource(strings = {"Uint8"})
    void setByte(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setByte,
                YdbPreparedStatement::setByte,
                ResultSet::getByte,
                Arrays.asList(
                        pair((byte) 1, (byte) 1),
                        pair((byte) 0, (byte) 0),
                        pair((byte) -1, (byte) -1),
                        pair((byte) 127, (byte) 127),
                        pair((byte) 4, (byte) 4)
                ),
                Arrays.asList(
                        pair(true, (byte) 1),
                        pair(false, (byte) 0),
                        pair(PrimitiveValue.newUint8((byte) 1), (byte) 1),
                        pair(PrimitiveValue.newUint8((byte) 0), (byte) 0)
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        (short) 5,
                        6,
                        7L,
                        8f,
                        9d,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional()
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Int32", "Uint32"})
    void setByteToInt(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setByte,
                YdbPreparedStatement::setByte,
                ResultSet::getInt,
                Arrays.asList(
                        pair((byte) 1, 1),
                        pair((byte) 0, 0),
                        pair((byte) -1, -1),
                        pair((byte) 127, 127),
                        pair((byte) 4, 4)
                ),
                Arrays.asList(
                        pair((short) 5, 5),
                        pair(6, 6),
                        pair(true, 1),
                        pair(false, 0)
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        7L,
                        8f,
                        9d,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newUint8((byte) 1)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Int64", "Uint64"})
    void setByteToLong(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setByte,
                YdbPreparedStatement::setByte,
                ResultSet::getLong,
                Arrays.asList(
                        pair((byte) 1, 1L),
                        pair((byte) 0, 0L),
                        pair((byte) -1, -1L),
                        pair((byte) 127, 127L),
                        pair((byte) 4, 4L)
                ),
                Arrays.asList(
                        pair((short) 5, 5L),
                        pair(6, 6L),
                        pair(7L, 7L),
                        pair(true, 1L),
                        pair(false, 0L),
                        pair(new BigInteger("10"), 10L)
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        8f,
                        9d,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newUint8((byte) 1),
                        new BigDecimal("10")
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Int32", "Uint32"})
    void setShortToInt(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setShort,
                YdbPreparedStatement::setShort,
                ResultSet::getInt,
                Arrays.asList(
                        pair((short) 1, 1),
                        pair((short) 0, 0),
                        pair((short) -1, -1),
                        pair((short) 127, 127),
                        pair((short) 5, 5)
                ),
                Arrays.asList(
                        pair((byte) 4, 4),
                        pair(6, 6),
                        pair(true, 1),
                        pair(false, 0)
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        7L,
                        8f,
                        9d,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newInt16((short) 1)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Int64", "Uint64"})
    void setShortToLong(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setShort,
                YdbPreparedStatement::setShort,
                ResultSet::getLong,
                Arrays.asList(
                        pair((short) 1, 1L),
                        pair((short) 0, 0L),
                        pair((short) -1, -1L),
                        pair((short) 127, 127L),
                        pair((short) 5, 5L)
                ),
                Arrays.asList(
                        pair((byte) 4, 4L),
                        pair(6, 6L),
                        pair(7L, 7L),
                        pair(true, 1L),
                        pair(false, 0L)
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        8f,
                        9d,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newInt16((short) 1)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Int32", "Uint32"})
    void setInt(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setInt,
                YdbPreparedStatement::setInt,
                ResultSet::getInt,
                Arrays.asList(
                        pair(1, 1),
                        pair(0, 0),
                        pair(-1, -1),
                        pair(127, 127),
                        pair(6, 6)
                ),
                Arrays.asList(
                        pair((byte) 4, 4),
                        pair((short) 5, 5),
                        pair(true, 1),
                        pair(false, 0)
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        7L,
                        8f,
                        9d,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newInt16((short) 1)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Int64", "Uint64"})
    void setIntToLong(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setInt,
                YdbPreparedStatement::setInt,
                ResultSet::getLong,
                Arrays.asList(
                        pair(1, 1L),
                        pair(0, 0L),
                        pair(-1, -1L),
                        pair(127, 127L),
                        pair(6, 6L)
                ),
                Arrays.asList(
                        pair((byte) 4, 4L),
                        pair((short) 5, 5L),
                        pair(7L, 7L),
                        pair(true, 1L),
                        pair(false, 0L)
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        8f,
                        9d,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newInt32(1)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Int64", "Uint64"})
    void setLong(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setLong,
                YdbPreparedStatement::setLong,
                ResultSet::getLong,
                Arrays.asList(
                        pair(1L, 1L),
                        pair(0L, 0L),
                        pair(-1L, -1L),
                        pair(127L, 127L),
                        pair(7L, 7L)
                ),
                Arrays.asList(
                        pair((byte) 4, 4L),
                        pair((short) 5, 5L),
                        pair(6, 6L),
                        pair(true, 1L),
                        pair(false, 0L),
                        pair(new BigInteger("1234567890123"), 1234567890123L)
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        8f,
                        9d,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newInt32(1),
                        new BigDecimal("123")
                )
        );
    }

    @Test
    void setFloat() throws SQLException {
        checkInsert("c_Float", "Float",
                YdbPreparedStatement::setFloat,
                YdbPreparedStatement::setFloat,
                ResultSet::getFloat,
                Arrays.asList(
                        pair(1f, 1f),
                        pair(0f, 0f),
                        pair(-1f, -1f),
                        pair(127f, 127f),
                        pair(8f, 8f)
                ),
                Arrays.asList(
                        pair((byte) 4, 4f),
                        pair((short) 5, 5f),
                        pair(6, 6f),
                        pair(true, 1f),
                        pair(false, 0f),
                        pair(PrimitiveValue.newFloat(1.1f), 1.1f)
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        7L,
                        9d,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1)
                )
        );
    }

    @Test
    void setFloatToDouble() throws SQLException {
        checkInsert("c_Double", "Double",
                YdbPreparedStatement::setFloat,
                YdbPreparedStatement::setFloat,
                ResultSet::getDouble,
                Arrays.asList(
                        pair(1f, 1d),
                        pair(0f, 0d),
                        pair(-1f, -1d),
                        pair(127f, 127d),
                        pair(8f, 8d)
                ),
                Arrays.asList(
                        pair((byte) 4, 4d),
                        pair((short) 5, 5d),
                        pair(6, 6d),
                        pair(7L, 7d),
                        pair(9d, 9d),
                        pair(true, 1d),
                        pair(false, 0d),
                        pair(PrimitiveValue.newDouble(1.1f), (double) 1.1f) // lost double precision
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newFloat(1)
                )
        );
    }

    @Test
    void setDouble() throws SQLException {
        checkInsert("c_Double", "Double",
                YdbPreparedStatement::setDouble,
                YdbPreparedStatement::setDouble,
                ResultSet::getDouble,
                Arrays.asList(
                        pair(1d, 1d),
                        pair(0d, 0d),
                        pair(-1d, -1d),
                        pair(127d, 127d),
                        pair(9d, 9d)
                ),
                Arrays.asList(
                        pair((byte) 4, 4d),
                        pair((short) 5, 5d),
                        pair(6, 6d),
                        pair(7L, 7d),
                        pair(8f, 8d),
                        pair(true, 1d),
                        pair(false, 0d),
                        pair(PrimitiveValue.newDouble(1.1d), 1.1d)
                ),
                Arrays.asList(
                        "",
                        "".getBytes(),
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newFloat(1)
                )
        );
    }

    @Test
    void setBigDecimalDirect() throws SQLException {
        retry(connection -> {
            YdbPreparedStatement insert = getTestStatement(connection, "c_Decimal", "Decimal(22,9)?");
            insert.setInt("key", 1);
            insert.setObject("c_Decimal", new BigDecimal(0)); // Make sure this type is converted to Decimal(22,9) type
            insert.executeUpdate();
        });
    }

    @Test
    void setBigDecimal() throws SQLException {
        checkInsert("c_Decimal", "Decimal(22,9)",
                YdbPreparedStatement::setBigDecimal,
                YdbPreparedStatement::setBigDecimal,
                ResultSet::getBigDecimal,
                Arrays.asList(
                        pair(new BigDecimal("0.0"), new BigDecimal("0.000000000")),
                        pair(new BigDecimal("1.3"), new BigDecimal("1.300000000"))
                ),
                Arrays.asList(
                        pair(1, new BigDecimal("1.000000000")),
                        pair(0, new BigDecimal("0.000000000")),
                        pair(-1, new BigDecimal("-1.000000000")),
                        pair(127, new BigDecimal("127.000000000")),
                        pair((byte) 4, new BigDecimal("4.000000000")),
                        pair((short) 5, new BigDecimal("5.000000000")),
                        pair(6, new BigDecimal("6.000000000")),
                        pair(7L, new BigDecimal("7.000000000")),
                        pair("1", new BigDecimal("1.000000000")),
                        pair("1.1", new BigDecimal("1.100000000")),
                        pair(DecimalType.of(22, 9).newValue("1.2"), new BigDecimal("1.200000000")),
                        pair(new BigInteger("2"), new BigDecimal("2.000000000"))
                ),
                Arrays.asList(
                        true,
                        false,
                        8f,
                        9d,
                        "".getBytes(),
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1)
                )
        );
    }

    @ParameterizedTest
    @MethodSource("bytesAndText")
    void setString(String type, List<Pair<Object, String>> callSetObject, List<Object> unsupported) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setString,
                YdbPreparedStatement::setString,
                ResultSet::getString,
                Arrays.asList(
                        pair("", ""),
                        pair("test1", "test1")
                ),
                merge(callSetObject,
                        Arrays.asList(
                                pair(1d, "1.0"),
                                pair(0d, "0.0"),
                                pair(-1d, "-1.0"),
                                pair(127d, "127.0"),
                                pair((byte) 4, "4"),
                                pair((short) 5, "5"),
                                pair(6, "6"),
                                pair(7L, "7"),
                                pair(8f, "8.0"),
                                pair(9d, "9.0"),
                                pair(true, "true"),
                                pair(false, "false"),
                                pair("".getBytes(), ""),
                                pair("test2".getBytes(), "test2"),
                                pair(stream("test3"), "test3"),
                                pair(reader("test4"), "test4")
                        )),
                merge(unsupported,
                        Arrays.asList(
                                PrimitiveValue.newBool(true),
                                PrimitiveValue.newBool(true).makeOptional(),
                                PrimitiveValue.newDouble(1.1d),
                                PrimitiveValue.newJson("test")
                        ))
        );
    }

    @ParameterizedTest
    @MethodSource("jsonAndJsonDocumentAndYson")
    void setStringJson(String type, List<Pair<Object, String>> callSetObject, List<Object> unsupported)
            throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setString,
                YdbPreparedStatement::setString,
                ResultSet::getString,
                Arrays.asList(
                        pair("[1]", "[1]")
                ),
                merge(callSetObject,
                        Arrays.asList(
                                pair("[2]".getBytes(), "[2]"),
                                pair(stream("[3]"), "[3]"),
                                pair(reader("[4]"), "[4]")
                                // No empty values supported
                        )),
                merge(unsupported,
                        Arrays.asList(
                                6,
                                7L,
                                8f,
                                9d,
                                true,
                                false,
                                PrimitiveValue.newBool(true),
                                PrimitiveValue.newBool(true).makeOptional(),
                                PrimitiveValue.newDouble(1.1d),
                                PrimitiveValue.newText("test")
                        ))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text"})
    void setBytes(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setBytes,
                YdbPreparedStatement::setBytes,
                ResultSet::getBytes,
                Arrays.asList(
                        pair("".getBytes(), "".getBytes()),
                        pair("test2".getBytes(), "test2".getBytes())
                ),
                Arrays.asList(
                        pair(1d, "1.0".getBytes()),
                        pair(0d, "0.0".getBytes()),
                        pair(-1d, "-1.0".getBytes()),
                        pair(127d, "127.0".getBytes()),
                        pair((byte) 4, "4".getBytes()),
                        pair((short) 5, "5".getBytes()),
                        pair(6, "6".getBytes()),
                        pair(7L, "7".getBytes()),
                        pair(8f, "8.0".getBytes()),
                        pair(9d, "9.0".getBytes()),
                        pair(true, "true".getBytes()),
                        pair(false, "false".getBytes()),
                        pair("", "".getBytes()),
                        pair("test1", "test1".getBytes()),
                        pair(stream("test3"), "test3".getBytes()),
                        pair(reader("test4"), "test4".getBytes())
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newJson("test")
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Json", "JsonDocument", "Yson"})
    void setBytesJson(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setBytes,
                YdbPreparedStatement::setBytes,
                ResultSet::getBytes,
                Arrays.asList(
                        pair("[2]".getBytes(), "[2]".getBytes())
                ),
                Arrays.asList(
                        pair("[1]", "[1]".getBytes()),
                        pair(stream("[3]"), "[3]".getBytes()),
                        pair(reader("[4]"), "[4]".getBytes())
                        // No empty values supported
                ),
                Arrays.asList(
                        6,
                        7L,
                        8f,
                        9d,
                        true,
                        false,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newText("test")
                )
        );
    }

    @Test
    void setDateToDate() throws SQLException {
        checkInsert("c_Date", "Date",
                YdbPreparedStatement::setDate,
                YdbPreparedStatement::setDate,
                ResultSet::getDate,
                Arrays.asList(
                        pair(new Date(1), new Date(0)),
                        pair(new Date(0), new Date(0)),
                        pair(new Date(MILLIS_IN_DAY), new Date(MILLIS_IN_DAY)),
                        pair(new Date(MILLIS_IN_DAY * 2), new Date(MILLIS_IN_DAY * 2))
                ),
                Arrays.asList(
                        pair(Instant.ofEpochMilli(2), new Date(0)),
                        pair(Instant.ofEpochMilli(MILLIS_IN_DAY), new Date(MILLIS_IN_DAY)),
                        pair(Instant.parse("1970-01-01T00:00:03.111112Z"), new Date(0)),
                        pair("1970-01-01T00:00:03.111112Z", new Date(0)),
                        pair(3L, new Date(0)),
                        pair(MILLIS_IN_DAY * 3, new Date(MILLIS_IN_DAY * 3)),
                        pair(LocalDate.of(1970, 1, 2), new Date(MILLIS_IN_DAY)),
                        pair(new Timestamp(4), new Date(0)),
                        pair(new Timestamp(MILLIS_IN_DAY * 3), new Date(MILLIS_IN_DAY * 3)),
                        pair(new java.util.Date(5), new Date(0)),
                        pair(new java.util.Date(6999), new Date(0)),
                        pair(new java.util.Date(MILLIS_IN_DAY * 7), new Date(MILLIS_IN_DAY * 7))
                ),
                Arrays.asList(
                        6,
                        8f,
                        9d,
                        true,
                        false,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newText("test")
                )
        );
    }

    @Test
    void setDateToDatetime() throws SQLException {
        // precision - seconds
        checkInsert("c_Datetime", "Datetime",
                YdbPreparedStatement::setDate,
                YdbPreparedStatement::setDate,
                ResultSet::getDate,
                Arrays.asList(
                        pair(new Date(1), new Date(0)),
                        pair(new Date(0), new Date(0)),
                        pair(new Date(1000), new Date(1000)),
                        pair(new Date(1999), new Date(1000)),
                        pair(new Date(MILLIS_IN_DAY), new Date(MILLIS_IN_DAY)),
                        pair(new Date(MILLIS_IN_DAY * 2), new Date(MILLIS_IN_DAY * 2))
                ),
                Arrays.asList(
                        pair(Instant.ofEpochMilli(2), new Date(0)),
                        pair(Instant.ofEpochMilli(MILLIS_IN_DAY), new Date(MILLIS_IN_DAY)),
                        pair(Instant.parse("1970-01-01T00:00:03.111112Z"), new Date(3000)),
                        pair("1970-01-01T00:00:03.111112Z", new Date(3000)),
                        pair(3L, new Date(0)),
                        pair(2000L, new Date(2000L)),
                        pair(2999L, new Date(2000L)),
                        pair(MILLIS_IN_DAY * 3, new Date(MILLIS_IN_DAY * 3)),
                        pair(new Timestamp(4), new Date(0)),
                        pair(new Timestamp(4000), new Date(4000)),
                        pair(new Timestamp(4999), new Date(4000)),
                        pair(new Timestamp(MILLIS_IN_DAY * 3), new Date(MILLIS_IN_DAY * 3)),
                        pair(new Time(10), new Date(0)),
                        pair(new Time(5000), new Date(5000)),
                        pair(new Time(5999), new Date(5000)),
                        pair(new Time(MILLIS_IN_DAY * 4), new Date(MILLIS_IN_DAY * 4)),
                        pair(new java.util.Date(5), new Date(0)),
                        pair(new java.util.Date(6999), new Date(6000)),
                        pair(new java.util.Date(MILLIS_IN_DAY * 7), new Date(MILLIS_IN_DAY * 7))
                ),
                Arrays.asList(
                        6,
                        8f,
                        9d,
                        true,
                        false,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newText("test"),
                        LocalDate.of(1970, 1, 2)
                )
        );
    }

    @Test
    void setDateToTimestamp() throws SQLException {
        // precision - microseconds
        checkInsert("c_Timestamp", "Timestamp",
                YdbPreparedStatement::setDate,
                YdbPreparedStatement::setDate,
                ResultSet::getDate,
                Arrays.asList(
                        pair(new Date(1), new Date(1)),
                        pair(new Date(0), new Date(0)),
                        pair(new Date(1000), new Date(1000)),
                        pair(new Date(1999), new Date(1999)),
                        pair(new Date(MILLIS_IN_DAY), new Date(MILLIS_IN_DAY)),
                        pair(new Date(MILLIS_IN_DAY * 2), new Date(MILLIS_IN_DAY * 2))
                ),
                Arrays.asList(
                        pair(Instant.ofEpochMilli(2), new Date(2)),
                        pair(Instant.ofEpochMilli(MILLIS_IN_DAY), new Date(MILLIS_IN_DAY)),
                        pair(Instant.parse("1970-01-01T00:00:03.111112Z"), new Date(3111)),
                        pair("1970-01-01T00:00:03.111112Z", new Date(3111)),
                        pair(3L, new Date(3)),
                        pair(2000L, new Date(2000L)),
                        pair(2999L, new Date(2999L)),
                        pair(MILLIS_IN_DAY * 3, new Date(MILLIS_IN_DAY * 3)),
                        pair(new Timestamp(4), new Date(4)),
                        pair(new Timestamp(4000), new Date(4000)),
                        pair(new Timestamp(4999), new Date(4999)),
                        pair(new Timestamp(MILLIS_IN_DAY * 3), new Date(MILLIS_IN_DAY * 3)),
                        pair(new Time(10), new Date(10)),
                        pair(new Time(5000), new Date(5000)),
                        pair(new Time(5999), new Date(5999)),
                        pair(new Time(MILLIS_IN_DAY * 4), new Date(MILLIS_IN_DAY * 4)),
                        pair(new java.util.Date(5), new Date(5)),
                        pair(new java.util.Date(6999), new Date(6999)),
                        pair(new java.util.Date(MILLIS_IN_DAY * 7), new Date(MILLIS_IN_DAY * 7))
                ),
                Arrays.asList(
                        6,
                        8f,
                        9d,
                        true,
                        false,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newText("test"),
                        LocalDate.of(1970, 1, 2)
                )
        );
    }

    @Test
    void setTimeToDatetime() throws SQLException {
        checkInsert("c_Datetime", "Datetime",
                YdbPreparedStatement::setTime,
                YdbPreparedStatement::setTime,
                ResultSet::getTime,
                Arrays.asList(
                        pair(new Time(1), new Time(0)),
                        pair(new Time(0), new Time(0)),
                        pair(new Time(1000), new Time(1000)),
                        pair(new Time(1999), new Time(1000)),
                        pair(new Time(MILLIS_IN_DAY), new Time(MILLIS_IN_DAY)),
                        pair(new Time(MILLIS_IN_DAY * 2), new Time(MILLIS_IN_DAY * 2))
                ),
                Arrays.asList(
                        pair(Instant.ofEpochMilli(2), new Time(0)),
                        pair(Instant.ofEpochMilli(MILLIS_IN_DAY), new Time(MILLIS_IN_DAY)),
                        pair(Instant.parse("1970-01-01T00:00:03.111112Z"), new Date(3000)),
                        pair(3L, new Time(0)),
                        pair(2000L, new Time(2000L)),
                        pair(2999L, new Time(2000L)),
                        pair(MILLIS_IN_DAY * 3, new Time(MILLIS_IN_DAY * 3)),
                        pair(new Timestamp(4), new Time(0)),
                        pair(new Timestamp(4000), new Time(4000)),
                        pair(new Timestamp(4999), new Time(4000)),
                        pair(new Timestamp(MILLIS_IN_DAY * 3), new Time(MILLIS_IN_DAY * 3)),
                        pair(new Date(10), new Time(0)),
                        pair(new Date(5000), new Time(5000)),
                        pair(new Date(5999), new Time(5000)),
                        pair(new Date(MILLIS_IN_DAY * 4), new Time(MILLIS_IN_DAY * 4))
                ),
                Arrays.asList(
                        6,
                        8f,
                        9d,
                        true,
                        false,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newText("test"),
                        LocalDate.of(1970, 1, 2)
                )
        );
    }

    @Test
    void setTimeToTimestamp() throws SQLException {
        checkInsert("c_Timestamp", "Timestamp",
                YdbPreparedStatement::setTime,
                YdbPreparedStatement::setTime,
                ResultSet::getTime,
                Arrays.asList(
                        pair(new Time(1), new Time(1)),
                        pair(new Time(0), new Time(0)),
                        pair(new Time(1000), new Time(1000)),
                        pair(new Time(1999), new Time(1999)),
                        pair(new Time(MILLIS_IN_DAY), new Time(MILLIS_IN_DAY)),
                        pair(new Time(MILLIS_IN_DAY * 2), new Time(MILLIS_IN_DAY * 2))
                ),
                Arrays.asList(
                        pair(Instant.ofEpochMilli(2), new Time(2)),
                        pair(Instant.ofEpochMilli(MILLIS_IN_DAY), new Time(MILLIS_IN_DAY)),
                        pair(Instant.parse("1970-01-01T00:00:03.111112Z"), new Date(3111)),
                        pair(3L, new Time(3)),
                        pair(2000L, new Time(2000L)),
                        pair(2999L, new Time(2999L)),
                        pair(MILLIS_IN_DAY * 3, new Time(MILLIS_IN_DAY * 3)),
                        pair(new Timestamp(4), new Time(4)),
                        pair(new Timestamp(4000), new Time(4000)),
                        pair(new Timestamp(4999), new Time(4999)),
                        pair(new Timestamp(MILLIS_IN_DAY * 3), new Time(MILLIS_IN_DAY * 3)),
                        pair(new Date(10), new Time(10)),
                        pair(new Date(5000), new Time(5000)),
                        pair(new Date(5999), new Time(5999)),
                        pair(new Date(MILLIS_IN_DAY * 4), new Time(MILLIS_IN_DAY * 4))
                ),
                Arrays.asList(
                        6,
                        8f,
                        9d,
                        true,
                        false,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newText("test"),
                        LocalDate.of(1970, 1, 2)
                )
        );
    }

    @Test
    void setTimestampToDate() throws SQLException {
        checkInsert("c_Date", "Date",
                YdbPreparedStatement::setTimestamp,
                YdbPreparedStatement::setTimestamp,
                ResultSet::getTimestamp,
                Arrays.asList(
                        pair(new Timestamp(1), new Timestamp(0)),
                        pair(new Timestamp(0), new Timestamp(0)),
                        pair(new Timestamp(MILLIS_IN_DAY), new Timestamp(MILLIS_IN_DAY)),
                        pair(new Timestamp(MILLIS_IN_DAY * 2), new Timestamp(MILLIS_IN_DAY * 2))
                ),
                Arrays.asList(
                        pair(Instant.ofEpochMilli(2), new Timestamp(0)),
                        pair(Instant.ofEpochMilli(MILLIS_IN_DAY), new Timestamp(MILLIS_IN_DAY)),
                        pair(Instant.parse("1970-01-01T00:00:03.111112Z"), new Timestamp(0)),
                        pair(3L, new Timestamp(0)),
                        pair(MILLIS_IN_DAY * 3, new Timestamp(MILLIS_IN_DAY * 3)),
                        pair(LocalDate.of(1970, 1, 2), new Timestamp(MILLIS_IN_DAY)),
                        pair(new Timestamp(4), new Timestamp(0)),
                        pair(new Timestamp(MILLIS_IN_DAY * 3), new Timestamp(MILLIS_IN_DAY * 3))
                ),
                Arrays.asList(
                        6,
                        8f,
                        9d,
                        true,
                        false,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newText("test")
                )
        );
    }

    @Test
    void setTimestampToDatetime() throws SQLException {
        checkInsert("c_Datetime", "Datetime",
                YdbPreparedStatement::setTimestamp,
                YdbPreparedStatement::setTimestamp,
                ResultSet::getTimestamp,
                Arrays.asList(
                        pair(new Timestamp(1), new Timestamp(0)),
                        pair(new Timestamp(0), new Timestamp(0)),
                        pair(new Timestamp(1000), new Timestamp(1000)),
                        pair(new Timestamp(1999), new Timestamp(1000)),
                        pair(new Timestamp(MILLIS_IN_DAY), new Timestamp(MILLIS_IN_DAY)),
                        pair(new Timestamp(MILLIS_IN_DAY * 2), new Timestamp(MILLIS_IN_DAY * 2))
                ),
                Arrays.asList(
                        pair(Instant.ofEpochMilli(2), new Timestamp(0)),
                        pair(Instant.ofEpochMilli(MILLIS_IN_DAY), new Timestamp(MILLIS_IN_DAY)),
                        pair(Instant.parse("1970-01-01T00:00:03.111112Z"), new Timestamp(3000)),
                        pair(3L, new Timestamp(0)),
                        pair(2000L, new Timestamp(2000L)),
                        pair(2999L, new Timestamp(2000L)),
                        pair(MILLIS_IN_DAY * 3, new Timestamp(MILLIS_IN_DAY * 3)),
                        pair(new Timestamp(4), new Timestamp(0)),
                        pair(new Timestamp(4000), new Timestamp(4000)),
                        pair(new Timestamp(4999), new Timestamp(4000)),
                        pair(new Timestamp(MILLIS_IN_DAY * 3), new Timestamp(MILLIS_IN_DAY * 3)),
                        pair(new Date(10), new Timestamp(0)),
                        pair(new Date(5000), new Timestamp(5000)),
                        pair(new Date(5999), new Timestamp(5000)),
                        pair(new Date(MILLIS_IN_DAY * 4), new Timestamp(MILLIS_IN_DAY * 4)),
                        pair(new Time(10), new Timestamp(0)),
                        pair(new Time(5000), new Timestamp(5000)),
                        pair(new Time(5999), new Timestamp(5000)),
                        pair(new Time(MILLIS_IN_DAY * 4), new Timestamp(MILLIS_IN_DAY * 4))
                ),
                Arrays.asList(
                        6,
                        8f,
                        9d,
                        true,
                        false,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newText("test"),
                        LocalDate.of(1970, 1, 2)
                )
        );
    }

    @Test
    void setTimestampToTimestamp() throws SQLException {
        checkInsert("c_Timestamp", "Timestamp",
                YdbPreparedStatement::setTimestamp,
                YdbPreparedStatement::setTimestamp,
                ResultSet::getTimestamp,
                Arrays.asList(
                        pair(new Timestamp(1), new Timestamp(1)),
                        pair(new Timestamp(0), new Timestamp(0)),
                        pair(new Timestamp(1000), new Timestamp(1000)),
                        pair(new Timestamp(1999), new Timestamp(1999)),
                        pair(new Timestamp(MILLIS_IN_DAY), new Timestamp(MILLIS_IN_DAY)),
                        pair(new Timestamp(MILLIS_IN_DAY * 2), new Timestamp(MILLIS_IN_DAY * 2))
                ),
                Arrays.asList(
                        pair(Instant.ofEpochMilli(2), new Timestamp(2)),
                        pair(Instant.ofEpochMilli(MILLIS_IN_DAY), new Timestamp(MILLIS_IN_DAY)),
                        pair(Instant.parse("1970-01-01T00:00:03.111112Z"), new Timestamp(3111)),
                        pair(3L, new Timestamp(3)),
                        pair(2000L, new Timestamp(2000L)),
                        pair(2999L, new Timestamp(2999L)),
                        pair(MILLIS_IN_DAY * 3, new Timestamp(MILLIS_IN_DAY * 3)),
                        pair(new Timestamp(4), new Timestamp(4)),
                        pair(new Timestamp(4000), new Timestamp(4000)),
                        pair(new Timestamp(4999), new Timestamp(4999)),
                        pair(new Timestamp(MILLIS_IN_DAY * 3), new Timestamp(MILLIS_IN_DAY * 3)),
                        pair(new Date(10), new Timestamp(10)),
                        pair(new Date(5000), new Timestamp(5000)),
                        pair(new Date(5999), new Timestamp(5999)),
                        pair(new Date(MILLIS_IN_DAY * 4), new Timestamp(MILLIS_IN_DAY * 4)),
                        pair(new Time(10), new Timestamp(10)),
                        pair(new Time(5000), new Timestamp(5000)),
                        pair(new Time(5999), new Timestamp(5999)),
                        pair(new Time(MILLIS_IN_DAY * 4), new Timestamp(MILLIS_IN_DAY * 4))
                ),
                Arrays.asList(
                        6,
                        8f,
                        9d,
                        true,
                        false,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newText("test"),
                        LocalDate.of(1970, 1, 2)
                )
        );
    }

    @Test
    void setTimestampToInterval() throws SQLException {
        checkInsert("c_Interval", "Interval",
                YdbPreparedStatement::setLong,
                YdbPreparedStatement::setLong,
                ResultSet::getLong,
                Arrays.asList(
                        pair(1L, 1L),
                        pair(0L, 0L),
                        pair(1000L, 1000L)
                ),
                Arrays.asList(
                        pair(Duration.parse("PT3.111113S"), 3111113L),
                        pair(3L, 3L),
                        pair(2000L, 2000L),
                        pair(2999L, 2999L),
                        pair(MICROS_IN_DAY, MICROS_IN_DAY),
                        pair(MICROS_IN_DAY * 3, MICROS_IN_DAY * 3)
                ),
                Arrays.asList(
                        6,
                        8f,
                        9d,
                        true,
                        false,
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newText("test"),
                        LocalDate.of(1970, 1, 2),
                        new Timestamp(4),
                        new Date(10),
                        new Time(10)
                )
        );
    }

    @Test
    void setAsciiStream() throws SQLException {
        retry(connection ->
                assertThrowsMsg(SQLFeatureNotSupportedException.class,
                        () -> getTextStatement(connection).setAsciiStream("value", stream("value")),
                        "AsciiStreams are not supported"));
        retry(connection ->
                assertThrowsMsg(SQLFeatureNotSupportedException.class,
                        () -> getTextStatement(connection).setAsciiStream("value", stream("value"), 1),
                        "AsciiStreams are not supported"));
        retry(connection ->
                assertThrowsMsg(SQLFeatureNotSupportedException.class,
                        () -> getTextStatement(connection).setAsciiStream("value", stream("value"), 1L),
                        "AsciiStreams are not supported"));
    }

    @SuppressWarnings("deprecation")
    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void setUnicodeStream(String type) throws SQLException {
        checkInsert("c_" + type, type,
                (ps, name, value) -> ps.setUnicodeStream(name, value, 3),
                (ps, name, value) -> ps.setUnicodeStream(name, value, 3),
                ResultSet::getUnicodeStream,
                Arrays.asList(
                        pair(stream("[3]-limited!"), stream("[3]"))
                ),
                Arrays.asList(
                        pair("[1]", stream("[1]")),
                        pair("[2]".getBytes(), stream("[2]")),
                        pair(reader("[4]"), stream("[4]"))
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void setBinaryStream(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setBinaryStream,
                YdbPreparedStatement::setBinaryStream,
                ResultSet::getBinaryStream,
                Arrays.asList(
                        pair(stream("[3]"), stream("[3]"))
                ),
                Arrays.asList(
                        pair("[1]", stream("[1]")),
                        pair("[2]".getBytes(), stream("[2]")),
                        pair(reader("[4]"), stream("[4]"))
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void setBinaryStreamInt(String type) throws SQLException {
        checkInsert("c_" + type, type,
                (ps, name, value) -> ps.setBinaryStream(name, value, 3),
                (ps, name, value) -> ps.setBinaryStream(name, value, 3),
                ResultSet::getBinaryStream,
                Arrays.asList(
                        pair(stream("[3]-limited!"), stream("[3]"))
                ),
                Arrays.asList(
                        pair("[1]", stream("[1]")),
                        pair("[2]".getBytes(), stream("[2]")),
                        pair(reader("[4]"), stream("[4]"))
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void setBinaryStreamLong(String type) throws SQLException {
        checkInsert("c_" + type, type,
                (ps, name, value) -> ps.setBinaryStream(name, value, 3L),
                (ps, name, value) -> ps.setBinaryStream(name, value, 3L),
                ResultSet::getBinaryStream,
                Arrays.asList(
                        pair(stream("[3]-limited!"), stream("[3]"))
                ),
                Arrays.asList(
                        pair("[1]", stream("[1]")),
                        pair("[2]".getBytes(), stream("[2]")),
                        pair(reader("[4]"), stream("[4]"))
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void setCharacterStream(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setCharacterStream,
                YdbPreparedStatement::setCharacterStream,
                ResultSet::getCharacterStream,
                Arrays.asList(
                        pair(reader("[4]"), reader("[4]"))
                ),
                Arrays.asList(
                        pair("[1]", reader("[1]")),
                        pair("[2]".getBytes(), reader("[2]")),
                        pair(stream("[3]"), reader("[3]"))
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void setCharacterStreamInt(String type) throws SQLException {
        checkInsert("c_" + type, type,
                (ps, name, value) -> ps.setCharacterStream(name, value, 3),
                (ps, name, value) -> ps.setCharacterStream(name, value, 3),
                ResultSet::getCharacterStream,
                Arrays.asList(
                        pair(reader("[4]-limited!"), reader("[4]"))
                ),
                Arrays.asList(),
                Arrays.asList()
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text"})
    void setCharacterStreamIntEmpty(String type) throws SQLException {
        checkInsert("c_" + type, type,
                (ps, name, value) -> ps.setCharacterStream(name, value, 0),
                (ps, name, value) -> ps.setCharacterStream(name, value, 0),
                ResultSet::getCharacterStream,
                Arrays.asList(
                        pair(reader("[4]-limited!"), reader(""))
                ),
                Arrays.asList(),
                Arrays.asList()
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void setCharacterStreamLong(String type) throws SQLException {
        checkInsert("c_" + type, type,
                (ps, name, value) -> ps.setCharacterStream(name, value, 3L),
                (ps, name, value) -> ps.setCharacterStream(name, value, 3L),
                ResultSet::getCharacterStream,
                Arrays.asList(
                        pair(reader("[4]-limited!"), reader("[4]"))
                ),
                Arrays.asList(),
                Arrays.asList()
        );
    }


    @Test
    void setRef() throws SQLException {
        retry(connection ->
                assertThrowsMsg(SQLFeatureNotSupportedException.class,
                        () -> getTextStatement(connection).setRef("value", new RefImpl()),
                        "Refs are not supported"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void asBlob(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setBlob,
                YdbPreparedStatement::setBlob,
                ResultSet::getBinaryStream,
                Arrays.asList(
                        pair(stream("[3]"), stream("[3]"))
                ),
                Arrays.asList(
                        pair("[1]", stream("[1]")),
                        pair("[2]".getBytes(), stream("[2]")),
                        pair(reader("[4]"), stream("[4]"))
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void asBlobLong(String type) throws SQLException {
        checkInsert("c_" + type, type,
                (ps, name, value) -> ps.setBlob(name, value, 3L),
                (ps, name, value) -> ps.setBlob(name, value, 3L),
                ResultSet::getBinaryStream,
                Arrays.asList(
                        pair(stream("[3]-limited!"), stream("[3]"))
                ),
                Arrays.asList(),
                Arrays.asList()
        );
    }

    @Test
    void setBlobUnsupported() throws SQLException {
        retry(connection ->
                assertThrowsMsg(SQLFeatureNotSupportedException.class,
                        () -> getTextStatement(connection).setBlob("value", new SerialBlob("".getBytes())),
                        "Blobs are not supported"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void asClob(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setClob,
                YdbPreparedStatement::setClob,
                ResultSet::getCharacterStream,
                Arrays.asList(
                        pair(reader("[4]"), reader("[4]"))
                ),
                Arrays.asList(
                        pair("[1]", reader("[1]")),
                        pair("[2]".getBytes(), reader("[2]")),
                        pair(stream("[3]"), reader("[3]"))
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void asClobLong(String type) throws SQLException {
        checkInsert("c_" + type, type,
                (ps, name, value) -> ps.setClob(name, value, 3L),
                (ps, name, value) -> ps.setClob(name, value, 3L),
                ResultSet::getCharacterStream,
                Arrays.asList(
                        pair(reader("[4]-limited!"), reader("[4]"))
                ),
                Arrays.asList(),
                Arrays.asList()
        );
    }

    @Test
    void setClobUnsupported() throws SQLException {
        retry(connection ->
                assertThrowsMsg(SQLFeatureNotSupportedException.class,
                        () -> getTextStatement(connection).setClob("value", new NClobImpl("".toCharArray())),
                        "Clobs are not supported"));
    }

    @Test
    void setArray() throws SQLException {
        retry(connection ->
                assertThrowsMsg(SQLFeatureNotSupportedException.class,
                        () -> getTextStatement(connection).setArray("value", new ArrayImpl()),
                        "Arrays are not supported"));
    }

    @Test
    void getMetaData() throws SQLException {
        retry(connection ->
                assertThrowsMsg(SQLFeatureNotSupportedException.class,
                        () -> getTextStatement(connection).getMetaData(),
                        "ResultSet metadata is not supported in prepared statements"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text"})
    void setURL(String type) throws SQLException, MalformedURLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setURL,
                YdbPreparedStatement::setURL,
                ResultSet::getURL,
                Arrays.asList(
                        pair(new URL("https://localhost"), new URL("https://localhost")),
                        pair(new URL("ftp://localhost"), new URL("ftp://localhost"))
                ),
                Arrays.asList(
                        pair("https://localhost", new URL("https://localhost")),
                        pair("ftp://localhost".getBytes(), new URL("ftp://localhost")),
                        pair(stream("https://localhost"), new URL("https://localhost")),
                        pair(reader("ftp://localhost"), new URL("ftp://localhost"))
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d)
                )
        );
    }

    @Test
    void setRowId() throws SQLException {
        retry(connection ->
                assertThrowsMsg(SQLFeatureNotSupportedException.class,
                        () -> getTextStatement(connection).setRowId(1, new RowIdImpl()),
                        "RowIds are not supported"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text"})
    void setNString(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setNString,
                YdbPreparedStatement::setNString,
                ResultSet::getNString,
                Arrays.asList(
                        pair("", ""),
                        pair("test1", "test1")
                ),
                Arrays.asList(
                        pair(1d, "1.0"),
                        pair(0d, "0.0"),
                        pair(-1d, "-1.0"),
                        pair(127d, "127.0"),
                        pair((byte) 4, "4"),
                        pair((short) 5, "5"),
                        pair(6, "6"),
                        pair(7L, "7"),
                        pair(8f, "8.0"),
                        pair(9d, "9.0"),
                        pair(true, "true"),
                        pair(false, "false"),
                        pair("".getBytes(), ""),
                        pair("test2".getBytes(), "test2"),
                        pair(stream("test3"), "test3"),
                        pair(reader("test4"), "test4")
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d),
                        PrimitiveValue.newJson("test")
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void setNCharacterStream(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setNCharacterStream,
                YdbPreparedStatement::setNCharacterStream,
                ResultSet::getNCharacterStream,
                Arrays.asList(
                        pair(reader("[4]"), reader("[4]"))
                ),
                Arrays.asList(
                        pair("[1]", reader("[1]")),
                        pair("[2]".getBytes(), reader("[2]")),
                        pair(stream("[3]"), reader("[3]"))
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void setNCharacterStreamLong(String type) throws SQLException {
        checkInsert("c_" + type, type,
                (ps, name, value) -> ps.setNCharacterStream(name, value, 3L),
                (ps, name, value) -> ps.setNCharacterStream(name, value, 3L),
                ResultSet::getNCharacterStream,
                Arrays.asList(
                        pair(reader("[4]-limited!"), reader("[4]"))
                ),
                Arrays.asList(),
                Arrays.asList()
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void asNClob(String type) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setNClob,
                YdbPreparedStatement::setNClob,
                ResultSet::getCharacterStream,
                Arrays.asList(
                        pair(reader("[4]"), reader("[4]"))
                ),
                Arrays.asList(
                        pair("[1]", reader("[1]")),
                        pair("[2]".getBytes(), reader("[2]")),
                        pair(stream("[3]"), reader("[3]"))
                ),
                Arrays.asList(
                        PrimitiveValue.newBool(true),
                        PrimitiveValue.newBool(true).makeOptional(),
                        PrimitiveValue.newDouble(1.1d)
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"Bytes", "Text", "Json", "JsonDocument", "Yson"})
    void asNClobLong(String type) throws SQLException {
        checkInsert("c_" + type, type,
                (ps, name, reader) -> ps.setNClob(name, reader, 3L),
                (ps, name, reader) -> ps.setNClob(name, reader, 3L),
                ResultSet::getCharacterStream,
                Arrays.asList(
                        pair(reader("[4]-limited!"), reader("[4]"))
                ),
                Arrays.asList(),
                Arrays.asList()
        );
    }

    @Test
    void setNClobUnsupported() throws SQLException {
        retry(connection ->
                assertThrowsMsg(SQLFeatureNotSupportedException.class,
                        () -> getTextStatement(connection).setNClob("value", new NClobImpl("".toCharArray())),
                        "NClobs are not supported"));
    }

    @Test
    void setSQLXML() throws SQLException {
        retry(connection ->
                assertThrowsMsg(SQLFeatureNotSupportedException.class,
                        () -> getTextStatement(connection).setSQLXML("value", new SQLXMLImpl()),
                        "SQLXMLs are not supported"));
    }

    @ParameterizedTest
    @MethodSource("bytesAndText")
    void setObject(String type, List<Pair<Object, Object>> callSetObject, List<Object> unsupported) throws SQLException {
        checkInsert("c_" + type, type,
                YdbPreparedStatement::setObject,
                YdbPreparedStatement::setObject,
                ResultSet::getObject,
                Arrays.asList(
                        pair("", ""),
                        pair("test1", "test1"),
                        pair(1d, "1.0"),
                        pair(0d, "0.0"),
                        pair(-1d, "-1.0"),
                        pair(127d, "127.0"),
                        pair((byte) 4, "4"),
                        pair((short) 5, "5"),
                        pair(6, "6"),
                        pair(7L, "7"),
                        pair(8f, "8.0"),
                        pair(9d, "9.0"),
                        pair(true, "true"),
                        pair(false, "false"),
                        pair("".getBytes(), ""),
                        pair("test2".getBytes(), "test2"),
                        pair(stream("test3"), "test3"),
                        pair(reader("test4"), "test4")
                ),
                callSetObject,
                merge(unsupported,
                        Arrays.asList(
                                PrimitiveValue.newBool(true),
                                PrimitiveValue.newBool(true).makeOptional(),
                                PrimitiveValue.newDouble(1.1d),
                                PrimitiveValue.newJson("test")
                        ))
        );
    }

    @Test
    public void unknownColumns() throws SQLException {
        retry(connection ->
                assertThrowsMsg(SQLException.class,
                        () -> {
                            YdbPreparedStatement statement = getTextStatement(connection);
                            statement.setObject("column0", "value");
                            statement.execute();
                        },
                        "Parameter not found: " + (expectParameterPrefixed() ? "$column0" : "column0")));
    }

    @Test
    public void queryInList() throws SQLException {
        DecimalType defaultType = YdbTypes.DEFAULT_DECIMAL_TYPE;

        Set<String> skip = set("c_Bool", "c_Json", "c_JsonDocument", "c_Yson");
        List<Map<String, Object>> values = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            Map<String, Object> params = new LinkedHashMap<>();
            int prefix = 100 * i;
            params.put("c_Bool", i % 2 == 0);
            params.put("c_Int32", prefix++);
            params.put("c_Int64", (long) prefix++);
            params.put("c_Uint8", PrimitiveValue.newUint8((byte) prefix++).makeOptional());
            params.put("c_Uint32", PrimitiveValue.newUint32(prefix++).makeOptional());
            params.put("c_Uint64", PrimitiveValue.newUint64(prefix++).makeOptional());
            params.put("c_Float", (float) prefix++);
            params.put("c_Double", (double) prefix++);
            params.put("c_Bytes", PrimitiveValue.newBytes(String.valueOf(prefix++).getBytes()).makeOptional());
            params.put("c_Text", String.valueOf(prefix++));
            params.put("c_Json", PrimitiveValue.newJson("[" + (prefix++) + "]").makeOptional());
            params.put("c_JsonDocument", PrimitiveValue.newJsonDocument("[" + (prefix++) + "]").makeOptional());
            params.put("c_Yson", PrimitiveValue.newYson(("[" + (prefix++) + "]").getBytes()).makeOptional());
            params.put("c_Date", new Date(MILLIS_IN_DAY * (prefix++)));
            params.put("c_Datetime", new Time(MILLIS_IN_DAY * (prefix++) + 111000));
            params.put("c_Timestamp", Instant.ofEpochMilli(MILLIS_IN_DAY * (prefix++) + 112112));
            params.put("c_Interval", Duration.of(prefix++, ChronoUnit.MICROS));
            params.put("c_Decimal", defaultType.newValue((prefix) + ".1").makeOptional());
            values.add(params);
        }

        retry(connection -> {
            YdbPreparedStatement statement = getTestAllValuesStatement(connection);
            int key = 0;
            for (Map<String, Object> params : values) {
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    statement.setObject(entry.getKey(), entry.getValue());
                }
                statement.setInt("key", ++key);
                statement.executeUpdate();
            }
            connection.commit();
        });

        for (String key : values.get(0).keySet()) {
            if (skip.contains(key)) {
                continue;
            }
            retry(false, connection -> {
                String type = key.substring("c_".length());
                if (type.equals("Decimal")) {
                    type = defaultType.toString();
                }
                YdbPreparedStatement ps = connection.prepareStatement(String.format(
                        "declare $keys as List<%s?>;\n" +
                                "select count(1) as rows from unit_2 where %s in $keys",
                        type, key));

                ps.setObject("keys", Arrays.asList());
                checkRows(0, ps.executeQuery());

                ps.setObject("keys", Arrays.asList((Object) null));
                checkRows(0, ps.executeQuery());

                ps.setObject("keys", Arrays.asList(
                        values.get(0).get(key)));
                checkRows(1, ps.executeQuery());

                ps.setObject("keys", Arrays.asList(
                        values.get(0).get(key),
                        values.get(1).get(key)));
                checkRows(2, ps.executeQuery());

                ps.setObject("keys", Arrays.asList(
                        values.get(0).get(key),
                        null,
                        values.get(1).get(key)));
                checkRows(2, ps.executeQuery());

                ps.setObject("keys", Arrays.asList(
                        values.get(0).get(key),
                        values.get(1).get(key),
                        values.get(2).get(key)));
                checkRows(3, ps.executeQuery());
            });
        }

        retry(false, connection -> {
            YdbPreparedStatement ps = connection.prepareStatement(String.format(
                    "declare $keys as List<%s?>;\n" +
                            "select count(1) as rows from unit_2 where %s in $keys",
                    "Bool", "c_Bool"));

            ps.setObject("keys", Arrays.asList());
            checkRows(0, ps.executeQuery());

            ps.setObject("keys", Arrays.asList((Object) null));
            checkRows(0, ps.executeQuery());

            ps.setObject("keys", Arrays.asList(true));
            checkRows(1, ps.executeQuery());

            ps.setObject("keys", Arrays.asList(false));
            checkRows(2, ps.executeQuery());

            ps.setObject("keys", Arrays.asList(true, false));
            checkRows(3, ps.executeQuery());

            ps.setObject("keys", Arrays.asList(true, null, false));
            checkRows(3, ps.executeQuery());
        });
    }

    @Test
    void unwrap() throws SQLException {
        retry(connection -> {
            YdbPreparedStatement statement = getTextStatement(connection);
            assertTrue(statement.isWrapperFor(YdbPreparedStatement.class));
            assertSame(statement, statement.unwrap(YdbPreparedStatement.class));

            assertFalse(statement.isWrapperFor(YdbConnection.class));
            assertThrowsMsg(SQLException.class,
                    () -> statement.unwrap(YdbConnection.class),
                    "Cannot unwrap to " + YdbConnection.class);
        });
    }


    //

    static Collection<Arguments> bytesAndText() {
        return Arrays.asList(
                Arguments.of("Bytes",
                        Arrays.asList(
                                pair(PrimitiveValue.newBytes("test-bytes".getBytes()),
                                        "test-bytes"),
                                pair(PrimitiveValue.newBytes("test-bytes".getBytes()).makeOptional(),
                                        "test-bytes")
                        ),
                        Arrays.asList(
                                PrimitiveValue.newText("test-utf8"),
                                PrimitiveValue.newText("test-utf8").makeOptional()
                        )
                ),
                Arguments.of("Text",
                        Arrays.asList(
                                pair(PrimitiveValue.newText("test-utf8"), "test-utf8"),
                                pair(PrimitiveValue.newText("test-utf8").makeOptional(), "test-utf8")
                        ),
                        Arrays.asList(
                                PrimitiveValue.newBytes("test-bytes".getBytes()),
                                PrimitiveValue.newBytes("test-bytes".getBytes()).makeOptional()
                        )
                )
        );
    }

    static Collection<Arguments> jsonAndJsonDocumentAndYson() {
        return Arrays.asList(
                Arguments.of("Json",
                        Arrays.asList(
                                pair(PrimitiveValue.newJson("[1]"), "[1]"),
                                pair(PrimitiveValue.newJson("[1]").makeOptional(), "[1]")
                        ),
                        Arrays.asList(
                                PrimitiveValue.newText("test-utf8"),
                                PrimitiveValue.newText("test-utf8").makeOptional()
                        )
                ),
                Arguments.of("JsonDocument",
                        Arrays.asList(
                                pair(PrimitiveValue.newJsonDocument("[1]"), "[1]"),
                                pair(PrimitiveValue.newJsonDocument("[1]").makeOptional(), "[1]")
                        ),
                        Arrays.asList(
                                PrimitiveValue.newText("test-utf8"),
                                PrimitiveValue.newText("test-utf8").makeOptional()
                        )
                ),
                Arguments.of("Yson",
                        Arrays.asList(
                                pair(PrimitiveValue.newYson("[1]".getBytes()), "[1]"),
                                pair(PrimitiveValue.newYson("[1]".getBytes()).makeOptional(), "[1]")
                        ),
                        Arrays.asList(
                                PrimitiveValue.newText("test-utf8"),
                                PrimitiveValue.newText("test-utf8").makeOptional()
                        )
                )
        );
    }
*/
}
