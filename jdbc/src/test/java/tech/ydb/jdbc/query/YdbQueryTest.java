package tech.ydb.jdbc.query;


import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.ydb.jdbc.settings.YdbQueryProperties;



/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryTest {
    private class ParamsBuilder {
        private final Properties props = new Properties();

        ParamsBuilder with(String name, String value) {
            props.put(name, value);
            return this;
        }

        YdbQueryProperties build() throws SQLException {
            return new YdbQueryProperties(props);
        }
    }

    private static QueryType parsedQueryType(YdbQueryProperties opts, String sql) throws SQLException {
        return YdbQuery.parseQuery(sql, opts).getType();
    }

    private void assertMixType(YdbQueryProperties opts, String types, String sql) {
        SQLException ex = Assertions.assertThrows(SQLException.class,
                () -> YdbQuery.parseQuery(sql, opts),
                "Mix type query must throw SQLException"
        );
        Assertions.assertEquals("Query cannot contain expressions with different types: " + types, ex.getMessage());
    }

    @Test
    public void queryTypesTest() throws SQLException {
        YdbQueryProperties opts = new ParamsBuilder().build();

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parsedQueryType(opts,
                "CREATE TABLE test_table (id int, value text)"
        ));
        Assertions.assertEquals(QueryType.SCHEME_QUERY, parsedQueryType(opts,
                "\tcreate TABLE test_table2 (id int, value text);"
        ));

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parsedQueryType(opts,
                " drop TABLE test_table1 (id int, value text);" +
                "ALTER TABLE test_table2 (id int, value text);"
        ));

        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "UPSERT INTO test_table VALUES (?, ?)"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "DELETE FROM test_table"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table;\n" +
                "UPSERT INTO test_table VALUES (?, ?);" +
                "DELETE FROM test_table"
        ));

        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table;\n" +
                "UPDATE test_table SET value = ? WHERE id = ?;" +
                "SELECT id, value FROM test_table WHERE id=CREATE"
        ));

        Assertions.assertEquals(QueryType.SCAN_QUERY, parsedQueryType(opts,
                "SCAN SELECT id, value FROM test_table"
        ));

        Assertions.assertEquals(QueryType.EXPLAIN_QUERY, parsedQueryType(opts,
                "EXPLAIN SELECT id, value FROM test_table"
        ));
    }

    @Test
    public void mixQueryExceptionTest() throws SQLException {
        YdbQueryProperties opts = new ParamsBuilder().build();

        assertMixType(opts, "SCHEME_QUERY, DATA_QUERY",
                "CREATE TABLE test_table (id int, value text);" +
                "SELECT * FROM test_table;"
        );

        assertMixType(opts, "SCHEME_QUERY, DATA_QUERY",
                "DROP TABLE test_table (id int, value text);SELECT * FROM test_table;"
        );

        assertMixType(opts, "DATA_QUERY, SCHEME_QUERY",
                "SELECT * FROM test_table;CREATE TABLE test_table (id int, value text);"
        );

        assertMixType(opts, "DATA_QUERY, SCHEME_QUERY",
                "SELECT * FROM test_table;\n\tCREATE TABLE test_table (id int, value text);"
        );
    }

    @Test
    public void forsedScanSelects() throws SQLException {
        YdbQueryProperties opts = new ParamsBuilder()
                .with("forceScanSelect", "true")
                .build();

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parsedQueryType(opts,
                "CREATE TABLE test_table (id int, value text)"
        ));
        Assertions.assertEquals(QueryType.SCHEME_QUERY, parsedQueryType(opts,
                " drop TABLE test_table1 (id int, value text);" +
                "ALTER TABLE test_table2 (id int, value text);"
        ));

        Assertions.assertEquals(QueryType.SCAN_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table"
        ));
        Assertions.assertEquals(QueryType.SCAN_QUERY, parsedQueryType(opts,
                "SCAN SELECT id, value FROM test_table"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table;" +
                "SELECT id, value FROM test_table2;"
        ));

        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "UPSERT INTO test_table VALUES (?, ?)"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "DELETE FROM test_table"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table;\n" +
                "UPSERT INTO test_table VALUES (?, ?);" +
                "DELETE FROM test_table"
        ));

        Assertions.assertEquals(QueryType.EXPLAIN_QUERY, parsedQueryType(opts,
                "EXPLAIN SELECT id, value FROM test_table"
        ));
    }

    @Test
    public void forsedBulkUpsert() throws SQLException {
        YdbQueryProperties opts = new ParamsBuilder()
                .with("forceBulkUpsert", "true")
                .build();

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parsedQueryType(opts,
                "CREATE TABLE test_table (id int, value text)"
        ));
        Assertions.assertEquals(QueryType.SCHEME_QUERY, parsedQueryType(opts,
                " drop TABLE test_table1 (id int, value text);" +
                "ALTER TABLE test_table2 (id int, value text);"
        ));

        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table"
        ));
        Assertions.assertEquals(QueryType.SCAN_QUERY, parsedQueryType(opts,
                "SCAN SELECT id, value FROM test_table"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table;" +
                "SELECT id, value FROM test_table2;"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "INSERT INTO test_table(id, value) VALUES (?, ?)"
        ));
        Assertions.assertEquals(QueryType.BULK_QUERY, parsedQueryType(opts,
                "UPSERT INTO test_table(id, value) VALUES (?, ?)"
        ));

        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "DELETE FROM test_table"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table;\n" +
                "UPSERT INTO test_table VALUES (?, ?);" +
                "DELETE FROM test_table"
        ));

        Assertions.assertEquals(QueryType.EXPLAIN_QUERY, parsedQueryType(opts,
                "EXPLAIN SELECT id, value FROM test_table"
        ));
    }

    @Test
    public void forcedUpsert() throws SQLException {
        YdbQueryProperties opts = new ParamsBuilder()
                .with("replaceInsertByUpsert", "true")
                .with("forceBulkUpsert", "true")
                .build();

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parsedQueryType(opts,
                "CREATE TABLE test_table (id int, value text)"
        ));
        Assertions.assertEquals(QueryType.SCHEME_QUERY, parsedQueryType(opts,
                " drop TABLE test_table1 (id int, value text);" +
                "ALTER TABLE test_table2 (id int, value text);"
        ));

        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table"
        ));
        Assertions.assertEquals(QueryType.SCAN_QUERY, parsedQueryType(opts,
                "SCAN SELECT id, value FROM test_table"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table;" +
                "SELECT id, value FROM test_table2;"
        ));

        Assertions.assertEquals(QueryType.BULK_QUERY, parsedQueryType(opts,
                "INSERT INTO test_table(id, value) VALUES (?, ?)"
        ));
        Assertions.assertEquals(QueryType.BULK_QUERY, parsedQueryType(opts,
                "UPSERT INTO test_table(id, value) VALUES (?, ?)"
        ));

        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "DELETE FROM test_table"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parsedQueryType(opts,
                "SELECT id, value FROM test_table;\n" +
                "UPSERT INTO test_table VALUES (?, ?);" +
                "DELETE FROM test_table"
        ));

        Assertions.assertEquals(QueryType.EXPLAIN_QUERY, parsedQueryType(opts,
                "EXPLAIN SELECT id, value FROM test_table"
        ));
    }
}
