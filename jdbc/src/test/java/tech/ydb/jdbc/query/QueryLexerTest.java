package tech.ydb.jdbc.query;


import java.sql.SQLException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class QueryLexerTest {

    private QueryType parseQueryType(YdbQueryOptions opts, String sql) throws SQLException {
        return YdbQuery.from(opts, sql).type();
    }

    private String parseQuery(YdbQueryOptions opts, String sql) throws SQLException {
        return YdbQuery.from(opts, sql).getYqlQuery(null);
    }

    private void assertMixType(YdbQueryOptions opts, String types, String sql) {
        SQLException ex = Assertions.assertThrows(SQLException.class, () -> {
            YdbQueryBuilder builder = new YdbQueryBuilder(sql, null);
            JdbcQueryLexer.buildQuery(builder, opts);
        }, "Mix type query must throw SQLException");
        Assertions.assertEquals("Query cannot contain expressions with different types: " + types, ex.getMessage());
    }

    @Test
    public void enforceV1Test() throws SQLException {
        YdbQueryOptions disabled = new YdbQueryOptions(false, true, true, true, true, true, null);
        YdbQueryOptions enabled = new YdbQueryOptions(true, true, true, true, true, true, null);

        Assertions.assertEquals("CREATE TABLE test_table (id int, value text)",
                parseQuery(disabled, "CREATE TABLE test_table (id int, value text)"));

        Assertions.assertEquals("--!syntax_v1\nCREATE TABLE test_table (id int, value text)",
                parseQuery(enabled, "CREATE TABLE test_table (id int, value text)"));
    }

    @Test
    public void queryTypesTest() throws SQLException {
        YdbQueryOptions opts = new YdbQueryOptions(false, true, false, false, false, false, null);

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "CREATE TABLE test_table (id int, value text)"
        ));
        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "\tcreate TABLE test_table2 (id int, value text);"
        ));

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                " drop TABLE test_table1 (id int, value text);" +
                "ALTER TABLE test_table2 (id int, value text);"
        ));

        Assertions.assertEquals(QueryType.DATA_QUERY, parseQueryType(opts,
                "SELECT id, value FROM test_table"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parseQueryType(opts,
                "UPSERT INTO test_table VALUES (?, ?)"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parseQueryType(opts,
                "DELETE FROM test_table"
        ));
        Assertions.assertEquals(QueryType.DATA_QUERY, parseQueryType(opts,
                "SELECT id, value FROM test_table;\n" +
                "UPSERT INTO test_table VALUES (?, ?);" +
                "DELETE FROM test_table"
        ));

        Assertions.assertEquals(QueryType.DATA_QUERY, parseQueryType(opts,
                "SELECT id, value FROM test_table;\n" +
                "UPDATE test_table SET value = ? WHERE id = ?;" +
                "SELECT id, value FROM test_table WHERE id=CREATE"
        ));

        Assertions.assertEquals(QueryType.SCAN_QUERY, parseQueryType(opts,
                "SCAN SELECT id, value FROM test_table"
        ));

        Assertions.assertEquals(QueryType.EXPLAIN_QUERY, parseQueryType(opts,
                "EXPLAIN SELECT id, value FROM test_table"
        ));
    }

    @Test
    public void mixQueryExceptionTest() throws SQLException {
        YdbQueryOptions opts = new YdbQueryOptions(false, true, false, false, false, false, null);

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
    public void forsedTypeTest() throws SQLException {
        YdbQueryOptions opts = new YdbQueryOptions(false, true, false, false, false, false, QueryType.SCHEME_QUERY);

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "CREATE TABLE test_table (id int, value text)"
        ));
        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "\tcreate TABLE test_table2 (id int, value text);"
        ));

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                " drop TABLE test_table1 (id int, value text);" +
                "ALTER TABLE test_table2 (id int, value text);"
        ));

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "SELECT id, value FROM test_table"
        ));
        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "UPSERT INTO test_table VALUES (?, ?)"
        ));
        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "DELETE FROM test_table"
        ));
        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "SELECT id, value FROM test_table;\n" +
                "UPSERT INTO test_table VALUES (?, ?);" +
                "DELETE FROM test_table"
        ));

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "SELECT id, value FROM test_table;\n" +
                "UPDATE test_table SET value = ? WHERE id = ?;" +
                "SELECT id, value FROM test_table WHERE id=CREATE"
        ));

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "SCAN SELECT id, value FROM test_table"
        ));

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "EXPLAIN SELECT id, value FROM test_table"
        ));

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "CREATE TABLE test_table (id int, value text);" +
                "SELECT * FROM test_table;"
        ));

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "DROP TABLE test_table (id int, value text);SELECT * FROM test_table;"
        ));

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "SELECT * FROM test_table;CREATE TABLE test_table (id int, value text);"
        ));

        Assertions.assertEquals(QueryType.SCHEME_QUERY, parseQueryType(opts,
                "SELECT * FROM test_table;\n\tCREATE TABLE test_table (id int, value text);"
        ));
    }

}
