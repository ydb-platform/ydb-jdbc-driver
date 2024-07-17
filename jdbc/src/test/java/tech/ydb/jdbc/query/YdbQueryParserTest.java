package tech.ydb.jdbc.query;

import java.sql.SQLException;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.table.values.PrimitiveType;



/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryParserTest {
    @ParameterizedTest(name = "[{index}] {0} is explain query")
    @CsvSource(value = {
        "'Explain select * from table', ' select * from table'",
        "'exPlain\nupsert to', '\nupsert to'",
        "'  Explain select * from table', '   select * from table'",
        "'\texPlain\nupsert to', '\t\nupsert to'",
        "'EXPLAIN/*comment*/UPSERT INTO', '/*comment*/UPSERT INTO'",
        "'EXPLAIN',''",
    })
    public void explainQueryTest(String sql, String prepared) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        String parsed = parser.parseSQL(sql);

        Assertions.assertEquals(prepared, parsed);

        Assertions.assertEquals(1, parser.getStatements().size());
        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.EXPLAIN_QUERY, statement.getType());
    }

    @ParameterizedTest(name = "[{index}] {0} is scheme query")
    @ValueSource(strings = {
        "  Alter table set;",
        "Alter--comment\ntable set;",
        "drOp table 'test'",
        "-- comment \nCreate;",
    })
    public void schemeQueryTest(String sql) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        String parsed = parser.parseSQL(sql);

        Assertions.assertEquals(sql, parsed);

        Assertions.assertEquals(1, parser.getStatements().size());
        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.SCHEME_QUERY, statement.getType());
    }

    @Test
    public void wrongSqlCommandTest() throws SQLException {
        String query = "SC;";
        YdbQueryParser parser = new YdbQueryParser(true, true);
        String parsed = parser.parseSQL(query);
        Assertions.assertEquals(query, parsed);

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.UNKNOWN, parser.getStatements().get(0).getType());
    }

    @Test
    public void yqlUpsertTest() throws SQLException {
        String query = ""
                + "declare $p1 as Int32;\n"
                + "declare $p2 as Text;\n"
                + "upsert into tableName (key, c_Text) values ($p1, $p2)";

        YdbQueryParser parser = new YdbQueryParser(true, true);
        String parsed = parser.parseSQL(query);
        Assertions.assertEquals(query, parsed);

        Assertions.assertEquals(3, parser.getStatements().size());
        Assertions.assertEquals(QueryType.UNKNOWN, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryType.UNKNOWN, parser.getStatements().get(1).getType());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(2).getType());
    }

    @Test
    public void yqlSelectWithKeyTest() throws SQLException {
        String query = ""
                + "declare $key as Optional<Int32>;\n"
                + "select key, column from tableName where key=$key";

        YdbQueryParser parser = new YdbQueryParser(true, true);
        String parsed = parser.parseSQL(query);
        Assertions.assertEquals(query, parsed);

        Assertions.assertEquals(2, parser.getStatements().size());
        Assertions.assertEquals(QueryType.UNKNOWN, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(1).getType());

        Assertions.assertEquals(QueryType.DATA_QUERY, parser.detectQueryType());
    }

    @Test
    public void scanSelectWithKeyTest() throws SQLException {
        String query = ""
                + "declare $key as Optional<Int32>;\n"
                + "scan select key, column from tableName where key=$key";

        YdbQueryParser parser = new YdbQueryParser(true, true);
        String parsed = parser.parseSQL(query);
        Assertions.assertEquals("declare $key as Optional<Int32>;\n"
                + " select key, column from tableName where key=$key", parsed);

        Assertions.assertEquals(2, parser.getStatements().size());
        Assertions.assertEquals(QueryType.UNKNOWN, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryType.SCAN_QUERY, parser.getStatements().get(1).getType());

        Assertions.assertEquals(QueryType.SCAN_QUERY, parser.detectQueryType());
    }

    @Test
    public void offsetParameterTest() throws SQLException {
        String query = ""
                + "select * from test_table where true=true -- test request\n"
                + " offset /* comment */ ? limit 20";

        YdbQueryParser parser = new YdbQueryParser(true, true);
        String parsed = parser.parseSQL(query);
        Assertions.assertEquals(""
                + "select * from test_table where true=true -- test request\n"
                + " offset /* comment */ $jp1 limit 20",
                parsed);

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());
        Assertions.assertEquals(1, statement.getParams().size());

        ParamDescription prm1 = statement.getParams().get(0);
        Assertions.assertEquals("$jp1", prm1.name());
        Assertions.assertNotNull(prm1.type());
        Assertions.assertEquals(PrimitiveType.Uint64, prm1.type().ydbType());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched insert query")
    @ValueSource(strings = {
        "Insert into table_name(c1, c2, c3) values (?, ? , ?)",
        "\n  insert into `table_name`  (\t`c1`, c2, c3)values(?, ? , ?)",
        "/* comment */ Insert into `table_name`  (`c1`, /* commect */ c2, c3)values(?, ? , ?);\n-- post comment",
        ";;Insert into table_name (`c1`, /* comment */ c2, c3 )   values(?, ? , ?);",
    })
    public void validBatchedInsertTest(String sql) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        parser.parseSQL(sql);

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertFalse(batch.isUpsert());
        Assertions.assertTrue(batch.isInsert());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertEquals(3, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched upsert query")
    @ValueSource(strings = {
        "Upsert into table_name(c1, c2, c3) values (?, ? , ?)",
        "\n  upsert into `table_name`  (\t`c1`, c2, c3)values(?, ? , ?)",
        "/* comment */ Upsert into `table_name`  (`c1`, /* commect */ c2, c3)values(?, ? , ?);\n-- post comment",
        ";;Upsert/* comment */into table_name (`c1`, /* comment */ c2, c3 )   values(?, ? , ?);",
    })
    public void validBatchedUpsertTest(String sql) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        parser.parseSQL(sql);

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertTrue(batch.isUpsert());
        Assertions.assertFalse(batch.isInsert());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertEquals(3, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is not batched query")
    @ValueSource(strings = {
        "Insert into table_name(c1, c2, c3) values (?, ? , ?); Insert into table_name(c1, c2, c3) values (?, ? , ?);",
        "ipsert into table_name(c1, c2, c3) values (?, ? , ?)",
        "upsert into (c1, c2, c3) values (?, ? , ?)",
        "upsert into table_name (c1, c2, c3); values (?, ?, ?)",
        "upsert into table_name (c1, , c3) values (?, ?)",
        "upsert into table_name (c1, c2) values (?,,?)",
        "upsert into table_name (c1, c2, c3) values (?, ?, ?,)",
        "upsert into table_name (c1, c2, c3,) values (?, ?, ?)",
        "upsert into table_name (c1, c2, c3) (?, ?, ?)",
        "upsert into table_name (c1, c2, c3) values (?, ?)",
        "upsert into table_name (c1, c2, c3) values (?, ?, 123)",
    })
    public void invalidBatchedTest(String sql) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        parser.parseSQL(sql);

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertFalse(batch.isValidBatch());
    }
}
