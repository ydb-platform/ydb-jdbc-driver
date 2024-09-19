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
    @ParameterizedTest(name = "[{index}] {0} is empty query")
    @CsvSource(value = {
        "--just comment~--just comment",
        "EXPLAIN;~;",
        "SCAN;~;",
        "BULK;~;",
    }, delimiter = '~')
    public void emptyQueryTest(String sql, String prepared) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        String parsed = parser.parseSQL(sql);
        Assertions.assertEquals(prepared, parsed);
        Assertions.assertEquals(0, parser.getStatements().size());
    }

    @ParameterizedTest(name = "[{index}] {0} is explain query")
    @CsvSource(value = {
        "'Explain select * from table', ' select * from table', SELECT",
        "'exPlain\nupsert to', '\nupsert to', INSERT_UPSERT",
        "'  Explain select * from table', '   select * from table', SELECT",
        "'\texPlain\nupsert to', '\t\nupsert to', INSERT_UPSERT",
        "'EXPLAIN/*comment*/UPSERT INTO', '/*comment*/UPSERT INTO', INSERT_UPSERT",
    })
    public void explainQueryTest(String sql, String prepared, String cmd) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        String parsed = parser.parseSQL(sql);

        Assertions.assertEquals(prepared, parsed);

        Assertions.assertEquals(1, parser.getStatements().size());
        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.EXPLAIN_QUERY, statement.getType());
        Assertions.assertEquals(cmd, statement.getCmd().toString());
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
        Assertions.assertEquals(QueryCmd.CREATE_ALTER_DROP, statement.getCmd());
    }

    @ParameterizedTest(name = "[{index}] {0} is data query")
    @ValueSource(strings = {
        "ALTERED;",
        "SCANER SELECT 1;",
        "bulked select 1;",
        "\ndrops;",
        "BuLK_INSERT;",
    })
    public void unknownQueryTest(String sql) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        String parsed = parser.parseSQL(sql);

        Assertions.assertEquals(sql, parsed);

        Assertions.assertEquals(1, parser.getStatements().size());
        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.UNKNOWN, statement.getType());
        Assertions.assertEquals(QueryCmd.UNKNOWN, statement.getCmd());
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

    @ParameterizedTest(name = "[{index}] {0} has offset or limit parameter")
    @ValueSource(strings = {
        "select * from test_table where true=true -- test request\noffset /* comment */ ? limit 20",
        "select * from test_table where true=true /*comm*/offset ?\tlimit\t\n?;",
        "select offset, limit from test_table  offset 20 limit -- comment\n?;",
    })
    public void offsetParameterTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        parser.parseSQL(query);

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());

        Assertions.assertFalse(statement.getParams().isEmpty());
        int idx = 0;
        for (ParamDescription prm : statement.getParams()) {
            idx++;
            Assertions.assertEquals("$jp" + idx, prm.name());
            Assertions.assertNotNull(prm.type()); // forced UInt64 type
            Assertions.assertEquals(PrimitiveType.Uint64, prm.type().ydbType()); // forced UInt64 type
        }
    }

    @ParameterizedTest(name = "[{index}] {0} hasn't offset or limit parameter")
    @ValueSource(strings = {
        "select * from test_table where limit = ? or offset = ?",
        "update test_table set limit = ?, offset = ? where id = ?",
        "select * from test_table where limit=? or offset=?",
        "update test_table set limit=?, offset=? where id=?",
        "select * from test_table where limit? or offset?",
    })
    public void noOffsetParameterTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        parser.parseSQL(query);

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());

        Assertions.assertFalse(statement.getParams().isEmpty());
        int idx = 0;
        for (ParamDescription prm : statement.getParams()) {
            idx++;
            Assertions.assertEquals("$jp" + idx, prm.name());
            Assertions.assertNull(prm.type()); // uknown type
        }
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

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.INSERT_UPSERT, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertFalse(batch.isUpsert());
        Assertions.assertTrue(batch.isInsert());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertEquals(3, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is bulk insert query")
    @ValueSource(strings = {
        "Bulk\nInsert into table_name(c1, c2, c3) values (?, ? , ?)",
        "\n bUlk   insert into `table_name`  (\t`c1`, c2, c3)values(?, ? , ?)",
        "--comment1\nbulk/* comment */Insert into `table_name`  (`c1`, /* commect */ c2, c3)values(?, ? , ?);",
        ";;BUlk  Insert into table_name (`c1`, /* comment */ c2, c3 )   values(?, ? , ?);",
    })
    public void validBulkInsertTest(String sql) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        parser.parseSQL(sql);

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.BULK_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.INSERT_UPSERT, parser.getStatements().get(0).getCmd());

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

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.INSERT_UPSERT, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertTrue(batch.isUpsert());
        Assertions.assertFalse(batch.isInsert());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertEquals(3, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched upsert query")
    @ValueSource(strings = {
        "Bulk   Upsert into table_name(c1, c2, c3) values (?, ? , ?)",
        "\nbulk\t\nupsert into `table_name`  (\t`c1`, c2, c3)values(?, ? , ?)",
        "/* test */Bulk/* comment */ Upsert into `table_name`  (`c1`, c2, c3)values(?, ? , ?);\n-- post comment",
        ";;Bulk Upsert/* comment */into table_name (`c1`, /* comment */ c2, c3 )   values(?, ? , ?);",
    })
    public void validBulkUpsertTest(String sql) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        parser.parseSQL(sql);

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.BULK_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.INSERT_UPSERT, parser.getStatements().get(0).getCmd());

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
        "upsert into table_name (c1, c2, c3) values (?, ?, ?) returning c1, c2, c3;",
        "upsert into table_name (c1, c2, c3) values (?, ?, ?); select 1;",
    })
    public void invalidBatchedTest(String sql) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(true, true);
        parser.parseSQL(sql);

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertFalse(batch.isValidBatch());
    }
}
