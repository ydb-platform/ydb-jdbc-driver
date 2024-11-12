package tech.ydb.jdbc.query;

import java.sql.SQLException;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.jdbc.query.params.JdbcParameter;
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
    public void emptyQueryTest(String query, String prepared) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        String parsed = parser.parseSQL();
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
    public void explainQueryTest(String query, String prepared, String cmd) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        String parsed = parser.parseSQL();

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
    public void schemeQueryTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        String parsed = parser.parseSQL();

        Assertions.assertEquals(query, parsed);

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
    public void unknownQueryTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        String parsed = parser.parseSQL();

        Assertions.assertEquals(query, parsed);

        Assertions.assertEquals(1, parser.getStatements().size());
        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.UNKNOWN, statement.getType());
        Assertions.assertEquals(QueryCmd.UNKNOWN, statement.getCmd());
    }

    @Test
    public void wrongSqlCommandTest() throws SQLException {
        String query = "SC;";
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        String parsed = parser.parseSQL();
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

        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        String parsed = parser.parseSQL();
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

        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        String parsed = parser.parseSQL();
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

        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        String parsed = parser.parseSQL();
        Assertions.assertEquals("declare $key as Optional<Int32>;\n"
                + " select key, column from tableName where key=$key", parsed);

        Assertions.assertEquals(2, parser.getStatements().size());
        Assertions.assertEquals(QueryType.UNKNOWN, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryType.SCAN_QUERY, parser.getStatements().get(1).getType());

        Assertions.assertEquals(QueryType.SCAN_QUERY, parser.detectQueryType());
    }

    @ParameterizedTest(name = "[{index}] {0} has offset or limit parameter")
    @CsvSource(value = {
        "'select * from test_table where true=true -- test request\noffset /* comment */ ? limit 20'"
            + "@'select * from test_table where true=true -- test request\noffset /* comment */ $jp1 limit 20'",
        "'select * from test_table where true=true /*comm*/offset ?\tlimit\t\n?'"
            + "@'select * from test_table where true=true /*comm*/offset $jp1\tlimit\t\n$jp2'",
        "'select offset, limit from test_table  offset 20 limit -- comment\n?;'"
            + "@'select offset, limit from test_table  offset 20 limit -- comment\n$jp1;'",
    }, delimiter = '@')
    public void offsetParameterTest(String query, String parsed) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        Assertions.assertEquals(parsed, parser.parseSQL());

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());

        Assertions.assertFalse(statement.getParams().isEmpty());
        int idx = 0;
        for (JdbcParameter prm : statement.getParams()) {
            idx++;
            Assertions.assertEquals("$jp" + idx, prm.getName());
            Assertions.assertNotNull(prm.getForcedType()); // forced UInt64 type
            Assertions.assertEquals(PrimitiveType.Uint64, prm.getForcedType().ydbType()); // forced UInt64 type
        }
    }

    @ParameterizedTest(name = "[{index}] {0} hasn't offset or limit parameter")
    @ValueSource(strings = {
        "select * from test_table where limit = ? or offset = ?",
        "update test_table set limit = ?, offset = ? where id = ?",
        "select * from test_table where limit=? or offset=?",
        "update test_table set limit=?, offset=? where id=?",
        "select * from test_table where limit? or offset?",
        "select * from test_table where limit-?",
        "select * from test_table where limit/?",
    })
    public void noOffsetParameterTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());

        Assertions.assertFalse(statement.getParams().isEmpty());
        int idx = 0;
        for (JdbcParameter prm : statement.getParams()) {
            idx++;
            Assertions.assertEquals("$jp" + idx, prm.getName());
            Assertions.assertNull(prm.getForcedType()); // uknown type
        }
    }

    @ParameterizedTest(name = "[{index}] {0} is batched insert query")
    @ValueSource(strings = {
        "Insert into table_name(c1, c2, c3) values (?, ? , ?)",
        "\n  insert into `table_name`  (\t`c1`, c2, c3)values(?, ? , ?)",
        "/* comment */ Insert into `table_name`  (`c1`, /* commect */ c2, c3)values(?, ? , ?);\n-- post comment",
        ";;Insert into table_name (`c1`, /* comment */ c2, c3 )   values(?, ? , ?);;",
    })
    public void batchedInsertTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.INSERT_UPSERT, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.INSERT, batch.getCommand());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertTrue(batch.getKeyColumns().isEmpty());
        Assertions.assertTrue(batch.getKeyValues().isEmpty());
        Assertions.assertEquals(3, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched insert query")
    @ValueSource(strings = {
        "Insert into one_column(c1) values (?)",
        "\n  insert into `one_column`  (\t`c1`)values(?)",
        "/* comment */ Insert into `one_column`  (`c1`/* commect */)values(?);\n-- post comment",
        ";;Insert\tinto\tone_column(`c1`)   values(\t\n?);;;",
    })
    public void batchedInsertOneColumnTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.INSERT_UPSERT, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.INSERT, batch.getCommand());

        Assertions.assertEquals("one_column", batch.getTableName());
        Assertions.assertTrue(batch.getKeyColumns().isEmpty());
        Assertions.assertTrue(batch.getKeyValues().isEmpty());
        Assertions.assertEquals(1, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched upsert query")
    @ValueSource(strings = {
        "Upsert into table_name(c1, c2, c3) values (?, ? , ?)",
        "\n  upsert into `table_name`  (\t`c1`, c2, c3)values(?, ? , ?)",
        "/* comment */ Upsert into `table_name`  (`c1`, /* commect */ c2, c3)values(?, ? , ?);\n-- post comment",
        ";;Upsert/* comment */into table_name (`c1`, /* comment */ c2, c3 )   values(?, ? , ?);",
    })
    public void batchedUpsertTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.INSERT_UPSERT, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.UPSERT, batch.getCommand());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertTrue(batch.getKeyColumns().isEmpty());
        Assertions.assertTrue(batch.getKeyValues().isEmpty());
        Assertions.assertEquals(3, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched upsert query")
    @ValueSource(strings = {
        "Upsert into one_column(c1) values (?)",
        "\n  upsert into `one_column`  (\t`c1`)values(?)",
        "/* comment */ Upsert into `one_column`  (`c1`/* commect */)values(?);\n-- post comment",
        ";;Upsert\tinto\tone_column(`c1`)   values(\t\n?);;;",
    })
    public void batchedUpsertOneColumnTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.INSERT_UPSERT, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.UPSERT, batch.getCommand());

        Assertions.assertEquals("one_column", batch.getTableName());
        Assertions.assertTrue(batch.getKeyColumns().isEmpty());
        Assertions.assertTrue(batch.getKeyValues().isEmpty());
        Assertions.assertEquals(1, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched replace query")
    @ValueSource(strings = {
        "Replace iNto table_name(c1, c2, c3) values (?, ? , ?)",
        "\n  replace into `table_name`  (\t`c1`, c2, c3)values(?, ? , ?)",
        "/* comment */ Replace into `table_name`  (`c1`, /* commect */ c2, c3)values(?, ? , ?);\n-- post comment",
        ";;Replace/* comment */into table_name (`c1`, /* comment */ c2, c3 )   values(?, ? , ?);",
    })
    public void batchedReplaceTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.UPDATE_REPLACE_DELETE, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.REPLACE, batch.getCommand());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertTrue(batch.getKeyColumns().isEmpty());
        Assertions.assertTrue(batch.getKeyValues().isEmpty());
        Assertions.assertEquals(3, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched replace query")
    @ValueSource(strings = {
        "Replace into one_column(c1) values (?)",
        "\n  replace into `one_column`  (\t`c1`)values(?)",
        "/* comment */ Replace into `one_column`  (`c1`/* commect */)values(?);\n-- post comment",
        ";;Replace\tinto\tone_column(`c1`)   values(\t\n?);;;",
    })
    public void batchedReplaceOneColumnTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.UPDATE_REPLACE_DELETE, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.REPLACE, batch.getCommand());

        Assertions.assertEquals("one_column", batch.getTableName());
        Assertions.assertTrue(batch.getKeyColumns().isEmpty());
        Assertions.assertTrue(batch.getKeyValues().isEmpty());
        Assertions.assertEquals(1, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched update query")
    @ValueSource(strings = {
        "Update table_name set c1 = ?, c2 = ?, c3 = ? where k1 = ? AND k2 = ?",
        "\n  update `table_name` set\t`c1`=?,c2=?,c3=?\tWhere\nk1=? AND k2=?;;;",
        "/* comment */ upDaTe `table_name` set  `c1` /* commect */ = ?, c2 = \n?, c3 = ? WHERE k1=? AND k2=?;;\n-- com",
        ";;UPDATE/* comment */table_name set  `c1`= ?, c2 = ?, c3 = ? WHERE k1\n=\t?--comment\nAND k2=?",
    })
    public void batchedUpdateTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.UPDATE_REPLACE_DELETE, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.UPDATE, batch.getCommand());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertEquals(2, batch.getKeyColumns().size());
        Assertions.assertEquals(Arrays.asList("k1", "k2"), batch.getKeyColumns());
        Assertions.assertEquals(3, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched update query")
    @ValueSource(strings = {
        "Update one_column set c1 = ? where k1 = ?",
        "\n  update `one_column` set\t`c1`=?\tWhere\nk1=?;;;",
        "/* comment */ upDaTe `one_column` set  `c1` /* commect */ = ? WHERE k1=?;;\n-- com",
        ";;UPDATE/* comment */one_column set  `c1`= ? WHERE k1=--comment\n?",
    })
    public void batchedUpdateOneColumnTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.UPDATE_REPLACE_DELETE, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.UPDATE, batch.getCommand());

        Assertions.assertEquals("one_column", batch.getTableName());
        Assertions.assertEquals(1, batch.getKeyColumns().size());
        Assertions.assertEquals(Arrays.asList("k1"), batch.getKeyColumns());
        Assertions.assertEquals(1, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched delete query")
    @ValueSource(strings = {
        "Delete from table_name where k1 = ? AND k2 = ?",
        "\n  delete fRom `table_name`\tWhere\nk1=? AND k2=?;;;",
        "/* comment */ deLete from `table_name` WHERE k1=? AND k2=?;;\n-- com",
        ";;DELETE/* comment */FRom table_name WHERE k1\n=\t?--comment\nAND k2=?",
    })
    public void batchedDeleteTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.UPDATE_REPLACE_DELETE, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.DELETE, batch.getCommand());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertEquals(2, batch.getKeyColumns().size());
        Assertions.assertEquals(Arrays.asList("k1", "k2"), batch.getKeyColumns());
        Assertions.assertTrue(batch.getColumns().isEmpty());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched delete query")
    @ValueSource(strings = {
        "Delete from one_column where k1 = ?",
        "\n  delete FROM\t`one_column`\nWhere\nk1=?;;;",
        "/* comment */ deLeTe FrOm `one_column` WHERE k1=?;;\n-- com",
        ";;DELETE/* comment */FROM--\none_column WHERE k1=--comment\n?",
    })
    public void batchedDeleteOneColumnTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.UPDATE_REPLACE_DELETE, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.DELETE, batch.getCommand());

        Assertions.assertEquals("one_column", batch.getTableName());
        Assertions.assertEquals(1, batch.getKeyColumns().size());
        Assertions.assertEquals(Arrays.asList("k1"), batch.getKeyColumns());
        Assertions.assertTrue(batch.getColumns().isEmpty());
    }

    @ParameterizedTest(name = "[{index}] {0} is not batched query")
    @ValueSource(strings = {
        "Insert into table_name(c1, c2, c3) values (?, ? , ?); Insert into table_name(c1, c2, c3) values (?, ? , ?);",
        "ipsert into table_name(c1, c2, c3) values (?, ? , ?)",
        "upsert into table_name(c1,, c2, c3) values (?, ?, ?)",
        "upsert into table_name(c1, c2, c3) values (?, ?,, ?)",
        "upsert into table_name(c1, c2, c3) values (?, ?, ??)",
        "upsert into (c1, c2, c3) values (?, ? , ?)",
        "upsert into table_name(c1) values (?,)",
        "upsert into table_name(c1,) values (?)",
        "upsert into table_name (c1, c2, c3); values (?, ?, ?)",
        "upsert into table_name set c1 = ?, c2 = ?, c3 = ?",
        "upsert table_name (c1, c2, c3) values (?, ?, ?)",
        "upsert table_name set c1 = ?, c2 = ?, c3 = ?",
        "upsert into table_name (c1, , c3) values (?, ?)",
        "upsert into table_name (c1, c2) values (?,,?)",
        "upsert into table_name (c1, c2, c3) values (?, ?, ?,)",
        "upsert into table_name (c1, c2, c3,) values (?, ?, ?)",
        "upsert into table_name (c1, c2, c3) values (?, ?, ?",
        "upsert into table_name (c1, c2, c3) (?, ?, ?)",
        "upsert into table_name (c1, c2, c3) values (?, ?)",
        "upsert into table_name (c1, c2, c3) values (?, ?, 123)",
        "upsert into table_name (c1, c2, c3) values (?, ?, ?) returning c1, c2, c3;",
        "upsert into table_name (c1, c2, c3) values (?, ?, ?); select 1;",
    })
    public void notBatchedTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertFalse(batch.isValidBatch());
    }

    @ParameterizedTest(name = "[{index}] {0} is bulk insert query")
    @ValueSource(strings = {
        "Bulk\nInsert into table_name(c1, c2, c3) values (?, ? , ?)",
        "\n bUlk   insert into `table_name`  (\t`c1`, c2, c3)values(?, ? , ?)",
        "--comment1\nbulk/* comment */Insert into `table_name`  (`c1`, /* commect */ c2, c3)values(?, ? , ?);",
        ";;BUlk  Insert into table_name (`c1`, /* comment */ c2, c3 )   values(?, ? , ?);",
    })
    public void validBulkInsertTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.BULK_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.INSERT_UPSERT, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.INSERT, batch.getCommand());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertEquals(3, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched bulk upsert query")
    @ValueSource(strings = {
        "Bulk   Upsert into table_name(c1, c2, c3) values (?, ? , ?)",
        "\nbulk\t\nupsert into `table_name`  (\t`c1`, c2, c3)values(?, ? , ?)",
        "/* test */Bulk/* comment */ Upsert into `table_name`  (`c1`, c2, c3)values(?, ? , ?);\n-- post comment",
        ";;Bulk Upsert/* comment */into table_name (`c1`, /* comment */ c2, c3 )   values(?, ? , ?);",
    })
    public void validBulkUpsertTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(query, true, true, true);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.BULK_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.INSERT_UPSERT, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.UPSERT, batch.getCommand());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertEquals(3, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), batch.getColumns());
    }
}
