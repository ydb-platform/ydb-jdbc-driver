package tech.ydb.jdbc.query;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.jdbc.query.params.JdbcPrm;
import tech.ydb.jdbc.settings.YdbQueryProperties;
import tech.ydb.table.values.PrimitiveType;



/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryParserTest {
    private final YdbTypes types = new YdbTypes(false);
    private YdbQueryProperties props;

    @BeforeEach
    public void init() throws SQLException {
        props = new YdbQueryProperties(new Properties());
    }

    @ParameterizedTest(name = "[{index}] {0} is empty query")
    @CsvSource(value = {
        "--just comment~--just comment",
        "EXPLAIN;~;",
        "SCAN;~;",
        "BULK;~;",
    }, delimiter = '~')
    public void emptyQueryTest(String query, String prepared) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        String parsed = parser.parseSQL();
        Assertions.assertEquals(prepared, parsed);
        Assertions.assertEquals(0, parser.getStatements().size());
    }

    @ParameterizedTest(name = "[{index}] {0} is explain query")
    @CsvSource(value = {
        "'Explain select * from table', ' select * from table', SELECT",
        "'exPlain\nupsert to', '\nupsert to', DML",
        "'  Explain select * from table', '   select * from table', SELECT",
        "'\texPlain\nupsert to', '\t\nupsert to', DML",
        "'EXPLAIN/*comment*/UPSERT INTO', '/*comment*/UPSERT INTO', DML",
    })
    public void explainQueryTest(String query, String prepared, String cmd) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
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
        "Revoke--comment\npermission;",
        "GRant table 'test'",
        "-- comment \nCreate;",
    })
    public void schemeQueryTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        String parsed = parser.parseSQL();

        Assertions.assertEquals(query, parsed);

        Assertions.assertEquals(1, parser.getStatements().size());
        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.SCHEME_QUERY, statement.getType());
        Assertions.assertEquals(QueryCmd.DDL, statement.getCmd());
    }

    @ParameterizedTest(name = "[{index}] {0} is batch query")
    @ValueSource(strings = {
        "  Batch delete from table;",
        "BATCH--comment\nUPDATE table;",
        "baTCH SELECT table 'test'",
    })
    public void batchQueryTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        String parsed = parser.parseSQL();

        Assertions.assertEquals(query, parsed);

        Assertions.assertEquals(1, parser.getStatements().size());
        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.SCHEME_QUERY, statement.getType());
        Assertions.assertEquals(QueryCmd.BATCH, statement.getCmd());
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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
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

        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        String parsed = parser.parseSQL();
        Assertions.assertEquals(query, parsed);

        Assertions.assertEquals(3, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DECLARE, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryType.DECLARE, parser.getStatements().get(1).getType());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(2).getType());
    }

    @Test
    public void yqlSelectWithKeyTest() throws SQLException {
        String query = ""
                + "declare $key as Optional<Int32>;\n"
                + "select key, column from tableName where key=$key";

        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        String parsed = parser.parseSQL();
        Assertions.assertEquals(query, parsed);

        Assertions.assertEquals(2, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DECLARE, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(1).getType());

        Assertions.assertEquals(QueryType.DATA_QUERY, parser.detectQueryType());
    }

    @Test
    public void scanSelectWithKeyTest() throws SQLException {
        String query = ""
                + "declare $key as Optional<Int32>;\n"
                + "scan select key, column from tableName where key=$key";

        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        String parsed = parser.parseSQL();
        Assertions.assertEquals("declare $key as Optional<Int32>;\n"
                + " select key, column from tableName where key=$key", parsed);

        Assertions.assertEquals(2, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DECLARE, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryType.SCAN_QUERY, parser.getStatements().get(1).getType());

        Assertions.assertEquals(QueryType.SCAN_QUERY, parser.detectQueryType());
    }

    @ParameterizedTest(name = "[{index}] {0} jdbc arguments")
    @CsvSource(value = {
        "select * from test_table where col = ?"
            + "@select * from test_table where col = $jp1",
        "select * from test_table where col = ? and c1 = \'$jp1\'"
            + "@select * from test_table where col = $jp2 and c1 = \'$jp1\'",
        "select * from test_table where col = ? and c1 = \'\\\\$jp1"
            + "@select * from test_table where col = $jp2 and c1 = \'\\\\$jp1",
    }, delimiter = '@')
    public void simpleJdbcArgTest(String query, String parsed) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        Assertions.assertEquals(parsed, parser.parseSQL());
        Assertions.assertEquals(1, parser.getStatements().size());
        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());
        Assertions.assertTrue(statement.hasJdbcParameters());

        int prmCount = 0;
        for (JdbcPrm.Factory factory : statement.getJdbcPrmFactories()) {
            prmCount += factory.create().size();
        }
        Assertions.assertEquals(1, prmCount);
    }

    @Test
    public void forcedJdbcArgTest() throws SQLException {
        String query = ""
                + "DECLARE $jp1 AS Text?;"
                + "$test1 = SELECT id FROM c0 = $jp1 and c1 = ?;"
                + "SELECT * FROM $test1 WHERE c2 = ?;";

        String simpleParsed = ""
                + "DECLARE $jp1 AS Text?;"
                + "$test1 = SELECT id FROM c0 = $jp1 and c1 = ?;"
                + "SELECT * FROM $test1 WHERE c2 = $jp2;";

        String forcedParsed = ""
                + "DECLARE $jp1 AS Text?;"
                + "$test1 = SELECT id FROM c0 = $jp1 and c1 = $jp2;"
                + "SELECT * FROM $test1 WHERE c2 = $jp3;";

        Assertions.assertEquals(simpleParsed, new YdbQueryParser(types, query, props).parseSQL());

        Properties forced = new Properties();
        forced.put("forceJdbcParameters", "true");
        Assertions.assertEquals(forcedParsed, new YdbQueryParser(types, query, new YdbQueryProperties(forced)).parseSQL());
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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        Assertions.assertEquals(parsed, parser.parseSQL());

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());

        Assertions.assertTrue(statement.hasJdbcParameters());
        int idx = 0;
        for (JdbcPrm.Factory factory : statement.getJdbcPrmFactories()) {
            for (JdbcPrm prm: factory.create()) {
                idx++;
                Assertions.assertEquals("$jp" + idx, prm.getName());
                Assertions.assertNotNull(prm.getType()); // forced UInt64 type
                Assertions.assertEquals(PrimitiveType.Uint64, prm.getType().ydbType()); // forced UInt64 type
            }
        }
    }

    @ParameterizedTest(name = "[{index}] {0} hasn't offset or limit parameter")
    @ValueSource(strings = {
        "select * from test_table where limit = ? or offset = ?",
        "update test_table set limit = ?, offset = ? where id = ?",
        "select * from test_table where limit=? or offset=?",
        "select * from test_table where limit ??",
        "select * from test_table where limit",
        "select * from test_table where limit  ",
        "update test_table set limit=?, offset=? where id=?",
        "select * from test_table where limit? or offset?",
        "select * from test_table where limit-?",
        "select * from test_table where limit/?",
    })
    public void noOffsetParameterTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());

        int idx = 0;
        for (JdbcPrm.Factory factory : statement.getJdbcPrmFactories()) {
            for (JdbcPrm prm: factory.create()) {
                idx++;
                Assertions.assertEquals("$jp" + idx, prm.getName());
                Assertions.assertNull(prm.getType()); // uknown type
            }
        }
    }

    @ParameterizedTest(name = "[{index}] {0} has in list parameter")
    @CsvSource(value = {
        "'select * from test_table where id in (?)'"
            + "@'select * from test_table where id in  $jp1'",
        "'select * from test_table where id in (?,\n?, ?, \t?)'"
            + "@'select * from test_table where id in  $jp1'",
        "'select * from test_table where id In(?--comment\n,?,?/**other /** inner */ comment*/)'"
            + "@'select * from test_table where id In $jp1'",
        "'select * from test_table where (id, value) in ((?, ?), (?, ?))'"
            + "@'select * from test_table where (id, value) in  $jp1'",
        "'select * from test_table where tuple in ((?,\n?),(?, ?))'"
            + "@'select * from test_table where tuple in  $jp1'",
    }, delimiter = '@')
    public void inListParameterTest(String query, String parsed) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        Assertions.assertEquals(parsed, parser.parseSQL());

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());

        Assertions.assertTrue(statement.hasJdbcParameters());
        int idx = 0;
        for (JdbcPrm.Factory factory : statement.getJdbcPrmFactories()) {
            for (JdbcPrm prm: factory.create()) {
                Assertions.assertEquals("$jp1[" + idx + "]", prm.getName());
                idx++;
            }
        }
    }

    @ParameterizedTest(name = "[{index}] {0} has not in list parameter")
    @CsvSource(value = {
        "'select * from test_table where id in (?)'"
            + "@'select * from test_table where id in ($jp1)'",
        "'select * from test_table where (id, value) in ((?, ?), (?, ?))'"
            + "@'select * from test_table where (id, value) in (($jp1, $jp2), ($jp3, $jp4))'",
        "'select * from test_table where id in (?,\n?, ?, \t?)'"
            + "@'select * from test_table where id in ($jp1,\n$jp2, $jp3, \t$jp4)'",
        "'select * from test_table where id In(?--comment\n,?,?/**other /** inner */ comment*/)'"
            + "@'select * from test_table where id In($jp1--comment\n,$jp2,$jp3/**other /** inner */ comment*/)'",
    }, delimiter = '@')
    public void inListParameterDisabledTest(String query, String parsed) throws SQLException {
        Properties config = new Properties();
        config.put("replaceJdbcInByYqlList", "false");
        YdbQueryParser parser = new YdbQueryParser(types, query, new YdbQueryProperties(config));
        Assertions.assertEquals(parsed, parser.parseSQL());

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());

        Assertions.assertTrue(statement.hasJdbcParameters());
        int idx = 0;
        for (JdbcPrm.Factory factory : statement.getJdbcPrmFactories()) {
            for (JdbcPrm prm: factory.create()) {
                idx++;
                Assertions.assertEquals("$jp" + idx, prm.getName());
            }
        }
    }

    @ParameterizedTest(name = "[{index}] {0} doesn't have in list parameter")
    @CsvSource(value = {
        "'select * from test_table where id in (?, 1, ?)'"
            + "@'select * from test_table where id in ($jp1, 1, $jp2)'",
        "'select * from test_table where id in (??, ?)'"
            + "@'select * from test_table where id in (?, $jp1)'",
        "'select * from test_table where id in()'"
            + "@'select * from test_table where id in()'",
        "'select * from test_table where id in(?, ?, ?,)'"
            + "@'select * from test_table where id in($jp1, $jp2, $jp3,)'",
        "'select * from test_table where id in(?, ?, ?'"
            + "@'select * from test_table where id in($jp1, $jp2, $jp3'",
        "'select * from test_table where id in ?, ?, ?'"
            + "@'select * from test_table where id in $jp1, $jp2, $jp3'",
        "'select * from test_table where id in (((?)))'"
            + "@'select * from test_table where id in ((($jp1)))'",
        "'select * from test_table where id in ,?)'"
            + "@'select * from test_table where id in ,$jp1)'",
        "'select * from test_table where id in (())'"
            + "@'select * from test_table where id in (())'",
        "'select * from test_table where id in ((?, ?), ?)'"
            + "@'select * from test_table where id in (($jp1, $jp2), $jp3)'",
        "'select * from test_table where id in ((?,?),(?,?,?))'"
            + "@'select * from test_table where id in (($jp1,$jp2),($jp3,$jp4,$jp5))'",
    }, delimiter = '@')
    public void inListParametersUnparsableTest(String query, String parsed) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        Assertions.assertEquals(parsed, parser.parseSQL());

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());

        int idx = 0;
        for (JdbcPrm.Factory factory : statement.getJdbcPrmFactories()) {
            for (JdbcPrm prm: factory.create()) {
                idx++;
                Assertions.assertEquals("$jp" + idx, prm.getName());
            }
        }
    }

    @ParameterizedTest(name = "[{index}] {0} has as_table list parameter")
    @CsvSource(value = {
        "'select * from jdbc_table(?) as t join test_table on id=t.x'"
            + "@'select * from  AS_TABLE($jp1) as t join test_table on id=t.x'",
        "'select * from jdbc_table(?,\n?, ?, \t?)'"
            + "@'select * from  AS_TABLE($jp1)'",
        "'select * from JDbc_Table  (?--comment\n,?,?/**other /** inner */ comment*/)'"
            + "@'select * from  AS_TABLE($jp1)'",
    }, delimiter = '@')
    public void jdbcTableParameterTest(String query, String parsed) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        Assertions.assertEquals(parsed, parser.parseSQL());

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());

        Assertions.assertTrue(statement.hasJdbcParameters());
        int idx = 0;
        for (JdbcPrm.Factory factory : statement.getJdbcPrmFactories()) {
            for (JdbcPrm prm: factory.create()) {
                Assertions.assertEquals("$jp1[" + idx + "]", prm.getName());
                idx++;
            }
        }
    }

    @ParameterizedTest(name = "[{index}] {0} doesn't have as_table list parameter")
    @CsvSource(value = {
        "'select * from jdbc_table((?)) as t join test_table on id=t.x'"
            + "@'select * from jdbc_table(($jp1)) as t join test_table on id=t.x'",
        "'select * from jdbc_table(?,) as t join test_table on id=t.x'"
            + "@'select * from jdbc_table($jp1,) as t join test_table on id=t.x'",
        "'select * from jdbc_table where id = ?'"
            + "@'select * from jdbc_table where id = $jp1'",
        "'select * from jdbc_table,other_table where id = ?'"
            + "@'select * from jdbc_table,other_table where id = $jp1'",
        "'select * from jdbc_table(?,,?) as t join test_table on id=t.x'"
            + "@'select * from jdbc_table($jp1,,$jp2) as t join test_table on id=t.x'",
        "'select * from jdbc_table(?,'"
            + "@'select * from jdbc_table($jp1,'",
        "'select * from jdbc_table(??) where id=?'"
            + "@'select * from jdbc_table(?) where id=$jp1'",
    }, delimiter = '@')
    public void jdbcTableParameterUnparsableTest(String query, String parsed) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        Assertions.assertEquals(parsed, parser.parseSQL());

        Assertions.assertEquals(1, parser.getStatements().size());

        QueryStatement statement = parser.getStatements().get(0);
        Assertions.assertEquals(QueryType.DATA_QUERY, statement.getType());

        Assertions.assertTrue(statement.hasJdbcParameters());
        int idx = 0;
        for (JdbcPrm.Factory factory : statement.getJdbcPrmFactories()) {
            for (JdbcPrm prm: factory.create()) {
                idx++;
                Assertions.assertEquals("$jp" + idx, prm.getName());
            }
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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

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
        "Update table_name set c1 = ?, c2 = ?, c3 = ? where table_name .k1 = ? AND k2 = ?",
        "\n  update `table_name` set\t`c1`=?,c2=?,c3=?\tWhere\nk1=? AND `table_name`. `k2`=?;;;",
        "/* comment */ upDaTe `table_name` set  `c1` /* commect */ = ?, c2 = \n?, c3 = ? WHERE k1=? AND k2=?;;\n-- com",
        ";;UPDATE/* comment */table_name set  `c1`= ?, c2 = ?, c3 = ? WHERE k1\n=\t?--comment\nAND k2=?",
    })
    public void batchedUpdateTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

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
        "/* comment */ upDaTe `one_column` set  `c1` /* commect */ = ? WHERE `one_column`.k1=?;;\n-- com",
        ";;UPDATE/* comment */one_column set  `c1`= ? WHERE one_column.`k1`=--comment\n?",
    })
    public void batchedUpdateOneColumnTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.UPDATE, batch.getCommand());

        Assertions.assertEquals("one_column", batch.getTableName());
        Assertions.assertEquals(1, batch.getKeyColumns().size());
        Assertions.assertEquals(Arrays.asList("k1"), batch.getKeyColumns());
        Assertions.assertEquals(1, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched update query")
    @ValueSource(strings = {
        "Update test_table set column = ? where test_table = ?",
        "Update test_table set column = ? where test_table . test_table = ?",
        "Update test_table set column = ? where `test_table`.test_table = ?",
        "Update test_table set column = ? where test_table.`test_table` = ?",
    })
    public void batchedUpdateOneColumnWithTableName(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.UPDATE, batch.getCommand());

        Assertions.assertEquals("test_table", batch.getTableName());
        Assertions.assertEquals(1, batch.getKeyColumns().size());
        Assertions.assertEquals(Arrays.asList("test_table"), batch.getKeyColumns());
        Assertions.assertEquals(1, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("column"), batch.getColumns());
    }

    @ParameterizedTest(name = "[{index}] {0} is batched delete query")
    @ValueSource(strings = {
        "Delete from table_name where k1 = ? AND `table_name`.k2 = ?",
        "\n  delete fRom `table_name`\tWhere\nk1=? AND k2=?;;;",
        "/* comment */ deLete from `table_name` WHERE k1=? AND k2=?;;\n-- com",
        ";;DELETE/* comment */FRom table_name WHERE k1\n=\t?--comment\nAND table_name.`k2`=?",
    })
    public void batchedDeleteTest(String query) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.DATA_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

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
        "update table_name set c1 = ?, c2 = ?, c3 = ? where other.id=?",
        "update table_name set c1 = ?, c2 = ?, c3 = ? where table_name.name.id=?",
        "update table_name set c1 = ?, c2 = ?, c3 = ? where Table_name.id=?",
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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.BULK_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

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
        YdbQueryParser parser = new YdbQueryParser(types, query, props);
        parser.parseSQL();

        Assertions.assertEquals(1, parser.getStatements().size());
        Assertions.assertEquals(QueryType.BULK_QUERY, parser.getStatements().get(0).getType());
        Assertions.assertEquals(QueryCmd.DML, parser.getStatements().get(0).getCmd());

        YqlBatcher batch = parser.getYqlBatcher();
        Assertions.assertTrue(batch.isValidBatch());
        Assertions.assertEquals(YqlBatcher.Cmd.UPSERT, batch.getCommand());

        Assertions.assertEquals("table_name", batch.getTableName());
        Assertions.assertEquals(3, batch.getColumns().size());
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), batch.getColumns());
    }
}
