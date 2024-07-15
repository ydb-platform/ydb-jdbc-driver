package tech.ydb.jdbc.query;

import java.sql.SQLException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;


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
}
