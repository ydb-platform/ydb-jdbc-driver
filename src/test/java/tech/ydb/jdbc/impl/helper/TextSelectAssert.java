package tech.ydb.jdbc.impl.helper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class TextSelectAssert {
    private final TableAssert table = new TableAssert();
    private final TableAssert.IntColumn key = table.addIntColumn("key", "Int32").defaultNull();
    private final TableAssert.TextColumn text;
    private final TableAssert.ResultSetAssert check;

    private TextSelectAssert(ResultSet rs, String column, String type) throws SQLException {
        this.text = table.addTextColumn(column, type).defaultNull();
        this.check = table.check(rs);
        check.assertMetaColumns();
    }

    public TextSelectAssert nextRow(int keyValue, String columnValue) throws SQLException {
        check.nextRow(key.eq(keyValue), text.eq(columnValue));
        return this;
    }

    public TextSelectAssert nextRowIsEmpty() throws SQLException {
        check.nextRow();
        return this;
    }

    public void noNextRows() throws SQLException {
        check.assertNoRows();
    }

    public static TextSelectAssert of(ResultSet rs, String column, String type) throws SQLException {
        return new TextSelectAssert(rs, column, type);
    }
}
