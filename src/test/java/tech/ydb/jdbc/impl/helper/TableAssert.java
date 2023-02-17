package tech.ydb.jdbc.impl.helper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Assertions;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class TableAssert {
    private static final TableAssert EMPTY = new TableAssert();

    public static void assertEmpty(ResultSet rs) throws SQLException {
        EMPTY.check(rs)
                .assertMetaColumns()
                .assertNoRows();
    }

    private final List<Column> columns = new ArrayList<>();
    private final Map<Column, ValueAssert> defaultValues = new TreeMap<>();

    public BoolColumn addBoolColumn(String name, String typeName) {
        BoolColumn column = new BoolColumn(columns.size() + 1, name, typeName);
        columns.add(column);
        return column;
    }

    public TextColumn addTextColumn(String name, String typeName) {
        TextColumn column = new TextColumn(columns.size() + 1, name, typeName);
        columns.add(column);
        return column;
    }

    public IntColumn addIntColumn(String name, String typeName) {
        IntColumn column = new IntColumn(columns.size() + 1, name, typeName);
        columns.add(column);
        return column;
    }

    public ShortColumn addShortColumn(String name, String typeName) {
        ShortColumn column = new ShortColumn(columns.size() + 1, name, typeName);
        columns.add(column);
        return column;
    }

    public ResultSetAssert check(ResultSet rs) throws SQLException {
        return new ResultSetAssert(rs);
    }


    public class ResultSetAssert {
        private final ResultSet rs;

        public ResultSetAssert(ResultSet rs) throws SQLException {
            Assertions.assertFalse(rs.isClosed(), "Result set is closed");
            this.rs = rs;
        }

        public ResultSetAssert assertMetaColumns() throws SQLException {
            ResultSetMetaData meta = rs.getMetaData();
            Assertions.assertEquals(columns.size(), meta.getColumnCount(), "Wrong column count in ResultSetMetaData");
            for (int idx = 1; idx <= columns.size(); idx += 1) {
                Column c = columns.get(idx - 1);
                Assertions.assertEquals(c.name, meta.getColumnName(idx), "Wrong " + c.name + "[" + idx + "] name");
                Assertions.assertEquals(c.type, meta.getColumnType(idx), "Wrong " + c.name + "[" + idx + "] type");
                Assertions.assertEquals(c.typeName, meta.getColumnTypeName(idx),
                        "Wrong " + c.name + "[" + idx + "] type name");
            }
            return this;
        }

        public ResultSetAssert assertNoRows() throws SQLException {
            Assertions.assertFalse(rs.next(), "Unexpected non-empty result set");
            rs.close();
            Assertions.assertTrue(rs.isClosed(), "Result set is not closed");
            return this;
        }

        public Row nextRow(ValueAssert... values) throws SQLException {
            Assertions.assertTrue(rs.next(), "Unexpected end of result set");
            return new Row(values);
        }

        public class Row {
            private final Map<Column, ValueAssert> rowValues = new TreeMap<>();

            public Row(ValueAssert... values) {
                rowValues.putAll(defaultValues);
                for (ValueAssert v: values) {
                    rowValues.put(v.column, v);
                }

            }

            public ResultSetAssert assertAll() throws SQLException {
                Assertions.assertEquals(columns.size(), rowValues.size(), "Wrong count of values for assertAll");

                for (Column c: rowValues.keySet()) {
                    rowValues.get(c).assertValue(rs);
                }
                return ResultSetAssert.this;
            }
        }
    }

    public abstract class ValueAssert {
        protected final Column column;

        protected ValueAssert(Column column) {
            this.column = column;
        }

        abstract public void assertValue(ResultSet rs) throws SQLException;
    }

    private class NullValueAssert extends ValueAssert {
        public NullValueAssert(Column column) {
            super(column);
        }

        @Override
        public void assertValue(ResultSet rs) throws SQLException {
            Assertions.assertNull(rs.getObject(column.name), "Not empty value for column label " + column.name);
            Assertions.assertTrue(rs.wasNull(), "Not null value for column label " + column.name);

            Assertions.assertNull(rs.getObject(column.index), "Not empty value of column index " + column.index);
            Assertions.assertTrue(rs.wasNull(), "Not null value for column index " + column.index);
        }
    }

    public class Column implements Comparable<Column> {
        private final int index;
        private final String name;
        private final int type;
        private final String typeName;

        protected Column(int index, String name, int type, String typeName) {
            this.index = index;
            this.name = name;
            this.type = type;
            this.typeName = typeName;
        }

        public String name() {
            return this.name;
        }

        @Override
        public int compareTo(Column o) {
            return Integer.compare(index, o.index);
        }
    }

    public class BoolColumn extends Column {
        public BoolColumn(int index, String name, String typeName) {
            super(index, name, Types.BOOLEAN, typeName);
        }

        public BoolColumn defaultNull() {
            defaultValues.put(this, new NullValueAssert(this));
            return this;
        }

        public BoolColumn defaultValue(boolean defaultValue) {
            defaultValues.put(this, eq(defaultValue));
            return this;
        }

        public ValueAssert eq(boolean value) {
            return new ValueAssert(this) {
                @Override
                public void assertValue(ResultSet rs) throws SQLException {
                    Assertions.assertEquals(value, rs.getBoolean(column.name),
                            "Wrong bool value for column label " + column.name);
                    Assertions.assertFalse(rs.wasNull(), "Null value for column label " + column.name);

                    Assertions.assertEquals(value, rs.getBoolean(column.index),
                            "Wrong bool value of column index " + column.index);
                    Assertions.assertFalse(rs.wasNull(), "Null value for column index " + column.index);
                }
            };
        }
    }

    public class TextColumn extends Column {
        public TextColumn(int index, String name, String typeName) {
            super(index, name, Types.VARCHAR, typeName);
        }

        public TextColumn defaultNull() {
            defaultValues.put(this, new NullValueAssert(this));
            return this;
        }

        public TextColumn defaultValue(String defaultValue) {
            defaultValues.put(this, eq(defaultValue));
            return this;
        }

        public ValueAssert eq(String value) {
            return new ValueAssert(this) {
                @Override
                public void assertValue(ResultSet rs) throws SQLException {
                    Assertions.assertEquals(value, rs.getString(column.name),
                            "Wrong text value for column label " + column.name);
                    Assertions.assertFalse(rs.wasNull(), "Null value for column label " + column.name);

                    Assertions.assertEquals(value, rs.getString(column.index),
                            "Wrong text value of column index " + column.index);
                    Assertions.assertFalse(rs.wasNull(), "Null value for column index " + column.index);
                }
            };
        }
    }

    public class IntColumn extends Column {
        public IntColumn(int index, String name, String typeName) {
            super(index, name, Types.INTEGER, typeName);
        }

        public IntColumn defaultNull() {
            defaultValues.put(this, new NullValueAssert(this));
            return this;
        }

        public IntColumn defaultValue(int defaultValue) {
            defaultValues.put(this, eq(defaultValue));
            return this;
        }

        public ValueAssert eq(int value) {
            return new ValueAssert(this) {
                @Override
                public void assertValue(ResultSet rs) throws SQLException {
                    Assertions.assertEquals(value, rs.getInt(column.name),
                            "Wrong integer value for column label " + column.name);
                    Assertions.assertFalse(rs.wasNull(), "Null value for column label " + column.name);

                    Assertions.assertEquals(value, rs.getInt(column.index),
                            "Wrong integer value of column index " + column.index);
                    Assertions.assertFalse(rs.wasNull(), "Null value for column index " + column.index);
                }
            };
        }
    }

    public class ShortColumn extends Column {
        public ShortColumn(int index, String name, String typeName) {
            super(index, name, Types.SMALLINT, typeName);
        }

        public ShortColumn defaultNull() {
            defaultValues.put(this, new NullValueAssert(this));
            return this;
        }

        public ShortColumn defaultValue(short defaultValue) {
            defaultValues.put(this, eq(defaultValue));
            return this;
        }

        public ValueAssert eq(short value) {
            return new ValueAssert(this) {
                @Override
                public void assertValue(ResultSet rs) throws SQLException {
                    Assertions.assertEquals(value, rs.getShort(column.name),
                            "Wrong short value for column label " + column.name);
                    Assertions.assertFalse(rs.wasNull(), "Null value for column label " + column.name);

                    Assertions.assertEquals(value, rs.getShort(column.index),
                            "Wrong short value of column index " + column.index);
                    Assertions.assertFalse(rs.wasNull(), "Null value for column index " + column.index);
                }
            };
        }
    }
}
