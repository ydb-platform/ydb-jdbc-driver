package tech.ydb.jdbc.common;

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class FixedResultSetFactory {
    private final List<Column> columns;
    private final Map<String, Integer> columnIndexes;

    private FixedResultSetFactory(List<Column> columns) {
        this.columns = columns;
        this.columnIndexes = new HashMap<>();
        for (int idx = 0; idx < columns.size(); idx += 1) {
            columnIndexes.put(columns.get(idx).name, idx);
        }
    }

    public interface ResultSetBuilder {
        public interface RowBuilder {
            RowBuilder withTextValue(String name, String value);
            RowBuilder withIntValue(String name, int value);
            RowBuilder withShortValue(String name, short value);
            RowBuilder withLongValue(String name, long value);
            RowBuilder withBoolValue(String name, boolean value);

            ResultSetBuilder build();
        }

        RowBuilder newRow();
        ResultSetReader build();
    }

    public ResultSetBuilder createResultSet() {
        return new ResultSetBuilderImpl();
    }

    private class ResultSetBuilderImpl implements ResultSetBuilder {
        private final List<Map<Column, ValueReader>> rows = new ArrayList<>();

        private class RowImpl implements RowBuilder {
            private final Map<Column, ValueReader> values = new HashMap<>();

            @Override
            public RowBuilder withTextValue(String name, String value) {
                Column column = columns.get(columnIndexes.get(name));
                if (value != null) {
                    values.put(column, new FixedValueReader(PrimitiveValue.newText(value), column.type));
                }
                return this;
            }

            @Override
            public RowBuilder withIntValue(String name, int value) {
                Column column = columns.get(columnIndexes.get(name));
                values.put(column, new FixedValueReader(PrimitiveValue.newInt32(value), column.type));
                return this;
            }

            @Override
            public RowBuilder withShortValue(String name, short value) {
                Column column = columns.get(columnIndexes.get(name));
                values.put(column, new FixedValueReader(PrimitiveValue.newInt16(value), column.type));
                return this;
            }

            @Override
            public RowBuilder withLongValue(String name, long value) {
                Column column = columns.get(columnIndexes.get(name));
                values.put(column, new FixedValueReader(PrimitiveValue.newInt64(value), column.type));
                return this;
            }

            @Override
            public RowBuilder withBoolValue(String name, boolean value) {
                Column column = columns.get(columnIndexes.get(name));
                values.put(column, new FixedValueReader(PrimitiveValue.newBool(value), column.type));
                return this;
            }

            @Override
            public ResultSetBuilder build() {
                // init null values
                for (Column column: columns) {
                    if (!values.containsKey(column)) {
                        values.put(column, new FixedValueReader(null, column.type));
                    }
                }
                rows.add(this.values);
                return ResultSetBuilderImpl.this;
            }
        }


        @Override
        public RowBuilder newRow() {
            return new RowImpl();
        }

        @Override
        public ResultSetReader build() {
            return new FixedResultSet(rows);
        }
    }

    private class FixedResultSet implements ResultSetReader {
        private final List<Map<Column, ValueReader>> rows;
        private int rowIndex = 0;

        FixedResultSet(List<Map<Column, ValueReader>> rows) {
            this.rows = rows;
        }

        @Override
        public boolean isTruncated() {
            return false;
        }

        @Override
        public int getColumnCount() {
            return columns.size();
        }

        @Override
        public int getRowCount() {
            return rows.size();
        }

        @Override
        public void setRowIndex(int index) {
            rowIndex = index;
        }

        @Override
        public boolean next() {
            if (rowIndex >= rows.size()) {
                return false;
            }
            rowIndex += 1;
            return true;
        }

        @Override
        public String getColumnName(int index) {
            return columns.get(index).name;
        }

        @Override
        public int getColumnIndex(String name) {
            return columnIndexes.get(name);
        }

        @Override
        public ValueReader getColumn(int index) {
            return rows.get(rowIndex).get(columns.get(index));
        }

        @Override
        public ValueReader getColumn(String name) {
            return rows.get(rowIndex).get(columns.get(columnIndexes.get(name)));
        }

        @Override
        public Type getColumnType(int index) {
            return columns.get(index).type;
        }
    }

    private static class Column {
        private final String name;
        private final Type type;

        Column(String name, Type type) {
            this.name = name;
            this.type = type;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private final List<Column> columns = new ArrayList<>();

        public Builder addTextColumn(String name) {
            columns.add(new Column(name, PrimitiveType.Text.makeOptional()));
            return this;
        }

        public Builder addIntColumn(String name) {
            columns.add(new Column(name, PrimitiveType.Int32.makeOptional()));
            return this;
        }

        public Builder addShortColumn(String name) {
            columns.add(new Column(name, PrimitiveType.Int16.makeOptional()));
            return this;
        }

        public Builder addLongColumn(String name) {
            columns.add(new Column(name, PrimitiveType.Int64.makeOptional()));
            return this;
        }

        public Builder addBooleanColumn(String name) {
            columns.add(new Column(name, PrimitiveType.Bool.makeOptional()));
            return this;
        }

        public FixedResultSetFactory build() {
            return new FixedResultSetFactory(columns);
        }
    }

    private static class FixedValueReader implements ValueReader {
        private final PrimitiveValue value;
        private final Type type;

        FixedValueReader(PrimitiveValue value, Type type) {
            this.value = value;
            this.type = type;
        }

        @Override
        public boolean getBool() {
            return value.getBool();
        }

        @Override
        public byte getInt8() {
            return value.getInt8();
        }

        @Override
        public int getUint8() {
            return value.getUint8();
        }

        @Override
        public short getInt16() {
            return value.getInt16();
        }

        @Override
        public int getUint16() {
            return value.getUint16();
        }

        @Override
        public int getInt32() {
            return value.getInt32();
        }

        @Override
        public long getUint32() {
            return value.getUint32();
        }

        @Override
        public long getInt64() {
            return value.getInt64();
        }

        @Override
        public long getUint64() {
            return value.getUint64();
        }

        @Override
        public float getFloat() {
            return value.getFloat();
        }

        @Override
        public double getDouble() {
            return value.getDouble();
        }

        @Override
        public LocalDate getDate() {
            return value.getDate();
        }

        @Override
        public LocalDateTime getDatetime() {
            return value.getDatetime();
        }

        @Override
        public Instant getTimestamp() {
            return value.getTimestamp();
        }

        @Override
        public Duration getInterval() {
            return value.getInterval();
        }

        public LocalDate getDate32() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public LocalDateTime getDatetime64() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public Instant getTimestamp64() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public Duration getInterval64() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ZonedDateTime getTzDate() {
            return value.getTzDate();
        }

        @Override
        public ZonedDateTime getTzDatetime() {
            return value.getTzDatetime();
        }

        @Override
        public ZonedDateTime getTzTimestamp() {
            return value.getTzTimestamp();
        }

        @Override
        public byte[] getBytes() {
            return value.getBytes();
        }

        @Override
        public String getBytesAsString(Charset charset) {
            return value.getBytesAsString(charset);
        }

        @Override
        public UUID getUuid() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String getText() {
            return value.getText();
        }

        @Override
        public byte[] getYson() {
            return value.getYson();
        }

        @Override
        public String getJson() {
            return value.getJson();
        }

        @Override
        public String getJsonDocument() {
            return value.getJsonDocument();
        }

        @Override
        public DecimalValue getDecimal() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void toString(StringBuilder sb) {
            sb.append(value.toString());
        }

        @Override
        public Value<?> getValue() {
            return value;
        }

        @Override
        public Type getType() {
            return type;
        }

        @Override
        public boolean isOptionalItemPresent() {
            return value != null;
        }

        @Override
        public ValueReader getOptionalItem() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getTupleElementsCount() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ValueReader getTupleElement(int index) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getListItemsCount() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ValueReader getListItem(int index) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getDictItemsCount() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ValueReader getDictKey(int index) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ValueReader getDictValue(int index) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getStructMembersCount() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String getStructMemberName(int index) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ValueReader getStructMember(int index) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ValueReader getStructMember(String name) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getVariantTypeIndex() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ValueReader getVariantItem() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
