package tech.ydb.jdbc.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbResultSetMetaData;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.common.ColumnInfo;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public abstract class BaseYdbResultSet implements YdbResultSet {
    protected final YdbStatement statement;

    private final ColumnInfo[] columns;
    private final Map<String, Integer> columnNames = new HashMap<>();

    private YdbResultSetMetaData metaData = null;
    private boolean wasNull = false;

    protected BaseYdbResultSet(YdbStatement statement, ColumnInfo[] columns) {
        this.statement = Objects.requireNonNull(statement);
        this.columns = columns;

        for (int idx = 1; idx <= columns.length; idx += 1) {
            String name = columns[idx - 1].getName();
            if (!columnNames.containsKey(name)) {
                columnNames.put(name, idx);
            }
        }
    }

    protected abstract ValueReader getValue(int columnIndex) throws SQLException;

    public ColumnInfo getColumnInfo(int columnIndex) throws SQLException {
        if (columnIndex <= 0 || columnIndex > columns.length) {
            throw new SQLException(YdbConst.COLUMN_NUMBER_NOT_FOUND + columnIndex);
        }
        return columns[columnIndex - 1];
    }

    public int getColumnsLength() {
        return columns.length;
    }

    private int getColumnIndex(String name) throws SQLException {
        if (!columnNames.containsKey(name)) {
            throw new SQLException(YdbConst.COLUMN_NOT_FOUND + name);
        }
        return columnNames.get(name);
    }

    private ValueReader readValue(int columnIndex) throws SQLException {
        if (columnIndex <= 0 || columnIndex > columns.length) {
            throw new SQLException(YdbConst.COLUMN_NUMBER_NOT_FOUND + columnIndex);
        }

        ValueReader v = getValue(columnIndex - 1);
        ColumnInfo type = columns[columnIndex - 1];
        wasNull = type == null || v == null || type.isNull() || (type.isOptional() && !v.isOptionalItemPresent());
        return v;
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null; // getString supports all types, it's safe to check nullability here
        }
        return columns[columnIndex - 1].getGetters().readString(value);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return false;
        }
        return columns[columnIndex - 1].getGetters().readBoolean(value);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return 0;
        }
        return columns[columnIndex - 1].getGetters().readByte(value);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return 0;
        }
        return columns[columnIndex - 1].getGetters().readShort(value);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return 0;
        }
        return columns[columnIndex - 1].getGetters().readInt(value);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return 0;
        }
        return columns[columnIndex - 1].getGetters().readLong(value);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return 0;
        }
        return columns[columnIndex - 1].getGetters().readFloat(value);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return 0;
        }
        return columns[columnIndex - 1].getGetters().readDouble(value);
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal bigDecimal = getBigDecimal(columnIndex);
        if (bigDecimal != null) {
            return bigDecimal.setScale(scale, RoundingMode.HALF_EVEN); // TODO: not sure what to do here
        } else {
            return null;
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }
        return columns[columnIndex - 1].getGetters().readBytes(value);
    }


    @Override
    public Date getDate(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }

        ColumnInfo type = columns[columnIndex - 1];

        if (type.isNumber()) {
            long number = type.getGetters().readLong(value);
            if (!ChronoField.EPOCH_DAY.range().isValidValue(number)) {
                String msg = String.format(YdbConst.UNABLE_TO_CONVERT, type.getYdbType(), number, Date.class);
                throw new SQLException(msg);
            }
            return Date.valueOf(LocalDate.ofEpochDay(number));
        }

        Instant instant = type.getGetters().readInstant(value);
        if (type.isTimestamp()) {
            return new Date(instant.toEpochMilli());
        }

        return Date.valueOf(instant.atOffset(ZoneOffset.UTC).toLocalDate());
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(getColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }

        ColumnInfo type = columns[columnIndex - 1];
        if (type.isNumber()) {
            long number = type.getGetters().readLong(value);
            if (!ChronoField.EPOCH_DAY.range().isValidValue(number)) {
                String msg = String.format(YdbConst.UNABLE_TO_CONVERT, type.getYdbType(), number, Date.class);
                throw new SQLException(msg);
            }
            return Date.valueOf(LocalDate.ofEpochDay(number));
        }

        Instant instant = type.getGetters().readInstant(value);
        if (type.isTimestamp()) {
            final TimeZone tz = cal != null ? cal.getTimeZone() : Calendar.getInstance().getTimeZone();
            return Date.valueOf(instant.atZone(tz.toZoneId()).toLocalDate());
        }

        return Date.valueOf(instant.atOffset(ZoneOffset.UTC).toLocalDate());
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(getColumnIndex(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }

        ColumnInfo type = columns[columnIndex - 1];
        if (type.isNumber()) {
            long number = type.getGetters().readLong(value);
            if (!ChronoField.SECOND_OF_DAY.range().isValidValue(number)) {
                String msg = String.format(YdbConst.UNABLE_TO_CONVERT, type.getYdbType(), number, Time.class);
                throw new SQLException(msg);
            }
            return Time.valueOf(LocalTime.ofSecondOfDay(number));
        }

        Instant instant = type.getGetters().readInstant(value);
        if (type.isTimestamp()) {
            return new Time(instant.toEpochMilli());
        }

        return Time.valueOf(instant.atOffset(ZoneOffset.UTC).toLocalTime());
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }

        ColumnInfo type = columns[columnIndex - 1];
        if (type.isNumber()) {
            long number = type.getGetters().readLong(value);
            if (!ChronoField.SECOND_OF_DAY.range().isValidValue(number)) {
                String msg = String.format(YdbConst.UNABLE_TO_CONVERT, type.getYdbType(), number, Time.class);
                throw new SQLException(msg);
            }
            return Time.valueOf(LocalTime.ofSecondOfDay(number));
        }

        Instant instant = type.getGetters().readInstant(value);
        if (type.isTimestamp()) {
            final TimeZone tz = cal != null ? cal.getTimeZone() : Calendar.getInstance().getTimeZone();
            return Time.valueOf(instant.atZone(tz.toZoneId()).toLocalTime());
        }

        return Time.valueOf(instant.atOffset(ZoneOffset.UTC).toLocalTime());
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(getColumnIndex(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }

        ColumnInfo type = columns[columnIndex - 1];
        if (type.isNumber()) {
            long number = type.getGetters().readLong(value);
            return new Timestamp(number);
        }

        Instant instant = type.getGetters().readInstant(value);
        if (type.isTimestamp()) {
            return Timestamp.from(instant);
        }

        return Timestamp.valueOf(instant.atOffset(ZoneOffset.UTC).toLocalDateTime());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }

        ColumnInfo type = columns[columnIndex - 1];
        if (type.isNumber()) {
            long number = type.getGetters().readLong(value);
            return new Timestamp(number);
        }

        Instant instant = type.getGetters().readInstant(value);
        if (type.isTimestamp()) {
            final TimeZone tz = cal != null ? cal.getTimeZone() : Calendar.getInstance().getTimeZone();
            return Timestamp.valueOf(instant.atZone(tz.toZoneId()).toLocalDateTime());
        }

        return Timestamp.valueOf(instant.atOffset(ZoneOffset.UTC).toLocalDateTime());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(getColumnIndex(columnLabel), cal);
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        // TODO: implement Unicode stream?
        return getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        byte[] bytes = getBytes(columnIndex);
        return bytes == null ? null : new ByteArrayInputStream(bytes);
    }

    //

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(getColumnIndex(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(getColumnIndex(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(getColumnIndex(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(getColumnIndex(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(getColumnIndex(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(getColumnIndex(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(getColumnIndex(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(getColumnIndex(columnLabel));
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(getColumnIndex(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(getColumnIndex(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(getColumnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(getColumnIndex(columnLabel));
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(getColumnIndex(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(getColumnIndex(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() {
        return null; // TODO: Support warning
    }

    @Override
    public void clearWarnings() {
        // do nothing
    }

    @Override
    public YdbResultSetMetaData getMetaData() {
        if (metaData == null) {
            metaData = new YdbResultSetMetaDataImpl(this);
        }
        return metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }
        return columns[columnIndex - 1].getGetters().readObject(value);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(getColumnIndex(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return getColumnIndex(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }
        return columns[columnIndex - 1].getGetters().readReader(value);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(getColumnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }
        return columns[columnIndex - 1].getGetters().readBigDecimal(value);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(getColumnIndex(columnLabel));
    }

    @Override
    public void setFetchSize(int rows) {
        // do nothing
    }

    @Override
    public int getFetchSize() throws SQLException {
        return statement.getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return statement.getResultSetType();
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public YdbStatement getStatement() {
        return statement;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }

        String url = columns[columnIndex - 1].getGetters().readURL(value);
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new SQLException(YdbConst.UNABLE_TO_CONVERT_AS_URL + url, e);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(getColumnIndex(columnLabel));
    }

    @Override
    public int getHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }

        return columns[columnIndex - 1].getGetters().readNString(value);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(getColumnIndex(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(getColumnIndex(columnLabel));
    }

    @Override
    public Value<?> getNativeColumn(int columnIndex) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }
        while (value != null && value.getType().getKind() == Type.Kind.OPTIONAL) {
            value = value.getOptionalItem();
        }
        return value == null ? null : value.getValue();
    }

    @Override
    public Value<?> getNativeColumn(String columnLabel) throws SQLException {
        return getNativeColumn(getColumnIndex(columnLabel));
    }

    // UNSUPPORTED

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.NAMED_CURSORS_UNSUPPORTED);
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }


    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ROWID_UNSUPPORTED);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ROWID_UNSUPPORTED);
    }


    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }


    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }


    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }


    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CURSOR_UPDATING_UNSUPPORTED);
    }


    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.OBJECT_TYPED_UNSUPPORTED);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.REF_UNSUPPORTED);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.BLOB_UNSUPPORTED);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CLOB_UNSUPPORTED);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ARRAYS_UNSUPPORTED);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.OBJECT_TYPED_UNSUPPORTED);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.REF_UNSUPPORTED);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.BLOB_UNSUPPORTED);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.CLOB_UNSUPPORTED);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ARRAYS_UNSUPPORTED);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.NCLOB_UNSUPPORTED);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.NCLOB_UNSUPPORTED);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.SQLXML_UNSUPPORTED);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.SQLXML_UNSUPPORTED);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ASCII_STREAM_UNSUPPORTED);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(YdbConst.ASCII_STREAM_UNSUPPORTED);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        ValueReader value = readValue(columnIndex);
        if (wasNull) {
            return null;
        }
        return columns[columnIndex - 1].getGetters().readClass(value, type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException(YdbConst.CANNOT_UNWRAP_TO + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

}
