package tech.ydb.jdbc.common;

import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ColumnInfo {
    private final String name;
    private final Type ydbType;
    private final MappingGetters.SqlType sqlType;
    private final MappingGetters.Getters getters;

    private final boolean isOptional;
    private final boolean isTimestamp;
    private final boolean isNumber;
    private final boolean isNull;

    public ColumnInfo(String name, TypeDescription type) {
        this.name = name;

        this.sqlType = type.sqlType();
        this.getters = type.getters();
        this.isOptional = type.isOptional();
        this.ydbType = type.ydbType();

        this.isTimestamp = ydbType == PrimitiveType.Timestamp;
        this.isNumber = ydbType == PrimitiveType.Int8 || ydbType == PrimitiveType.Uint8
                || ydbType == PrimitiveType.Int16 || ydbType == PrimitiveType.Uint16
                || ydbType == PrimitiveType.Int32 || ydbType == PrimitiveType.Uint32
                || ydbType == PrimitiveType.Int64 || ydbType == PrimitiveType.Uint64;
        this.isNull = ydbType.getKind() == Type.Kind.NULL || ydbType.getKind() == Type.Kind.VOID;
    }

    public String getName() {
        return this.name;
    }

    public Type getYdbType() {
        return this.ydbType;
    }

    public boolean isNull() {
        return isNull;
    }

    public boolean isTimestamp() {
        return isTimestamp;
    }

    public boolean isNumber() {
        return isNumber;
    }

    public boolean isOptional() {
        return isOptional;
    }

    public MappingGetters.SqlType getSqlType() {
        return this.sqlType;
    }

    public MappingGetters.Getters getGetters() {
        return this.getters;
    }

    public static ColumnInfo[] fromResultSetReader(YdbTypes types, ResultSetReader rsr) {
        ColumnInfo[] columns = new ColumnInfo[rsr.getColumnCount()];
        for (int idx = 0; idx < rsr.getColumnCount(); idx += 1) {
            TypeDescription type = types.find(rsr.getColumnType(idx));
            columns[idx] = new ColumnInfo(rsr.getColumnName(idx), type);
        }
        return columns;
    }

}
